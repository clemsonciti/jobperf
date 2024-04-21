package slurm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/clemsonciti/jobperf"
)

func IsAvailable() bool {
	_, err := exec.LookPath("squeue")
	if err == nil {
		slog.Debug("found squeue")
		return true
	}
	slog.Debug("failed to find squeue", "err", err)
	return false
}

type slurmMode int

const (
	slurmModeJSON slurmMode = iota
	slurmModeYAML
)

type jobEngine struct {
	mode          slurmMode
	nodeStatsMode slurmNodeStatsMode
}

func NewJobEngine() jobperf.JobEngine {
	engine := jobEngine{
		mode:          slurmModeJSON,
		nodeStatsMode: slurmNodeStatsModeCGroup,
	}
	cmd := exec.Command("sacct", "--json")
	err := cmd.Run()
	if err != nil {
		cmd = exec.Command("sacct", "--yaml")
		err = cmd.Run()
		if err == nil {
			slog.Debug("using yaml slurm mode")
			engine.mode = slurmModeYAML
		} else {
			slog.Debug("both json and yaml fail")
		}
	} else {
		slog.Debug("using json slurm mode")
	}

	cmd = exec.Command("scontrol", "show", "config")
	var cmdOut bytes.Buffer
	cmd.Stdout = &cmdOut
	err = cmd.Run()
	if err != nil {
		slog.Error("failed to run scontrol show config", "err", err)
	} else {
		scanner := bufio.NewScanner(&cmdOut)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "JobAcctGatherType") {
				continue
			}
			gatherType := line[strings.Index(line, "=")+2:]
			slog.Debug("found JobAcctGatherType", "JobAcctGatherType", gatherType)
			switch gatherType {
			case "jobacct_gather/cgroup":
				engine.nodeStatsMode = slurmNodeStatsModeCGroup
			case "jobacct_gather/linux":
				engine.nodeStatsMode = slurmNodeStatsModeLinux
			default:
				slog.Warn("unknown JobAcctGatherType", "JobAcctGatherType", gatherType)
			}
		}

	}

	return engine
}

func (e jobEngine) Warning() string {
	return "The job usage comes from slurm's accounting and has some limitations in accuracy."
}
func (e jobEngine) NodeStatsWarning() string {
	if e.nodeStatsMode == slurmNodeStatsModeLinux {
		return `CGroup gather plugin not in use on this cluster. The fallback approach only 
gathers perfomance metrics while jobperf is running.
* Max Memory Used above may be inaccurate
* The CPU Cores above may be inaccurate`
	}

	return ""
}

func (e jobEngine) GetJobByID(jobID string) (*jobperf.Job, error) {
	squeueJob, squeueErr := e.squeueGetJobByID(jobID)
	sacctJob, sacctErr := e.sacctGetJobByID(jobID)
	slog.Debug("fetching complete", "squeueErr", squeueErr, "sacctErr", sacctErr)

	if squeueErr != nil && sacctErr != nil {
		return nil, fmt.Errorf("failed to fetch jobID from squeue and sacct: %w , %w", squeueErr, sacctErr)
	}
	if squeueErr != nil {
		return sacctJob, nil
	}
	if sacctErr != nil {
		return squeueJob, nil
	}
	// Node information is more accurate from squeue:
	sacctJob.Nodes = squeueJob.Nodes
	return sacctJob, nil
}

func (_ jobEngine) SelectJobIDs(q jobperf.JobQuery) ([]string, error) {
	return nil, nil
}

func (e jobEngine) NodeStatsSession(j *jobperf.Job, hostname string) (jobperf.NodeStatsSession, error) {
	var session nodeStatsSession
	session.mode = e.nodeStatsMode
	var err error

	me, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get current user: %w", err)
	}
	ex, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed find jobperf: %w", err)
	}
	cmdName := "srun"
	params := []string{"--jobid", j.ID, "--overlap",
		"--mem", "100mb",
		"--exact",
		"--nodes", "1",
		"--ntasks", "1",
		"-w", hostname}
	if rawSacctJob, ok := j.Raw.(*sacctJob); ok {
		if rawSacctJob.Partition != "" {
			params = append(params, "-p", rawSacctJob.Partition)
		}
	}
	// The "-time.Second*5" is a fudge factor and may not be needed. I'm not
	// sure the behaviour if you request a step with timelimit longer than we
	// have left.
	timeLeft := j.Walltime - time.Since(j.StartTime) - time.Second*5
	if timeLeft > 0 {
		params = append(params, "-t", strconv.Itoa(int(timeLeft.Minutes())))
	}

	params = append(params,
		ex, "-nodestats")
	if me.Username == "root" {
		cmdName = "sudo"
		params = append([]string{"-u", j.Owner, "srun"}, params...)
	}
	slog.Debug("running nodestats", "cmd", cmdName, "params", params)
	session.cmd = exec.Command(cmdName, params...)

	session.jobID = j.ID
	session.hostname = hostname

	u, err := user.Lookup(j.Owner)
	if err != nil {
		return nil, fmt.Errorf("failed to get uid for user %v : %w", j.Owner, err)
	}
	session.userID = u.Uid

	session.reqWriter, err = session.cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to make stdin pipe to node %v with srun: %w", hostname, err)
	}
	session.resReader, err = session.cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to make stdout pipe to node %v with srun: %w", hostname, err)
	}
	session.cmd.Stderr = os.Stderr
	err = session.cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start nodestats command on node %v with srun: %w", hostname, err)
	}

	return &session, nil
}

type slurmNodeStatsMode int

const (
	slurmNodeStatsModeCGroup slurmNodeStatsMode = iota
	slurmNodeStatsModeLinux
)

type nodeStatsSession struct {
	cmd       *exec.Cmd
	hostname  string
	reqWriter io.Writer
	resReader io.Reader
	jobID     string
	userID    string
	mode      slurmNodeStatsMode
}

func (s *nodeStatsSession) RequestCPUStats() (*jobperf.NodeStatsCPUMem, error) {
	payload, err := json.Marshal(jobperf.NodeStatsCPUMemSlurmPayload{
		JobID:  s.jobID,
		UserID: s.userID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write payload for slurm node stats request: %w", err)
	}
	slog.Debug("sending slurm cpu stats request", "payload", string(payload))
	requestType := jobperf.NodeStatsRequestTypeSampleCPUMemSlurmCGroup
	if s.mode == slurmNodeStatsModeLinux {
		requestType = jobperf.NodeStatsRequestTypeSampleCPUMemSlurmLinux
	}
	err = json.NewEncoder(s.reqWriter).Encode(jobperf.NodeStatsRequest{
		RequestType: requestType,
		Payload:     payload,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode request for slurm node stats request: %w", err)
	}
	_, err = s.reqWriter.Write([]byte("\n"))
	if err != nil {
		return nil, fmt.Errorf("failed to send newline: %w", err)
	}
	slog.Debug("waiting for response...")
	var res jobperf.NodeStatsCPUMem
	err = json.NewDecoder(s.resReader).Decode(&res)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cpu/mem response from node stats: %w", err)
	}
	res.Hostname = s.hostname
	slog.Debug("decoded response", "res", res)
	return &res, nil
}

func (s *nodeStatsSession) RequestGPUStats() (*jobperf.NodeStatsGPU, error) {
	err := json.NewEncoder(s.reqWriter).Encode(jobperf.NodeStatsRequest{
		RequestType: jobperf.NodeStatsRequestTypeSampleGPUNvidia,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode gpu stats request for slurm node stats request: %w", err)
	}
	var res jobperf.NodeStatsGPU
	err = json.NewDecoder(s.resReader).Decode(&res)
	if err != nil {
		return nil, fmt.Errorf("failed to decode gpu response from node stats: %w", err)
	}
	for i := range res {
		res[i].Hostname = s.hostname
	}
	return &res, nil
}

func (s *nodeStatsSession) Close() error {
	// Ask nicely...
	err := json.NewEncoder(s.reqWriter).Encode(jobperf.NodeStatsRequest{
		RequestType: jobperf.NodeStatsRequestTypeExit,
	})
	if err != nil {
		return fmt.Errorf("failed to encode exit request for slurm node stats request: %w", err)
	}

	go func() {
		// Wait up to 2 seconds, then ask not so nicely.
		time.Sleep(2 * time.Second)
		err := s.cmd.Process.Kill()
		if err != nil {
			slog.Error("error killing nodestats command", "err", err, "host", s.hostname)
		}
	}()
	return s.cmd.Wait()
}
