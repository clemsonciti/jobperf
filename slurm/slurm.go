package slurm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
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

type jobEngine struct{}

func NewJobEngine() jobperf.JobEngine {
	return jobEngine{}
}

func (_ jobEngine) GetJobByID(jobID string) (*jobperf.Job, error) {
	squeueJob, squeueErr := squeueGetJobByID(jobID)
	sacctJob, sacctErr := sacctGetJobByID(jobID)
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

func (_ jobEngine) NodeStatsSession(j *jobperf.Job, hostname string) (jobperf.NodeStatsSession, error) {
	var session nodeStatsSession
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
		"-w", hostname,
		ex, "-nodestats"}
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
	stderr, err := session.cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to make stderr pipe to node %v with srun: %w", hostname, err)
	}
	err = session.cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start nodestats command on node %v with srun: %w", hostname, err)
	}

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			slog.Debug("nodestats stderr", "node", hostname, "stderr", scanner.Text())
		}
	}()

	return &session, nil
}

type nodeStatsSession struct {
	cmd       *exec.Cmd
	hostname  string
	reqWriter io.Writer
	resReader io.Reader
	jobID     string
	userID    string
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
	err = json.NewEncoder(s.reqWriter).Encode(jobperf.NodeStatsRequest{
		RequestType: jobperf.NodeStatsRequestTypeSampleCPUMemSlurm,
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
