package pbs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/clemsonciti/jobperf"
)

type qstatResponse struct {
	PBSVersion string              `json:"pbs_version"`
	PBSServer  string              `json:"pbs_server"`
	Jobs       map[string]qstatJob `json:"Jobs"`
}

type qstatResourceList struct {
	Mem      string `json:"mem"`
	NCPUs    int    `json:"ncpus"`
	NGPUs    int    `json:"ngpus"`
	NodeCt   int    `json:"nodect"`
	Walltime string `json:"walltime"`
	Select   string `json:"select"`
	Qcat     string `json:"qcat"`
}

type qstatResourcesUsed struct {
	Mem      string `json:"mem"`
	CPUT     string `json:"cput"`
	Walltime string `json:"walltime"`
}

type qstatJob struct {
	Name          interface{}        `json:"Job_Name"`
	Owner         string             `json:"Job_Owner"`
	Queue         string             `json:"queue"`
	State         string             `json:"job_state"`
	ResourceList  qstatResourceList  `json:"Resource_List"`
	ResourcesUsed qstatResourcesUsed `json:"resources_used"`
	ExecVNode     string             `json:"exec_vnode"`
	STime         string             `json:"stime"`
}

func parseWalltime(w string) (time.Duration, error) {
	parts := strings.Split(w, ":")
	if len(parts) != 3 {
		return time.Duration(0), fmt.Errorf("failed to parse walltime %v into 3 parts", w)
	}

	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Duration(0), fmt.Errorf("failed to parse walltime %v into failed to parse hours: %w", w, err)
	}
	minutes, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Duration(0), fmt.Errorf("failed to parse walltime %v into failed to parse minutes: %w", w, err)
	}
	seconds, err := strconv.Atoi(parts[2])
	if err != nil {
		return time.Duration(0), fmt.Errorf("failed to parse walltime %v into failed to parse seconds: %w", w, err)
	}
	return time.Second * time.Duration(seconds+60*minutes+60*60*hours), nil
}

func parseExecVNode(execVNode string) ([]jobperf.Node, error) {
	nodeStrs := strings.Split(execVNode, "+")
	nodeSet := map[string]*jobperf.Node{}

	for _, nStr := range nodeStrs {
		nStr = strings.Trim(nStr, "()")
		parts := strings.Split(nStr, ":")
		n := nodeSet[parts[0]]
		if n == nil {
			n = new(jobperf.Node)
			n.Hostname = parts[0]
			nodeSet[parts[0]] = n
		}
		for _, p := range parts[1:] {
			keyValArr := strings.Split(p, "=")
			if len(keyValArr) != 2 {
				return nil, fmt.Errorf("failed to parse part %v in node %v in exec_vnode of %v", p, nStr, execVNode)
			}
			key := keyValArr[0]
			val := keyValArr[1]
			if key == "ncpus" {
				ncpus, err := strconv.Atoi(val)
				if err != nil {
					return nil, fmt.Errorf("failed to parse ncpus %v in node %v in exec_vnode of %v: %w", p, nStr, execVNode, err)
				}
				n.NCores += ncpus
			}
			if key == "ngpus" {
				ngpus, err := strconv.Atoi(val)
				if err != nil {
					return nil, fmt.Errorf("failed to parse ngpus %v in node %v in exec_vnode of %v: %w", p, nStr, execVNode, err)
				}
				n.NGPUs += ngpus
			}
			if key == "mem" {
				mem, err := jobperf.ParseBytes(val)
				if err != nil {
					return nil, fmt.Errorf("failed to parse mem %v in node %v in exec_vnode of %v: %w", p, nStr, execVNode, err)
				}
				n.Memory += mem
			}
		}
	}
	var nodes []jobperf.Node
	for _, n := range nodeSet {
		nodes = append(nodes, *n)
	}
	return nodes, nil
}

func qstatGetJob(jobID string) (*jobperf.Job, error) {
	slog.Debug("fetching job by id", "jobID", jobID)
	cmd := exec.Command(os.Getenv("PBS_EXEC") + "/bin/qstat", "-xf", "-Fjson", jobID)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run qstat for job id %v: %w", jobID, err)
	}
	slog.Debug("succesfully got qstat response", "res", out.String())
	var parsed qstatResponse
	err = json.Unmarshal(out.Bytes(), &parsed)
	if err != nil {
		return nil, fmt.Errorf("failed to parse qstat response for job id %v: %w", jobID, err)
	}

	if len(parsed.Jobs) != 1 {
		return nil, fmt.Errorf("unexpected number of jobs returned from qstat: %v", len(parsed.Jobs))
	}
	var jobIDFull string
	for k := range parsed.Jobs {
		jobIDFull = k
	}
	parsedJob := parsed.Jobs[jobIDFull]
	ownerParts := strings.Split(parsedJob.Owner, "@")
	if len(ownerParts) != 2 {
		return nil, fmt.Errorf("unexpected number of owner parts for owner %v: %v", parsedJob.Owner, len(ownerParts))
	}

	memTotalParsed, err := jobperf.ParseBytes(parsedJob.ResourceList.Mem)
	if err != nil {
		return nil, fmt.Errorf("failed to parse total memory: %w", err)
	}
	walltimeParsed, err := parseWalltime(parsedJob.ResourceList.Walltime)
	if err != nil {
		return nil, fmt.Errorf("failed to parse walltime: %w", err)
	}

	var jobName string
	switch v := parsedJob.Name.(type) {
	case string:
		jobName = v
	case float64:
		jobName = fmt.Sprintf("%v", v)
	case int:
		jobName = strconv.Itoa(v)
	default:
		slog.Error("Job name is an unexpected type", "jobName", v, "tpye", fmt.Sprintf("%T", v))
		os.Exit(1)
	}

	j := jobperf.Job{
		ID:    jobIDFull,
		Name:  jobName,
		Owner: ownerParts[0],
		State: parsedJob.State,
		//ChunkCount:  parsedJob.ResourceList.NodeCt,
		CoresTotal:  parsedJob.ResourceList.NCPUs,
		MemoryTotal: memTotalParsed,
		GPUsTotal:   parsedJob.ResourceList.NGPUs,
		Walltime:    walltimeParsed,
	}

	if parsedJob.STime != "" {
		j.StartTime, err = time.ParseInLocation("Mon Jan 2 15:04:05 2006", parsedJob.STime, time.Local)
		if err != nil {
			return nil, fmt.Errorf("failed to parse start time: %w", err)

		}
	}

	if parsedJob.ResourcesUsed.Walltime != "" {
		j.UsedWalltime, err = parseWalltime(parsedJob.ResourcesUsed.Walltime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse used walltime: %w", err)
		}
		j.UsedCPUTime, err = parseWalltime(parsedJob.ResourcesUsed.CPUT)
		if err != nil {
			return nil, fmt.Errorf("failed to parse used cput: %w", err)
		}
		j.UsedMemory, err = jobperf.ParseBytes(parsedJob.ResourcesUsed.Mem)
		if err != nil {
			return nil, fmt.Errorf("failed to parse used mem: %w", err)
		}
	}
	if parsedJob.ExecVNode != "" {
		j.Nodes, err = parseExecVNode(parsedJob.ExecVNode)
		if err != nil {
			return nil, fmt.Errorf("failed to parse exec_vnode: %w", err)
		}
	}
	slog.Debug("succesfully parsed qstat response", "job", j)

	return &j, nil
}

type QSelectOptions struct {
	State string
	// TODO: support other options
}

func QSelectJobs(opt QSelectOptions) ([]string, error) {
	cmd := exec.Command(os.Getenv("PBS_EXEC") + "/bin/qselect", "-s", opt.State)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run qselect with opt %v: %w", opt, err)
	}

	return strings.Split(out.String(), "\n"), nil
}
