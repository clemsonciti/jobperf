package slurm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/clemsonciti/jobperf"
)

// Note: these were automatically generated with json-to-go.  They could use some cleanup.

type squeueResponse struct {
	Meta     meta   `json:"meta"`
	Jobs     []jobs `json:"jobs"`
	Warnings []any  `json:"warnings"`
	Errors   []any  `json:"errors"`
}
type plugins struct {
	DataParser        string `json:"data_parser"`
	AccountingStorage string `json:"accounting_storage"`
}
type slurmInfo struct {
	//Version version `json:"version"`
	Release string `json:"release"`
}
type meta struct {
	Plugins plugins   `json:"plugins"`
	Command []string  `json:"command"`
	Slurm   slurmInfo `json:"Slurm"`
}
type Cores struct {
	Num0 string `json:"0"`
	Num1 string `json:"1"`
}
type Num0 struct {
	Cores Cores `json:"cores"`
}
type sockets map[string]struct {
	Cores map[string]string `json:"cores"`
}

type AllocatedNodes struct {
	Sockets         sockets `json:"sockets"`
	Nodename        string  `json:"nodename"`
	CpusUsed        int     `json:"cpus_used"`
	MemoryUsed      int     `json:"memory_used"`
	MemoryAllocated int     `json:"memory_allocated"`
}
type jobResources struct {
	Nodes          string           `json:"nodes"`
	AllocatedCores int              `json:"allocated_cores"`
	AllocatedCpus  int              `json:"allocated_cpus"`
	AllocatedHosts int              `json:"allocated_hosts"`
	AllocatedNodes []AllocatedNodes `json:"allocated_nodes"`
}

type optionalValue struct {
	set      bool
	infinite bool
	number   int64
}

func (v *optionalValue) UnmarshalJSON(b []byte) error {
	var intVal int64
	if json.Unmarshal(b, &intVal) == nil {
		v.set = true
		v.infinite = false
		v.number = intVal
		return nil
	}
	var objVal struct {
		Set      bool  `json:"set"`
		Infinite bool  `json:"infinite"`
		Number   int64 `json:"number"`
	}
	if err := json.Unmarshal(b, &objVal); err != nil {
		return err
	}
	v.set = objVal.Set
	v.infinite = objVal.Infinite
	v.number = objVal.Number
	return nil
}

type jobs struct {
	GresDetail   []string        `json:"gres_detail"`   // keep
	JobID        int             `json:"job_id"`        // keep
	JobResources jobResources    `json:"job_resources"` // keep
	JobStateRaw  json.RawMessage `json:"job_state"`     // keep
	Name         string          `json:"name"`          // keep
	StartTime    optionalValue   `json:"start_time"`    // keep
	TimeLimit    optionalValue   `json:"time_limit"`    // keep
	UserName     string          `json:"user_name"`     // keep
}

func (j *jobs) JobState() string {
	var stringState string
	err := json.Unmarshal(j.JobStateRaw, &stringState)
	if err == nil {
		return stringState
	}
	var sliceState []string
	err = json.Unmarshal(j.JobStateRaw, &sliceState)
	if err == nil {
		return sliceState[0]
	}

	return "Unknown"
}

var gresDetailsFmt1 = regexp.MustCompile(`([^:]+):(?:([^:]+):)?([0-9]+)\(IDX`)
var gresDetailsFmt2 = regexp.MustCompile(`([^:]+)(?::([^:]+))?\(CNT:([0-9]+)`)

func nGPUFromGRESDetails(details string) (int, error) {
	// Some of this is discovered by examining the gres_ctld_job_build_details function in slurm:
	// https://github.com/SchedMD/slurm/blob/7da8ddbf14d11afe67a17a10df0ea025d1e6cfbf/src/slurmctld/gres_ctld.c#L1836

	gpuCnt := 0
	resources := strings.Split(details, ",")
	for _, r := range resources {
		match := gresDetailsFmt1.FindStringSubmatch(r)
		if match != nil {
			if match[1] != "gpu" {
				continue
			}
			n, err := strconv.Atoi(match[3])
			if err != nil {
				return 0, fmt.Errorf("in gres resource %v, could not parse gpu count: %w", r, err)
			}
			gpuCnt += n
			continue
		}
		match = gresDetailsFmt2.FindStringSubmatch(r)
		if match == nil {
			return 0, fmt.Errorf("in gres resource %v, unknown format", r)
		}
		if match[1] != "gpu" {
			continue
		}
		n, err := strconv.Atoi(match[3])
		if err != nil {
			return 0, fmt.Errorf("in gres resource %v, could not parse gpu count: %w", r, err)
		}
		gpuCnt += n
	}
	return gpuCnt, nil
}

func squeueGetJobByID(jobID string) (*jobperf.Job, error) {
	slog.Debug("fetching job by id", "jobID", jobID, "method", "squeue")
	cmd := exec.Command("squeue", "--job", jobID, "--json")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run squeue for job id %v: %w", jobID, err)
	}

	var parsed squeueResponse
	err = json.Unmarshal(out.Bytes(), &parsed)
	if err != nil {
		return nil, fmt.Errorf("failed to parse squeue response for job id %v: %w", jobID, err)
	}

	if len(parsed.Jobs) != 1 {
		return nil, fmt.Errorf("unexpected number of jobs returned from squeue: %v", len(parsed.Jobs))
	}
	parsedJob := parsed.Jobs[0]
	if len(parsedJob.GresDetail) > 0 && len(parsedJob.GresDetail) != len(parsedJob.JobResources.AllocatedNodes) {
		return nil, fmt.Errorf("expected gres_detail to be empty or equal to number of nodes: %v != %v", len(parsedJob.GresDetail), len(parsedJob.JobResources.AllocatedNodes))
	}

	var nodes []jobperf.Node

	totalCores := 0
	totalGPUs := 0
	var totalMemoryBytes jobperf.Bytes = 0
	for i, n := range parsedJob.JobResources.AllocatedNodes {
		nCores := 0
		for _, s := range n.Sockets {
			nCores += len(s.Cores)
		}
		nGPUs := 0
		if len(parsedJob.GresDetail) > 0 {
			nGPUs, err = nGPUFromGRESDetails(parsedJob.GresDetail[i])
			if err != nil {
				return nil, fmt.Errorf("failed to get ngpus from gres_details %v: %w", parsedJob.GresDetail[i], err)
			}
		}
		node := jobperf.Node{
			Hostname: n.Nodename,
			Memory:   jobperf.Bytes(1024 * 1024 * n.MemoryAllocated),
			NCores:   nCores,
			NGPUs:    nGPUs,
		}
		nodes = append(nodes, node)

		totalCores += nCores
		totalGPUs += nGPUs
		totalMemoryBytes += node.Memory
	}

	jobOut := jobperf.Job{
		ID:          strconv.Itoa(parsedJob.JobID),
		Name:        parsedJob.Name,
		Owner:       parsedJob.UserName,
		CoresTotal:  totalCores,
		MemoryTotal: totalMemoryBytes,
		GPUsTotal:   totalGPUs,
		Walltime:    time.Minute * time.Duration(parsedJob.TimeLimit.number),
		State:       parsedJob.JobState(),
		Nodes:       nodes,
	}

	if jobOut.IsRunning() || jobOut.IsComplete() {
		jobOut.UsedWalltime = time.Since(time.Unix(parsedJob.StartTime.number, 0)).Round(time.Second)
		jobOut.StartTime = time.Unix(parsedJob.StartTime.number, 0)
	}

	return &jobOut, nil

}
