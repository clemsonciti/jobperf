package slurm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/clemsonciti/jobperf"
	"gopkg.in/yaml.v3"
)

type squeueResponse struct {
	Meta     meta   `json:"meta" yaml:"meta"`
	Jobs     []jobs `json:"jobs" yaml:"jobs"`
	Warnings []any  `json:"warnings" yaml:"warnings"`
	Errors   []any  `json:"errors" yaml:"errors"`
}
type plugins struct {
	DataParser        string `json:"data_parser" yaml:"data_parser"`
	AccountingStorage string `json:"accounting_storage" yaml:"accounting_storage"`
}
type slurmInfo struct {
	//Version version `json:"version" yaml:"version"`
	Release string `json:"release" yaml:"release"`
}
type meta struct {
	Plugins plugins   `json:"plugins" yaml:"plugins"`
	Command []string  `json:"command" yaml:"command"`
	Slurm   slurmInfo `json:"Slurm" yaml:"Slurm"`
}

type slurm23Sockets map[string]struct {
	Cores map[string]string `json:"cores" yaml:"cores"`
}

type slurm23AllocatedNodes struct {
	Sockets         slurm23Sockets `json:"sockets" yaml:"sockets"`
	Nodename        string         `json:"nodename" yaml:"nodename"`
	CpusUsed        int            `json:"cpus_used" yaml:"cpus_used"`
	MemoryUsed      int            `json:"memory_used" yaml:"memory_used"`
	MemoryAllocated int            `json:"memory_allocated" yaml:"memory_allocated"`
}
type slurm23JobResources struct {
	Nodes          string                  `json:"nodes" yaml:"nodes"`
	AllocatedCores int                     `json:"allocated_cores" yaml:"allocated_cores"`
	AllocatedCpus  int                     `json:"allocated_cpus" yaml:"allocated_cpus"`
	AllocatedHosts int                     `json:"allocated_hosts" yaml:"allocated_hosts"`
	AllocatedNodes []slurm23AllocatedNodes `json:"allocated_nodes" yaml:"allocated_nodes"`
}

type slurm24JobResources struct {
	Nodes struct {
		Count      int `json:"count" yaml:"count"`
		Allocation []struct {
			Name   string `json:"name" yaml:"name"`
			Memory struct {
				Allocated int `json:"allocated" yaml:"allocated"`
			} `json:"memory" yaml:"memory"`
			Sockets []struct {
				Index int `json:"index" yaml:"index"`
				Cores []struct {
					Index  int      `json:"index" yaml:"index"`
					Status []string `json:"status" yaml:"status"`
				} `json:"cores" yaml:"cores"`
			} `json:"sockets" yaml:"sockets"`
		} `json:"allocation" yaml:"allocation"`
	} `json:"nodes" yaml:"nodes"`
}

type jobResourcesNode struct {
	Hostname string
	NCores   int
	Memory   jobperf.Bytes
}

type jobResources struct {
	Nodes []jobResourcesNode
}

func (jr *jobResources) fromSlurm23JobResources(s23jr *slurm23JobResources) error {
	jr.Nodes = make([]jobResourcesNode, 0, len(s23jr.AllocatedNodes))
	for _, n := range s23jr.AllocatedNodes {
		nCores := 0
		for _, s := range n.Sockets {
			nCores += len(s.Cores)
		}
		node := jobResourcesNode{
			Hostname: n.Nodename,
			Memory:   jobperf.Bytes(1024 * 1024 * n.MemoryAllocated),
			NCores:   nCores,
		}
		jr.Nodes = append(jr.Nodes, node)
	}
	return nil
}
func (jr *jobResources) fromSlurm24JobResources(s24jr *slurm24JobResources) error {
	jr.Nodes = make([]jobResourcesNode, 0, len(s24jr.Nodes.Allocation))
	for _, n := range s24jr.Nodes.Allocation {
		nCores := 0
		for _, s := range n.Sockets {
			for _, c := range s.Cores {
				if slices.Contains(c.Status, "ALLOCATED") {
					nCores++
				}
			}
		}
		node := jobResourcesNode{
			Hostname: n.Name,
			Memory:   jobperf.Bytes(1024 * 1024 * n.Memory.Allocated),
			NCores:   nCores,
		}
		jr.Nodes = append(jr.Nodes, node)
	}
	return nil
}

func (jr *jobResources) UnmarshalJSON(b []byte) error {
	var s23jr slurm23JobResources
	s23err := json.Unmarshal(b, &s23jr)
	if s23err == nil {
		return jr.fromSlurm23JobResources(&s23jr)
	}
	var s24jr slurm24JobResources
	s24err := json.Unmarshal(b, &s24jr)
	if s24err == nil {
		return jr.fromSlurm24JobResources(&s24jr)
	}
	return fmt.Errorf("failed to parse job resource using both slurm23 format: %v and slurm24: %w", s23err, s24err)
}

func (jr *jobResources) UnmarshalYAML(n *yaml.Node) error {
	var s23jr slurm23JobResources
	s23err := n.Decode(&s23jr)
	if s23err == nil {
		return jr.fromSlurm23JobResources(&s23jr)
	}
	var s24jr slurm24JobResources
	s24err := n.Decode(&s24jr)
	if s24err == nil {
		return jr.fromSlurm24JobResources(&s24jr)
	}
	return fmt.Errorf("failed to parse job resource using both slurm23 format: %v and slurm24: %w", s23err, s24err)
}

type optionalValue struct {
	set      bool
	infinite bool
	number   int64
}

func (v *optionalValue) UnmarshalYAML(n *yaml.Node) error {
	var intVal int64
	if n.Decode(&intVal) == nil {
		v.set = true
		v.infinite = false
		v.number = intVal
		return nil
	}
	var objVal struct {
		Set      bool  `yaml:"set"`
		Infinite bool  `yaml:"infinite"`
		Number   int64 `yaml:"number"`
	}
	if err := n.Decode(&objVal); err != nil {
		return err
	}
	v.set = objVal.Set
	v.infinite = objVal.Infinite
	v.number = objVal.Number
	return nil
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
	GresDetail   []string      `json:"gres_detail" yaml:"gres_detail"`
	JobID        int           `json:"job_id" yaml:"job_id"`
	JobResources jobResources  `json:"job_resources" yaml:"job_resources"`
	JobState     jobStatus     `json:"job_state" yaml:"job_state"`
	Name         string        `json:"name" yaml:"name"`
	StartTime    optionalValue `json:"start_time" yaml:"start_time"`
	TimeLimit    optionalValue `json:"time_limit" yaml:"time_limit"`
	UserName     string        `json:"user_name" yaml:"user_name"`
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

func (e jobEngine) squeueGetJobByID(jobID string) (*jobperf.Job, error) {
	slog.Debug("fetching job by id", "jobID", jobID, "method", "squeue")
	var cmd *exec.Cmd
	if e.mode == slurmModeJSON {
		cmd = exec.Command("squeue", "--job", jobID, "--json")
	} else {
		cmd = exec.Command("squeue", "--job", jobID, "--yaml")
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run squeue for job id %v: %w", jobID, err)
	}

	var parsed squeueResponse
	if e.mode == slurmModeJSON {
		err = json.Unmarshal(out.Bytes(), &parsed)
	} else {
		err = yaml.Unmarshal(out.Bytes(), &parsed)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse squeue response for job id %v: %w", jobID, err)
	}

	if len(parsed.Jobs) != 1 {
		return nil, fmt.Errorf("unexpected number of jobs returned from squeue: %v", len(parsed.Jobs))
	}
	parsedJob := parsed.Jobs[0]
	if len(parsedJob.GresDetail) > 0 && len(parsedJob.GresDetail) != len(parsedJob.JobResources.Nodes) {
		return nil, fmt.Errorf("expected gres_detail to be empty or equal to number of nodes: %v != %v", len(parsedJob.GresDetail), len(parsedJob.JobResources.Nodes))
	}

	var nodes []jobperf.Node

	totalCores := 0
	totalGPUs := 0
	var totalMemoryBytes jobperf.Bytes = 0
	for i, n := range parsedJob.JobResources.Nodes {
		nGPUs := 0
		if len(parsedJob.GresDetail) > 0 {
			nGPUs, err = nGPUFromGRESDetails(parsedJob.GresDetail[i])
			if err != nil {
				return nil, fmt.Errorf("failed to get ngpus from gres_details %v: %w", parsedJob.GresDetail[i], err)
			}
		}
		node := jobperf.Node{
			Hostname: n.Hostname,
			Memory:   n.Memory,
			NCores:   n.NCores,
			NGPUs:    nGPUs,
		}
		nodes = append(nodes, node)

		totalCores += n.NCores
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
		State:       string(parsedJob.JobState),
		Nodes:       nodes,
		Raw:         parsedJob,
	}

	if jobOut.IsRunning() || jobOut.IsComplete() {
		jobOut.UsedWalltime = time.Since(time.Unix(parsedJob.StartTime.number, 0)).Round(time.Second)
		jobOut.StartTime = time.Unix(parsedJob.StartTime.number, 0)
	}

	return &jobOut, nil

}
