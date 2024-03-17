package slurm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"time"

	"github.com/clemsonciti/jobperf"
	"gopkg.in/yaml.v3"
)

type sacctResponse struct {
	Meta     meta       `json:"meta" yaml:"meta"`
	Jobs     []sacctJob `json:"jobs" yaml:"jobs"`
	Warnings []any      `json:"warnings" yaml:"warnings"`
	Errors   []any      `json:"errors" yaml:"errors"`
}

type sacctTresUnit struct {
	Type  string `json:"type" yaml:"type"`
	Name  string `json:"name" yaml:"name"`
	ID    int    `json:"id" yaml:"id"`
	Count int    `json:"count" yaml:"count"`
}

type sacctTresTaskUnit struct {
	sacctTresUnit
	Node string `json:"node" yaml:"node"`
	Task int    `json:"task" yaml:"task"`
}

type sacctJobTres struct {
	Allocated []sacctTresUnit `json:"allocated" yaml:"allocated"`
	Requested []sacctTresUnit `json:"requested" yaml:"requested"`
}

type sacctJobState struct {
	Current jobStatus `json:"current" yaml:"current"`
	Reason  string    `json:"reason" yaml:"reason"`
}

type sacctJobTime struct {
	// Elapsed seems to be in seconds.
	Elapsed    int           `json:"elapsed" yaml:"elapsed"`
	End        optionalValue `json:"end" yaml:"end"`
	Start      optionalValue `json:"start" yaml:"start"`
	Eligible   int           `json:"eligible" yaml:"eligible"`
	Submission int           `json:"submission" yaml:"submission"`
	Suspended  int           `json:"suspended" yaml:"suspended"`
	// Limit seems to be in minutes.
	Limit optionalValue `json:"limit" yaml:"limit"`
	Total sacctCPUTime  `json:"total" yaml:"total"`
	User  sacctCPUTime  `json:"user" yaml:"user"`
}

type sacctCPUTime struct {
	Seconds      int `json:"seconds" yaml:"seconds"`
	Microseconds int `json:"microseconds" yaml:"microseconds"`
}

func (t *sacctCPUTime) toDuration() time.Duration {
	return (time.Duration(t.Seconds)*time.Second +
		time.Duration(t.Microseconds)*time.Microsecond)
}

type sacctJobStepTime struct {
	Elapsed int           `json:"elapsed" yaml:"elapsed"`
	End     optionalValue `json:"end" yaml:"end"`
	Start   optionalValue `json:"start" yaml:"start"`
	System  sacctCPUTime  `json:"system" yaml:"system"`
	User    sacctCPUTime  `json:"user" yaml:"user"`
}

type sacctJobStepExitCode struct {
	Status     jobStatus     `json:"status" yaml:"status"`
	ReturnCode optionalValue `json:"return_code" yaml:"return_code"`
}

type jobStatus string

func (s *jobStatus) UnmarshalYAML(n *yaml.Node) error {
	var stringState string
	if n.Decode(&stringState) == nil {
		*s = jobStatus(stringState)
		return nil
	}
	var sliceState []string
	if err := n.Decode(&sliceState); err != nil {
		return err
	}
	*s = jobStatus(sliceState[0])
	return nil
}

func (s *jobStatus) UnmarshalJSON(b []byte) error {
	var stringState string
	if json.Unmarshal(b, &stringState) == nil {
		*s = jobStatus(stringState)
		return nil
	}
	var sliceState []string
	if err := json.Unmarshal(b, &sliceState); err != nil {
		return err
	}
	*s = jobStatus(sliceState[0])
	return nil
}

type sacctJobStepNodes struct {
	Count int      `json:"count" yaml:"count"`
	Range string   `json:"range" yaml:"range"`
	List  []string `json:"list" yaml:"list"`
}
type sacctJobStepTasks struct {
	Count int `json:"count" yaml:"count"`
}
type sacctJobStepInfo struct {
	/*
		ID struct {
			JobID  int    `json:"job_id"`
			StepID string `json:"step_id"`
		} `json:"id"`
	*/
	Name string `json:"name" yaml:"name"`
}

type sacctJobStepTresStats struct {
	Total   []sacctTresUnit     `json:"total" yaml:"total"`
	Average []sacctTresUnit     `json:"average" yaml:"average"`
	Min     []sacctTresTaskUnit `json:"min" yaml:"min"`
	Max     []sacctTresTaskUnit `json:"max" yaml:"max"`
}

type sacctJobStepTres struct {
	Requested sacctJobStepTresStats `json:"requested" yaml:"requested"`
	Consumed  sacctJobStepTresStats `json:"consumed" yaml:"consumed"`
	Allocated []sacctTresUnit       `json:"allocated" yaml:"allocated"`
}

type sacctJobStep struct {
	//State    string               `json:"state"`
	Time     sacctJobStepTime     `json:"time" yaml:"time"`
	ExitCode sacctJobStepExitCode `json:"exit_code" yaml:"exit_code"`
	Nodes    sacctJobStepNodes    `json:"nodes" yaml:"nodes"`
	Tasks    sacctJobStepTasks    `json:"tasks" yaml:"tasks"`
	Step     sacctJobStepInfo     `json:"step" yaml:"step"`
	Tres     sacctJobStepTres     `json:"tres" yaml:"tres"`
}

type sacctJob struct {
	Account         string         `json:"account" yaml:"account"`
	AllocationNodes int            `json:"allocation_nodes" yaml:"allocation_nodes"`
	Cluster         string         `json:"cluster" yaml:"cluster"`
	JobID           int            `json:"job_id" yaml:"job_id"`
	Name            string         `json:"name" yaml:"name"`
	Partition       string         `json:"partition" yaml:"partition"`
	User            string         `json:"user" yaml:"user"`
	State           sacctJobState  `json:"state" yaml:"state"`
	Steps           []sacctJobStep `json:"steps" yaml:"steps"`
	Tres            sacctJobTres   `json:"tres" yaml:"tres"`
	Time            sacctJobTime   `json:"time" yaml:"time"`
}

func getTresByTypeName(tres []sacctTresUnit, typeName string, name string) (bool, int) {
	for _, t := range tres {
		if t.Type == typeName && t.Name == name {
			return true, t.Count
		}
	}
	return false, 0
}
func getTresTaskByTypeName(tres []sacctTresTaskUnit, typeName string, name string) (bool, int) {
	for _, t := range tres {
		if t.Type == typeName && t.Name == name {
			return true, t.Count
		}
	}
	return false, 0
}

func (e jobEngine) sacctGetJobByID(jobID string) (*jobperf.Job, error) {
	slog.Debug("fetching job by id", "jobID", jobID, "method", "sacct")
	var cmd *exec.Cmd
	if e.mode == slurmModeJSON {
		cmd = exec.Command("sacct", "--job", jobID, "--json")
	} else {
		cmd = exec.Command("sacct", "--job", jobID, "--yaml")
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run sacct for job id %v: %w", jobID, err)
	}

	var parsed sacctResponse
	if e.mode == slurmModeJSON {
		err = json.Unmarshal(out.Bytes(), &parsed)
	} else {
		err = yaml.Unmarshal(out.Bytes(), &parsed)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse sacct response for job id %v: %w", jobID, err)
	}
	if len(parsed.Jobs) != 1 {
		return nil, fmt.Errorf("unexpected number of jobs returned from sacct: %v", len(parsed.Jobs))
	}
	parsedJob := &parsed.Jobs[0]

	jobOut := jobperf.Job{
		ID:           strconv.Itoa(parsedJob.JobID),
		Name:         parsedJob.Name,
		Owner:        parsedJob.User,
		State:        string(parsedJob.State.Current),
		StartTime:    time.Unix(parsedJob.Time.Start.number, 0),
		Walltime:     time.Duration(parsedJob.Time.Limit.number) * time.Minute,
		UsedWalltime: time.Duration(parsedJob.Time.Elapsed) * time.Second,
		UsedCPUTime:  parsedJob.Time.Total.toDuration(),
		Raw:          parsedJob,
	}
	_, jobOut.CoresTotal = getTresByTypeName(parsedJob.Tres.Allocated, "cpu", "")
	_, memMb := getTresByTypeName(parsedJob.Tres.Allocated, "mem", "")
	jobOut.MemoryTotal = jobperf.Bytes(memMb * 1024 * 1024)
	_, jobOut.GPUsTotal = getTresByTypeName(parsedJob.Tres.Allocated, "gres", "gpu")

	// We now need to calculate the memory used. The slurmdb does not store
	// a total, max memory used. Instead it stores the max memory used by
	// any task for each step (TresUsageInMax column in salloc).
	//
	// The seff algorithm is as follows:
	//    For each step, calculate approx max memory usage by multiplying
	//      the memory from TresUsageInMax times the number of tasks.
	//    Report the highest memory used (caluclated above) by any step.
	//
	// This works well in most cases.
	// * For single step, single task jobs it is accurate
	// * For single step, multiple task jobs it may overestimate, but should
	//   work well if most of the step tasks are doing the same thing.
	// * For multiple non-overlapping step, single task jobs it is accurate
	// * For multiple non-overlapping step, multiple task jobs it may
	//   overestimate, but should work well if most of the step tasks are
	//   doing the same thing.
	// * For overlapping steps, it may be quite inaccurate.
	//
	// Jobperf uses the following, slightly more complicated, algorithm:
	//   Collect all start and end times for each step.
	//   For each collected time, t:
	//     Find all steps that were running (step start time <= t && end time > t)
	//     For each running step, compute approx max memory use as
	//       TresUsageInMax * number of steps.
	//     Record this time's memory as the sum of all steps max memory used.
	//   Report the highest memory usage of all times.
	//
	// It should provide the same numbers as seff for single step jobs, and
	// may have higher numbers for multiple steps (if they overlap).

	var timeStamps []int64
	for _, s := range parsedJob.Steps {
		if s.Time.Start.number > 0 {
			timeStamps = append(timeStamps, s.Time.Start.number)
		}
		if s.Time.End.number > 0 {
			timeStamps = append(timeStamps, s.Time.End.number)
		}
	}
	maxMemoryBytes := 0
	for _, t := range timeStamps {
		memTotal := 0
		for _, s := range parsedJob.Steps {
			// Ignore steps missing start time or steps that started after t.
			start := s.Time.Start.number
			if start == 0 || start > t {
				continue
			}
			// Ignore steps that have an end time and it is before t.
			end := s.Time.End.number
			if end == 0 && end <= t {
				continue
			}
			ok, mem := getTresTaskByTypeName(s.Tres.Requested.Max, "mem", "")
			if !ok {
				continue
			}
			memTotal += mem * s.Tasks.Count
		}
		if memTotal > maxMemoryBytes {
			slog.Debug("new high memory found", "t", t, "mem", memTotal)
			maxMemoryBytes = memTotal
		}
	}

	jobOut.UsedMemory = jobperf.Bytes(maxMemoryBytes)
	jobOut.Nodes = make([]jobperf.Node, parsedJob.AllocationNodes)

	return &jobOut, nil
}
