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
)

type sacctResponse struct {
	Meta     meta       `json:"meta"`
	Jobs     []sacctJob `json:"jobs"`
	Warnings []any      `json:"warnings"`
	Errors   []any      `json:"errors"`
}

type sacctTresUnit struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	ID    int    `json:"id"`
	Count int    `json:"count"`
}

type sacctTresTaskUnit struct {
	sacctTresUnit
	Node string `json:"node"`
	Task int    `json:"task"`
}

type sacctJobTres struct {
	Allocated []sacctTresUnit `json:"allocated"`
	Requested []sacctTresUnit `json:"requested"`
}

type sacctJobState struct {
	CurrentRaw json.RawMessage `json:"current"`
	Reason     string          `json:"reason"`
}

func (s *sacctJobState) Current() string {
	var stringState string
	err := json.Unmarshal(s.CurrentRaw, &stringState)
	if err == nil {
		return stringState
	}
	var sliceState []string
	err = json.Unmarshal(s.CurrentRaw, &sliceState)
	if err == nil {
		return sliceState[0]
	}

	return "Unknown"
}

type sacctJobTime struct {
	// Elapsed seems to be in seconds.
	Elapsed    int           `json:"elapsed"`
	End        optionalValue `json:"end"`
	Start      optionalValue `json:"start"`
	Eligible   int           `json:"eligible"`
	Submission int           `json:"submission"`
	Suspended  int           `json:"suspended"`
	Limit      optionalValue `json:"limit"` // Limit seems to be in minutes.
	Total      sacctCPUTime  `json:"total"`
	User       sacctCPUTime  `json:"user"`
}

type sacctCPUTime struct {
	Seconds      int `json:"seconds"`
	Microseconds int `json:"microseconds"`
}

func (t *sacctCPUTime) toDuration() time.Duration {
	return (time.Duration(t.Seconds)*time.Second +
		time.Duration(t.Microseconds)*time.Microsecond)
}

type sacctJobStepTime struct {
	Elapsed int           `json:"elapsed"`
	End     optionalValue `json:"end"`
	Start   optionalValue `json:"start"`
	System  sacctCPUTime  `json:"system"`
	User    sacctCPUTime  `json:"user"`
}

type sacctJobStepExitCode struct {
	StatusRaw  json.RawMessage `json:"status"`
	ReturnCode optionalValue   `json:"return_code"`
}

func (s *sacctJobStepExitCode) Status() string {
	var stringState string
	err := json.Unmarshal(s.StatusRaw, &stringState)
	if err == nil {
		return stringState
	}
	var sliceState []string
	err = json.Unmarshal(s.StatusRaw, &sliceState)
	if err == nil {
		return sliceState[0]
	}

	return "Unknown"
}

type sacctJobStepNodes struct {
	Count int      `json:"count"`
	Range string   `json:"range"`
	List  []string `json:"list"`
}
type sacctJobStepTasks struct {
	Count int `json:"count"`
}
type sacctJobStepInfo struct {
	/*
		ID struct {
			JobID  int    `json:"job_id"`
			StepID string `json:"step_id"`
		} `json:"id"`
	*/
	Name string `json:"name"`
}

type sacctJobStepTresStats struct {
	Total   []sacctTresUnit     `json:"total"`
	Average []sacctTresUnit     `json:"average"`
	Min     []sacctTresTaskUnit `json:"min"`
	Max     []sacctTresTaskUnit `json:"max"`
}

type sacctJobStepTres struct {
	Requested sacctJobStepTresStats `json:"requested"`
	Consumed  sacctJobStepTresStats `json:"consumed"`
	Allocated []sacctTresUnit       `json:"allocated"`
}

type sacctJobStep struct {
	//State    string               `json:"state"`
	Time     sacctJobStepTime     `json:"time"`
	ExitCode sacctJobStepExitCode `json:"exit_code"`
	Nodes    sacctJobStepNodes    `json:"nodes"`
	Tasks    sacctJobStepTasks    `json:"tasks"`
	Step     sacctJobStepInfo     `json:"step"`
	Tres     sacctJobStepTres     `json:"tres"`
}

type sacctJob struct {
	Account         string         `json:"account"`
	AllocationNodes int            `json:"allocation_nodes"`
	Cluster         string         `json:"cluster"`
	JobID           int            `json:"job_id"`
	Name            string         `json:"name"`
	Partition       string         `json:"partition"`
	User            string         `json:"user"`
	State           sacctJobState  `json:"state"`
	Steps           []sacctJobStep `json:"steps"`
	Tres            sacctJobTres   `json:"tres"`
	Time            sacctJobTime   `json:"time"`
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

func sacctGetJobByID(jobID string) (*jobperf.Job, error) {
	slog.Debug("fetching job by id", "jobID", jobID, "method", "sacct")
	cmd := exec.Command("sacct", "--job", jobID, "--json")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to run sacct for job id %v: %w", jobID, err)
	}

	var parsed sacctResponse
	err = json.Unmarshal(out.Bytes(), &parsed)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sacct response for job id %v: %w", jobID, err)
	}
	if len(parsed.Jobs) != 1 {
		return nil, fmt.Errorf("unexpected number of jobs returned from sacct: %v", len(parsed.Jobs))
	}
	parsedJob := parsed.Jobs[0]

	jobOut := jobperf.Job{
		ID:           strconv.Itoa(parsedJob.JobID),
		Name:         parsedJob.Name,
		Owner:        parsedJob.User,
		State:        parsedJob.State.Current(),
		StartTime:    time.Unix(parsedJob.Time.Start.number, 0),
		Walltime:     time.Duration(parsedJob.Time.Limit.number) * time.Minute,
		UsedWalltime: time.Duration(parsedJob.Time.Elapsed) * time.Second,
		UsedCPUTime:  parsedJob.Time.Total.toDuration(),
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
