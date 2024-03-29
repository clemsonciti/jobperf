package jobperf

import (
	"time"
)

type JobState interface {
	IsRunning() bool
	IsComplete() bool
	IsQueued() bool
	String() string
}

type JobQuery struct {
	Username    string
	OnlyRunning bool
}

type JobEngine interface {
	GetJobByID(jobID string) (*Job, error)
	SelectJobIDs(q JobQuery) ([]string, error)
	NodeStatsSession(j *Job, hostname string) (NodeStatsSession, error)
	Warning() string
	NodeStatsWarning() string
}

type Job struct {
	ID    string
	Name  string
	Owner string
	//ChunkCount  int
	CoresTotal  int
	MemoryTotal Bytes
	GPUsTotal   int
	Walltime    time.Duration
	State       string

	StartTime    time.Time
	UsedWalltime time.Duration
	UsedCPUTime  time.Duration
	UsedMemory   Bytes

	Nodes []Node

	// Raw holds a scheduler specific type. It can be used by the scheduler
	// plugin when creating a nodestats session.
	Raw interface{}
}

func (j *Job) IsRunning() bool {
	return j.State == "R" || j.State == "RUNNING"
}
func (j *Job) IsComplete() bool {
	for _, s := range []string{"E", "F", "COMPLETED", "CANCELED", "TIMEOUT", "FAILED"} {
		if j.State == s {
			return true
		}
	}
	return false
}
