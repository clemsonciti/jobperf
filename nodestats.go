package jobperf

import (
	"encoding/json"
	"time"
)

type NodeStatsRequestType int

const (
	NodeStatsRequestTypeSampleCPUMemSlurm NodeStatsRequestType = 0
	NodeStatsRequestTypeSampleCPUMemPBS   NodeStatsRequestType = 1
	NodeStatsRequestTypeSampleGPUNvidia   NodeStatsRequestType = 2
	NodeStatsRequestTypeExit              NodeStatsRequestType = 3
)

type NodeStatsRequest struct {
	RequestType NodeStatsRequestType `json:"type"`
	Payload     json.RawMessage      `json:"payload"`
}

type NodeStatsCPUMemSlurmPayload struct {
	JobID  string `json:"job_id"`
	UserID string `json:"user_id"`
}

type NodeStatsCPUMemPBSPayload struct {
	JobID string `json:"job_id"`
}

type NodeStatsCPUMem struct {
	SampleTime         time.Time     `json:"sample_time"`
	CPUTime            time.Duration `json:"cpu_time"`
	MemoryUsedBytes    Bytes         `json:"memory_bytes"`
	MaxMemoryUsedBytes Bytes         `json:"max_memory_bytes"`
	Hostname           string        `json:"hostname"`
}

type NodeStatsGPU []GPUStat

type NodeStatsSession interface {
	RequestCPUStats() (*NodeStatsCPUMem, error)
	RequestGPUStats() (*NodeStatsGPU, error)
	Close() error
}
