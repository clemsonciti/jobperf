package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/clemsonciti/jobperf"
	"github.com/clemsonciti/jobperf/nvidia"
)

func getCGroupMetric(cgroupName, metricType, metricName string) (int64, error) {
	slog.Debug("reading cgroup metric", "cgroupName", cgroupName, "metricType", metricType, "metricName", metricName)
	fileName := fmt.Sprintf("/sys/fs/cgroup/%s/%s/%s", metricType, cgroupName, metricName)
	metricStr, err := os.ReadFile(fileName)
	if err != nil {
		return 0, fmt.Errorf("failed to read metric file %v: %w", fileName, err)
	}
	metricInt, err := strconv.ParseInt(strings.TrimSpace(string(metricStr)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to decode metric string %v from file %v: %w", metricStr, fileName, err)
	}
	slog.Debug("cgroup reading result", "cgroupName", cgroupName, "metricType", metricType, "metricName", metricName, "result", metricInt)
	return metricInt, nil

}

func getCPUMemStatsForCGroup(cgroup string) (*jobperf.NodeStatsCPUMem, error) {
	var stat jobperf.NodeStatsCPUMem
	cpuTimeInt, err := getCGroupMetric(cgroup, "cpu", "cpuacct.usage")
	if err != nil {
		return nil, err
	}
	stat.CPUTime = time.Duration(cpuTimeInt)

	memUsed, err := getCGroupMetric(cgroup, "memory", "memory.usage_in_bytes")
	if err != nil {
		return nil, err
	}
	stat.MemoryUsedBytes = jobperf.Bytes(memUsed)

	maxMemUsed, err := getCGroupMetric(cgroup, "memory", "memory.max_usage_in_bytes")
	if err != nil {
		return nil, err
	}
	stat.MaxMemoryUsedBytes = jobperf.Bytes(maxMemUsed)
	stat.SampleTime = time.Now()
	return &stat, nil
}

func handleCPUMemSlurmRequest(req *jobperf.NodeStatsRequest, w *json.Encoder) error {
	var payload jobperf.NodeStatsCPUMemSlurmPayload
	err := json.Unmarshal(req.Payload, &payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cpu mem slurm payload: %w", err)
	}

	cgroup := fmt.Sprintf("slurm/uid_%v/job_%v", payload.UserID, payload.JobID)

	res, err := getCPUMemStatsForCGroup(cgroup)
	if err != nil {
		return fmt.Errorf("failed to fetch cgroup stats for slurm request %v : %w", req, err)
	}
	err = w.Encode(*res)
	if err != nil {
		return fmt.Errorf("failed to write cpu mem response to slurm request : %w", err)
	}
	return nil
}

func handleCPUMemPBSRequest(req *jobperf.NodeStatsRequest, w *json.Encoder) error {
	var payload jobperf.NodeStatsCPUMemPBSPayload
	err := json.Unmarshal(req.Payload, &payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cpu mem pbs payload: %w", err)
	}

	cgroup := fmt.Sprintf("pbspro.service/jobid/%s", payload.JobID)

	res, err := getCPUMemStatsForCGroup(cgroup)
	if err != nil {
		return fmt.Errorf("failed to fetch cgroup stats for pbs request %v : %w", req, err)
	}
	err = w.Encode(*res)
	if err != nil {
		return fmt.Errorf("failed to write cpu mem response to pbs request : %w", err)
	}
	return nil
}

func handleGPUNvidiaRequest(req *jobperf.NodeStatsRequest, w *json.Encoder) error {
	log, err := nvidia.GetSMILog()
	if err != nil {
		return err
	}
	var stats []jobperf.GPUStat
	for _, gpu := range log.GPUs {

		computeUsage, err := strconv.Atoi(strings.TrimRight(gpu.Utilization.GPUUtil, " %"))
		if err != nil {
			return fmt.Errorf("failed to convert gpu id %s untilization %s to int: %w", gpu.ID, gpu.Utilization.GPUUtil, err)
		}
		memUsage, err := jobperf.ParseBytes(gpu.MemoryUsage.Used)
		if err != nil {
			return fmt.Errorf("failed to convert gpu id %s memory used %s to bytes: %w", gpu.ID, gpu.MemoryUsage.Used, err)
		}
		memTotal, err := jobperf.ParseBytes(gpu.MemoryUsage.Total)
		if err != nil {
			return fmt.Errorf("failed to convert gpu id %s memory total %s to bytes: %w", gpu.ID, gpu.MemoryUsage.Total, err)
		}
		stats = append(stats, jobperf.GPUStat{
			SampleTime:    time.Now(),
			ProductName:   gpu.ProductName,
			ID:            gpu.ID,
			ComputeUsage:  computeUsage,
			MemUsageBytes: memUsage,
			MemTotalBytes: memTotal,
		})
	}

	err = w.Encode(jobperf.NodeStatsGPU(stats))
	if err != nil {
		return fmt.Errorf("failed to encode gpu stats response: %w", err)
	}
	return nil
}

func runNodeStats() {
	requestDecoder := json.NewDecoder(os.Stdin)
	responseEncoder := json.NewEncoder(os.Stdout)

	// When we are sent a signal to exit, wait a second to finish any processing, then die.
	stopReq := make(chan os.Signal, 1)
	signal.Notify(stopReq, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-stopReq
		slog.Debug("got stop signal...")
		time.Sleep(time.Second)
		slog.Debug("ending with exit 1")
		os.Exit(1)
	}()

	for {
		var req jobperf.NodeStatsRequest
		err := requestDecoder.Decode(&req)
		if err != nil {
			slog.Error("failed to decode request", "err", err)
			os.Exit(1)
		}
		slog.Debug("got request", "req", req)
		switch req.RequestType {
		case jobperf.NodeStatsRequestTypeSampleCPUMemSlurm:
			err = handleCPUMemSlurmRequest(&req, responseEncoder)
		case jobperf.NodeStatsRequestTypeSampleCPUMemPBS:
			err = handleCPUMemPBSRequest(&req, responseEncoder)
		case jobperf.NodeStatsRequestTypeSampleGPUNvidia:
			err = handleGPUNvidiaRequest(&req, responseEncoder)
		case jobperf.NodeStatsRequestTypeExit:
			slog.Debug("exiting...")
			return
		default:
			err = errors.New("unknown request type")
		}
		if err != nil {
			slog.Error("failed handle request", "req", req, "err", err)
			os.Exit(1)
		}
	}

}
