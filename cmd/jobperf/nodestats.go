package main

import (
	"bufio"
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
	var fileName string
	if metricType == "" {
		fileName = fmt.Sprintf("/sys/fs/cgroup/%s/%s", cgroupName, metricName)
	} else {
		// cgroupv1
		fileName = fmt.Sprintf("/sys/fs/cgroup/%s/%s/%s", metricType, cgroupName, metricName)
	}
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

func getCGroupStat(cgroupName, metricName string) (map[string]int64, error) {
	res := make(map[string]int64)
	slog.Debug("reading cgroup metric", "cgroupName", cgroupName, "metricName", metricName)
	fileName := fmt.Sprintf("/sys/fs/cgroup/%s/%s", cgroupName, metricName)
	file, err := os.Open(fileName)
	if err != nil {
		return res, fmt.Errorf("Unable to access metric file %v: %w", fileName, err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key_value := strings.Split(scanner.Text(), " ")
		if len(key_value) != 2 {
			return res, fmt.Errorf(
				"failed to split line '%v' (%v tokens) from file %v",
				scanner.Text(), len(key_value), fileName)
		}
		metricValue, err := strconv.ParseInt(key_value[1], 10, 64)
		if err != nil {
			return res, fmt.Errorf("failed to decode metric string %v from file %v: %w", key_value, fileName, err)
		}
		res[key_value[0]] = metricValue
	}
	if err := scanner.Err(); err != nil {
		return res, fmt.Errorf("failed to scan metrics from file %v: %w", fileName, err)
	}
	slog.Debug("cgroup reading result", "cgroupName", cgroupName, "metricName", metricName, "result", res)
	return res, nil
}

func getCPUMemStatsForCGroup(cgroup string) (*jobperf.NodeStatsCPUMem, error) {
	slog.Debug("using cgroupv1 to get cpu mem stats")
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

func getCPUMemStatsForCGroupV2(cgroup string) (*jobperf.NodeStatsCPUMem, error) {
	slog.Debug("using cgroupv2 to get cpu mem stats")
	var stat jobperf.NodeStatsCPUMem

	cpuStats, err := getCGroupStat(cgroup, "cpu.stat")
	if err != nil {
		return nil, err
	}
	usage_usec, ok := cpuStats["usage_usec"]
	if !ok {
		return nil, fmt.Errorf("no usage_usec in cgroup file %v/cpu.stat", cgroup)
	}

	stat.CPUTime = time.Duration(usage_usec * 1000)

	memUsed, err := getCGroupMetric(cgroup, "", "memory.current")
	if err != nil {
		return nil, err
	}
	stat.MemoryUsedBytes = jobperf.Bytes(memUsed)

	maxMemUsed, err := getCGroupMetric(cgroup, "", "memory.peak")
	if err != nil {
		return nil, err
	}
	stat.MaxMemoryUsedBytes = jobperf.Bytes(maxMemUsed)
	stat.SampleTime = time.Now()
	return &stat, nil
}

func handleCPUMemSlurmCGroupRequest(req *jobperf.NodeStatsRequest, w *json.Encoder) error {
	var payload jobperf.NodeStatsCPUMemSlurmPayload
	var res *jobperf.NodeStatsCPUMem
	var err error
	err = json.Unmarshal(req.Payload, &payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cpu mem slurm payload: %w", err)
	}

	// detect cgroups v2 as the absence of the cpu controller
	// slurm has a more advanced detection in autodetect_cgroup_version() in src/interfaces/cgroup.c
	_, err = os.Stat("/sys/fs/cgroup/cpu")
	cgroupsV2 := err != nil

	if cgroupsV2 {
		cgroup := fmt.Sprintf("system.slice/slurmstepd.scope/job_%v", payload.JobID)
		res, err = getCPUMemStatsForCGroupV2(cgroup)
	} else {
		cgroup := fmt.Sprintf("slurm/uid_%v/job_%v", payload.UserID, payload.JobID)
		res, err = getCPUMemStatsForCGroup(cgroup)
	}
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

	var sgl slurmGatherLinux

	for {
		var req jobperf.NodeStatsRequest
		err := requestDecoder.Decode(&req)
		if err != nil {
			slog.Error("failed to decode request", "err", err)
			os.Exit(1)
		}
		slog.Debug("got request", "req", req)
		switch req.RequestType {
		case jobperf.NodeStatsRequestTypeSampleCPUMemSlurmCGroup:
			err = handleCPUMemSlurmCGroupRequest(&req, responseEncoder)
		case jobperf.NodeStatsRequestTypeSampleCPUMemSlurmLinux:
			err = sgl.handleCPUMemSlurmLinuxRequest(&req, responseEncoder)
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
