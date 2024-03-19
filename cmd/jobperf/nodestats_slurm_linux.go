package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/clemsonciti/jobperf"
)

// Contains code to emulate the Slurm gather_linux plugin.

// This is ideally pulls from sysconf(_SC_CLK_TCK), but in practice on most
// modern systems USER_HZ is 100.
// https://stackoverflow.com/questions/17410841/how-does-user-hz-solve-the-jiffy-scaling-issue
const userHz = 100

type slurmGatherCPUMem struct {
	cpuTime time.Duration
	mem     jobperf.Bytes
}

type slurmGatherLinux struct {
	lastCPUTimePerPID map[int]time.Duration
	maxMem            jobperf.Bytes
}

func getPIDsForJobID(jobID string) ([]int, error) {
	cmd := exec.Command("scontrol", "listpids", jobID)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to collect pids: %w", err)
	}

	scanner := bufio.NewScanner(&out)
	// Skip header
	scanner.Scan()

	var pids []int
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		pid, err := strconv.Atoi(parts[0])
		if err != nil {
			slog.Warn("bad PID found", "pid", parts[0])
			continue
		}
		if pid > 0 {
			pids = append(pids, pid)
		}
	}
	err = scanner.Err()
	if err != nil {
		return nil, fmt.Errorf("failed to parse listpids response")
	}

	return pids, nil
}

func getCPUMemForPID(pid int) (slurmGatherCPUMem, error) {
	var out slurmGatherCPUMem
	statFileName := fmt.Sprintf("/proc/%v/stat", pid)
	statsBuf, err := os.ReadFile(statFileName)
	if err != nil {
		return out, fmt.Errorf("failed to open proc stat file %v: %w", statFileName, err)
	}
	cmdEnd := bytes.LastIndex(statsBuf, []byte(")"))

	var ignoreStr string
	var ignoreInt int
	var utime, stime, cutime, cstime, vsize, rss, ppid uint64

	_, err = fmt.Fscan(bytes.NewBuffer(statsBuf[cmdEnd+2:]),
		&ignoreStr, // state
		&ppid,      // ppid: parent pid
		&ignoreInt, // pgrp: process group id
		&ignoreInt, // session
		&ignoreInt, // tty_nr
		&ignoreInt, // tpgid
		&ignoreInt, // flags
		&ignoreInt, // minflt
		&ignoreInt, // cminflt
		&ignoreInt, // majflt
		&ignoreInt, // cmajflt
		&utime,
		&stime,
		&cutime,
		&cstime,
		&ignoreInt, // priority
		&ignoreInt, // nice
		&ignoreInt, // num_threads
		&ignoreInt, // itrealvalue
		&ignoreInt, // starttime
		&vsize,
		&rss,
	)
	if err != nil {
		return out, fmt.Errorf("failed to parse proc stat file %v: %w", statFileName, err)
	}

	out.cpuTime = time.Duration((utime + stime) * (1e9 / userHz))
	out.mem = jobperf.Bytes(rss * uint64(os.Getpagesize()))

	return out, nil
}

func (s *slurmGatherLinux) handleCPUMemSlurmLinuxRequest(req *jobperf.NodeStatsRequest, w *json.Encoder) error {
	if s.lastCPUTimePerPID == nil {
		s.lastCPUTimePerPID = make(map[int]time.Duration)
	}

	var payload jobperf.NodeStatsCPUMemSlurmPayload
	err := json.Unmarshal(req.Payload, &payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cpu mem slurm payload: %w", err)
	}

	pids, err := getPIDsForJobID(payload.JobID)
	if err != nil {
		return fmt.Errorf("failed to get pids for job %v: %w", payload.JobID, err)
	}

	var stat jobperf.NodeStatsCPUMem
	stat.SampleTime = time.Now()
	for _, p := range pids {
		stats, err := getCPUMemForPID(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("failed to get cpu/mem for pid %v: %w", p, err)
		}
		s.lastCPUTimePerPID[p] = stats.cpuTime
		stat.MemoryUsedBytes += stats.mem
	}
	if stat.MemoryUsedBytes > s.maxMem {
		s.maxMem = stat.MemoryUsedBytes
	}

	for _, t := range s.lastCPUTimePerPID {
		stat.CPUTime += t
	}
	stat.MaxMemoryUsedBytes = s.maxMem
	err = w.Encode(stat)
	if err != nil {
		return fmt.Errorf("failed to write cpu mem response to slurm (linux) request : %w", err)
	}

	return nil
}
