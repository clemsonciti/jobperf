package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"os/user"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/clemsonciti/jobperf"
	"github.com/clemsonciti/jobperf/pbs"
	"github.com/clemsonciti/jobperf/recorder"
	"github.com/clemsonciti/jobperf/slurm"
)

var (
	// These will get overridden goreleaser.
	buildVersion = "dev"
	buildCommit  = "none"
	buildDate    = "unknown"
)

var sampleRate = flag.Duration("rate", 2*time.Second, "Node polling rate")
var watch = flag.Bool("w", false, "Periodically poll current status from each node.")
var debug = flag.Bool("debug", false, "Enable debug logging.")
var engineFlag = flag.String("engine", "auto", "Which job engine to use (pbs/slurm/auto).")
var nodeStatsMode = flag.Bool("nodestats", false, "Starts jobperf in 'nodestats' mode where it operates as a stat collection server. This is not intended to be used directly by users.")
var record = flag.Bool("record", false, "Enables record mode and records stats to an sqlite db.")
var load = flag.Bool("load", false, "Load job and stats recorded to DB rather than job engine.")
var useHTTPServer = flag.Bool("http", false, "Display stats in http server.")
var httpServerPort = flag.Int("http-port", 0, "Display stats in http server. The default is to automatically pick a free port.")
var httpDisableAuth = flag.Bool("http-disable-auth", false, "Disable authentication (allows anyone to connect to the http server and see the job status).")
var showVersion = flag.Bool("version", false, "Show version and exit.")
var showLicense = flag.Bool("license", false, "Show license and exit.")
var recordFilename string

func init() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&recordFilename, "record-db", homeDir+"/.local/share/jobstats.db", "Location of record DB (if enabled).")
}

type nodeStats struct {
	cpuMem   []jobperf.NodeStatsCPUMem
	gpuStats map[string][]jobperf.GPUStat
	session  jobperf.NodeStatsSession

	gpuIDList []string
}

type allNodeStats map[string]*nodeStats

type app struct {
	rec             *recorder.Recorder
	job             *jobperf.Job
	stats           allNodeStats
	statsMu         *sync.RWMutex
	engine          jobperf.JobEngine
	nStatsCollected int
	config          *config

	*statPubSub
}

func (a *app) Close() {
	a.stats.Close()
	if a.rec != nil {
		a.rec.Close()
	}
}

func (s allNodeStats) Close() {
	for n, si := range s {
		if si.session != nil {
			slog.Debug("closing nodestats session", "node", n)
			err := si.session.Close()
			slog.Debug("closed nodestats session", "node", n, "err", err)
		}
	}
}

func (a *app) updateNodeStats() error {
	a.statsMu.Lock()
	defer a.statsMu.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(a.job.Nodes))
	slog.Debug("updating node stats")

	errCh := make(chan error, len(a.job.Nodes))

	for i := range a.job.Nodes {
		host := a.job.Nodes[i].Hostname
		if a.stats[host] == nil {
			slog.Debug("creating node stat session", "host", host)
			session, err := a.engine.NodeStatsSession(a.job, host)
			if err != nil {
				return err
			}
			a.stats[host] = &nodeStats{
				session: session,
			}
		}
	}

	for i := range a.job.Nodes {
		go func(host string, i int) {
			defer wg.Done()
			slog.Debug("requesting cpu stats", "host", host)
			cpuMem, err := a.stats[host].session.RequestCPUStats()
			if err != nil {
				errCh <- err
				return
			}
			a.stats[host].cpuMem = append(a.stats[host].cpuMem, *cpuMem)
			a.PublishCPUMem(*cpuMem)

			if a.job.Nodes[i].NGPUs == 0 {
				slog.Debug("not requesting gpu stats", "host", host)
				return
			}

			slog.Debug("requesting gpu stats", "host", host)
			if a.stats[host].gpuStats == nil {
				a.stats[host].gpuStats = make(map[string][]jobperf.GPUStat)
			}
			updatedGPU, err := a.stats[host].session.RequestGPUStats()
			if err != nil {
				errCh <- err
				return
			}
			for _, s := range *updatedGPU {
				a.PublishGPUStat(s)

				// IF this is the first by this ID, add to list:
				if len(a.stats[host].gpuStats[s.ID]) == 0 {
					a.stats[host].gpuIDList = append(a.stats[host].gpuIDList, s.ID)
				}
				a.stats[host].gpuStats[s.ID] = append(a.stats[host].gpuStats[s.ID], s)

			}

		}(a.job.Nodes[i].Hostname, i)
	}
	slog.Debug("waiting for all node stats...")

	wg.Wait()
	close(errCh)
	slog.Debug("done waiting for node updates...")

	var errOut error
	for e := range errCh {
		if errOut == nil {
			errOut = e
		} else {
			errOut = fmt.Errorf("%v, %w", errOut, e)
		}
	}
	if errOut == nil {
		a.nStatsCollected++
	}
	slog.Debug("done updating node stats", "nodes", a.job.Nodes)
	return errOut
}

func (a *app) initDB() {
	// If record or load not provided, skip.
	if !*record && !*load {
		return
	}
	var err error
	slog.Debug("opening record file", "filename", recordFilename)
	a.rec, err = recorder.New(recordFilename)
	if err != nil {
		slog.Error("failed to start record db", "err", err)
		os.Exit(1)
	}
	if *record {
		a.startDBStatsSaver()
	}
}

func (a *app) saveJobToDB() {
	if a.rec == nil {
		return
	}
	err := a.rec.RecordJob(a.job)
	if err != nil {
		slog.Error("failed to write job to db", "err", err)
	}
}

func (a *app) startDBStatsSaver() {
	if a.rec == nil {
		return
	}
	go func() {
		cpuMemCh := make(chan jobperf.NodeStatsCPUMem)
		gpuCh := make(chan jobperf.GPUStat)
		a.AddCPUMemListener(cpuMemCh)
		a.AddGPUListener(gpuCh)
		for {
			select {
			case stat := <-cpuMemCh:
				slog.Debug("writing cpu mem data to db", "stat", stat)
				err := a.rec.RecordNodeStat(a.job.ID, stat)
				if err != nil {
					slog.Error("failed to write node stats", "err", err)
				}

			case stat := <-gpuCh:
				slog.Debug("writing gpu data to db", "stat", stat)
				err := a.rec.RecordGPUStats(a.job.ID, []jobperf.GPUStat{stat})
				if err != nil {
					slog.Error("failed to write node gpu stats", "err", err)
				}
			}
		}
	}()
}

func (a *app) printSummary() {
	fmt.Println("Job Summary")
	fmt.Println("-----------")
	for _, line := range [][]interface{}{
		{"Job ID", a.job.ID},
		{"Job Name", a.job.Name},
		{"Nodes", len(a.job.Nodes)},
		{"Total CPU Cores", a.job.CoresTotal},
		{"Total Mem", a.job.MemoryTotal},
		{"Total GPUs", a.job.GPUsTotal},
		{"Walltime Requested", a.job.Walltime},
		{"Status", a.job.State},
	} {
		fmt.Printf("%20v: %-20v\n", line[0], line[1])
	}
	fmt.Println()
}

func (a *app) printBasicUsage() {
	if a.job.UsedWalltime == 0 || a.job.UsedCPUTime == 0 {
		fmt.Println("No usage available")
		return
	}

	fmt.Println("Overall Job Resource Usage")
	fmt.Println("--------------------------")
	avgCores := float64(a.job.UsedCPUTime) / float64(a.job.UsedWalltime)
	avgCoresStr := fmt.Sprintf("%0.2f", avgCores)
	walltimePer := fmt.Sprintf("%0.2f %%", float64(a.job.UsedWalltime)/float64(a.job.Walltime)*100.0)
	avgCoresPer := fmt.Sprintf("%0.2f %%", float64(avgCores)/float64(a.job.CoresTotal)*100.0)
	memoryPer := fmt.Sprintf("%0.2f %%", float64(a.job.UsedMemory)/float64(a.job.MemoryTotal)*100.0)
	for _, line := range [][]interface{}{
		{"", "", "Percent of Request"},
		{"Walltime Used", a.job.UsedWalltime, walltimePer},
		{"Avg CPU Cores Used", avgCoresStr, avgCoresPer},
		{"Memory Used", a.job.UsedMemory, memoryPer},
	} {
		fmt.Printf("%20v   %-18v %-12v\n", line[0], line[1], line[2])
	}
	if warning := a.engine.Warning(); warning != "" {
		fmt.Printf("\n⚠️  %v\n\n", warning)
	} else {
		fmt.Println()
	}
}

func (a *app) printAvgNodeStats() {
	a.statsMu.RLock()
	defer a.statsMu.RUnlock()
	if a.nStatsCollected < 1 {
		return
	}
	fmt.Println("Per Node Stats Over Whole Job")
	fmt.Println("--------------------------")
	fmt.Printf("%-12v%-30v%-30v\n", "", "CPU Cores", "Memory")
	fmt.Printf("%-12v%-12v%-18v%-12v%-18v\n", "Node", "Requested", "Avg Used", "Requested", "Max Used")

	for _, n := range a.job.Nodes {
		nodeStats := a.stats[n.Hostname]
		stat := nodeStats.cpuMem[len(nodeStats.cpuMem)-1]
		//stat := n.Stats[len(n.Stats)-1]
		coresUsed := float64(stat.CPUTime) / float64(stat.SampleTime.Sub(a.job.StartTime))
		coresUsedPer := fmt.Sprintf("%0.2f %%", float64(coresUsed)/float64(n.NCores)*100.0)
		memUsedPer := fmt.Sprintf("%0.2f %%", float64(stat.MaxMemoryUsedBytes)/float64(n.Memory)*100.0)
		fmt.Printf("%-12v%-12v%-18v%-12v%-18v\n",
			n.Hostname,
			n.NCores,
			fmt.Sprintf("%.2f (%v)", coresUsed, coresUsedPer),
			n.Memory,
			fmt.Sprintf("%v (%v)", stat.MaxMemoryUsedBytes, memUsedPer),
		)
	}
	if warning := a.engine.NodeStatsWarning(); warning != "" {
		fmt.Printf("\n⚠️  %v\n\n", warning)
	} else {
		fmt.Println()
	}
}
func (a *app) printCurrentNodeStats() {
	a.statsMu.RLock()
	defer a.statsMu.RUnlock()
	if a.nStatsCollected < 2 || *load {
		return
	}
	fmt.Println("Current Per Node Stats --", time.Now().Format(time.Stamp))
	fmt.Println("--------------------------")
	fmt.Printf("%-12v%-30v%-30v\n", "", "CPU Cores", "Memory")
	fmt.Printf("%-12v%-12v%-18v%-12v%-15v%-20v\n", "Node", "Requested", "Usage", "Requested", "Current Use", "Max Used")

	for _, n := range a.job.Nodes {
		nodeStats := a.stats[n.Hostname]
		stat1 := nodeStats.cpuMem[len(nodeStats.cpuMem)-1]
		stat2 := nodeStats.cpuMem[len(nodeStats.cpuMem)-2]
		//stat1 := n.Stats[len(n.Stats)-1]
		//stat2 := n.Stats[len(n.Stats)-2]
		coresUsed := float64(stat1.CPUTime-stat2.CPUTime) / float64(stat1.SampleTime.Sub(stat2.SampleTime))
		coresUsedPer := fmt.Sprintf("%0.2f %%", float64(coresUsed)/float64(n.NCores)*100.0)
		maxMemUsedPer := fmt.Sprintf("%0.2f %%", float64(stat1.MaxMemoryUsedBytes)/float64(n.Memory)*100.0)
		fmt.Printf("%-12v%-12v%-18v%-12v%-15v%-20v\n",
			n.Hostname,
			n.NCores,
			fmt.Sprintf("%.2f (%v)", coresUsed, coresUsedPer),
			n.Memory,
			stat1.MemoryUsedBytes,
			fmt.Sprintf("%v (%v)", stat1.MaxMemoryUsedBytes, maxMemUsedPer),
		)
	}
	if a.job.GPUsTotal == 0 {
		return
	}
	fmt.Println()
	fmt.Printf("%-12v%-30v%-15v%-15v\n", "Node", "GPU Model", "Compute Usage", "Memory Usage")

	for _, n := range a.job.Nodes {
		nodeStats := a.stats[n.Hostname]
		for _, gpuID := range nodeStats.gpuIDList {
			statIdx := len(nodeStats.gpuStats[gpuID]) - 1
			stat := nodeStats.gpuStats[gpuID][statIdx]
			fmt.Printf("%-12v%-30v%-15v%-15v\n",
				n.Hostname, stat.ProductName,
				fmt.Sprintf("%v %%", stat.ComputeUsage),
				stat.MemUsageBytes)

		}
	}
	fmt.Println()
}

func (a *app) printJob() {
	a.printSummary()
	a.printBasicUsage()
	a.printAvgNodeStats()
	a.printCurrentNodeStats()
}

func (a *app) collectInitialNodeStats() {
	if !a.job.IsRunning() {
		return
	}
	var err error
	err = a.updateNodeStats()
	if err != nil {
		log.Fatalf("failed to update node stats: %v", err)
	}

	// To get current node stats, we fetch twice over a short
	// inverval.
	time.Sleep(time.Second / 2)
	err = a.updateNodeStats()
	if err != nil {
		log.Fatalf("failed to update node stats: %v", err)
	}
}

func (a *app) loadJobFromDB(jobID string) *jobperf.Job {
	a.statsMu.Lock()
	defer a.statsMu.Unlock()
	job, err := a.rec.GetJob(jobID)
	if err != nil {
		slog.Error("failed to load job from db", "jobID", jobID, "err", err)
		os.Exit(1)
	}
	cpuMemStats, err := a.rec.GetNodeStats(jobID)
	if err != nil {
		slog.Error("failed to cpu mem stats from db", "jobID", jobID, "err", err)
		os.Exit(1)
	}
	slices.SortFunc(cpuMemStats, func(a, b jobperf.NodeStatsCPUMem) int {
		return a.SampleTime.Compare(b.SampleTime)
	})
	for _, s := range cpuMemStats {
		host := s.Hostname
		if a.stats[host] == nil {
			a.stats[host] = &nodeStats{
				gpuStats: make(map[string][]jobperf.GPUStat),
			}
		}
		a.stats[host].cpuMem = append(a.stats[host].cpuMem, s)
	}
	gpuStats, err := a.rec.GetGPUStats(jobID)
	if err != nil {
		slog.Error("failed to gpu stats from db", "jobID", jobID, "err", err)
		os.Exit(1)
	}
	slices.SortFunc(gpuStats, func(a, b jobperf.GPUStat) int {
		return a.SampleTime.Compare(b.SampleTime)
	})
	for _, s := range gpuStats {
		host := s.Hostname
		if a.stats[host].gpuStats[s.ID] == nil {
			a.stats[host].gpuIDList = append(a.stats[host].gpuIDList, s.ID)
		}
		a.stats[host].gpuStats[s.ID] = append(a.stats[host].gpuStats[s.ID], s)
	}

	// Assume same number of stats on each node.
	// Use an arbitrary node to determine nStatsCollected.
	for _, stats := range a.stats {
		a.nStatsCollected = len(stats.cpuMem)
		break
	}
	return job
}

func (a *app) getJob(jobID string) *jobperf.Job {
	if *load {
		loadJob := a.loadJobFromDB(jobID)
		// Also fetch from engine to get new stats.
		job, err := a.engine.GetJobByID(jobID)
		if err != nil {
			return loadJob
		}
		loadJob.State = job.State
		loadJob.UsedWalltime = job.UsedWalltime
		loadJob.UsedMemory = job.UsedMemory
		loadJob.UsedCPUTime = job.UsedCPUTime
		return loadJob
	}

	job, err := a.engine.GetJobByID(jobID)
	if err != nil {
		slog.Error("Failed to get job", "err", err)
		os.Exit(1)
	}
	u, err := user.Current()
	if err != nil {
		slog.Error("Failed to get user", "err", err)
		os.Exit(1)
	}
	if u.Username == "root" {
		hostname, err := os.Hostname()
		if err != nil {
			slog.Error("failed to get hostname", "err", err)
			os.Exit(1)
		}
		if !strings.HasPrefix(hostname, "master") {
			fmt.Printf("⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ \n")
			fmt.Printf("!!!! You are running jobperf as root on a host %v -- this may fail on running jobs! Try instead running form master (or another system without root squash mounts). !!!!\n", hostname)
			fmt.Printf("⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ ⚠️ \n")
		}

	}
	if u.Username != job.Owner && u.Username != "root" {
		fmt.Println("Job is not owned by you.")
		os.Exit(1)
	}

	// If we are watching the job and the current status is PENDING, retry
	// every 500ms up to 20 times to see if the job starts running.
	pendingCount := 0
	for job.State == "PENDING" && pendingCount < 20 {
		time.Sleep(500 * time.Millisecond)
		job, err = a.engine.GetJobByID(jobID)
		if err != nil {
			slog.Error("Failed to get qstat jobs", "err", err)
			os.Exit(1)
		}
		pendingCount++
	}
	return job
}

func (a *app) getJobIDAndUpdateEngine(args []string) string {
	var engineName string

	if *engineFlag == "pbs" {
		if !pbs.IsAvailable() {
			slog.Error("failed to find PBS job engine")
			os.Exit(1)
		}
		a.engine = pbs.NewJobEngine()
		engineName = "pbs"
	} else if *engineFlag == "slurm" {
		if !slurm.IsAvailable() {
			slog.Error("failed to find Slurm job engine")
			os.Exit(1)
		}
		a.engine = slurm.NewJobEngine()
		engineName = "slurm"
	} else if *engineFlag == "auto" {
		if slurm.IsAvailable() {
			a.engine = slurm.NewJobEngine()
			engineName = "slurm"
		} else if pbs.IsAvailable() {
			a.engine = pbs.NewJobEngine()
			engineName = "pbs"
		} else {
			slog.Error("Failed to detect job engine")
			os.Exit(1)
		}
	} else {
		slog.Error("Unknown job engine", "engine", *engineFlag)
		os.Exit(1)
	}

	var jobID string
	if len(args) != 1 {
		if engineName == "pbs" {
			jobID = os.Getenv("PBS_JOBID")
		} else {
			jobID = os.Getenv("SLURM_JOB_ID")
		}
	} else {
		jobID = args[0]
	}
	if jobID == "" {
		fmt.Println("No Job ID provided. Run as 'jobperf [options] <jobID>'.  Use 'qstat -u <username>' (on PBS) or 'squeue -u <username>` (on Slurm) to see current and queued jobs.")
		os.Exit(1)
	}

	return jobID

}

func main() {
	flag.Parse()
	args := flag.Args()
	logOpts := slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if *debug {
		logOpts.Level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &logOpts)))
	slog.Debug("jobperf started", "version", buildVersion, "dateBuilt", buildDate, "commitBuilt", buildCommit)

	if *showVersion {
		fmt.Printf(`jobperf %v git-%v. Built %v

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

A copy of the GNU General Public License is bundled with the 
executable. Run jobperf -license to view this copy. You can 
also write to the Free Software Foundation, Inc., 
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

`, buildVersion, buildCommit, buildDate)
		os.Exit(0)
	}

	if *showLicense {
		fmt.Printf(jobperf.License + "\n\n")
		os.Exit(0)
	}

	if *nodeStatsMode {
		runNodeStats()
		os.Exit(0)
	}

	var app app
	var err error
	app.config, err = loadConfig()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	app.stats = make(allNodeStats)
	app.statsMu = new(sync.RWMutex)
	app.statPubSub = newStatsPubSub()
	app.initDB()
	defer app.Close()

	jobID := app.getJobIDAndUpdateEngine(args)
	app.job = app.getJob(jobID)
	app.saveJobToDB()
	app.collectInitialNodeStats()

	if *useHTTPServer {
		app.startServer()
		return
	}
	app.printJob()

	if !app.shouldCollectStats() {
		// Done.
		return
	}

	ticker := time.NewTicker(*sampleRate)
	stopReq := make(chan os.Signal, 1)
	signal.Notify(stopReq, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		select {
		case <-stopReq:
			fmt.Printf("\nJobperf exiting... Final overall average node stats:\n\n")
			app.printAvgNodeStats()
			return
		case <-ticker.C:
			err := app.updateNodeStats()
			if err != nil {
				log.Fatalf("failed to update node stats: %v", err)
			}
			fmt.Println()
			app.printCurrentNodeStats()
		}
	}
}
