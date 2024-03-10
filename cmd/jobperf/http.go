package main

import (
	"bytes"
	"cmp"
	"context"
	"embed"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/clemsonciti/jobperf"
	"nhooyr.io/websocket"
)

//go:generate ./fetchVendor.sh

//go:embed vendor
var vendorFiles embed.FS

type closeCh chan struct{}
type statPubSub struct {
	sync.RWMutex
	cpuMemListeners  map[chan<- jobperf.NodeStatsCPUMem]closeCh
	gpuStatListeners map[chan<- jobperf.GPUStat]closeCh
}

func newStatsPubSub() *statPubSub {
	return &statPubSub{
		cpuMemListeners:  make(map[chan<- jobperf.NodeStatsCPUMem]closeCh),
		gpuStatListeners: make(map[chan<- jobperf.GPUStat]closeCh),
	}
}

func (sps *statPubSub) AddGPUListener(ch chan<- jobperf.GPUStat) {
	sps.Lock()
	defer sps.Unlock()
	sps.gpuStatListeners[ch] = make(closeCh)
}

func (sps *statPubSub) RemoveGPUListener(ch chan<- jobperf.GPUStat) {
	sps.Lock()
	defer sps.Unlock()

	quit, ok := sps.gpuStatListeners[ch]
	if !ok {
		// Already unsubscribed?
		return
	}
	close(quit)
	delete(sps.gpuStatListeners, ch)
	close(ch)
}

func (sps *statPubSub) AddCPUMemListener(ch chan<- jobperf.NodeStatsCPUMem) {
	sps.Lock()
	defer sps.Unlock()
	sps.cpuMemListeners[ch] = make(closeCh)

}
func (sps *statPubSub) RemoveCPUMemListener(ch chan<- jobperf.NodeStatsCPUMem) {
	sps.Lock()
	defer sps.Unlock()

	quit, ok := sps.cpuMemListeners[ch]
	if !ok {
		// Already unsubscribed?
		return
	}
	close(quit)
	delete(sps.cpuMemListeners, ch)
	close(ch)
}

func (sps *statPubSub) PublishGPUStat(s jobperf.GPUStat) {
	sps.RLock()
	defer sps.RUnlock()
	slog.Debug("will send gpu stat to listeners", "nChs", len(sps.gpuStatListeners))
	for ch, quit := range sps.gpuStatListeners {
		go func(ch chan<- jobperf.GPUStat, quit closeCh) {
			// Wait for either the message to be recieved
			// or the channel to have been unsubscribed
			// (through closing quit).
			select {
			case <-quit:
			case ch <- s:
			}
		}(ch, quit)
	}
}
func (sps *statPubSub) PublishCPUMem(s jobperf.NodeStatsCPUMem) {
	sps.RLock()
	defer sps.RUnlock()
	slog.Debug("will send cpumem stat to listeners", "nChs", len(sps.gpuStatListeners))
	for ch, quit := range sps.cpuMemListeners {
		go func(ch chan<- jobperf.NodeStatsCPUMem, quit closeCh) {
			// Wait for either the message to be recieved
			// or the channel to have been unsubscribed
			// (through closing quit).
			select {
			case <-quit:
			case ch <- s:
			}
		}(ch, quit)
	}
}

func (a *app) sendGPUEvent(ctx context.Context, c *websocket.Conn, e jobperf.GPUStat) error {
	msg := new(bytes.Buffer)
	err := jobViewHTML.ExecuteTemplate(msg, "gpu-current-row", e)
	if err != nil {
		return err
	}
	err = c.Write(ctx, websocket.MessageText, msg.Bytes())
	if err != nil {
		return err
	}

	name := fmt.Sprintf("%v - %v", e.Hostname, e.ID)

	msg = new(bytes.Buffer)
	err = jobViewHTML.ExecuteTemplate(msg, "update-gpu-chart", struct {
		Name string
		X    time.Time
		Y    float64
	}{
		Name: name,
		X:    e.SampleTime,
		Y:    float64(e.ComputeUsage),
	})
	if err != nil {
		return err
	}
	err = c.Write(ctx, websocket.MessageText, msg.Bytes())
	if err != nil {
		return err
	}
	msg = new(bytes.Buffer)
	err = jobViewHTML.ExecuteTemplate(msg, "update-gpu-mem-chart", struct {
		Name string
		X    time.Time
		Y    float64
	}{
		Name: name,
		X:    e.SampleTime,
		Y:    float64(e.MemUsageBytes),
	})
	if err != nil {
		return err
	}
	err = c.Write(ctx, websocket.MessageText, msg.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (a *app) sendCPUMemEvent(ctx context.Context, c *websocket.Conn, host string) error {
	var err error
	a.statsMu.RLock()
	defer a.statsMu.RUnlock()

	var n *jobperf.Node
	for i := range a.job.Nodes {
		if a.job.Nodes[i].Hostname == host {
			n = &a.job.Nodes[i]
			break
		}
	}
	if n == nil {
		return fmt.Errorf("failed to find node %v", host)
	}

	stats := a.stats[host]
	if len(stats.cpuMem) < 2 {
		return nil
	}

	s1 := stats.cpuMem[len(stats.cpuMem)-1]
	s2 := stats.cpuMem[len(stats.cpuMem)-2]
	nodeCoresUsed := float64(s1.CPUTime-s2.CPUTime) / float64(s1.SampleTime.Sub(s2.SampleTime))
	msg := new(bytes.Buffer)
	err = jobViewHTML.ExecuteTemplate(msg, "node-current-row", struct {
		Hostname             string
		CoresUsed            float64
		CoresUsedPercent     float64
		CoresTotal           int
		MemoryUsed           jobperf.Bytes
		MemoryUsedPercent    float64
		MaxMemoryUsed        jobperf.Bytes
		MaxMemoryUsedPercent float64
		MemoryTotal          jobperf.Bytes
	}{
		Hostname:             host,
		CoresUsed:            nodeCoresUsed,
		CoresUsedPercent:     nodeCoresUsed / float64(n.NCores) * 100.0,
		CoresTotal:           n.NCores,
		MemoryUsed:           s1.MemoryUsedBytes,
		MemoryUsedPercent:    float64(s1.MemoryUsedBytes) / float64(n.Memory) * 100,
		MaxMemoryUsed:        s1.MaxMemoryUsedBytes,
		MaxMemoryUsedPercent: float64(s1.MaxMemoryUsedBytes) / float64(n.Memory) * 100,
		MemoryTotal:          n.Memory,
	})
	if err != nil {
		return err
	}
	err = c.Write(ctx, websocket.MessageText, msg.Bytes())
	if err != nil {
		return err
	}

	msg = new(bytes.Buffer)
	err = jobViewHTML.ExecuteTemplate(msg, "update-cpu-chart", struct {
		Hostname string
		X        time.Time
		Y        float64
	}{
		Hostname: host,
		X:        s1.SampleTime,
		Y:        nodeCoresUsed,
	})
	if err != nil {
		return err
	}
	err = c.Write(ctx, websocket.MessageText, msg.Bytes())
	if err != nil {
		return err
	}
	msg = new(bytes.Buffer)
	err = jobViewHTML.ExecuteTemplate(msg, "update-mem-chart", struct {
		Hostname string
		X        time.Time
		Y        float64
	}{
		Hostname: host,
		X:        s1.SampleTime,
		Y:        float64(s1.MemoryUsedBytes),
	})
	if err != nil {
		return err
	}
	err = c.Write(ctx, websocket.MessageText, msg.Bytes())
	if err != nil {
		return err
	}

	nodeCoresUsed = float64(s1.CPUTime) / float64(s1.SampleTime.Sub(a.job.StartTime))
	msg = new(bytes.Buffer)
	err = jobViewHTML.ExecuteTemplate(msg, "node-avg-row", struct {
		Hostname             string
		CoresUsed            float64
		CoresUsedPercent     float64
		CoresTotal           int
		MemoryUsed           jobperf.Bytes
		MemoryUsedPercent    float64
		MaxMemoryUsed        jobperf.Bytes
		MaxMemoryUsedPercent float64
		MemoryTotal          jobperf.Bytes
	}{
		Hostname:             host,
		CoresUsed:            nodeCoresUsed,
		CoresUsedPercent:     nodeCoresUsed / float64(n.NCores) * 100.0,
		CoresTotal:           n.NCores,
		MemoryUsed:           s1.MemoryUsedBytes,
		MemoryUsedPercent:    float64(s1.MemoryUsedBytes) / float64(n.Memory) * 100,
		MaxMemoryUsed:        s1.MaxMemoryUsedBytes,
		MaxMemoryUsedPercent: float64(s1.MaxMemoryUsedBytes) / float64(n.Memory) * 100,
		MemoryTotal:          n.Memory,
	})
	if err != nil {
		return err
	}
	err = c.Write(ctx, websocket.MessageText, msg.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (a *app) jobWSHandler(w http.ResponseWriter, req *http.Request) {
	// When Open OnDemand proxies the connection, it will rewrite the host header so ot appears cross-origin.
	var originPatterns []string
	if a.config.UseOpenOnDemand {
		u, err := url.Parse(a.config.OpenOnDemandURL)
		if err != nil {
			slog.Warn("could not parse Open OnDemandURL", "OpenOnDemandURL", a.config.OpenOnDemandURL, "err", err)
		} else {
			originPatterns = append(originPatterns, u.Host)
		}
	}
	c, err := websocket.Accept(w, req, &websocket.AcceptOptions{
		OriginPatterns: originPatterns,
	})
	if err != nil {
		slog.Error("failed to accept ws connection", "err", err)
		return
	}
	defer c.CloseNow() // nolint: errcheck

	ctx, cancel := context.WithTimeout(req.Context(), time.Minute*10)
	defer cancel()

	ctx = c.CloseRead(ctx)

	cpuMemCh := make(chan jobperf.NodeStatsCPUMem)
	gpuCh := make(chan jobperf.GPUStat)
	a.AddCPUMemListener(cpuMemCh)
	defer a.RemoveCPUMemListener(cpuMemCh)
	a.AddGPUListener(gpuCh)
	defer a.RemoveGPUListener(gpuCh)

	for {
		select {
		case <-ctx.Done():
			c.Close(websocket.StatusNormalClosure, "")
			return
		case e := <-cpuMemCh:
			err = a.sendCPUMemEvent(ctx, c, e.Hostname)
			if err != nil {
				slog.Error("failed to send cpu event to ws", "err", err)
				return
			}
		case e := <-gpuCh:
			err = a.sendGPUEvent(ctx, c, e)
			if err != nil {
				slog.Error("failed to send gpu event to ws", "err", err)
				return
			}
		}
	}

}

func (a *app) jobIndexHandler(w http.ResponseWriter, req *http.Request) {
	req.Header.Set("Cache-Control", "no-store")
	//a.job = a.getJob(a.job.ID)
	var nodeHostsList []string
	for _, n := range a.job.Nodes {
		nodeHostsList = append(nodeHostsList, n.Hostname)
	}
	nodeHosts := "Unknown"
	if len(nodeHostsList) > 0 {
		nodeHosts = strings.Join(nodeHostsList, ", ")
	}
	usedCores := float64(a.job.UsedCPUTime) / float64(a.job.UsedWalltime)

	type nodeUsage struct {
		Hostname             string
		CoresUsed            float64
		CoresUsedPercent     float64
		CoresTotal           int
		MemoryUsed           jobperf.Bytes
		MemoryUsedPercent    float64
		MaxMemoryUsed        jobperf.Bytes
		MaxMemoryUsedPercent float64
		MemoryTotal          jobperf.Bytes
	}

	a.statsMu.RLock()
	defer a.statsMu.RUnlock()

	var avgNodeUsage []nodeUsage
	for _, n := range a.job.Nodes {
		stats := a.stats[n.Hostname]
		if stats == nil || len(stats.cpuMem) < 1 {
			continue
		}
		s := stats.cpuMem[len(stats.cpuMem)-1]
		nodeCoresUsed := float64(s.CPUTime) / float64(s.SampleTime.Sub(a.job.StartTime))
		avgNodeUsage = append(avgNodeUsage, nodeUsage{
			Hostname:             n.Hostname,
			CoresUsed:            nodeCoresUsed,
			CoresUsedPercent:     nodeCoresUsed / float64(n.NCores) * 100.0,
			CoresTotal:           n.NCores,
			MemoryUsed:           s.MemoryUsedBytes,
			MemoryUsedPercent:    float64(s.MemoryUsedBytes) / float64(n.Memory) * 100,
			MaxMemoryUsed:        s.MaxMemoryUsedBytes,
			MaxMemoryUsedPercent: float64(s.MaxMemoryUsedBytes) / float64(n.Memory) * 100,
			MemoryTotal:          n.Memory,
		})
	}

	var currentNodeUsage []nodeUsage
	for _, n := range a.job.Nodes {
		stats := a.stats[n.Hostname]
		if stats == nil || len(stats.cpuMem) < 2 {
			continue
		}
		s1 := stats.cpuMem[len(stats.cpuMem)-1]
		s2 := stats.cpuMem[len(stats.cpuMem)-2]
		nodeCoresUsed := float64(s1.CPUTime-s2.CPUTime) / float64(s1.SampleTime.Sub(s2.SampleTime))
		currentNodeUsage = append(currentNodeUsage, nodeUsage{
			Hostname:             n.Hostname,
			CoresUsed:            nodeCoresUsed,
			CoresUsedPercent:     nodeCoresUsed / float64(n.NCores) * 100.0,
			CoresTotal:           n.NCores,
			MemoryUsed:           s1.MemoryUsedBytes,
			MemoryUsedPercent:    float64(s1.MemoryUsedBytes) / float64(n.Memory) * 100,
			MaxMemoryUsed:        s1.MaxMemoryUsedBytes,
			MaxMemoryUsedPercent: float64(s1.MaxMemoryUsedBytes) / float64(n.Memory) * 100,
			MemoryTotal:          n.Memory,
		})
	}
	var currentGPUStats []jobperf.GPUStat
	for _, n := range a.job.Nodes {
		stats := a.stats[n.Hostname]
		if stats == nil || len(stats.gpuStats) < 1 {
			continue
		}
		for _, gStats := range stats.gpuStats {
			if len(gStats) < 1 {
				continue
			}
			s := gStats[len(gStats)-1]
			currentGPUStats = append(currentGPUStats, s)
		}
	}
	slices.SortFunc(avgNodeUsage, func(a, b nodeUsage) int {
		return cmp.Compare(a.Hostname, b.Hostname)
	})
	slices.SortFunc(currentNodeUsage, func(a, b nodeUsage) int {
		return cmp.Compare(a.Hostname, b.Hostname)
	})
	slices.SortFunc(currentGPUStats, func(a, b jobperf.GPUStat) int {
		hostCmp := cmp.Compare(a.Hostname, b.Hostname)
		if hostCmp != 0 {
			return hostCmp
		}
		return cmp.Compare(a.ID, b.ID)
	})

	type timeSeriesPt struct {
		X time.Time
		Y float64
	}
	pastCPUUsagePerNode := make(map[string][]timeSeriesPt)
	pastMemUsagePerNode := make(map[string][]timeSeriesPt)
	pastGPUUsagePerNodeGPU := make(map[string][]timeSeriesPt)
	pastGPUMemUsagePerNodeGPU := make(map[string][]timeSeriesPt)
	for _, n := range a.job.Nodes {
		stats := a.stats[n.Hostname]
		if stats == nil {
			continue
		}
		for i := range stats.cpuMem {
			s1 := stats.cpuMem[i]
			pastMemUsagePerNode[n.Hostname] = append(pastMemUsagePerNode[n.Hostname], timeSeriesPt{
				X: s1.SampleTime,
				Y: float64(s1.MemoryUsedBytes),
			})

			if i == 0 {
				continue
			}
			s2 := stats.cpuMem[i-1]
			usage := float64(s1.CPUTime-s2.CPUTime) / float64(s1.SampleTime.Sub(s2.SampleTime))
			pastCPUUsagePerNode[n.Hostname] = append(pastCPUUsagePerNode[n.Hostname], timeSeriesPt{
				X: s1.SampleTime,
				Y: usage,
			})
		}
		for id, gpuStats := range stats.gpuStats {
			name := fmt.Sprintf("%v - %v", n.Hostname, id)
			for _, s := range gpuStats {
				pastGPUUsagePerNodeGPU[name] = append(pastGPUUsagePerNodeGPU[name], timeSeriesPt{
					X: s.SampleTime,
					Y: float64(s.ComputeUsage),
				})
				pastGPUMemUsagePerNodeGPU[name] = append(pastGPUMemUsagePerNodeGPU[name], timeSeriesPt{
					X: s.SampleTime,
					Y: float64(s.MemUsageBytes),
				})
			}
		}
	}

	err := jobViewHTML.Execute(w, struct {
		Job                       *jobperf.Job
		NodeHosts                 string
		UsedCores                 float64
		UsedWalltimePercent       string
		UsedCoresPercent          string
		UsedMemoryPercent         string
		AvgNodeUsage              []nodeUsage
		CurrentNodeUsage          []nodeUsage
		CurrentGPUUsage           []jobperf.GPUStat
		PastCPUUsagePerNode       map[string][]timeSeriesPt
		PastMemUsagePerNode       map[string][]timeSeriesPt
		PastGPUUsagePerNodeGPU    map[string][]timeSeriesPt
		PastGPUMemUsagePerNodeGPU map[string][]timeSeriesPt
		ShouldCollectStats        bool
		DocsURL                   string
		SupportURL                string
	}{
		Job:       a.job,
		NodeHosts: nodeHosts,
		UsedCores: usedCores,
		UsedWalltimePercent: fmt.Sprintf("%.1f%%",
			float64(a.job.UsedWalltime)/float64(a.job.Walltime)*100),
		UsedCoresPercent: fmt.Sprintf("%.1f%%",
			float64(usedCores)/float64(a.job.CoresTotal)*100),
		UsedMemoryPercent: fmt.Sprintf("%.1f%%",
			float64(a.job.UsedMemory)/float64(a.job.MemoryTotal)*100),
		AvgNodeUsage:              avgNodeUsage,
		CurrentNodeUsage:          currentNodeUsage,
		CurrentGPUUsage:           currentGPUStats,
		PastCPUUsagePerNode:       pastCPUUsagePerNode,
		PastMemUsagePerNode:       pastMemUsagePerNode,
		PastGPUUsagePerNodeGPU:    pastGPUUsagePerNodeGPU,
		PastGPUMemUsagePerNodeGPU: pastGPUMemUsagePerNodeGPU,
		ShouldCollectStats:        a.shouldCollectStats(),
		DocsURL:                   a.config.DocsURL,
		SupportURL:                a.config.SupportURL,
	})
	if err != nil {
		slog.Error("failed to render job index template", "err", err)
		http.Error(w, "failed to render job template", http.StatusInternalServerError)
		return
	}
}

func getHostname() (string, error) {
	cmd := exec.Command("/bin/hostname", "-f")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func (a *app) startServer() {
	hostname, err := getHostname()
	if err != nil {
		slog.Error("failed to get hostname", "err", err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", *httpServerPort))
	if err != nil {
		slog.Error("failed to open http server", "err", err)
		os.Exit(1)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	http.HandleFunc("/", a.jobIndexHandler)
	http.HandleFunc("/ws", a.jobWSHandler)
	http.Handle("/vendor/", http.FileServer(http.FS(vendorFiles)))

	go func() {
		if a.config.UseOpenOnDemand {
			fmt.Printf("\nStarted server on port %v. View in Open OnDemand: \n%v/rnode/%v/%v/ \n\n", port, a.config.OpenOnDemandURL, hostname, port)
		} else {
			fmt.Printf("\nStarted server on port %v. Go to: \nhttp://%v:%v/ \n\n", port, hostname, port)
		}
		err = http.Serve(ln, a.logAndAuth(http.DefaultServeMux))

		slog.Debug("server exit", "err", err)
	}()

	var updateStatsCh <-chan time.Time
	if a.shouldCollectStats() {
		ticker := time.NewTicker(*sampleRate)
		defer ticker.Stop()
		updateStatsCh = ticker.C
	}
	stopReq := make(chan os.Signal, 1)
	signal.Notify(stopReq, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		select {
		case <-stopReq:
			return
		case <-updateStatsCh:
			err := a.updateNodeStats()
			if err != nil {
				slog.Error("failed to update node stats", "err", err)
			}
		}

	}

}

func (a *app) shouldCollectStats() bool {
	return a.job.IsRunning() && *watch && !*load
}

func (a *app) logAndAuth(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		realIP := r.Header.Get("X-Real-Ip")
		username := r.Header.Get("X-Forwarded-User")
		slog.Debug("request", "method", r.Method, "url", r.URL, "remoteAddr", r.RemoteAddr, "realIP", realIP)
		if !*httpDisableAuth && username != a.job.Owner {
			slog.Error("rejected request", "username", username, "jobOwner", a.job.Owner)
			http.Error(w, "This is not your job.", http.StatusForbidden)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

var slugRegex = regexp.MustCompile("[:.]")

//go:embed job_view.tmpl.html
var jobViewHTMLString string
var jobViewHTML = template.Must(
	template.New("job-index").Funcs(template.FuncMap{
		"slugify": func(s string) string {
			return slugRegex.ReplaceAllString(s, "-")
		},
	}).Parse(jobViewHTMLString))
