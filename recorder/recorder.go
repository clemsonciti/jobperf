package recorder

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/clemsonciti/jobperf"
	_ "github.com/mattn/go-sqlite3"
)

type Recorder struct {
	db *sql.DB
}

func New(filename string) (*Recorder, error) {
	var r Recorder
	var err error

	dirName := path.Dir(filename)
	err = os.MkdirAll(dirName, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory %v: %w", dirName, err)
	}

	r.db, err = sql.Open("sqlite3", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open db filename %v: %w", filename, err)
	}
	err = r.migrate()
	if err != nil {
		return nil, fmt.Errorf("failed to migrate db filename %v: %w", filename, err)
	}

	return &r, nil
}

func (r *Recorder) migrate() error {
	var err error
	_, err = r.db.Exec(`
	CREATE TABLE IF NOT EXISTS job (
		job_id TEXT PRIMARY KEY,
		job_name TEXT NOT NULL,
		owner TEXT NOT NULL,
		cores_total INTEGER NOT NULL,
		memory_total_bytes INTEGER NOT NULL,
		gpus_total INTEGER NOT NULL,
		walltime_ns INTEGER NOT NULL,
		state TEXT NOT NULL,
		start_time_unix INTEGER,
		used_walltime_ns INTEGER,
		used_cpu_time_ns INTEGER,
		used_memory_bytes INTEGER
	)`)
	if err != nil {
		return fmt.Errorf("failed to create job table: %w", err)
	}

	_, err = r.db.Exec(`
	CREATE TABLE IF NOT EXISTS job_node (
		job_id TEXT NOT NULL,
		hostname TEXT NOT NULL,
		num_cores INTEGER NOT NULL,
		memory_bytes INTEGER NOT NULL,
		num_gpus INTEGER NOT NULL,
		PRIMARY KEY (job_id, hostname)
	)`)
	if err != nil {
		return fmt.Errorf("failed to create job_node table: %w", err)
	}

	_, err = r.db.Exec(`
	CREATE TABLE IF NOT EXISTS job_node_stat (
		job_id TEXT NOT NULL,
		hostname TEXT NOT NULL,
		sample_time DATETIME NOT NULL,
		cpu_time_ns INTEGER NOT NULL,
		memory_used_bytes INTEGER NOT NULL,
		max_memory_used_bytes INTEGER NOT NULL,
		PRIMARY KEY (job_id, hostname, sample_time)
	)`)
	if err != nil {
		return fmt.Errorf("failed to create job_node_stat table: %w", err)
	}

	_, err = r.db.Exec(`
	CREATE TABLE IF NOT EXISTS job_gpu_stat (
		job_id TEXT NOT NULL,
		hostname TEXT NOT NULL,
		gpu_id TEXT NOT NULL,
		sample_time DATETIME NOT NULL,
		product_name TEXT NOT NULL,
		compute_usage INTEGER NOT NULL,
		mem_usage_bytes INTEGER NOT NULL,
		mem_total_bytes INTEGER NOT NULL,
		PRIMARY KEY (job_id, hostname, gpu_id, sample_time)
	)`)
	if err != nil {
		return fmt.Errorf("failed to create job_gpu_stat table: %w", err)
	}

	return nil
}

func (r *Recorder) recordNodes(job *jobperf.Job, tx *sql.Tx) error {
	if len(job.Nodes) == 0 {
		return nil
	}

	var params []any
	var placeholders []string
	for _, n := range job.Nodes {
		params = append(params, job.ID,
			n.Hostname, n.NCores,
			int64(n.Memory), n.NGPUs)
		placeholders = append(placeholders, "(?, ?, ?, ?, ?)")
	}
	var err error
	_, err = tx.Exec(`
	INSERT OR IGNORE INTO job_node (
		job_id,
		hostname,
		num_cores,
		memory_bytes,
		num_gpus
	) VALUES `+strings.Join(placeholders, ","), params...)
	if err != nil {
		return fmt.Errorf("failed to add job nodes: %v", err)
	}

	return nil

}

func (r *Recorder) RecordJob(job *jobperf.Job) error {

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to create transaction to record job: %v", err)
	}
	defer tx.Rollback() // nolint: errcheck

	_, err = tx.Exec(`
	INSERT INTO job(job_id, job_name, owner, 
		cores_total, memory_total_bytes,
		gpus_total, walltime_ns, state) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (job_id) 
	DO UPDATE SET state = excluded.state
	`, job.ID, job.Name, job.Owner, job.CoresTotal,
		int64(job.MemoryTotal), job.GPUsTotal,
		job.Walltime.Nanoseconds(), job.State)
	if err != nil {
		return err
	}

	err = r.recordNodes(job, tx)
	if err != nil {
		return err
	}

	if !job.StartTime.IsZero() {
		_, err = tx.Exec(`
			UPDATE job 
			SET 
				start_time_unix = ?,
				used_walltime_ns = ?,
				used_cpu_time_ns = ?,
				used_memory_bytes = ?
			WHERE job_id = ?
			`, job.StartTime.Unix(),
			job.UsedWalltime.Nanoseconds(),
			job.UsedCPUTime.Nanoseconds(),
			int64(job.UsedMemory),
			job.ID)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit new job: %v", err)
	}
	return nil
}

func (r *Recorder) GetJob(jobID string) (*jobperf.Job, error) {
	var job jobperf.Job
	var startTimeUnix int64
	tx, err := r.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction to fetch job: %v", err)
	}
	defer tx.Rollback() // nolint: errcheck

	err = tx.QueryRow(`
		SELECT 
			job_id, job_name, owner, 
			cores_total, memory_total_bytes,
			gpus_total, walltime_ns, state,
			start_time_unix, used_walltime_ns,
			used_cpu_time_ns, used_memory_bytes
		FROM job
		WHERE job_id = ?
		`, jobID).Scan(
		&job.ID, &job.Name, &job.Owner,
		&job.CoresTotal, &job.MemoryTotal,
		&job.GPUsTotal, &job.Walltime,
		&job.State, &startTimeUnix,
		&job.UsedWalltime, &job.UsedCPUTime,
		&job.UsedMemory)
	if err != nil {
		return nil, err
	}
	job.StartTime = time.Unix(startTimeUnix, 0)

	rows, err := tx.Query(`
		SELECT hostname, num_cores, memory_bytes, num_gpus
		FROM job_node
		WHERE job_id = ?
		`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var node jobperf.Node
		err = rows.Scan(&node.Hostname,
			&node.NCores, &node.Memory, &node.NGPUs)
		if err != nil {
			return nil, err
		}
		job.Nodes = append(job.Nodes, node)
	}

	return &job, nil
}

func (r *Recorder) RecordNodeStat(jobID string, stat jobperf.NodeStatsCPUMem) error {
	_, err := r.db.Exec(`
		INSERT INTO job_node_stat (
			job_id, hostname, sample_time, cpu_time_ns,
			memory_used_bytes, max_memory_used_bytes
		) VALUES (?, ?, ?, ?, ?, ?)
		`, jobID, stat.Hostname, stat.SampleTime,
		int64(stat.CPUTime), stat.MemoryUsedBytes,
		stat.MaxMemoryUsedBytes)
	return err
}

func (r *Recorder) GetNodeStats(jobID string) ([]jobperf.NodeStatsCPUMem, error) {
	var out []jobperf.NodeStatsCPUMem

	rows, err := r.db.Query(`
		SELECT 
			hostname, sample_time, cpu_time_ns,
			memory_used_bytes, max_memory_used_bytes
		FROM job_node_stat
		WHERE job_id = ?
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s jobperf.NodeStatsCPUMem
		err = rows.Scan(&s.Hostname, &s.SampleTime, &s.CPUTime,
			&s.MemoryUsedBytes, &s.MaxMemoryUsedBytes)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *Recorder) RecordGPUStats(jobID string, stats []jobperf.GPUStat) error {

	var params []any
	var placeholders []string
	for _, s := range stats {
		params = append(params, jobID, s.Hostname,
			s.ID, s.SampleTime, s.ProductName,
			s.ComputeUsage, s.MemUsageBytes, s.MemTotalBytes)
		placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, ?, ?)")
	}

	_, err := r.db.Exec(`
		INSERT INTO job_gpu_stat (
			job_id, hostname, gpu_id, sample_time, 
			product_name, compute_usage, 
			mem_usage_bytes, mem_total_bytes
		) VALUES `+strings.Join(placeholders, ","), params...)
	return err
}

func (r *Recorder) GetGPUStats(jobID string) ([]jobperf.GPUStat, error) {
	var out []jobperf.GPUStat

	rows, err := r.db.Query(`
		SELECT 
			hostname, gpu_id, sample_time, product_name,
			compute_usage, mem_usage_bytes, mem_total_bytes
		FROM job_gpu_stat
		WHERE job_id = ?
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s jobperf.GPUStat
		err = rows.Scan(&s.Hostname, &s.ID, &s.SampleTime,
			&s.ProductName, &s.ComputeUsage,
			&s.MemUsageBytes, &s.MemTotalBytes)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *Recorder) Close() error {
	return r.db.Close()
}
