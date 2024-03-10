# Jobperf

A tool to check resource usage of jobs on HPC clusters. Jobperf was originally
developed by Clemson University for the use on their HPC cluster, Palmetto.

For more information on usage, see the Clemson's [jobperf
documentation](https://docs.rcd.clemson.edu/palmetto/jobs_slurm/monitoring/#using-jobperf-with-slurm).

## Install

Pre-built binaries are available as GitHub releases for Linux/amd64.

### Requirements

- Job scheduler:
  - Slurm: tested with 23.11.3.
  - PBS: tested on PBS used on Palmetto at Clemson University.
- For GPUs, `nvidia-smi` should be installed and available.

### Configuartion

## Build

To build this tool, you need a recent version of go (at least version 1.21).
For complete install instructions see the [official website](https://go.dev/doc/install).

Then build like many other go tools, including running `go generate` to fetch
the JS dependencies:

```
go generate ./...
go build ./cmd/jobperf
```

The built binary will be available as `jobperf`.

### Default Configuartion
