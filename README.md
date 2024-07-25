# Jobperf

A tool to check resource usage of jobs on HPC clusters. Jobperf was originally
developed by Clemson University for the use on their HPC cluster, Palmetto.

For more information on usage, see the Clemson's [jobperf
documentation](https://docs.rcd.clemson.edu/palmetto/jobs_slurm/monitoring/#using-jobperf-with-slurm).

The design of this system was documented in ["A Simple Resource Usage Monitor for Users of PBS and Slurm"](https://dl.acm.org/doi/10.1145/3626203.3670608), presented at PEARC24.

## Install

Pre-built binaries are available as GitHub releases for Linux/amd64.

### Requirements

- For GPUs, `nvidia-smi` should be installed and available.
- Jobperf has been run on both PBS and Slurm. However, scheduler deployments
  vary wildly and it is not expected that it will work on all clusters. Here
  are results of testing jobperf on a variety of cluster.

| Cluster           | Scheduler | Version  | `JobAcctGatherType`     | CLI works? | Web works? |
| ----------------- | --------- | -------- | ----------------------- | ---------- | ---------- |
| Palmetto 1        | OpenPBS   | 20.0.0   | N/A                     | Yes        | Yes        |
| Palmetto 2        | Slurm     | 23.11.3  | `jobacct_gather/cgroup` | Yes        | Yes        |
| Stampede 3 (TACC) | Slurm     | 23.11.1  | `jobacct_gather/linux`  | No         | No         |
| Anvil (Purdue)    | Slurm     | 23.11.1  | `jobacct_gather/linux`  | Yes        | Yes        |
| Delta (NCSA)      | Slurm     | 23.02.7  | `jobacct_gather/cgroup` | Yes        | No         |
| Bridges-2 (PSC)   | Slurm     | 22.05.11 | `jobacct_gather/cgroup` | No         | No         |
| Expanse (SDSC)    | Slurm     | 23.02.7  | `jobacct_gather/linux`  | Yes        | Yes        |

- Jobperf failed on Stampede 3 due to not having expected cgroups and `scontrol
listpids` not working as expected.
- Jobperf failed on Bridges-2 due to not `squeue` failing with the `--json` flag
  while filtering by job ID. This could be due to an older version of Slurm.

Jobperf may break on future versions of Slurm as it relies on consistent output
from the JSON formatted output of `squeue` and `sacct`. Usually it is not to
hard to fix Jobperf once the new version's output is known.

### Configuration

TODO

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

### Build Configuration

When building the binary, you can embed the version and some default
configuration parameters with `-ldflags`:

| Parameter                | Meaning                                                                       |
| ------------------------ | ----------------------------------------------------------------------------- |
| `buildVersion`           | The build version.                                                            |
| `buildCommit`            | The build commit.                                                             |
| `buildDate`              | The build date.                                                               |
| `defaultSupportURL`      | The URL used for the support link.                                            |
| `defaultDocsURL`         | The URL used for the documentation link.                                      |
| `defaultUseOpenOnDemand` | If Open OnDemand should be used as a reverse proxy when HTTP mode is enabled. |
| `defaultOpenOnDemandURL` | The URL of the Open OnDemand instance to use as reverse proxy.                |

For example, to set the documentation URL to https://example.com when building,
run:

```
go build -ldflags='-X main.defaultDocsURL=https://example.com' ./cmd/jobperf
```

This repo also has a goreleaser configuration file (`.goreleaser.yaml`) which sets these appropriately for the releases.
