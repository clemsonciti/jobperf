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
