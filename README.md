# Simple-Spark

Open source data platform for managing Apache Spark resources and job orchestration.

## Project Goals

1. Provide a single CLI tool to perform all functions:
    - Orchestrate Spark clusters
    - Manage metastore / data warehouse
    - Run jobs and keep history
    - Setup notebook server with access to data warehouse
2. Compile all required configurations in single JSON/YAML file
3. Improve Spark job development:
    - Use same toolset to run jobs locally as on a cluster
    - Keep all resources in central directory for transparency and customization
    - Provide `SimpleSpark` object for easy access to easy access to all resources in codebase
    - Connect to remote resources through single API

## Features

**Modes**

| Mode       | Description | Complete |
|------------|-------------|----------|
| local      |             | YES      |
| standalone |             | YES      |
| yarn       |             | NO       |


## Quick Start Guide

Once installed, the `simplespark` command can be used to
create and switch between different Spark environments. 
Each environment contains a single cluster and a collection
of resources to run on the cluster.

The following are general steps for using an environment:

1. Define environment using JSON/YAML file
   - Generate template for specific `mode` 
2. Import configuration into `SIMPLESPARK_HOME` directory
   - Create environment activation script
   - Download and prepare required libraries
3. Activate environment to point specific resources
4. Start environment to spin up cluster and services
5. Run jobs (TODO)


### Install

The CLI tool is hosted on PyPi and can be installed using `pip`

```bash
pip install simplespark
```

### Define Configuration

The configuration for a

#### MODE: local

The `local` mode will create a standalone "cluster" on a single
machine which can be used for local development. The "cluster" 
remains active between executions mirroring the environment
for a cluster on `standalone` mode.

### Import Configuration

TODO

### Activate Environment

TODO

### Start/Stop Clusters

TODO



