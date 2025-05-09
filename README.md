# Simple-Spark

Open source data platform for managing Apache Spark resources and job orchestration.

**NOTE: Still working on initial release**

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

- `local`: Single node cluster setup on local machine
- `standalone`: Multi node cluster using Spark's [standalone setup](https://spark.apache.org/docs/latest/spark-standalone.html)
- `yarn`: Multi node cluster built using [Hadoop YARN setup](https://spark.apache.org/docs/latest/running-on-yarn.html) _(in development)_
- `kubernetes`: Multi node cluster built [Kubernetes setup](https://spark.apache.org/docs/latest/running-on-kubernetes.html) (In development)

**Services**

- Cluster manager: Start/stop clusters, Spark UI for cluster
- Metastore: Central SQL server managing HIVE metastore _(in development)_
- Job orchestrator: Job scheduler via cron _(in development)_
- History server: SparkUI for past runs _(in development)_

# Quick Start Guide

Once installed, the `simplespark` command can be used to
create and switch between different Spark environments. 
Each environment contains a single cluster and a collection
of resources to run on the cluster.

## Install `simplespark`

The `simplespark` library is written in Python and can be
installed in two different ways:

### A: Python Library

For machines with Python already installed, use `pip` to install
both the Python module and the CLI tool.

```bash
pip install simplespark
```

### B: Binary Executable

For machines without Python (workers for example), you can 
install the CLI as a binary executable.

```bash
wget TODO
echo "export PATH=$PATH:/<bin-folder>" >> ~/.bashrc
```

## Create Configuration

The configuration can be expressed in a single JSON file or
be defined in multiple files which are merged on import.

Each `mode` will require different configuration proprieties
to be defined and within each mode there are optional settings
for specific add ins.

### Templates

The easiest way to start is to create a template for the 
specific mode by running the command below:

```bash
simplespark template <mode> <file-path>
```

### Required Properties

For all configurations, the following proprieties must be defined.

- `name`: Identifier of simplespark "environment"
- `simplespark_home`: Full path to directory used by simplespark to:
  - Store environment configurations
  - Store any required libraries
  - Store any scripts or custom modifications
- `bash_profile_file`: Full path to bash profile file used to set `SIMPLESPARK_HOME` environment variable
- `packages`: TODO
- `driver`: TODO

## Import Configuration

TODO

## Activate Environment

Activating a specific environment sets the `JAVA/SCALA/SPARK_HOME` variables
for a shell session to point to that environment, as well as any other 
required setup.

This is done by calling an "activation" script generated in the import step:

`source <environment-name>.sh`

The environment will only be activated for the session in which this command
called so that it is possible to interact with multiple environments at once.

The following native Spark commands will automatically connect
to the activated environment:

- `spark-shell`
- `spark-submit`
- `pyspark`
- `spark-sql`

## Start/Stop Clusters

A cluster can be started / stopped by running the command below.
Once started, a cluster will stay up until stopped, even if a 
user switches to another environment.

```bash
simplespark start <environment-name>
simplespark stop <environment-name>
```
