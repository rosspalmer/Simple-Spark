# Simple-Spark

Open source toolset for managing Apache Spark resources and job orchestration.

- CLI tool for managing Spark clusters and related resources 
- Python package for integrating setup into job code
- All resources managed with single configuration file
- Local development environment mirroring production environment
- Required libraries all stored in single location

**NOTE: Still working on initial release**

## Features

**Modes**

- `local`: Single node cluster setup on local machine
- `standalone`: Multi node cluster using Spark's [standalone setup](https://spark.apache.org/docs/latest/spark-standalone.html)
- `yarn`: Multi node cluster built using [Hadoop YARN setup](https://spark.apache.org/docs/latest/running-on-yarn.html) _(future release)_
- `kubernetes`: Multi node cluster built [Kubernetes setup](https://spark.apache.org/docs/latest/running-on-kubernetes.html) _(future release)_

**Services**

- Cluster manager: Start/stop clusters, Spark UI for cluster
- JDBC access server: Integrated HIVE Thriftserver for JDBC calls to Spark warehouse 
- Metastore: Central SQL server managing HIVE metastore _(future release)_
- Job orchestrator: Job scheduler via cron _(future release)_
- History server: SparkUI for past runs _(future release)_

# Quick Start Guide

Once installed, the `simplespark` command can be used to
create and switch between different Spark environments. 
Each environment contains a single cluster and a collection
of resources to run on the cluster.

## I. Install `simplespark`

The `simplespark` library is written in Python and can be
installed in two different ways:

### A: Python Library

For machines with Python already installed, use `pip` to install
both the Python module and the CLI tool.

```bash
pip install simplespark
```

### B: Source Code (Poetry Install)

TODO

## II. Create Configuration

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

## II. Build Environment

## IV. Activate Environment

Activating a specific environment sets the `JAVA/SCALA/SPARK_HOME` variables
for a shell session to point to that environment, as well as any other shell
updates required for specific build.

This is done by calling an "activation" script generated during build:

`source <environment-name>.spark`

The environment will only be activated for the session in which this command
called so that it is possible to interact with multiple environments at once.

The following native Spark commands will automatically connect
to the activated environment:

- `spark-shell`
- `spark-submit`
- `pyspark`
- `spark-sql`

## V. Start/Stop Environment

An environment can be started and stopped which will spin up 
or down the associated cluster and any additional resources 
defined in the configuration.

```bash
# Not required if already in desired environment
source <environment-name>.spark

simplespark start
simplespark stop
```

# Configuration

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
