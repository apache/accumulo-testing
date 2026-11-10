<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Apache Accumulo Testing Suite
[![Build Status][ti]][tl]

The Apache Accumulo testing suite contains applications that test and verify the
correctness of Accumulo.

## Installation

In order to run the Apache Accumulo testing suite, you will need Java 8 and Maven installed
on your machine as well as an Accumulo instance to use for testing.

1. First clone this repository.

```sh
git clone git@github.com:apache/accumulo-testing.git
cd accumulo-testing
```

2. All configuration files for the test suite are in `conf/`. Only the `accumulo-testing.properties`
   configuration file needs to be edited as all other configuration files are optional.
   In `accumulo-testing.properites`, review the properties with `test.common.*` prefix as these are
   used by all tests.

```sh
cd conf/
vim accumulo-testing.properties
```

### Run tests locally

Tests are run using the following scripts in `bin/`:

  * `cingest` - Runs continuous ingest tests
  * `rwalk` - Runs random walk tests
  * `performance` - Runs performance test
  * `agitator` - Runs agitator
  * `gcs` - Runs garbage collection simulation
  * `monitor` - Runs availability monitor probe
  * `manager-stress/managerstress.sh` - Stresses Manager and Compaction Coordinator operations
  * `manager-stress/stresscompactor.sh` - Starts in-JVM Compactor simulators in a separate JVM
  * `manager-stress/configure-mgr-stress-test.sh` - Prompts for settings and prints both commands

Run the scripts without arguments to view usage.

### Run tests in Docker

While test scripts can be run from a single machine, they will put more stress if they are run from
multiple machines. The easiest way to do this is using Docker. However, only the tests below can be
run in Docker:

  * `cingest` - All applications can be run except `verify` & `moru` which launch a MapReduce job.
  * `rwalk` - All modules can be run.
  * `monitor` - All modules can be run.

1. To create the `accumulo-testing` docker image, make sure the following files exist in your clone:

    * `conf/accumulo-client.properties` - Configure this file from your Accumulo install
    * `conf/accumulo-testing.properties` - Configure this file for testing
    * `target/accumulo-testing-2.1.0-SNAPSHOT-shaded.jar` - Can be created using `./bin/build`

   Run the following command to create the image. `HADOOP_HOME` should be where Hadoop is installed on your cluster.
   `HADOOP_USER_NAME` should match the user running Hadoop on your cluster.

   ```bash
   docker build --build-arg HADOOP_HOME=$HADOOP_HOME --build-arg HADOOP_USER_NAME=`whoami` -t accumulo-testing .
   ```

2. The `accumulo-testing` image can run a single command:

   ```bash
   docker run --network="host" accumulo-testing cingest createtable
   ```

3. Multiple containers can also be run (if you have [Docker Swarm] enabled):

   ```bash
   # the following can be used to get the image on all nodes if you do not have a registry.
   for HOST in node1 node2 node3; do
     docker save accumulo-testing | ssh -C $HOST docker load &
   done

   docker service create --network="host" --replicas 2 --name ci accumulo-testing cingest ingest
   ```

## Random walk test

The random walk test generates client behavior on an Apache Accumulo instance by randomly walking a
graph of client operations.

Before running random walk, review the `test.common.*` properties in `accumulo-testing.properties`
file. A test module must also be specified. See the [modules] directory for a list of available ones.

The command below will start a single random walker in a local process using the [Image.xml][image]
module.

```bash
./bin/rwalk Image.xml
```

## Continuous Ingest & Query

The Continuous Ingest test runs many ingest clients that continually create linked lists of data
in Accumulo. During ingest, query applications can be run to continuously walk and verify the
linked lists and put a query load on Accumulo. At some point, the ingest clients are stopped and
a MapReduce job is run to ensure that there are no holes in any linked list.

The nodes in the linked list are random. This causes each linked list to spread across the table.
Therefore, if one part of a table loses data, then it will be detected by references in another
part of table.

Before running any of the Continuous Ingest applications, make sure that the
`accumulo-testing.properties` file exists in `conf/` and review all properties with the
`test.ci.*` prefix.

First, run the command below to create an Accumulo table for the continuous ingest tests. The name of the
table is set by the property `test.ci.common.accumulo.table` (its value defaults to `ci`) in the file
`accumulo-testing.properties`:

```bash
./bin/cingest createtable {-o test.<prop>=<value>}
```

The continuous ingest tests have several applications that start a local application which will run
continuously until you stop using `ctrl-c`:

```bash
./bin/cingest <application> {-o test.<prop>=<value>}
```

Below is a list of available continuous ingest applications. You should run the `ingest` application
first to add data to your table.

* `ingest` - Inserts data into Accumulo that will form a random graph.
* `walk` - Randomly walks the graph created by ingest application using scanner. Each walker
  produces detailed statistics on query/scan times.
* `batchwalk` - Randomly walks the graph created by ingest using a batch scanner.
* `scan` - Scans the graph
* `verify` - Runs a MapReduce job that verifies all data created by continuous ingest. Before
running, review all `test.ci.verify.*` properties. Do not run ingest while running this command as
it will cause erroneous reporting of UNDEFINED nodes. Each entry, except for the first batch of
entries, inserted by continuous ingest references a previously flushed entry. Since we are
referencing flushed entries, they should always exist. The MapReduce job checks that all referenced
entries exist. If it finds any that do not exist it will increment the UNDEFINED counter and emit
the referenced but undefined node. The MapReduce job produces two other counts: REFERENCED and
UNREFERENCED. It is expected that these two counts are non-zero. REFERENCED counts nodes that are
defined and referenced. UNREFERENCED counts nodes that defined and unreferenced, these are the
latest nodes inserted.
* `bulk` - Runs a MapReduce job that generates data for bulk import. See [bulk-test.md](docs/bulk-test.md).
* `moru` - Runs a MapReduce job that stresses Accumulo by reading and writing the continuous ingest
table. This MapReduce job will write out an entry for every entry in the table (except for ones
created by the MapReduce job itself). Stop ingest before running this MapReduce job. Do not run more
than one instance of this MapReduce job concurrently against a table.

Check out [ingest-test.md](docs/ingest-test.md) for pointers on running a long-running ingest and
verification test.

## Garbage Collection Simulator

See [gcs.md](docs/gcs.md).

## Manager and Compaction Coordinator stress test

The `managerstress` command starts local client JVMs that issue weighted create-table, delete-table,
split, merge, tablet-availability, compaction, and bulk-import tasks against an existing Accumulo
instance. It creates the requested number of uniquely named tables before starting workers and
replenishes the table pool after deletes. A selected create task creates a new table generation
before deleting the old generation so the pool returns to its configured size. Weights apply to
selected workload tasks; the follow-up table creations after deletes are unweighted pool maintenance.

All created table generations start with `TabletAvailability.UNHOSTED` and 10–999 evenly spaced
initial splits, leaving at most 1,000 tablets. Split tasks add a random number of unique split rows
without exceeding that tablet limit. Merge tasks sample a target from 1–100, clamp it to a valid
2–100 tablet range, and use the table's actual split boundaries for the merge. Split rows and the
bulk-import seed key use the same fixed-width hexadecimal row-key scheme.

Availability tasks sample a random number of unique tablets from one through the table's full
tablet count. Each selected tablet is assigned a random availability state different from its
current state. Nonadjacent selections remain separate; adjacent selections with the same target
state are grouped into one row-range request.

Bulk import requires a shared HDFS directory accessible to all local workers. Each worker creates
one one-entry RFile there at startup and stages a copy of it for every bulk import. Compaction tasks
submit non-blocking major compactions. The coordinator removes the run-specific HDFS staging
directory during shutdown, including any files left by workers that did not exit cleanly.

`managerstress` temporarily configures the `mgrstress` compaction service in the system
configuration. It uses `org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner`, with
`compaction.service.mgrstress.planner.opts.groups` set to the group specified by
the required `--compactor-resource-group` option. The stress namespace is created with
`table.compaction.dispatcher.opts.service=mgrstress`. Previous ZooKeeper system-property overrides
are restored when the test finishes; pre-existing values do not prevent startup.

The required `--namespace` must name a namespace that does not already exist. `managerstress` creates
it before creating tables, places all stress tables in it, and removes its tables and namespace when
the run finishes. The configured Accumulo user needs permission to create and drop namespaces and
tables, alter tables and namespaces, modify system configuration, compact, and bulk import. It also
needs access to the configured HDFS directory.

For example, the command below runs four client processes for 30 minutes, maintaining eight tables.
Operation weights are relative; setting a weight to zero disables that operation.

```bash
./bin/manager-stress/managerstress.sh \
  --namespace mgrstress_example \
  --compactor-resource-group default \
  --duration 30m \
  --clients 4 \
  --tables 8 \
  --hdfs-dir hdfs://namenode:8020/tmp/accumulo-managerstress \
  --create-weight 1 \
  --delete-weight 1 \
  --split-weight 2 \
  --merge-weight 2 \
  --availability-weight 1 \
  --compact-weight 4 \
  --bulk-import-weight 1
```

Use `--table-prefix` to choose a prefix for the generated tables; if omitted, a unique prefix is
created for the run. Choose a namespace name that is unused on the Accumulo instance; the test fails
if it already exists. The test removes all tables in its namespace and deletes the namespace after
workers stop. Per-worker logs include operation attempts, completions, skips, races, errors, and
average latency.

When workers stop, `managerstress` logs totals aggregated from each worker's atomic metrics snapshot.
For each operation, `submitted` counts workload operations that reached their Manager API call,
`completed` counts successful synchronous operations, and `asyncAccepted` counts compaction and
tablet-availability requests accepted by Accumulo. Availability and compaction requests do not wait
for tablets to reach their target state. `failed` counts operation errors; `outcomeUnknown` is the
subset with a transport failure or interruption after submission. Snapshot files are refreshed
periodically in the run's temporary control directory and removed after the summary is logged.

### Compactor simulator

`stresscompactor` is designed to be used with the `managerstress` test framework. Run it alongside
`managerstress` to provide Compactor simulators for external compaction requests generated by the
stress tables. Each invocation starts the configured number of independently registered Compactor
simulators as threads in that JVM. Start additional JVMs by invoking the command again (or on other
hosts); set `JAVA_OPTS` separately for each process to tune its heap and garbage collector.

Use the same group for `managerstress --compactor-resource-group` and
`stresscompactor --resource-group`. Run `./bin/manager-stress/configure-mgr-stress-test.sh` to
interactively enter settings for both processes and print the two commands. Press Enter to accept
the displayed defaults. The client config prompt defaults to `../../conf/accumulo-client.properties`
relative to the helper script; credentials should be stored in that file. The helper prints commands
to stdout without starting the processes.

The Manager must be able to reach the advertised host and callback ports. Configure the resource
group to match the Compactor resource group used by the external compaction service for the stress
tables. The configured Accumulo user must be permitted to perform system actions. When using SASL,
the advertised host should be the host's canonical name.

```bash
JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseG1GC" ./bin/manager-stress/stresscompactor.sh \
  --compactors-per-jvm 16 \
  --duration 30m \
  --resource-group default \
  --host compactor-client01.example.net \
  --port-range 9600-9699 \
  --success-weight 1 \
  --failure-weight 1 \
  --cancellation-weight 1
```

Each simulator applies `--resource-group` to its own `compactor.group` configuration property,
registers its Compactor address, and serves the Manager's status, wake, and cancel RPCs. On receiving
a job it immediately reports a result according to the configured relative weights. A successful
result reports zero output entries, which commits as a zero-output compaction and removes the
input-file references from the stress table. Simulators unregister and close their connections when
the JVM's duration expires or it is stopped.

Each simulator logs its job outcomes on shutdown, followed by a per-JVM aggregate. These counts are
external compaction jobs, which may be multiple for one table-level compaction request; they are
reported separately from `managerstress`'s accepted asynchronous request count.

## Agitator

The agitator will periodically kill the Accumulo manager, tablet server, and Hadoop data node
processes on random nodes. Before running the agitator you should create `accumulo-testing-env.sh`
in `conf/` and review all the agitator settings. The command below will start the agitator:

```bash
./bin/agitator start
```

Running this script as root will properly start processes as the user you configured in
`env.sh` (`AGTR_HDFS_USER` for the data node and `AGTR_ACCUMULO_USER` for Accumulo
processes). If you run it as yourself and the `AGTR_HDFS_USER` and `AGTR_ACCUMULO_USER` values are
the same as your user, the agitator will not change users. In the case where you run the agitator as
a non-privileged user which isn't the same as `AGTR_HDFS_USER` or `AGTR_ACCUMULO_USER`, the agitator
will attempt to `sudo` to these users, which relies on correct configuration of sudo. Also, be sure
that your `AGTR_HDFS_USER` has password-less `ssh` configured.

Run the command below stop the agitator:

```bash
./bin/agitator stop
```

## Performance Test

To run performance test a `cluster-control.sh` script is needed to assist with starting, stopping,
wiping, and configuring an Accumulo instance. This script should define the following functions.

```bash
function get_hadoop_client {
  # TODO return hadoop client libs in a form suitable for appending to a classpath
}

function get_version {
  case $1 in
    ACCUMULO)
      # TODO echo accumulo version
      ;;
    HADOOP)
      # TODO echo hadoop version
      ;;
    ZOOKEEPER)
      # TODO echo zookeeper version
      ;;
    *)
      return 1
  esac
}

function start_cluster {
  # TODO start Hadoop and Zookeeper if needed
}

function setup_accumulo {
  # TODO kill any running Accumulo instance
  # TODO setup a fresh install of Accumulo w/o starting it
}

function get_config_file {
  local file_to_get=$1
  local dest_dir=$2
  # TODO copy $file_to_get from Accumulo conf dir to $dest_dir
}

function put_config_file {
  local config_file=$1
  # TODO copy $config_file to Accumulo conf dir
}

function put_server_code {
  local jar_file=$1
  # TODO add $jar_file to Accumulo's server side classpath. Could put it in $ACCUMULO_HOME/lib/ext
}

function start_accumulo {
  # TODO start accumulo
}

function stop_cluster {
  # TODO kill Accumulo, Hadoop, and Zookeeper
}
```

An example script for [Uno] is provided. To use this, do the following and set
`UNO_HOME` after copying.

```bash
cp conf/cluster-control.sh.uno conf/cluster-control.sh
```

After the cluster control script is set up, the following will run performance
test and produce JSON result files in the provided output directory.

```bash
./bin/performance run <output dir>
```

The example above will run all performance tests in order. To run a single test, a filter can be
applied. The example below will run just the DurabilityWriteSpeedPT.

```bash
./bin/performance run <output dir> DurabilityWriteSpeedPT
```

Some performance tests alter the system properties of the cluster it is trying to test on. These
may require fine-tuning in order to work on some hardware.

There are some utilities for working with the JSON result files, run the `performance` script
with no options to see them.

## Availability Monitor
Monitor class aims at verifying availability of overall accumulo cluster by continually doing
scans of random values across various tablet servers and capturing timing
information related to how long such scans take.

## Automated Cluster Testing
See the [readme.md](/test/automation/README.md).

[Uno]: https://github.com/apache/fluo-uno
[modules]: src/main/resources/randomwalk/modules
[image]: src/main/resources/randomwalk/modules/Image.xml
[Docker Swarm]: https://docs.docker.com/engine/swarm/swarm-tutorial/
[ti]: https://github.com/apache/accumulo-testing/workflows/QA/badge.svg
[tl]: https://github.com/apache/accumulo-testing/actions
