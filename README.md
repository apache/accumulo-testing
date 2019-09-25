# Apache Accumulo Testing Suite
[![Build Status][ti]][tl]

The Apache Accumulo testing suite contains applications that test and verify the
correctness of Accumulo.

## Installation

In order to run the Apache Accumulo testing suite, you will need Java 8 and Maven installed
on your machine as well as an Accumulo instance to use for testing.

1. First clone this repository.

        git clone git@github.com:apache/accumulo-testing.git
        cd accumulo-testing

2. All configuation files for the test suite are in `conf/`. Only the `accumulo-testing.properties`
   configuration file needs to be edited as all other configuration files are optional.
   In `accumulo-testing.properites`, review the properties with `test.common.*` prefix as these are
   used by all tests.

        cd conf/
        vim accumulo-testing.properties


### Run tests locally

Tests are run using the following scripts in `bin/`:

  * `cingest` - Runs continous ingest tests
  * `rwalk` - Runs random walk tests
  * `performance` - Runs performance test
  * `agitator` - Runs agitator
  * `gcs` - Runs garbage collection simultation

Run the scripts without arguments to view usage.

### Run tests in Docker

While test scripts can be run from a single machine, they will put more stress if they are run from
multiple machines. The easiest way to do this is using Docker. However, only the tests below can be
run in Docker:

  * `cingest` - All applications can be run except `verify` & `moru` which launch a MapReduce job.
  * `rwalk` - All modules can be run.

1. To create the `accumulo-testing` docker image, make sure the following files exist in your clone:

    * `conf/accumulo-client.properties` - Configure this file from your Accumulo install
    * `conf/accumulo-testing.properties` - Configure this file for testing
    * `target/accumulo-testing-2.0.0-SNAPSHOT-shaded.jar` - Can be created using `./bin/build`

   Run the following command to create the image. `HADOOP_HOME` should be where Hadoop is installed on your cluster.
   `HADOOP_USER_NAME` should match the user running Hadoop on your cluster.

   ```
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

        ./bin/rwalk Image.xml

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

          ./bin/cingest createtable {-o test.<prop>=<value>}

The continuous ingest tests have several applications that start a local application which will run
continuously until you stop using `ctrl-c`:

          ./bin/cingest <application> {-o test.<prop>=<value>}

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
the referenced but undefined node.  The MapReduce job produces two other counts: REFERENCED and
UNREFERENCED. It is expected that these two counts are non zero. REFERENCED counts nodes that are
defined and referenced. UNREFERENCED counts nodes that defined and unreferenced, these are the
latest nodes inserted.
* `bulk` - Runs a MapReduce job that generates data for bulk import.  See [bulk-test.md](docs/bulk-test.md).
* `moru` - Runs a MapReduce job that stresses Accumulo by reading and writing the continuous ingest
table. This MapReduce job will write out an entry for every entry in the table (except for ones
created by the MapReduce job itself). Stop ingest before running this MapReduce job. Do not run more
than one instance of this MapReduce job concurrently against a table.

Checkout [ingest-test.md](docs/ingest-test.md) for pointers on running a long
running ingest and verification test.

## Garbage Collection Simulator

See [gcs.md](docs/gcs.md).

## Agitator

The agitator will periodically kill the Accumulo master, tablet server, and Hadoop data node
processes on random nodes. Before running the agitator you should create `accumulo-testing-env.sh`
in `conf/` and review all of the agitator settings. The command below will start the agitator:

            ./bin/agitator start

You can run this script as root and it will properly start processes as the user you configured in
`env.sh` (`AGTR_HDFS_USER` for the data node and `AGTR_ACCUMULO_USER` for Accumulo
processes). If you run it as yourself and the `AGTR_HDFS_USER` and `AGTR_ACCUMULO_USER` values are
the same as your user, the agitator will not change users. In the case where you run the agitator as
a non-privileged user which isn't the same as `AGTR_HDFS_USER` or `AGTR_ACCUMULO_USER`, the agitator
will attempt to `sudo` to these users, which relies on correct configuration of sudo. Also, be sure
that your `AGTR_HDFS_USER` has password-less `ssh` configured.

Run the command below stop the agitator:

            ./bin/agitator stop

## Performance Test

To run performance test a `cluster-control.sh` script is needed to assist with starting, stopping,
wiping, and confguring an Accumulo instance.  This script should define the following functions.

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
  # TODO add $jar_file to Accumulo's server side classpath.  Could put it in $ACCUMULO_HOME/lib/ext
}

function start_accumulo {
  # TODO start accumulo
}

function stop_cluster {
  # TODO kill Accumulo, Hadoop, and Zookeeper
}
```

An example script for [Uno] is provided.  To use this do the following and set
`UNO_HOME` after copying.

    cp conf/cluster-control.sh.uno conf/cluster-control.sh

After the cluster control script is setup, the following will run performance
test and produce json result files.

    ./bin/performance run <output dir>

There are some utilities for working with the json result files, run the `performance` script
with no options to see them.

## Automated Cluster Testing
See the [readme.md](/test/automation/README.md).

[Uno]: https://github.com/apache/fluo-uno
[modules]: core/src/main/resources/randomwalk/modules
[image]: core/src/main/resources/randomwalk/modules/Image.xml
[Docker Swarm]: https://docs.docker.com/engine/swarm/swarm-tutorial/
[ti]: https://travis-ci.org/apache/accumulo-testing.svg?branch=master
[tl]: https://travis-ci.org/apache/accumulo-testing
