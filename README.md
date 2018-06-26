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
   configuration file needs to be created and edited as all other configuration files are optional.
   In `accumulo-testing.properites`, review the properties with `test.common.*` prefix as these are
   used by all tests.

        cd conf/
        cp accumulo-testing.properties.example accumulo-testing.properties
        vim accumulo-testing.properties

3. Tests are run using the `accumulo-testing` command which is located in the `bin/`
   directory. Run this command without any arguments to view its usage and see available tests.

        ./bin/accumulo-testing

## Random walk test

The random walk test generates client behavior on an Apache Accumulo instance by randomly walking a
graph of client operations. 

Before running random walk, review the `test.common.*` properties in `accumulo-testing.properties`
file. A test module must also be specified. See the [modules] directory for a list of available ones.

The command below will start a single random walker in a local process using the [Image.xml][image]
module.

        ./bin/accumulo-testing rw-local Image.xml

If you would like to run multiple, distributed random walkers, run the command below to start random
walkers in 5 containers in YARN using the Image.xml module.

        ./bin/accumulo-testing rw-yarn 5 Image.xml

This command will create an application in YARN and exit when all containers for the test have
started. While its running, you can view logs for each random walker using the YARN resource manager.
The YARN application can be killed at any time using the YARN resource manager or command line tool.

## Continuous Ingest & Query

The Continuous Ingest test runs many ingest clients that continually create linked lists of data
in Accumulo. During ingest, query applications can be run to continously walk and verify the the
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

          ./bin/accumulo-testing ci-createtable

The continuous ingest tests have several applications that can either be started in a local process
or run in multiple containers across a cluster using YARN. The `ci-local` command starts a local
application which will run continuously until you stop using `ctrl-c`:

          ./bin/accumulo-testing ci-local <application>

The `ci-yarn` command starts an application in `<num>` containers in YARN. All containers will run
continuously performing the same work until you kill the application in YARN. The logs for the
application can be viewed using the YARN resource manager.

          ./bin/accumulo-testing ci-yarn <num> <application>

Below is a list of available continuous ingest applications. You should run the `ingest` application
first to add data to your table.

* `ingest` - Inserts data into Accumulo that will form a random graph.
* `walk` - Randomly walks the graph created by ingest application using scanner. Each walker
  produces detailed statistics on query/scan times.
* `batchwalk` - Randomly walks the graph created by ingest using a batch scanner.
* `scan` - Scans the graph

The continuous ingest test has two MapReduce jobs that are used to verify and stress
Accumulo and have the following command:

          ./bin/accumulo-testing ci-mapred <application>

Below is a list of available MapReduce applications:

1. `verify` - Runs a MapReduce job that verifies all data created by continuous ingest. Before
running, review all `test.ci.verify.*` properties. Do not run ingest while running this command as
it will cause erroneous reporting of UNDEFINED nodes. Each entry, except for the first batch of
entries, inserted by continuous ingest references a previously flushed entry. Since we are
referencing flushed entries, they should always exist. The MapReduce job checks that all referenced
entries exist. If it finds any that do not exist it will increment the UNDEFINED counter and emit
the referenced but undefined node.  The MapReduce job produces two other counts: REFERENCED and
UNREFERENCED. It is expected that these two counts are non zero. REFERENCED counts nodes that are
defined and referenced. UNREFERENCED counts nodes that defined and unreferenced, these are the
latest nodes inserted.

2. `moru` - Runs a MapReduce job that stresses Accumulo by reading and writing the continuous ingest
table. This MapReduce job will write out an entry for every entry in the table (except for ones
created by the MapReduce job itself). Stop ingest before running this MapReduce job. Do not run more
than one instance of this MapReduce job concurrently against a table.

## Agitator

The agitator will periodically kill the Accumulo master, tablet server, and Hadoop data node
processes on random nodes. Before running the agitator you should create `accumulo-testing-env.sh`
in `conf/` and review all of the agitator settings. The command below will start the agitator:

            ./bin/accumulo-testing agitator start

You can run this script as root and it will properly start processes as the user you configured in
`accumulo-testing-env.sh` (`AGTR_HDFS_USER` for the data node and `AGTR_ACCUMULO_USER` for Accumulo
processes). If you run it as yourself and the `AGTR_HDFS_USER` and `AGTR_ACCUMULO_USER` values are
the same as your user, the agitator will not change users. In the case where you run the agitator as
a non-privileged user which isn't the same as `AGTR_HDFS_USER` or `AGTR_ACCUMULO_USER`, the agitator
will attempt to `sudo` to these users, which relies on correct configuration of sudo. Also, be sure
that your `AGTR_HDFS_USER` has password-less `ssh` configured.

Run the command below stop the agitator:

            ./bin/accumulo-testing agitator stop

[modules]: core/src/main/resources/randomwalk/modules
[image]: core/src/main/resources/randomwalk/modules/Image.xml
[ti]: https://travis-ci.org/apache/accumulo-testing.svg?branch=master
[tl]: https://travis-ci.org/apache/accumulo-testing
