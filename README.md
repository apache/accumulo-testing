# Apache Accumulo Testing Suite

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

Before running random walk, review the `test.common.*` and `test.randomwalk.*` properties in
`accumulo-testing.properties` file. A test module must also be specified. See the [modules][modules]
directory for a list of available ones.

The command below will start a single random walker in a local process using the [Image.xml][image]
module.

        ./bin/accumulo-testing rw-local Image.xml

If you would like to run multiple, distributed random walkers, run the command below to start random
walkers in 5 containers in YARN using the Image.xml module.

        ./bin/accumulo-testing rw-yarn 5 Image.xml

This command will create an application in YARN and exit when all containers for the test have started.
While its running, you can view logs for each random walker using the YARN resource manager. The YARN
application can be killed at any time using the YARN resource manager or command line tool.

[modules]: core/src/main/resources/randomwalk/modules
[image]: core/src/main/resources/randomwalk/modules/Image.xml
