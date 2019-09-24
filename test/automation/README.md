# Automated Cluster Testing

Testing a snapshot version of Accumulo on a cluster using this respository requires many repetive steps.  Luckily, there is script that automates this using [Muchos].  This script is found in [test/automation/automateEC2.sh](automateEC2.sh).  

Before running the script, edit [cluster_props.sh](cluster_props.sh). All repositories are set to the master branch of the corresponding Apache project by default. You can change these values to your specific forks and branches as desired.

A path to `muchos.props` is required in order to run the script. You can find the required configurations and an example of `muchos.props` [in the official Fluo-Muchos readme][Muchos].

### Execution takes no arguments as followed: 
    ./automateEC2.sh

### Things to consider:
* The script will read from the `MUCHOS_PROPS` environment variable if defined. 
* Accumulo versions between `muchos.props` and `pom.xml` file of a given Accumulo branch should be consistent.
* Accumulo and Fluo-Muchos will be installed locally in a temporary directory under `/tmp`, which is typically cleared by the operating system periodically (system-specific, possibly on reboots, daily, weekly, etc.).

[Muchos]: https://github.com/apache/fluo-muchos
