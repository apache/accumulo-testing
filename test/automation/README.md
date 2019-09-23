# Automate EC2 Cluster

The automateEC2 script is an executable that allows you to set up Accumulo on an AWS cluster for testing purposes. The cluster_props file is where all of the properties are defined for the script.

All repositories are set to the master branch of the apache projects by default. You can change these values to your specific forks and branches as desired.

A path to muchos.props is required in order to run the script. You can find the required configurations and an example of muchos.props [in the official Fluo-Muchos readme](https://github.com/apache/fluo-muchos).

### Execution takes no arguments as followed: 
	./automateEC2.sh

### Things to consider:
* The script will read from the MUCHOS_PROPS environment variable if defined. 

* Accumulo versions between muchos.props and pom file of a given Accumulo branch should be consistent.

* Accumulo and Fluo-Muchos will be installed locally in a temporary directory under /tmp which is typically removed every reboot.
