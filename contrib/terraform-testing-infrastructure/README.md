# Accumulo Testing Infrastructure

## Description

This Git repository contains several [Terraform](https://www.terraform.io/) configurations.

  - `shared_state` creates an AWS S3 Bucket and DynamoDB table that are a prerequisite for the Terraform configuration in `aws`.
  - `aws` creates the following AWS resources:
    1. Creates an EFS filesystem in AWS (think NFS). On this filesystem we are going to install Maven, ZooKeeper, Hadoop, Accumulo, and Accumulo Testing
    2. Creates IP addresses for the EFS filesystem in each AWS availability zone that we use. The IP address allows us to mount the EFS filesystem to EC2 nodes.
    3. Creates one or more EC2 nodes for running the different components. Currently the configuration uses the m5.2xlarge instance type which provides 8 vCPUs, 32GB RAM, and an EBS backed root volume.
    4. Runs commands on the EC2 nodes after they are started (5 minutes according to the docs) to install software and configure them.
    5. Creates DNS A records for the EC2 nodes.

## Prerequisites

You will need to download and install the correct Terraform [CLI](https://www.terraform.io/downloads) for your platform. Put the `terraform` binary on your PATH.

## Shared State

The `shared_state` directory contains a Terraform configuration for creating an S3 bucket and DynamoDB table. These objects only need to be created once and are used for sharing the Terraform state with a team. To read more about this see [remote state](https://www.terraform.io/docs/language/state/remote.html). I used [these](https://blog.gruntwork.io/how-to-manage-terraform-state-28f5697e68fa) instructions for creating these objects. This configuration is simple, there are no variables, but you may need to change the aws region in `main.tf`. If you change the name of the S3 bucket or DynamoDB table, then you will need to update the `main.tf` file in the `aws` directory. To create the S3 bucket and DynamoDB table, run `terraform init`, followed by `terraform apply`.

## Test Cluster

The `aws` directory contains a Terrform configuration and related files for creating an Accumulo cluster on AWS infrastructure. This configuration consists of the following items in this directory:

  - main.tf - the Terraform configuration file
  - variables.tf - the declaration and default values for variables
  - templates/ - template files that are processed during the `apply` operation
  - files/ - non-template files that are uploaded to the cluster during the `apply` operation
  - conf/ - a non-git tracked directory that contains the rendered template files that are uploaded to the cluster

### Variables

The table below lists the variables used in this configuration and their default values.

| Variable Name | Default Value | Required |
|---------------|---------------|----------|
| instance_count | 2 | true |
| instance_type | m5.2xlarge | true |
| root_volume_gb | 300 | true |
| efs_mount | /efs | true |
| security_group | | true |
| us_east_1b_subnet | |  true |
| us_east_1e_subnet | |  true |
| route53_zone | |  true |
| ami_owner | |  true |
| ami_name_pattern | |  true |
| authorized_ssh_keys | | true |
| zookeeper_dir | /data/zookeeper | true |
| hadoop_dir | /data/hadoop | true |
| accumulo_dir | /data/accumulo | true |
| maven_version | 3.8.4 |  true |
| zookeeper_version | 3.5.9 | true |
| hadoop_version | 3.3.1 | true |
| accumulo_version | 2.1.0-SNAPSHOT | true |
| accumulo_repo | https://github.com/apache/accumulo.git | true |
| accumulo_branch_name | main | true |
| accumulo_testing_repo | https://github.com/apache/accumulo-testing.git | true |
| accumulo_testing_branch_name | main | true |
| local_sources_dir | | false |

### Configuration

You will need to provide values for the required variables with no default value. You can also override the default values by supplying new values. There are several [ways](https://www.terraform.io/language/values/variables#assigning-values-to-root-module-variables) to do this. Below is an example of a JSON file that contains values, which can be placed in the `aws` directory with a name that ends with `.auto.tfvars.json`, that will be applied during the `terraform apply` command. 

```
{
  "security_group": "sg-ABCDEF001",
  "route53_zone": "some.domain.com",
  "us_east_1b_subnet": "subnet-ABCDEF123",
  "us_east_1e_subnet": "subnet-ABCDEF124",
  "ami_owner": "000000000001",
  "ami_name_pattern": "MY_AMI_*",
  "authorized_ssh_keys": [
    "ssh-rsa dev_key_1",
    "ssh-rsa dev_key_2"
  ]
}
```

## AWS Resources

This Terraform configuration creates:

  1. An EFS filesystem with IP addresses in the `${us_east_1b_subnet}` and `${us_east_1e_subnet}` subnets
  2. `${instance_count}` EC2 nodes of `${instance_type}` with the latest AMI matching `${ami_name_pattern}` from the `${ami_owner}`. Each EC2 node will have a `${root_volume_gb}`GB root volume. The EFS filesystem is NFS mounted to each node at `${efs_mount}`.
  3. DNS entries in Route53 for each EC2 node.

## Software Layout

This Terraform configuration:

  1. Downloads, if necessary, the Apache Maven `${maven_version}` binary tarball to `${efs_mount}/sources`, then untars it to `${efs_mount}/apache-maven/apache-maven-${maven_version}`
  2. Downloads, if necessary, the Apache Zookeeper `${zookeer_version}` binary tarball to `${efs_mount}/sources`, then untars it to `${efs_mount}/zookeeper/apache-zookeeper-${zookeeper_version}-bin`
  3. Downloads, if necessary, the Apache Hadoop `${hadoop_version}` binary tarball to `${efs_mount}/sources`, then untars it to `${efs_mount}/hadoop/hadoop-${hadoop_version}`
  4. Clones, if necessary, the Apache Accumulo Git repo from `${accumulo_repo}` into `${efs_mount}/sources/accumulo-repo`. It switches to the `${accumulo_branch_name}` branch and builds the software using Maven, then untars the binary tarball to `${efs_mount}/accumulo/accumulo-${accumulo_version}`
  5. Downloads the [OpenTelemetry](https://opentelemetry.io/) Java Agent jar file and copies it to `${efs_mount}/accumulo/accumulo-${accumulo_version}/lib/opentelemetry-javaagent-1.7.1.jar`
  6. Copies the Accumulo `test` jar to `${efs_mount}/accumulo/accumulo-${accumulo_version}/lib` so that `org.apache.accumulo.test.metrics.TestStatsDRegistryFactory` is on the classpath
  7. Downloads the [Micrometer](https://micrometer.io/) StatsD Registry jar file and copies it to `${efs_mount}/accumulo/accumulo-${accumulo_version}/lib/micrometer-registry-statsd-1.7.4.jar`
  8. Clones, if necessary, the Apache Accumulo Testing Git repo from `${accumulo_testing_repo}` into `${efs_mount}/sources/accumulo-testing-repo`. It switches to the `${accumulo_testing_branch_name}` branch and builds the software using Maven.

### Supplying your own software

If you want to supply your own Apache Maven, Apache ZooKeeper, Apache Hadoop, Apache Accumulo, or Apache Accumulo Testing binary tar files, then you can put them into a directory on your local machine and set the `${local_sources_dir}` variable to the full path to the directory. These files will be uploaded to `${efs_mount}/sources` and the installation script will use them instead of downloading them. If the version of the supplied binary tarball is different than the default version, then you will also need to override that property. Supplying your own binary tarballs does speed up the deployment. However, if you provide the Apache Accumulo binary tarball, then it will be harder to update the software on the cluster.

**NOTE**: If you supply your own binary tarball of Accumulo, then you will need to copy the `accumulo-test-${accumulo_version}.jar` file to the `lib` directory manually as it's not part of the binary tarball.

### Updating Apache Accumulo on the cluster

If you did not provide a binary tarball, then you can update the software running on the cluster by doing the following and then restarting Accumulo:

```
cd ${efs_mount}/sources/accumulo-repo
git pull
mvn -s ${efs_mount}/apache-maven/settings.xml clean package -DskipTests -DskipITs
tar zxf assemble/target/accumulo-${accumulo_version}-bin.tar.gz -C ${efs_mount}/accumulo
```

### Updating Apache Accumulo Testing on the cluster

If you did not provide a binary tarball, then you can update the software running on the cluster by doing the following:

```
cd ${efs_mount}/sources/accumulo-testing-repo
git pull
mvn -s ${efs_mount}/apache-maven/settings.xml clean package -DskipTests -DskipITs
```

## Deployment Overiew

The first node that is created is called the `manager`, the others are `worker` nodes. The following components will run on the `manager` node:

- Apache ZooKeeper
- Apache Hadoop NameNode
- Apache Accumulo Manager
- Apache Accumulo Monitor
- Apache Accumulo GarbageCollector
- Apache Accumulo CompactionCoordinator
- Docker
- Jaeger Tracing Docker Container
- Telegraf/InfluxDB/Grafana Docker Container

The following components will run on the `worker` nodes:

- Apache Hadoop DataNode
- Apache Accumulo TabletServer
- Apache Accumulo Compactor(s)

### Logs

The logs for each service (zookeeper, hadoop, accumulo) are located in their respective local directory on each node (`/data/${service}/logs` unless you changed the properties).

### DNS entries

The Terraform configuration creates DNS entries of the following form:

  <node_name>-<branch_name>-<workspace_name>.${route53_zone}

For example:

- manager-main-default.${route53_zone}
- worker#-main-default.${route53_zone} (where # is 0, 1, 2, ...)

## Instructions

  1. Once you have created a `.auto.tfvars.json` file, or set the properties some other way, run `terraform init`.
  2. Run `terraform apply` to create the AWS resources.
  3. Run `terraform destroy` to tear down the AWS resources.

**NOTE**: If you get an Access Denied error then try setting the AWS Short Term access keys in your environment

## Post Cluster Creation Instructions

If you are creating a new cluster, then you will need to do the following things:

```
manager node:

  docker run -d --name jaeger \
         --restart always \
         -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
         -p 5775:5775/udp -p 6831:6831/udp \
         -p 6832:6832/udp -p 5778:5778 \
         -p 16686:16686 -p 14268:14268 \
         -p 14250:14250 -p 9411:9411 \
         jaegertracing/all-in-one:1.29

  docker run --ulimit nofile=66000:66000 -d \
         --restart always \
         --name tig-stack \
         -p 3003:3003 \
         -p 3004:8888 \
         -p 8086:8086 \
         -p 22022:22 \
         -p 8125:8125/udp \
         -v /data/metrics/influxdb:/var/lib/influxdb \
         -v /data/metrics/grafana:/var/lib/grafana \
         -v /efs/telegraf/conf:/etc/telegraf \
         -v /efs/grafana/dashboards:/etc/grafana/provisioning/dashboards \
         artlov/docker-telegraf-influxdb-grafana:latest

  accumulo init
  accumulo-cluster start

  Update the $ACCUMULO_HOME/conf/accumulo-client.properties file with instance name, user, and password.
```

### Accessing Web Pages

- Hadoop NameNode: http://manager-main-default.${route53_zone}:9870
- Hadoop DataNode: http://worker#-main-default.${route53_zone}:9864
- Accumulo Monitor: http://manager-main-default.${route53_zone}:9995
- Jaeger Tracing UI: http://manager-main-default.${route53_zone}:16686
- Grafana: http://manager-main-default.${route53_zone}:3003

## Accessing the EC2 nodes

The `templates/cloud-init.tpl` template file is a [cloud-init](https://cloudinit.readthedocs.io/en/latest/) configuration that is used as part of the EC2 instance creation process to create groups, users, etc. In the `hadoop` user section you will see an `ssh_authorized_keys` area. Place your ssh public key in this section and then you will be able to log into the created EC2 nodes as the hadoop user without having to supply a password. You can then ssh to the nodes using either their DNS address (e.g. `ssh hadoop@manager-main-default.${route53_zone}`) or via their IP addresses (e.g. `ssh hadoop@10.0.0.1`).

### SSH'ing to other nodes

The `/etc/hosts` file on each node has been updated with the names (manager, worker0, worker1, etc..) and IP addresses of the EC2 nodes. `pdsh` has been installed and `/etc/genders` has been configured. You should be able to `ssh` to any node as the `hadoop` user without a password. Likewise you should be able to `pdsh` commands to groups of nodes as the hadoop user.

## Shutdown / Startup Instructions

Once the cluster is created you can simply stop or start the EC2 instances from the AWS console. Terraform is just for creating, updating, or destroying the AWS resources. ZooKeeper and Hadoop are setup to use SystemD service files, but Accumulo is not. You could log into the manager node and run `accumulo-cluster stop` before stopping the EC2 nodes. Or, you could just shut them down and force Accumulo to recover (which might be good for testing). When restarting the EC2 nodes from the AWS Console, ZooKeeper and Hadoop should start on their own. For Accumulo, you should only need to run `accumulo-cluster start` on the manager node.
