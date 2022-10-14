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

# Accumulo Testing Infrastructure

## Description

This Git repository contains several [Terraform](https://www.terraform.io/) configurations.

  - `shared_state` creates Terraform state storage in either Azure or AWS, which is a prerequisite
    for the Terraform configurations
    in `aws` or `azure`.
    - `shared_state/aws` creates an AWS S3 Bucket and DynamoDB table that are a prerequisite for
      the Terraform configuration in `aws`.
    - `shared_state/azure` creates an Azure resource group and storage account that are a
      prerequisite for the Terraform configuration in `azure`.
  - `aws` creates the following AWS resources:
    1. Creates one or more EC2 nodes for running the different components. Currently, the
       configuration uses the m5.2xlarge instance type which provides 8 vCPUs, 32GB RAM, and an EBS
       backed root volume.
    2. Runs commands on the EC2 nodes after they are started (5 minutes according to the docs) to
       install software and configure them.
    3. Creates DNS A records for the EC2 nodes.
  - `azure` creates the following Azure resources:
    1. Creates a resource group to hold all of the created resources.
    2. Creates networking resources (vnet, subnet, network security group).
    3. Creates two or more Azure VMs (along with associated NICs and public IP addresses) for
       running the different components. The default configuration creates
       [D8s v4](https://docs.microsoft.com/en-us/azure/virtual-machines/dv4-dsv4-series#dsv4-series)
       VMs, providing 8 vCPUs and 32GiB RAM with an Azure storage backed OS drive.
    4. Runs commands on the VMs after cloud-init provisioning is complete in order to install and
       configure Hadoop, Zookeeper, Accumulo, and the Accumulo Testing repository. 

## Prerequisites

You will need to download and install the correct Terraform [CLI](https://www.terraform.io/downloads)
for your platform. Put the `terraform` binary on your PATH. You can optionally install
[Terraform Docs](https://terraform-docs.io/user-guide/installation/) if you want to be able
to generate documentation or an example variables file for either the shared state or
`aws` or `azure` configurations.

## Shared State

The `shared_state` directory contains Terraform configurations for creating either an AWS S3 Bucket
or DynamoDB table, or an Azure resource group, storage account, and container. These objects only
need to be created once and are used for sharing the Terraform state with a team. To read more
about this see [remote state](https://www.terraform.io/docs/language/state/remote.html). The AWS
shared state instructions are based on
[this article](https://blog.gruntwork.io/how-to-manage-terraform-state-28f5697e68fa). 

To generate the storage, run `terraform init` followed by `terraform apply`. Note that the shell
working directory must be the `shared_state/aws` or `shared_state/azure` directory when you run
the terraform commands for shared state creation.

The default AWS configuration generates the S3 bucket name when `terraform apply` is run. This
ensures that a globally unique S3 bucket name is used. It is not required to set any variables for
the shared state. However, if you wish to override any variable values, this can be done by
creating an `aws.auto.tfvars` file in the `shared_state/aws` directory. For example:
```bash
cd shared_state/aws
cat > aws.auto.tfvars << EOF
bucket_force_destroy = true
EOF
```

Assuming the bucket variable is not overridden, the generated S3 bucket name will appear in the
`terraform` apply output, like the following example:
```
Outputs:

bucket_name = "terraform-20220209131315353700000001"
```
This value should be supplied to `terraform init` in the [aws](./aws) directory as described below.
Using the example above, the init command for the aws directory would be:
```bash
terraform init -backend-config=bucket=terraform-20220209131315353700000001
```

If you change any of the backend storage configuration parameters over their defaults, you will
need to override them when you initialize terraform for the `aws` or `azure` configuration
below. For example, if you change the region where the S3 bucket is deployed from `us-east-1` to
`us-west-2`, then you would need to run `terraform init` in the `aws` directory (not the
shared_state initialization, but the main `aws` directory initialization) with:
```bash
terraform init -backend-config=region=us-west-2
```

The following backend configuration can be overridden from with `-backend-config=<name>=<value>`
options to `terraform init`. This prevents the need to modify the `backend` sections in
[aws/main.tf](./aws/main.tf) or [azure/main.tf](./azure/main.tf).

For AWS:
* `-backend-config=bucket=<bucket_name>`: Override the S3 bucket name
* `-backend-config=key=<key_name>`: Override the key in the S3 bucket
* `-backend-config=region=<region>`: Override AWS region
* `-backend-config=dynamodb_table=<dynamodb_table_name>`: Override the DynamoDB table name

For Azure:
* `-backend-config=resource_group_name=<resource_group_name>`: Override the resource group where the storage account is located
* `-backend-config=storage_account_name=<storage_account_name>`: Override the name of the Azure storage account holding Terraform state
* `-backend-config=container_name=<container_name>`: Override the name of the container within the storage account that is holding Terraform state
* `-backend-config=key=<blob_name>`: Override the name of the blob within the container that will be used to hold Terraform state


## Test Cluster

The `aws` and `azure` directories contain Terraform configurations for creating an Accumulo cluster
on AWS or Azure respectively. The `aws` and `azure` directories contain the following Terraform
configuration items:
  - main.tf - The Terraform configuration file
  - variables.tf - The declaration and default values for Terraform variables
These configurations both use shared Terraform module and configuration files that can be found in
the following directories/files:
  - modules/ - This contains several shared Terraform modules that are used by the `aws` and `azure`
    Terraform configurations
    - `cloud-init-config` - contains templates to generate a
      [Cloud Init](https://cloudinit.readthedocs.org/) configuration to configure AWS instances or
      Azure VMs with necessary Linux packages, user accounts, etc.
    - `config-files` - contains template configuration files for various components of the cluster
      (e.g., HDFS, Accumulo, Grafana, etc.) as well as helper scripts to install the software
      components that cannot be installed via cloud-init.
    - `upload-software` - if pre-built binaries for downloaded software components (Hadoop, Accumulo,
      Zookeeper, Maven) are included, this module uploads them to the cluster
    - `configure-nodes` - this module is responsible for executing scripts on the cluster to install
      and configure software, initialize the HDFS filesystem and Accumulo cluster, and start them.
  - conf/ - a non-git tracked directory that contains rendered template files with variables replaced
    by selected runtime configuration. These files are uploaded to the cluster.

### AWS Variables

The table below lists the variables and their default values that are used in the `aws` configuration.

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| accumulo\_branch\_name | The name of the branch to build and install | `string` | `"main"` | no |
| accumulo\_dir | The Accumulo directory on each EC2 node | `string` | `"/data/accumulo"` | no |
| accumulo\_instance\_name | The accumulo instance name. | `string` | `"accumulo-testing"` | no |
| accumulo\_repo | URL of the Accumulo git repo | `string` | `"https://github.com/apache/accumulo.git"` | no |
| accumulo\_root\_password | The password for the accumulo root user. A randomly generated password will be used if none is specified here. | `string` | `null` | no |
| accumulo\_testing\_branch\_name | The name of the branch to build and install | `string` | `"main"` | no |
| accumulo\_testing\_repo | URL of the Accumulo Testing git repo | `string` | `"https://github.com/apache/accumulo-testing.git"` | no |
| accumulo\_version | The branch of Accumulo to download and install | `string` | `"2.1.0-SNAPSHOT"` | no |
| ami\_name\_pattern | The pattern of the name of the AMI to use | `any` | n/a | yes |
| ami\_owner | The id of the AMI owner | `any` | n/a | yes |
| authorized\_ssh\_key\_files | List of SSH public key files for the developers that will log into the cluster | `list(string)` | `[]` | no |
| authorized\_ssh\_keys | List of SSH keys for the developers that will log into the cluster | `list(string)` | n/a | yes |
| cloudinit\_merge\_type | Describes the merge behavior for overlapping config blocks in cloud-init. | `string` | `null` | no |
| create\_route53\_records | Indicates whether or not route53 records will be created | `bool` | `false` | no |
| hadoop\_dir | The Hadoop directory on each EC2 node | `string` | `"/data/hadoop"` | no |
| hadoop\_version | The version of Hadoop to download and install | `string` | `"3.3.4"` | no |
| instance\_count | The number of EC2 instances to create | `string` | `"2"` | no |
| instance\_type | The type of EC2 instances to create | `string` | `"m5.2xlarge"` | no |
| local\_sources\_dir | Directory on local machine that contains Maven, ZooKeeper or Hadoop binary distributions or Accumulo source tarball | `string` | `""` | no |
| maven\_version | The version of Maven to download and install | `string` | `"3.8.6"` | no |
| optional\_cloudinit\_config | An optional config block for the cloud-init script. If you set this, you should consider setting cloudinit\_merge\_type to handle merging with the default script as you need. | `string` | `null` | no |
| private\_network | Indicates whether or not the user is on a private network and access to hosts should be through the private IP addresses rather than public ones. | `bool` | `false` | no |
| root\_volume\_gb | The size, in GB, of the EC2 instance root volume | `string` | `"300"` | no |
| route53\_zone | The name of the Route53 zone in which to create DNS addresses | `any` | n/a | yes |
| security\_group | The Security Group to use when creating AWS objects | `any` | n/a | yes |
| software\_root | The full directory root where software will be installed | `string` | `"/opt/accumulo-testing"` | no |
| us\_east\_1b\_subnet | The AWS subnet id for the us-east-1b subnet | `any` | n/a | yes |
| us\_east\_1e\_subnet | The AWS subnet id for the us-east-1e subnet | `any` | n/a | yes |
| zookeeper\_dir | The ZooKeeper directory on each EC2 node | `string` | `"/data/zookeeper"` | no |
| zookeeper\_version | The version of ZooKeeper to download and install | `string` | `"3.8.0"` | no |

The following outputs are returned by the `aws` Terraform configuration.

| Name | Description |
|------|-------------|
| accumulo\_root\_password | The supplied, or automatically generated Accumulo root user password. |
| manager\_ip | The IP address of the manager instance. |
| worker\_ips | The IP addresses of the worker instances. |

### Azure Variables

The table below lists the variables and their default values that are used in the `azure` configuration.

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| accumulo\_branch\_name | The name of the branch to build and install | `string` | `"main"` | no |
| accumulo\_dir | The Accumulo directory on each node | `string` | `"/data/accumulo"` | no |
| accumulo\_instance\_name | The accumulo instance name. | `string` | `"accumulo-testing"` | no |
| accumulo\_repo | URL of the Accumulo git repo | `string` | `"https://github.com/apache/accumulo.git"` | no |
| accumulo\_root\_password | The password for the accumulo root user. A randomly generated password will be used if none is specified here. | `string` | `null` | no |
| accumulo\_testing\_branch\_name | The name of the branch to build and install | `string` | `"main"` | no |
| accumulo\_testing\_repo | URL of the Accumulo Testing git repo | `string` | `"https://github.com/apache/accumulo-testing.git"` | no |
| accumulo\_version | The branch of Accumulo to download and install | `string` | `"2.1.0-SNAPSHOT"` | no |
| admin\_username | The username of the admin user, that can be authenticated with the first public ssh key. | `string` | `"azureuser"` | no |
| authorized\_ssh\_key\_files | List of SSH public key files for the developers that will log into the cluster | `list(string)` | `[]` | no |
| authorized\_ssh\_keys | List of SSH keys for the developers that will log into the cluster | `list(string)` | n/a | yes |
| cloudinit\_merge\_type | Describes the merge behavior for overlapping config blocks in cloud-init. | `string` | `null` | no |
| create\_resource\_group | Indicates whether or not resource\_group\_name should be created or is an existing resource group. | `bool` | `true` | no |
| hadoop\_dir | The Hadoop directory on each node | `string` | `"/data/hadoop"` | no |
| hadoop\_version | The version of Hadoop to download and install | `string` | `"3.3.4"` | no |
| local\_sources\_dir | Directory on local machine that contains Maven, ZooKeeper or Hadoop binary distributions or Accumulo source tarball | `string` | `""` | no |
| location | The Azure region where resources are to be created. If an existing resource group is specified, this value is ignored and the resource group's location is used. | `string` | n/a | yes |
| managed\_disk\_configuration | Optional managed disk configuration. If supplied, the managed disks on each VM will be combined into an LVM volume mounted at the named mount point. | <pre>object({<br>    mount_point          = string<br>    disk_count           = number<br>    storage_account_type = string<br>    disk_size_gb         = number<br>  })</pre> | `null` | no |
| maven\_version | The version of Maven to download and install | `string` | `"3.8.6"` | no |
| network\_address\_space | The network address space to use for the virtual network. | `list(string)` | <pre>[<br>  "10.0.0.0/16"<br>]</pre> | no |
| optional\_cloudinit\_config | An optional config block for the cloud-init script. If you set this, you should consider setting cloudinit\_merge\_type to handle merging with the default script as you need. | `string` | `null` | no |
| os\_disk\_caching | The type of caching to use for the OS disk. Possible values are None, ReadOnly, and ReadWrite. | `string` | `"ReadOnly"` | no |
| os\_disk\_size\_gb | The size, in GB, of the OS disk | `number` | `300` | no |
| os\_disk\_type | The disk type to use for OS disks. Possible values are Standard\_LRS, StandardSSD\_LRS, and Premium\_LRS. | `string` | `"Standard_LRS"` | no |
| resource\_group\_name | The name of the resource group to create or reuse. If not specified, the name is generated based on resource\_name\_prefix. | `string` | `""` | no |
| resource\_name\_prefix | A prefix applied to all resource names created by this template. | `string` | `"accumulo-testing"` | no |
| software\_root | The full directory root where software will be installed | `string` | `"/opt/accumulo-testing"` | no |
| subnet\_address\_prefixes | The subnet address prefixes to use for the accumulo testing subnet. | `list(string)` | <pre>[<br>  "10.0.2.0/24"<br>]</pre> | no |
| vm\_image | n/a | <pre>object({<br>    publisher = string<br>    offer     = string<br>    sku       = string<br>    version   = string<br>  })</pre> | <pre>{<br>  "offer": "0001-com-ubuntu-server-focal",<br>  "publisher": "Canonical",<br>  "sku": "20_04-lts-gen2",<br>  "version": "latest"<br>}</pre> | no |
| vm\_sku | The SKU of Azure VMs to create | `string` | `"Standard_D8s_v4"` | no |
| worker\_count | The number of worker VMs to create | `number` | `1` | no |
| zookeeper\_dir | The ZooKeeper directory on each node | `string` | `"/data/zookeeper"` | no |
| zookeeper\_version | The version of ZooKeeper to download and install | `string` | `"3.8.0"` | no |

The following outputs are returned by the `azure` Terraform configuration.

| Name | Description |
|------|-------------|
| accumulo\_root\_password | The user-supplied or automatically generated Accumulo root user password. |
| manager\_ip | The public IP address of the manager VM. |
| worker\_ips | The public IP addresses of the worker VMs. |

### Configuration

When using either the `aws` or `azure` configuration, you will need to supply values for required
variables that have no default value. There are several
[ways](https://www.terraform.io/language/values/variables#assigning-values-to-root-module-variables)
to do this. If you installed Terraform Docs, it can generate the file for you. You can then edit the
generated file to configure values as desired:

```bash
CLOUD=<enter either aws or azure>
cd $CLOUD
terraform-docs tfvars hcl . > ${CLOUD}.auto.tfvars
# If you prefer JSON over HCL, then the command would be
# terraform-docs tfvars json . > ${CLOUD}.auto.tfvars.json
```

Note that these generated variable files will include values for all variables, where those with
defaults will be set to their default value. You can also refer to the tables above and simply
add the values that are required (and have no default, or a default that you wish to change).
Below is an example JSON file containing configuration for `aws`. This content can be customized
and placed in the `aws` directory in a file whose name ends with `.auto.tfvars.json`. Any variable
files whose name ends in `.auto.tfvars` or `.auto.tfvars.json` are automatically included when
`terraform` commands are executed.

```json
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

#### Cloud-Init Customization

The cloud-init template can be found in [cloud-init.tftpl](./modules/cloud-init-config/templates/cloud-init.tftpl).
If you need to customize this configuration, one method is to use the Terraform variable
`optional_cloudinit_config` to supply your own additional configuration. For example, some CentOS 7
images are out of date, and will need software packages to be updated before the rest of the
software download/install will work. This can be accomplished by adding the following to your
`.auto.tfvars` file:

```hcl
optional_cloudinit_config = <<-EOT
  package_upgrade: true
EOT
```

You can add any other cloud-init configuration that you wish here. One factor to consider here is
the cloud-init [merging behavior](https://cloudinit.readthedocs.io/en/latest/topics/merging.html)
with sections in the default template. The merging behavior can be controlled by setting the
`cloudinit_merge_type` variable to your desired merge algorithm. The default is set to
`dict(recurse_array,no_replace)+list(append)` which will attempt to keep all lists from the default
configuration, rather than new ones overwriting them.

Another factor to consider is the size of the generated cloud-init template. Cloud providers place
a limit on the size of this file. AWS limits this content to 16KB, before Base64 encoding, and
Azure limits it to 64KB after Base64 encoding.

## AWS Resources

This Terraform configuration creates:

  1. `${instance_count}` EC2 nodes of `${instance_type}` with the latest AMI matching
    `${ami_name_pattern}` from the `${ami_owner}`. Each EC2 node will have a `${root_volume_gb}`GB
    root volume. The EFS filesystem is NFS mounted to each node at `${software_root}`.
  2. DNS entries in Route53 for each EC2 node.

## Software Layout

This Terraform configuration:

  1. Downloads, if necessary, the Apache Maven `${maven_version}` binary tarball to
     `${software_root}/sources`, then untars it to `${software_root}/apache-maven/apache-maven-${maven_version}`
  2. Downloads, if necessary, the Apache Zookeeper `${zookeer_version}` binary tarball to
     `${software_root}/sources`, then untars it to `${software_root}/zookeeper/apache-zookeeper-${zookeeper_version}-bin`
  3. Downloads, if necessary, the Apache Hadoop `${hadoop_version}` binary tarball to
     `${software_root}/sources`, then untars it to `${software_root}/hadoop/hadoop-${hadoop_version}`
  4. Clones, if necessary, the Apache Accumulo Git repo from `${accumulo_repo}` into
     `${software_root}/sources/accumulo-repo`. It switches to the `${accumulo_branch_name}` branch
     and builds the software using Maven, then untars the binary tarball to
     `${software_root}/accumulo/accumulo-${accumulo_version}`
  5. Downloads the [OpenTelemetry](https://opentelemetry.io/) Java Agent jar file and copies it to
     `${software_root}/accumulo/accumulo-${accumulo_version}/lib/opentelemetry-javaagent-1.19.0.jar`
  6. Copies the Accumulo `test` jar to `${software_root}/accumulo/accumulo-${accumulo_version}/lib`
     so that `org.apache.accumulo.test.metrics.TestStatsDRegistryFactory` is on the classpath
  7. Downloads the [Micrometer](https://micrometer.io/) StatsD Registry jar file and copies it to
     `${software_root}/accumulo/accumulo-${accumulo_version}/lib/micrometer-registry-statsd-1.9.5.jar`
  8. Clones, if necessary, the Apache Accumulo Testing Git repo from `${accumulo_testing_repo}`
     into `${software_root}/sources/accumulo-testing-repo`. It switches to the
     `${accumulo_testing_branch_name}` branch and builds the software using Maven.

### Supplying your own software

If you want to supply your own Apache Maven, Apache ZooKeeper, Apache Hadoop, Apache Accumulo, or
Apache Accumulo Testing binary tar files, then you can put them into a directory on your local
machine and set the `${local_sources_dir}` variable to the full path to the directory. These files
will be uploaded to `${software_root}/sources` and the installation script will use them instead of
downloading them. If the version of the supplied binary tarball is different than the default
version, then you will also need to override that property. Supplying your own binary tarballs does
speed up the deployment. However, if you provide the Apache Accumulo binary tarball, then it will
be harder to update the software on the cluster.

**NOTE**: If you supply your own binary tarball of Accumulo, then you will need to copy the
`accumulo-test-${accumulo_version}.jar` file to the `lib` directory manually as it's not part of
the binary tarball.

### Updating Apache Accumulo on the cluster

If you did not provide a binary tarball, then you can update the software running on the cluster by
doing the following and then restarting Accumulo:

```bash
cd ${software_root}/sources/accumulo-repo
git pull
mvn clean package -DskipTests -DskipITs
# Backup the Accumulo configs
mkdir -p ~/accumulo-config-backup
cp ${software_root}/accumulo/accumulo-${accumulo_version}/conf/* ~/accumulo-config-backup/.
# Lay down the updated Accumulo distribution
tar zxf assemble/target/accumulo-${accumulo_version}-bin.tar.gz -C ${software_root}/accumulo
# Restore the Accumulo configs
cp ~/accumulo-config-backup/* ${software_root}/accumulo/accumulo-${accumulo_version}/conf/.
# Sync the Accumulo changes with the worker nodes
pdsh -R exec -g worker rsync -az ${software_root}/accumulo/ %h:${software_root}/accumulo/
```

### Updating Apache Accumulo Testing on the cluster

If you did not provide a binary tarball, then you can update the software running on the cluster by
doing the following:

```bash
cd ${software_root}/sources/accumulo-testing-repo
git pull
mvn clean package -DskipTests -DskipITs
```

## Deployment Overview

The first node that is created is called the `manager`, the others are `worker` nodes. The
following components will run on the `manager` node:

- Apache ZooKeeper
- Apache Hadoop NameNode
- Apache Hadoop Yarn ResourceManager
- Apache Accumulo Manager
- Apache Accumulo Monitor
- Apache Accumulo GarbageCollector
- Apache Accumulo CompactionCoordinator
- Docker
- Jaeger Tracing Docker Container
- Telegraf/InfluxDB/Grafana Docker Container

The following components will run on the `worker` nodes:

- Apache Hadoop DataNode
- Apache Hadoop Yarn NodeManager
- Apache Accumulo TabletServer
- Apache Accumulo Compactor(s)
- Apache Accumulo Scan Server(s)

### Logs

The logs for each service (zookeeper, hadoop, accumulo) are located in their respective local
directory on each node (`/data/${service}/logs` unless you changed the properties).

### DNS entries

The `aws` Terraform configuration creates DNS entries of the following form:

  <node_name>-<branch_name>-<workspace_name>.${route53_zone}

For example:

- manager-main-default.${route53_zone}
- worker#-main-default.${route53_zone} (where # is 0, 1, 2, ...)

The `azure` configuration does not current create public DNS entries for the nodes, and it is
recommended that the public IP addresses be used instead.

## Instructions

  1. Change to either the `aws` or `azure` directory in your shell. This must be the current
     directory when you run the following `terraform` commands.
  2. Once you have created a `.auto.tfvars` file, or set the properties some other way, run
     `terraform init`. If you have modified shared_state backend configuration over the default,
     you can override the values here. For example, the following configuration updates the
     `resource_group_name` and `storage_account_name` for the `azurerm` backend:
     ```bash
     terraform init -backend-config=resource_group_name=my-tfstate-resource-group -backend-config=storage_account_name=mystorageaccountname
     ```
     Once values are supplied to `terraform init`, they are stored in the local state and it is not
     necessary to supply these overrides to the `terraform apply` or `terraform destroy` commands.
  3. Ensure that the private key associated with the first public SSH key listed for the value
     of either `authorized_ssh_keys` or `authorized_ssh_key_files` in your `.auto.tfvars` file
     is loaded into your SSH agent. During resource creation, Terraform will connect to the newly
     created VMs using SSH in order copy files and configure the VMs to run Accumulo. If the
     appropriate private key is not available to your SSH agent, then the connection will fail and
     resource creation will eventually fail.
  4. Run `terraform apply` to create the AWS/Azure resources.
  5. Run `terraform destroy` to tear down the AWS/Azure resources.

**NOTE**: If you are working with `aws` and get an Access Denied error then try setting the AWS
Short Term access keys in your environment

### Accessing Web Pages

For an `aws` cluster, you can access the software configuration/management web pages here:
- Hadoop NameNode: http://manager-main-default.${route53_zone}:9870
- Yarn ResourceManager: http://manager-main-default.${route53_zone}:8088
- Hadoop DataNode: http://worker#-main-default.${route53_zone}:9864
- Yarn NodeManager: http://worker#-main-default.${route53_zone}:8042
- Accumulo Monitor: http://manager-main-default.${route53_zone}:9995
- Jaeger Tracing UI: http://manager-main-default.${route53_zone}:16686
- Grafana: http://manager-main-default.${route53_zone}:3003

The `azure` cluster creates a network security group that limits public access to port 22 (SSH).
Therefore, to access configuration/management web pages, you should create a SOCKS proxy and use
a browser plugin such as
[FoxyProxy Standard](https://chrome.google.com/webstore/detail/foxyproxy-standard/gcknhkkoolaabfmlnjonogaaifnjlfnp)
to point the browser to the SOCKS proxy. Create the proxy with
```bash
ssh -C2qTnNf -D 9876 hadoop@<manager-public-ip-address>
```
Configure FoxyProxy (or your browser directly) to connect to the proxy on localhost port 9876
(change the port specified in the `-D` option above to use a different proxy port). If you
configure FoxyProxy with a SOCKS 5 proxy to match the URL regex patterns `https?://manager:.*` and
`https?://worker[0-9]+:.*`, then you can leave FoxyProxy set to
"Use proxies based on their pre-defined patterns and priorities" and access the web pages through
the proxy while other web pages will not use the proxy.
- Hadoop NameNode: http://manager:9870
- Yarn ResourceManager: http://manager:8088
- Hadoop DataNode: http://worker#:9864
- Yarn NodeManager: http://worker#:8042
- Accumulo Monitor: http://manager:9995
- Jaeger Tracing UI: http://manager:16686
- Grafana: http://manager:3003


## Accessing the cluster nodes

The [cloud-init](https://cloudinit.readthedocs.io/en/latest/) configuration applied to each
AWS instance or Azure VM creates a `hadoop` user. Any public SSH keys specified in the Terraform
configuration variable `authorized_ssh_keys` (or public key file named in
`authorized_ssh_key_files`) will be included in the cloud-init template as an authorized key for
the `hadoop` user.

If you wish to use your default ssh key, typically stored in `~/.ssh/id_rsa.pub`, you would add the
following to your HCL `.auto.tfvars` file:

```hcl
authorized_ssh_key_files = [ "~/.ssh/id_rsa.pub" ]
```

Then, when the cluster is created, you can log in to a node with
`ssh hadoop@<node-public-ip-address>`.

### SSH'ing to other nodes

The `/etc/hosts` file on each node has been updated with the names (manager, worker0, worker1,
etc.) and IP addresses of the nodes. `pdsh` has been installed and `/etc/genders` has been
configured. You should be able to `ssh` to any node as the `hadoop` user without a password.
Likewise, you should be able to `pdsh` commands to groups of nodes as the hadoop user. The `pdsh`
genders group `manager` specifies the manager node, and the `worker` group will specify all
worker nodes.

## Shutdown / Startup Instructions

Once the cluster is created you can simply stop or start the nodes from the AWS console or Azure
portal. Terraform is just for creating, updating, or destroying the resources. ZooKeeper and Hadoop
are setup to use SystemD service files, but Accumulo is not. You could log into the manager node
and run `accumulo-cluster stop` before stopping the nodes. Or, you could just shut them down and
force Accumulo to recover (which might be good for testing). When restarting the nodes from the AWS
Console/Azure Portal, ZooKeeper and Hadoop should start on their own. For Accumulo, you should only
need to run `accumulo-cluster start` on the manager node.
