#
# To install Terraform download the appropriate version from https://www.terraform.io/downloads.html
# and copy the binary to /usr/local/bin or some other location on your PATH.
#
# Run "terraform init" in this directory to download the plugins that Terraform will need to run
# this plan.
#
# Run "terraform plan" to see the changes that would be made if you applied this plan.
#
# Run "terraform apply" to see the changes that would be made and to optionally apply the plan.
#  
#
# This Terraform configuration does the following:
#
# 1. Creates an EFS filesystem in AWS (think NFS). On this filesystem we
#    are going to install Maven, ZooKeeper, Hadoop, Accumulo, and Accumulo Testing
#
# 2. Creates IP addresses for the EFS filesystem in each AWS availability zone that
#    we use. The IP address allows us to mount the EFS filesystem to the EC2 nodes.
#
# 3. Creates one or more EC2 nodes for running the different components. Currently
#    the configuration uses the m5.2xlarge instance type which provides 8 vCPUs, 32GB RAM,
#    and an EBS backed root volume. 
#
# 4. Runs commands on the EC2 nodes after they are started (5 minutes according
#    to the docs) to install software and configure them.
#
# 5. Creates DNS entries for the manager (the first node created) and the workers (the remaining nodes).
#
#
# PRICING:
#
# As of Dec 7 2021:
#
#   Each m5.2xlarge costs $0.384 per hour
#   A 300GB EBS volume running for 40 hours per month is $1.50
#   3GB of EFS storage will cost $0.90 per month
#
#   Currently the storage used is about 2.5GB with Maven, ZooKeeper, Hadoop,
#   Accumulo, and Accumulo-Testing installed and built.
#

################################
# Core Terraform Configuration #
################################

terraform {
  required_version = ">= 1.1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.68.0"
    }
  }
  backend "s3" {
    bucket = "accumulo-testing-tf-state"
    key = "accumulo-testing/terraform.tfstate"
    region = "us-east-1"
    dynamodb_table = "accumulo-testing-tf-locks"
    encrypt = true
  }
}

provider "aws" {
  region = "us-east-1"
}


#
# Retrieves Account Information from AWS #
#
data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

#
# Looks up the subnet us-east-1b
#
data "aws_subnet" "subnet_1b" {
  id = "${var.us_east_1b_subnet}"
}

#
# Looks up the subnet us-east-1e
#
data "aws_subnet" "subnet_1e" {
  id = "${var.us_east_1e_subnet}"
}

#
# Looks up the latest CentOS AMI
#
data "aws_ami" "centos_ami" {
  owners = ["${var.ami_owner}"]
  filter {
    name   = "name"
    values = ["${var.ami_name_pattern}"]
  }
}

#
# Lookup the AWS private zone
#
data "aws_route53_zone" "private_zone" {
  name = "${var.route53_zone}"
  private_zone = true
}

#
# Pass variables into the templates/cloud-init.tpl to create
# the cloud-init script that will be used when creating the
# EC2 nodes later
#
locals {
  cloud_init_script = templatefile("${path.module}/templates/cloud-init.tpl", {
    zookeeper_dir = var.zookeeper_dir
    hadoop_dir = var.hadoop_dir
    accumulo_dir = var.accumulo_dir
    maven_version = var.maven_version
    zookeeper_version = var.zookeeper_version
    hadoop_version = var.hadoop_version
    accumulo_branch_name = var.accumulo_branch_name
    accumulo_version = var.accumulo_version
    authorized_ssh_keys = var.authorized_ssh_keys[*]
  })
}

data "template_cloudinit_config" "cloud_init_data" {
  gzip = false
  base64_encode = false
  part {
    filename = "init.cfg"
    content_type = "text/cloud-config"
    content = "${local.cloud_init_script}"
  }
}

#####################
# EFS Configuration #
#####################

#
# Create the EFS filesystem. Once created this can be found in the
# AWS Management Console in the EFS service. The mount points that
# are created below can be found on the Network tab of the EFS
# filesystem named accumulo-testing-efs. You can use the Attach
# button on the top right to get the instructions for mounting the
# EFS filesystem to an EC2 node manually.
#
resource "aws_efs_file_system" "accumulo-testing-efs" {
  creation_token = "accumulo-testing-${var.accumulo_branch_name}-branch-${terraform.workspace}-ws"
  tags = {
    Name = "accumulo-testing-${var.accumulo_branch_name}-branch-${terraform.workspace}-ws"
    Branch = "${var.accumulo_branch_name}"
    Workspace = "${terraform.workspace}"
  }
}

#
# Create an IP address in the us-east-1b availability zone for the EFS filesystem
#
resource "aws_efs_mount_target" "accumulo_testing_efs_mount_point_1b" {
  file_system_id  = aws_efs_file_system.accumulo-testing-efs.id
  subnet_id       = data.aws_subnet.subnet_1b.id
  security_groups = [var.security_group]
}

#
# Create an IP address in the us-east-1e availibility zone for the EFS filesystem
#
resource "aws_efs_mount_target" "accumulo_testing_efs_mount_point_1e" {
  file_system_id  = aws_efs_file_system.accumulo-testing-efs.id
  subnet_id       = data.aws_subnet.subnet_1e.id
  security_groups = [var.security_group]
}

##########################
# EC2 Node Configuration #
##########################

#
# Definition for the EC2 nodes to include:
#
# 1. AMI
# 2. Instance Type
# 3. Number of instances to create
# 4. Availability Zone subnet
# 5. VPC security group
# 6. Size of the root block device
# 7. Cloud-init script (see https://cloudinit.readthedocs.io/en/latest/) which
#    - creates the hadoop group and user
#    - installs packages via yum
#    - creates some files on the filesystem to use later
# 8. A provisioner which will connect to the EC2 node once created and mount EFS
#
resource "aws_instance" "accumulo-testing" {
  ami                    = data.aws_ami.centos_ami.id
  instance_type          = var.instance_type
  count                  = var.instance_count
  subnet_id              = data.aws_subnet.subnet_1b.id
  vpc_security_group_ids = [var.security_group]
  root_block_device {
    volume_size = var.root_volume_gb
    delete_on_termination = true
    tags = {
      Name = "accumulo-testing-${var.accumulo_branch_name}-branch-${terraform.workspace}-ws-${count.index}"
    }
  }
  #
  # User data section will run cloud-init configuration that
  # was created above from the template
  #
  user_data = "${data.template_cloudinit_config.cloud_init_data.rendered}"
  #
  # Mount the EFS filesystem and put an entry in /etc/fstab on each node
  #
  provisioner "remote-exec" {
    inline = [
      "efs_mount=${var.efs_mount}",
      "sudo mkdir -p $efs_mount",
      "efs_ip=${aws_efs_mount_target.accumulo_testing_efs_mount_point_1b.ip_address}",
      "sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport $efs_ip:/ $efs_mount 2>&1 >> /tmp/mounting || exit 1",
      "sudo bash -c \"echo $efs_ip:/ $efs_mount nfs4 defaults,_netdev 0 0 >> /etc/fstab\"",
      "sudo chown -R hadoop.hadoop $efs_mount"
    ]
    connection {
      type        = "ssh"
      host        = self.private_ip
      user        = "hadoop"
    }
  }
  tags = {
    Name       = "accumulo-testing-${var.accumulo_branch_name}-branch-${terraform.workspace}-ws-${count.index}"
    Branch = "${var.accumulo_branch_name}"
    Workspace = "${terraform.workspace}"
    noshutdown = true
    nostartup  = true
  }
}

##############################
# Create configuration files #
##############################

#
# This section creates the ZooKeeper, Hadoop, and Accumulo confguration files
# using templates in the templates directory and IP addresses from the EC2
# nodes that we created above and variables.
#

locals {
  manager_ip = element(aws_instance.accumulo-testing.*.private_ip, 0)
  worker_ips = var.instance_count > 1 ? slice(aws_instance.accumulo-testing.*.private_ip, 1, var.instance_count) : [element(aws_instance.accumulo-testing.*.private_ip, 0)]
}

resource "local_file" "etc-hosts" {
  filename = "${path.module}/conf/hosts"
  file_permission = "644"
  content = templatefile("templates/hosts.tpl",
    {
      manager_ip = local.manager_ip
      worker_ips = local.worker_ips
    }
  )
}

resource "local_file" "etc-genders" {
  filename = "${path.module}/conf/genders"
  file_permission = "644"
  content = templatefile("templates/genders.tpl",
    {
      manager_ip = local.manager_ip
      worker_ips = local.worker_ips
    }
  )
}

resource "local_file" "zookeeper-config" {
  filename = "${path.module}/conf/zoo.cfg"
  file_permission = "644"
  content = templatefile("templates/zoo.cfg.tpl",
    {
      zookeeper_dir = var.zookeeper_dir
    }
  )
}

resource "local_file" "hadoop-core-config" {
  filename = "${path.module}/conf/core-site.xml"
  file_permission = "644"
  content = templatefile("templates/core-site.xml.tpl",
    {
      manager_ip = local.manager_ip
      hadoop_dir = var.hadoop_dir
    }
  )
}

resource "local_file" "hadoop-hdfs-config" {
  filename = "${path.module}/conf/hdfs-site.xml"
  file_permission = "644"
  content = templatefile("templates/hdfs-site.xml.tpl",
    {
      manager_ip = local.manager_ip
      hadoop_dir = var.hadoop_dir
    }
  )
}

resource "local_file" "accumulo-cluster-config" {
  filename = "${path.module}/conf/cluster.yaml"
  file_permission = "644"
  content = templatefile("templates/cluster.yaml.tpl",
    {
      manager_ip = local.manager_ip
      worker_ips = local.worker_ips
    }
  )
}

resource "local_file" "accumulo-properties-config" {
  filename = "${path.module}/conf/accumulo.properties"
  file_permission = "644"
  content = templatefile("templates/accumulo-properties.tpl",
    {
      manager_ip = local.manager_ip
    }
  )
}

resource "local_file" "telegraf-config" {
  filename = "${path.module}/conf/telegraf.conf"
  file_permission = "644"
  content = templatefile("templates/telegraf.conf.tpl",
    {
      manager_ip = local.manager_ip
    }
  )
}

resource "local_file" "namenode-systemd" {
  filename = "${path.module}/conf/hadoop-namenode.service"
  file_permission = "644"
  content = templatefile("templates/hadoop-namenode.service.tpl",
    {
      efs_mount = var.efs_mount
      hadoop_dir = var.hadoop_dir
      hadoop_version = var.hadoop_version
    }
  )
}

resource "local_file" "datanode-systemd" {
  filename = "${path.module}/conf/hadoop-datanode.service"
  file_permission = "644"
  content = templatefile("templates/hadoop-datanode.service.tpl",
    {
      efs_mount = var.efs_mount
      hadoop_dir = var.hadoop_dir
      hadoop_version = var.hadoop_version
    }
  )
}

resource "local_file" "zookeeper-systemd" {
  filename = "${path.module}/conf/zookeeper.service"
  file_permission = "644"
  content = templatefile("templates/zookeeper.service.tpl",
    {
      efs_mount = var.efs_mount
      zookeeper_dir = var.zookeeper_dir
      zookeeper_version = var.zookeeper_version
    }
  )
}

resource "local_file" "hadoop-bash-profile" {
  filename = "${path.module}/conf/hadoop_bash_profile"
  file_permission = "600"
  content = templatefile("templates/hadoop_bash_profile.tpl",
    {
    }
  )
}

resource "local_file" "hadoop-bashrc" {
  filename = "${path.module}/conf/hadoop_bashrc"
  file_permission = "600"
  content = templatefile("templates/hadoop_bashrc.tpl",
    {
      accumulo_version = var.accumulo_version
      accumulo_dir = var.accumulo_dir
      efs_mount = var.efs_mount
      hadoop_version = var.hadoop_version
      manager_ip = local.manager_ip
      maven_version = var.maven_version
      zookeeper_version = var.zookeeper_version
    }
  )
}

resource "local_file" "install-software" {
  filename = "${path.module}/conf/install_sw.sh"
  file_permission = "755"
  content = templatefile("templates/install_sw.sh.tpl",
    {
      accumulo_branch_name = var.accumulo_branch_name
      accumulo_dir = var.accumulo_dir
      accumulo_repo = var.accumulo_repo
      accumulo_testing_repo = var.accumulo_testing_repo
      accumulo_testing_branch_name = var.accumulo_testing_branch_name
      accumulo_version = var.accumulo_version
      efs_mount = var.efs_mount
      hadoop_dir = var.hadoop_dir
      hadoop_version = var.hadoop_version
      maven_version = var.maven_version
      zookeeper_dir = var.zookeeper_dir
      zookeeper_version = var.zookeeper_version
    }
  )
}

##################################
# Upload the Configuration files #
##################################

#
# This provisioner connects to the 0th node and uploads the generated
# config files to ${var.efs_mount}/conf
#
resource "null_resource" "upload_config_files" {
  depends_on = [
    local_file.etc-hosts,
    local_file.etc-genders,
    local_file.zookeeper-config,
    local_file.hadoop-core-config,
    local_file.hadoop-hdfs-config,
    local_file.accumulo-cluster-config,
    local_file.accumulo-properties-config,
    local_file.telegraf-config,
    local_file.namenode-systemd,
    local_file.datanode-systemd,
    local_file.zookeeper-systemd,
    local_file.hadoop-bash-profile,
    local_file.hadoop-bashrc,
    local_file.install-software
  ]
  triggers = {
    cluster_instance_ids = join(",", aws_instance.accumulo-testing.*.id)
  }
  connection {
    type        = "ssh"
    host        = element(aws_instance.accumulo-testing.*.private_ip, 0)
    user        = "hadoop"
  }
  provisioner "remote-exec" {
    inline = [<<EOT
      until [[ -d ${var.efs_mount} ]]; do
        echo "waiting for ${var.efs_mount} to be mounted on manager node..."
        sleep 30s
      done
      mkdir -p ${var.efs_mount}/grafana/dashboards
    EOT
    ]
  }
  provisioner "file" {
    source = "conf"
    destination = "${var.efs_mount}"
  }
  provisioner "file" {
    source = "files/accumulo-dashboard.json"
    destination = "${var.efs_mount}/grafana/dashboards/accumulo-dashboard.json"
  }
  provisioner "file" {
    source = "files/accumulo-dashboard.yaml"
    destination = "${var.efs_mount}/grafana/dashboards/accumulo-dashboard.yaml"
  }
}

#
# If local_sources_dir is not-null, then upload the contents of the directory
#
resource "null_resource" "upload_software" {
  count  = var.local_sources_dir == "" ? 0 : 1
  depends_on = [
    null_resource.upload_config_files
  ]
  triggers = {
    cluster_instance_ids = join(",", aws_instance.accumulo-testing.*.id)
  }
  connection {
    type        = "ssh"
    host        = element(aws_instance.accumulo-testing.*.private_ip, 0)
    user        = "hadoop"
  }
  provisioner "file" {
    source = "${var.local_sources_dir}"
    destination = "${var.efs_mount}/sources"
  }
}

#####################
# Create Hadoop Key #
#####################
resource "null_resource" "create_hadoop_key" {
  depends_on = [
    null_resource.upload_config_files
  ]
  triggers = {
    cluster_instance_ids = join(",", aws_instance.accumulo-testing.*.id)
  }
  connection {
    type        = "ssh"
    host        = element(aws_instance.accumulo-testing.*.private_ip, 0)
    user        = "hadoop"
  }
  provisioner "remote-exec" {
    inline = [<<EOT
      until [[ -d ${var.efs_mount} ]]; do
        echo "waiting for ${var.efs_mount} to be mounted to configure manager node..."
        sleep 30s
      done
      # Generate a new private key for hadoop if needed. Pass in 'n'
      # in case this is run on more than one node at the same time.
      if [ ! -f ${var.efs_mount}/hadoop_id_rsa ]; then
        ssh-keygen -t rsa -b 4096 -N "" -f ${var.efs_mount}/hadoop_id_rsa <<< n
      fi
    EOT
    ]
  }
}

########################
# Configure the nodes  #
########################
#
# This connects to each node and sets the environment for the
# hadoop user.
#
resource "null_resource" "configure_nodes" {
  count                  = length(aws_instance.accumulo-testing.*.id)
  depends_on = [
    null_resource.create_hadoop_key
  ]
  triggers = {
    cluster_instance_ids = join(",", aws_instance.accumulo-testing.*.id)
  }
  connection {
    type        = "ssh"
    host        = element(aws_instance.accumulo-testing.*.private_ip, count.index)
    user        = "hadoop"
  }
  provisioner "remote-exec" {
    inline = [<<EOT
      until [[ -d ${var.efs_mount} ]]; do
        echo "waiting for ${var.efs_mount} to be mounted to configure node ..."
        sleep 30s
      done
      until [[ -f /var/lib/cloud/instance/boot-finished ]]; do
        echo "waiting for boot process to finish..."
        sleep 30s
      done
      until [[ -f ${var.efs_mount}/hadoop_id_rsa && -f ${var.efs_mount}/conf/hadoop_bashrc && -f ${var.efs_mount}/conf/hadoop_bash_profile && -f ${var.efs_mount}/conf/hadoop-datanode.service ]]; do
        echo "waiting for hadoop user environment files to be written..."
        sleep 30s
      done
      sudo bash -c 'echo "hadoop           soft    nofile          4096" >> /etc/security/limits.conf'
      sudo bash -c 'echo "hadoop           hard    nofile         65535" >> /etc/security/limits.conf'
      sudo bash -c 'cat ${var.efs_mount}/conf/hosts >> /etc/hosts'
      sudo bash -c 'cat ${var.efs_mount}/conf/genders >> /etc/genders'
      sudo sysctl -w vm.swappiness=0
      sudo sysctl -p
      sudo chown -R hadoop.hadoop /home/hadoop
      cp ${var.efs_mount}/conf/hadoop_bash_profile /home/hadoop/.bash_profile
      cp ${var.efs_mount}/conf/hadoop_bashrc /home/hadoop/.bashrc
      cp ${var.efs_mount}/hadoop_id_rsa.pub /home/hadoop/.ssh/id_rsa.pub
      cp ${var.efs_mount}/hadoop_id_rsa /home/hadoop/.ssh/id_rsa
      mv /home/hadoop/ssh_config /home/hadoop/.ssh/config
      chmod 600 /home/hadoop/.ssh/config
      cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys
      sudo cp ${var.efs_mount}/conf/hadoop-datanode.service /etc/systemd/system/hadoop-datanode.service
      sudo systemctl daemon-reload
      sudo systemctl enable hadoop-datanode
      until [[ -d ${var.accumulo_dir} ]]; do
        echo "waiting for data directories to be created..."
        sleep 30s
      done
      sudo touch /data/node_configured
    EOT
    ]
  }
}

#####################################################
# Run the install software script on the first node #
#####################################################

#
# This connects to the first node and runs the install_sw.sh script.
#
resource "null_resource" "configure_manager_node" {
  depends_on = [
    null_resource.upload_config_files,
    null_resource.upload_software
  ]
  triggers = {
    cluster_instance_ids = join(",", aws_instance.accumulo-testing.*.id)
  }
  connection {
    type        = "ssh"
    host        = element(aws_instance.accumulo-testing.*.private_ip, 0)
    user        = "hadoop"
  }
  provisioner "remote-exec" {
    inline = [<<EOT
      until [[ -d ${var.efs_mount} ]]; do
        echo "waiting for ${var.efs_mount} to be mounted to configure manager node..."
        sleep 30s
      done
      until [[ -f /data/node_configured ]]; do
        echo "waiting for node to be configured..."
        sleep 30s
      done
      source /home/hadoop/.bash_profile
      sudo cp ${var.efs_mount}/conf/zookeeper.service /etc/systemd/system/zookeeper.service
      sudo cp ${var.efs_mount}/conf/hadoop-namenode.service /etc/systemd/system/hadoop-namenode.service
      sudo systemctl daemon-reload
      sudo systemctl enable zookeeper
      sudo systemctl enable hadoop-namenode
      sudo systemctl disable hadoop-datanode
      sudo mkdir -p /data/metrics/influxdb
      sudo mkdir -p /data/metrics/grafana
      sudo chown -R hadoop.hadoop /data/metrics
      sudo chmod 777 /data/metrics/influxdb
      bash ${var.efs_mount}/conf/install_sw.sh
      sudo systemctl start zookeeper
      hdfs namenode -format
      sudo systemctl start hadoop-namenode
      pdsh -g worker 'sudo systemctl start hadoop-datanode'
      accumulo-util build-native
    EOT
    ]
  }
}

####################################################
# Create the Route53 A record for the manager node #
####################################################

resource "aws_route53_record" "manager" {
  zone_id = data.aws_route53_zone.private_zone.zone_id
  name = "manager-${var.accumulo_branch_name}-${terraform.workspace}.${data.aws_route53_zone.private_zone.name}"
  type = "A"
  ttl = "300"
  records = [local.manager_ip]
}

resource "aws_route53_record" "worker" {
  count = length(local.worker_ips)
  zone_id = data.aws_route53_zone.private_zone.zone_id
  name = "worker${count.index}-${var.accumulo_branch_name}-${terraform.workspace}.${data.aws_route53_zone.private_zone.name}"
  type = "A"
  ttl = "300"
  records = [local.worker_ips[count.index]]
}

