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
    bucket         = "accumulo-testing-tf-state"
    key            = "accumulo-testing/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "accumulo-testing-tf-locks"
    encrypt        = true
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
  id = var.us_east_1b_subnet
}

#
# Looks up the subnet us-east-1e
#
data "aws_subnet" "subnet_1e" {
  id = var.us_east_1e_subnet
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
  name         = var.route53_zone
  private_zone = true
}

# Generate cloud-init data to use when creating EC2 nodes.
module "cloud_init_config" {
  source = "../modules/cloud-init-config"

  software_root        = var.software_root
  zookeeper_dir        = var.zookeeper_dir
  hadoop_dir           = var.hadoop_dir
  accumulo_dir         = var.accumulo_dir
  maven_version        = var.maven_version
  zookeeper_version    = var.zookeeper_version
  hadoop_version       = var.hadoop_version
  accumulo_branch_name = var.accumulo_branch_name
  accumulo_version     = var.accumulo_version
  authorized_ssh_keys  = var.authorized_ssh_keys[*]
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
    volume_size           = var.root_volume_gb
    delete_on_termination = true
    tags = {
      Name = "accumulo-testing-${var.accumulo_branch_name}-branch-${terraform.workspace}-ws-${count.index}"
    }
  }
  #
  # User data section will run cloud-init configuration that
  # was created above from the template
  #
  user_data = module.cloud_init_config.cloud_init_data
  #
  # Wait for cloud-init to complete, so we're sure the instance is fully provisioned.
  #
  provisioner "remote-exec" {
    inline = [
      "echo Waiting for cloud init to complete...",
      "cloud-init status --wait > /dev/null",
      "cloud-init status --long"
    ]
    connection {
      type = "ssh"
      host = self.private_ip
      user = "hadoop"
    }
  }
  tags = {
    Name       = "accumulo-testing-${var.accumulo_branch_name}-branch-${terraform.workspace}-ws-${count.index}"
    Branch     = "${var.accumulo_branch_name}"
    Workspace  = "${terraform.workspace}"
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

##############################
# Cluster Configuration      #
##############################

#
# This section creates the ZooKeeper, Hadoop, and Accumulo confguration files
# using templates in the templates directory and IP addresses from the EC2
# nodes that we created above and variables.
#
module "config_files" {
  source = "../modules/config-files"

  software_root = var.software_root
  upload_ip     = local.manager_private_ip
  manager_ip    = local.manager_private_ip
  worker_ips    = local.worker_private_ips

  zookeeper_dir = var.zookeeper_dir
  hadoop_dir    = var.hadoop_dir
  accumulo_dir  = var.accumulo_dir

  maven_version     = var.maven_version
  zookeeper_version = var.zookeeper_version
  hadoop_version    = var.hadoop_version
  accumulo_version  = var.accumulo_version

  accumulo_repo                = var.accumulo_repo
  accumulo_branch_name         = var.accumulo_branch_name
  accumulo_testing_repo        = var.accumulo_testing_repo
  accumulo_testing_branch_name = var.accumulo_testing_branch_name
}

#
# This module updloads any local tarballs to the manager VM and
# stores them on the NFS share.
#
module "upload_software" {
  source = "../modules/upload-software"

  local_sources_dir = var.local_sources_dir
  upload_dir        = var.software_root
  upload_host       = local.manager_ip
}

#
# This section performs final configuration of the Accumulo cluster.
#
module "configure_nodes" {
  source = "../modules/configure-nodes"

  software_root = var.software_root
  manager_ip    = local.manager_ip
  worker_ips    = local.worker_ips
  accumulo_dir  = var.accumulo_dir

  depends_on = [
    module.upload_software,
    module.config_files
  ]
}

####################################################
# Create the Route53 A record for the manager node #
####################################################

resource "aws_route53_record" "manager" {
  zone_id = data.aws_route53_zone.private_zone.zone_id
  name    = "manager-${var.accumulo_branch_name}-${terraform.workspace}.${data.aws_route53_zone.private_zone.name}"
  type    = "A"
  ttl     = "300"
  records = [local.manager_ip]
}

resource "aws_route53_record" "worker" {
  count   = length(local.worker_ips)
  zone_id = data.aws_route53_zone.private_zone.zone_id
  name    = "worker${count.index}-${var.accumulo_branch_name}-${terraform.workspace}.${data.aws_route53_zone.private_zone.name}"
  type    = "A"
  ttl     = "300"
  records = [local.worker_ips[count.index]]
}

