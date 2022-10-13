#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

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
# 1. Creates one or more EC2 nodes for running the different components. Currently
#    the configuration uses the m5.2xlarge instance type which provides 8 vCPUs, 32GB RAM,
#    and an EBS backed root volume. 
#
# 2. Runs commands on the EC2 nodes after they are started (5 minutes according
#    to the docs) to install software and configure them.
#
# 3. Creates DNS entries for the manager (the first node created) and the workers (the remaining nodes).
#
#
# PRICING:
#
# As of Dec 7 2021:
#
#   Each m5.2xlarge costs $0.384 per hour
#   A 300GB EBS volume running for 40 hours per month is $1.50
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
      version = "~> 4.31.0"
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
  count        = var.create_route53_records ? 1 : 0
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
  authorized_ssh_keys  = local.ssh_keys[*]
  cluster_type         = "aws"

  optional_cloudinit_config = var.optional_cloudinit_config
  cloudinit_merge_type      = var.cloudinit_merge_type
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
      "sudo cloud-init status --wait > /dev/null",
      "sudo cloud-init status --long"
    ]
    connection {
      type = "ssh"
      host = var.private_network ? self.private_ip : self.public_ip
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
# This section creates the ZooKeeper, Hadoop, and Accumulo configuration files
# using templates in the templates directory and IP addresses from the EC2
# nodes that we created above and variables.
#

locals {
  ssh_keys           = toset(concat(var.authorized_ssh_keys, [for k in var.authorized_ssh_key_files : file(k)]))
  manager_ip         = aws_instance.accumulo-testing[0].public_ip
  worker_ips         = var.instance_count > 1 ? slice(aws_instance.accumulo-testing[*].public_ip, 1, var.instance_count) : aws_instance.accumulo-testing[0].public_ip[*]
  manager_private_ip = aws_instance.accumulo-testing[0].private_ip
  worker_private_ips = var.instance_count > 1 ? slice(aws_instance.accumulo-testing[*].private_ip, 1, var.instance_count) : aws_instance.accumulo-testing[0].private_ip[*]
}

##############################
# Cluster Configuration      #
##############################

#
# This section creates the ZooKeeper, Hadoop, and Accumulo configuration files
# using templates in the templates directory and IP addresses from the EC2
# nodes that we created above and variables.
#
module "config_files" {
  source = "../modules/config-files"

  software_root = var.software_root
  upload_host   = var.private_network ? local.manager_private_ip : local.manager_ip
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

  accumulo_instance_name = var.accumulo_instance_name
  accumulo_root_password = var.accumulo_root_password
}

#
# This module uploads any local tarballs to the manager VM and
# stores them on the NFS share.
#
module "upload_software" {
  source = "../modules/upload-software"

  local_sources_dir = var.local_sources_dir
  upload_dir        = var.software_root
  upload_host       = var.private_network ? local.manager_private_ip : local.manager_ip
}

#
# This section performs final configuration of the Accumulo cluster.
#
module "configure_nodes" {
  source = "../modules/configure-nodes"

  software_root = var.software_root
  upload_host   = var.private_network ? local.manager_private_ip : local.manager_ip

  accumulo_instance_name = module.config_files.accumulo_instance_name
  accumulo_root_password = module.config_files.accumulo_root_password

  depends_on = [
    module.upload_software,
    module.config_files
  ]
}

####################################################
# Create the Route53 A record for the manager node #
####################################################

resource "aws_route53_record" "manager" {
  count   = var.create_route53_records ? 1 : 0
  zone_id = data.aws_route53_zone.private_zone[0].zone_id
  name    = "manager-${var.accumulo_branch_name}-${terraform.workspace}.${data.aws_route53_zone.private_zone[0].name}"
  type    = "A"
  ttl     = "300"
  records = [var.private_network ? local.manager_private_ip : local.manager_ip]
}

resource "aws_route53_record" "worker" {
  count   = var.create_route53_records ? length(local.worker_ips) : 0
  zone_id = data.aws_route53_zone.private_zone[0].zone_id
  name    = "worker${count.index}-${var.accumulo_branch_name}-${terraform.workspace}.${data.aws_route53_zone.private_zone[0].name}"
  type    = "A"
  ttl     = "300"
  records = [var.private_network ? local.worker_private_ips[count.index] : local.worker_ips[count.index]]
}

##############################
# Outputs                    #
##############################
output "manager_ip" {
  value       = var.private_network ? local.manager_private_ip : local.manager_ip
  description = "The IP address of the manager instance."
}

output "worker_ips" {
  value       = var.private_network ? local.worker_private_ips : local.worker_ips
  description = "The IP addresses of the worker instances."
}

output "accumulo_root_password" {
  value       = module.config_files.accumulo_root_password
  description = "The supplied, or automatically generated Accumulo root user password."
}
