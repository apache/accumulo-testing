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

variable "instance_count" {
  default     = "2"
  description = "The number of EC2 instances to create"
  nullable    = false
}

variable "instance_type" {
  default     = "m5.2xlarge"
  description = "The type of EC2 instances to create"
  nullable    = false
}

variable "root_volume_gb" {
  default     = "300"
  description = "The size, in GB, of the EC2 instance root volume"
  nullable    = false
}

variable "software_root" {
  default     = "/opt/accumulo-testing"
  description = "The full directory root where software will be installed"
  nullable    = false
}

variable "security_group" {
  description = "The Security Group to use when creating AWS objects"
  nullable    = false
}

variable "us_east_1b_subnet" {
  description = "The AWS subnet id for the us-east-1b subnet"
  nullable    = false
}

variable "us_east_1e_subnet" {
  description = "The AWS subnet id for the us-east-1e subnet"
  nullable    = false
}

variable "route53_zone" {
  description = "The name of the Route53 zone in which to create DNS addresses"
  nullable    = false
}

variable "create_route53_records" {
  default     = false
  description = "Indicates whether or not route53 records will be created"
  type        = bool
  nullable    = false
}

variable "private_network" {
  default     = false
  description = "Indicates whether or not the user is on a private network and access to hosts should be through the private IP addresses rather than public ones."
  type        = bool
  nullable    = false
}

variable "ami_owner" {
  description = "The id of the AMI owner"
  nullable    = false
}

variable "ami_name_pattern" {
  description = "The pattern of the name of the AMI to use"
  nullable    = false
}

variable "authorized_ssh_keys" {
  description = "List of SSH keys for the developers that will log into the cluster"
  type        = list(string)
  nullable    = false
}

variable "authorized_ssh_key_files" {
  default     = []
  description = "List of SSH public key files for the developers that will log into the cluster"
  type        = list(string)
  nullable    = false
}

variable "accumulo_instance_name" {
  default     = "accumulo-testing"
  type        = string
  description = "The accumulo instance name."
  nullable    = false
}

variable "accumulo_root_password" {
  default     = null
  type        = string
  description = "The password for the accumulo root user. A randomly generated password will be used if none is specified here."
  nullable    = true
}

variable "zookeeper_dir" {
  default     = "/data/zookeeper"
  description = "The ZooKeeper directory on each EC2 node"
  nullable    = false
}

variable "hadoop_dir" {
  default     = "/data/hadoop"
  description = "The Hadoop directory on each EC2 node"
  nullable    = false
}

variable "accumulo_dir" {
  default     = "/data/accumulo"
  description = "The Accumulo directory on each EC2 node"
  nullable    = false
}

variable "maven_version" {
  default     = "3.8.6"
  description = "The version of Maven to download and install"
  nullable    = false
}

variable "zookeeper_version" {
  default     = "3.8.0"
  description = "The version of ZooKeeper to download and install"
  nullable    = false
}

variable "hadoop_version" {
  default     = "3.3.4"
  description = "The version of Hadoop to download and install"
  nullable    = false
}

variable "accumulo_version" {
  default     = "2.1.0-SNAPSHOT"
  description = "The branch of Accumulo to download and install"
  nullable    = false
}

variable "accumulo_repo" {
  default     = "https://github.com/apache/accumulo.git"
  description = "URL of the Accumulo git repo"
  nullable    = false
}

variable "accumulo_branch_name" {
  default     = "main"
  description = "The name of the branch to build and install"
  nullable    = false
}

variable "accumulo_testing_repo" {
  default     = "https://github.com/apache/accumulo-testing.git"
  description = "URL of the Accumulo Testing git repo"
  nullable    = false
}

variable "accumulo_testing_branch_name" {
  default     = "main"
  description = "The name of the branch to build and install"
  nullable    = false
}

variable "local_sources_dir" {
  default     = ""
  description = "Directory on local machine that contains Maven, ZooKeeper or Hadoop binary distributions or Accumulo source tarball"
  nullable    = true
}

variable "optional_cloudinit_config" {
  default     = null
  type        = string
  description = "An optional config block for the cloud-init script. If you set this, you should consider setting cloudinit_merge_type to handle merging with the default script as you need."
  nullable    = true
}

variable "cloudinit_merge_type" {
  default     = null
  type        = string
  description = "Describes the merge behavior for overlapping config blocks in cloud-init."
  nullable    = true
}
