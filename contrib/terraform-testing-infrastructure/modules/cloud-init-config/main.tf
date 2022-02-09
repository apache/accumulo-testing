#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

variable "software_root" {}
variable "zookeeper_dir" {}
variable "hadoop_dir" {}
variable "accumulo_dir" {}
variable "maven_version" {}
variable "zookeeper_version" {}
variable "hadoop_version" {}
variable "accumulo_branch_name" {}
variable "accumulo_version" {}
variable "authorized_ssh_keys" {}
variable "cloudinit_merge_type" {
  default  = "dict(recurse_array,no_replace)+list(append)"
  nullable = false
}
variable "optional_cloudinit_config" {
  default  = ""
  nullable = false
}
variable "os_type" {
  default  = "centos"
  type     = string
  nullable = false
  validation {
    condition     = contains(["centos", "ubuntu"], var.os_type)
    error_message = "The value of os_type must be either 'centos' or 'ubuntu'."
  }
}

#####################
# Create Hadoop Key #
#####################
resource "tls_private_key" "hadoop" {
  algorithm = "RSA"
  rsa_bits  = "4096"
}

################################
# Generate Cloud Init Template #
################################
locals {
  ssh_keys = concat(var.authorized_ssh_keys, tls_private_key.hadoop.public_key_openssh[*])
  cloud_init_script = templatefile("${path.module}/templates/cloud-init.tftpl", {
    files_path           = "${path.module}/files"
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
    os_type              = var.os_type
    hadoop_public_key    = indent(6, tls_private_key.hadoop.public_key_openssh)
    hadoop_private_key   = indent(6, tls_private_key.hadoop.private_key_pem)
  })
}

data "cloudinit_config" "cfg" {
  gzip          = false
  base64_encode = false
  part {
    filename     = "init.cfg"
    content_type = "text/cloud-config"
    content      = local.cloud_init_script
  }

  # Allow for a user-specified cloud-init script to be passed in.
  # This will always be included, but if it's empty then cloud-init
  # will ignore it.
  part {
    filename     = "userdefined.cfg"
    content_type = "text/cloud-config"
    merge_type   = var.cloudinit_merge_type
    content      = var.optional_cloudinit_config
  }
}

output "cloud_init_data" {
  value = data.cloudinit_config.cfg.rendered
}


