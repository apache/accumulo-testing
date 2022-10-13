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

variable "create_resource_group" {
  default     = true
  type        = bool
  description = "Indicates whether or not resource_group_name should be created or is an existing resource group."
  nullable    = false
}

variable "resource_name_prefix" {
  default     = "accumulo-testing"
  type        = string
  description = "A prefix applied to all resource names created by this template."
  nullable    = false
}

variable "resource_group_name" {
  default     = ""
  type        = string
  description = "The name of the resource group to create or reuse. If not specified, the name is generated based on resource_name_prefix."
  nullable    = false
}

variable "location" {
  type        = string
  description = "The Azure region where resources are to be created. If an existing resource group is specified, this value is ignored and the resource group's location is used."
  nullable    = false
}

variable "network_address_space" {
  default     = ["10.0.0.0/16"]
  type        = list(string)
  description = "The network address space to use for the virtual network."
  nullable    = false
}

variable "subnet_address_prefixes" {
  default     = ["10.0.2.0/24"]
  type        = list(string)
  description = "The subnet address prefixes to use for the accumulo testing subnet."
  nullable    = false
}

variable "worker_count" {
  default     = 1
  type        = number
  description = "The number of worker VMs to create"
  nullable    = false
  validation {
    condition     = var.worker_count > 0
    error_message = "The number of VMs must be at least 1."
  }
}

variable "vm_sku" {
  default     = "Standard_D8s_v4"
  description = "The SKU of Azure VMs to create"
  nullable    = false
}

variable "admin_username" {
  default     = "azureuser"
  type        = string
  description = "The username of the admin user, that can be authenticated with the first public ssh key."
  nullable    = false
}

variable "vm_image" {
  default = {
    "publisher" = "Canonical"
    "offer"     = "0001-com-ubuntu-server-focal"
    "sku"       = "20_04-lts-gen2"
    "version"   = "latest"
  }
  type = object({
    publisher = string
    offer     = string
    sku       = string
    version   = string
  })
}

variable "os_disk_size_gb" {
  default     = 300
  type        = number
  description = "The size, in GB, of the OS disk"
  nullable    = false
  validation {
    condition     = var.os_disk_size_gb >= 30
    error_message = "The OS disk size must be >= 30GB."
  }
}

variable "os_disk_type" {
  default     = "Standard_LRS"
  type        = string
  description = "The disk type to use for OS disks. Possible values are Standard_LRS, StandardSSD_LRS, and Premium_LRS."
  validation {
    condition     = contains(["Standard_LRS", "StandardSSD_LRS", "Premium_LRS"], var.os_disk_type)
    error_message = "The value of os_disk_type must be one of Standard_LRS, StandardSSD_LRS, or Premium_LRS."
  }
}

variable "os_disk_caching" {
  default     = "ReadOnly"
  type        = string
  description = "The type of caching to use for the OS disk. Possible values are None, ReadOnly, and ReadWrite."
  validation {
    condition     = contains(["None", "ReadOnly", "ReadWrite"], var.os_disk_caching)
    error_message = "The value of os_disk_caching must be one of None, ReadOnly, or ReadWrite."
  }
}

variable "managed_disk_configuration" {
  default = null
  type = object({
    mount_point          = string
    disk_count           = number
    storage_account_type = string
    disk_size_gb         = number
  })
  description = "Optional managed disk configuration. If supplied, the managed disks on each VM will be combined into an LVM volume mounted at the named mount point."
  nullable    = true

  validation {
    condition     = var.managed_disk_configuration == null || can(var.managed_disk_configuration.mount_point != null)
    error_message = "The mount point must be specified."
  }
  validation {
    condition     = var.managed_disk_configuration == null || can(var.managed_disk_configuration.disk_count > 0)
    error_message = "The number of disks must be at least 1."
  }
  validation {
    condition     = var.managed_disk_configuration == null || can(contains(["Standard_LRS", "StandardSSD_LRS", "Premium_LRS"], var.managed_disk_configuration.storage_account_type))
    error_message = "The storage account type must be one of 'Standard_LRS', 'StandardSSD_LRS', or 'Premium_LRS'."
  }
  validation {
    condition     = var.managed_disk_configuration == null || can(var.managed_disk_configuration.disk_size_gb > 0 && var.managed_disk_configuration.disk_size_gb <= 32767)
    error_message = "The disk size must be at least 1GB and less than 32768GB."
  }
}

variable "software_root" {
  default     = "/opt/accumulo-testing"
  description = "The full directory root where software will be installed"
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
  description = "The ZooKeeper directory on each node"
  nullable    = false
}

variable "hadoop_dir" {
  default     = "/data/hadoop"
  description = "The Hadoop directory on each node"
  nullable    = false
}

variable "accumulo_dir" {
  default     = "/data/accumulo"
  description = "The Accumulo directory on each node"
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
