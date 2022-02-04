variable "create_resource_group" {
  default = true
  type = bool
  description = "Indicates whether or not resource_group_name should be created or is an existing resource group."
  nullable = false
}

variable "resource_name_prefix" {
  default = "accumulo-testing"
  type = string
  description = "A prefix applied to all resource names created by this template."
  nullable = false
}

variable "resource_group_name" {
  default = ""
  type = string
  description = "The name of the resource group to create or reuse. If not specified, the name is generated based on resource_name_prefix."
  nullable = false
}

variable "location" {
  type = string
  description = "The Azure region where resources are to be created. If an existing resource group is specified, this value is ignored and the resource group's location is used."
  nullable = false
}

variable "network_address_space" {
  default = ["10.0.0.0/16"]
  type = list(string)
  description = "The network address space to use for the virtual network."
  nullable = false
}

variable "subnet_address_prefixes" {
  default = ["10.0.2.0/24"]
  type = list(string)
  description = "The subnet address prefixes to use for the accumulo testing subnet."
  nullable = false
}

variable "worker_count" {
  default = 1
  type = number
  description = "The number of worker VMs to create"
  nullable = false
  validation {
    condition = var.worker_count > 0
    error_message = "The number of VMs must be at least 1."
  }
}

variable "vm_sku" {
  default = "Standard_D8s_v4"
  description = "The SKU of Azure VMs to create"
  nullable = false
}

variable "admin_username" {
  default = "azureuser"
  type = string
  description = "The username of the admin user, that can be authenticated with the first public ssh key."
  nullable = false
}

variable "vm_image" {
  default = {
    "publisher" = "Canonical"
    "offer" = "0001-com-ubuntu-server-focal"
    "sku" = "20_04-lts-gen2"
    "version" = "latest"
  }
  type = object({
    publisher = string
    offer     = string
    sku       = string
    version   = string
  })
}

variable "root_volume_gb" {
  default = 30
  type = number
  description = "The size, in GB, of the instance root volume"
  nullable = false
  validation {
    condition = var.root_volume_gb >= 30
    error_message = "The root volume size must be >= 30GB."
  }
}

variable "software_root" {
  default = "/opt/accumulo-testing"
  description = "The full directory root where software will be installed"
  nullable = false
}

variable "authorized_ssh_keys" {
  description = "List of SSH keys for the developers that will log into the cluster"
  type = list(string)
  nullable = false
}

variable "zookeeper_dir" {
  default = "/data/zookeeper"
  description = "The ZooKeeper directory on each node"
  nullable = false
}

variable "hadoop_dir" {
  default = "/data/hadoop"
  description = "The Hadoop directory on each node"
  nullable = false
}

variable "accumulo_dir" {
  default = "/data/accumulo"
  description = "The Accumulo directory on each node"
  nullable = false
}

variable "maven_version" {
  default = "3.8.4"
  description = "The version of Maven to download and install"
  nullable = false
}

variable "zookeeper_version" {
  default = "3.5.9"
  description = "The version of ZooKeeper to download and install"
  nullable = false
}

variable "hadoop_version" {
  default = "3.3.1"
  description = "The version of Hadoop to download and install"
  nullable = false
}

variable "accumulo_version" {
  default = "2.1.0-SNAPSHOT"
  description = "The branch of Accumulo to download and install"
  nullable = false
}

variable "accumulo_repo" {
  default = "https://github.com/apache/accumulo.git"
  description = "URL of the Accumulo git repo"
  nullable = false
}

variable "accumulo_branch_name" {
  default = "main"
  description = "The name of the branch to build and install"
  nullable = false
}

variable "accumulo_testing_repo" {
  default = "https://github.com/apache/accumulo-testing.git"
  description = "URL of the Accumulo Testing git repo"
  nullable = false
}

variable "accumulo_testing_branch_name" {
  default = "main"
  description = "The name of the branch to build and install"
  nullable = false
}

variable "local_sources_dir" {
  default = ""
  description = "Directory on local machine that contains Maven, ZooKeeper or Hadoop binary distributions or Accumulo source tarball"
  nullable = true
}
