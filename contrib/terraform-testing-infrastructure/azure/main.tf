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
# 1. Create a virtual network, subnet, and network security group in Azure.
#
# 2. Create a NIC (attached to the security group) and VM in Azure for the manager.
#
# 3. Create a NIC (attached to the security group) and VM in Azure for each worker node.
#
# 4. VMs are created with a customized cloud-init script, and we wait for this script to complete.
#
# 5. Upload config files and installs the software on each node.
#

################################
# Core Terraform Configuration #
################################

terraform {
  required_version = ">= 1.1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
  backend "azurerm" {
    resource_group_name  = "accumulo-testing-tf-state"
    storage_account_name = "accumulotesttfsteast"
    container_name       = "accumulo-testing-tf-state"
    key                  = "accumulo-testing/terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

locals {
  os_type = can(regex("^.*[Uu]buntu.*$", var.vm_image.offer)) ? "ubuntu" : "centos"

  ssh_keys = toset(concat(var.authorized_ssh_keys, [for k in var.authorized_ssh_key_files : file(k)]))

  # Resource group name and location
  # This is pulled either from the resource group that was created (if create_resource_group is true)
  # or from the resource group that already exists (if create_resource_group is false). Keeping
  # references to the resource group or data object rather than just using var.resource_group_name
  # allows for terraform to automatically create the dependency graph and wait for the resource group
  # to be created if necessary.
  rg_name = var.create_resource_group ? azurerm_resource_group.rg[0].name : data.azurerm_resource_group.existing_rg[0].name
  location = var.create_resource_group ? azurerm_resource_group.rg[0].location : data.azurerm_resource_group.existing_rg[0].location

  # Save the public/private IP addresses of the VMs to pass to sub-modules.
  manager_ip         = azurerm_linux_virtual_machine.manager.public_ip_address
  worker_ips         = azurerm_linux_virtual_machine.workers[*].public_ip_address
  manager_private_ip = azurerm_linux_virtual_machine.manager.private_ip_address
  worker_private_ips = azurerm_linux_virtual_machine.workers[*].private_ip_address

  # This script is run on all node to ensure a "ready" state.
  # Ready means ready to continue provisioning.
  ready_script = [
    "echo Waiting for cloud init to complete...",
    "sudo cloud-init status --wait > /dev/null",
    "sudo cloud-init status --long"
  ]
}

data "azurerm_resource_group" "existing_rg" {
  count = var.create_resource_group ? 0 : 1
  name = var.resource_group_name
}

# Place all resources in a resource group
resource "azurerm_resource_group" "rg" {
  count    = var.create_resource_group ? 1 : 0
  name     = var.resource_group_name
  location = var.location
}

#########################
# Network Configuration #
#########################

# Creates a virtual network for use by this cluster.
resource "azurerm_virtual_network" "accumulo_vnet" {
  name                = "${var.resource_name_prefix}-vnet"
  resource_group_name = local.rg_name
  location            = local.location
  address_space       = var.network_address_space
}

# Create a subnet for this cluster. Give storage a service endpoint
# so that we'll be able to create an NFS share.
resource "azurerm_subnet" "internal" {
  name                 = "${var.resource_name_prefix}-subnet"
  resource_group_name  = local.rg_name
  virtual_network_name = azurerm_virtual_network.accumulo_vnet.name
  address_prefixes     = var.subnet_address_prefixes
}

# Create a Network Security Group that only allows SSH (22)
# traffic from the internet and denies everything else.
resource "azurerm_network_security_group" "nsg" {
  name                = "${var.resource_name_prefix}-nsg"
  location            = local.location
  resource_group_name = local.rg_name

  security_rule {
    name                       = "allow-ssh"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

####################
# VM Configuration #
####################

# Generate cloud-init data to use when creating nodes.
module "cloud_init_config" {
  source = "../modules/cloud-init-config"

  lvm_mount_point      = var.managed_disk_configuration != null ? var.managed_disk_configuration.mount_point : null
  lvm_disk_count       = var.managed_disk_configuration != null ? var.managed_disk_configuration.disk_count : null
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
  os_type              = local.os_type
  cluster_type         = "azure"

  optional_cloudinit_config = var.optional_cloudinit_config
  cloudinit_merge_type      = var.cloudinit_merge_type
}

# Create a static public IP address for the manager node.
resource "azurerm_public_ip" "manager" {
  name                = "${var.resource_name_prefix}-manager-ip"
  resource_group_name = local.rg_name
  location            = local.location
  allocation_method   = "Static"
}

# Create a NIC for the manager node.
resource "azurerm_network_interface" "manager" {
  name                = "${var.resource_name_prefix}-manager-nic"
  location            = local.location
  resource_group_name = local.rg_name

  enable_accelerated_networking = true

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.internal.id
    public_ip_address_id          = azurerm_public_ip.manager.id
    private_ip_address_allocation = "Dynamic"
  }
}

# Associate the manager node's NIC with the network security group.
resource "azurerm_network_interface_security_group_association" "manager" {
  network_interface_id      = azurerm_network_interface.manager.id
  network_security_group_id = azurerm_network_security_group.nsg.id
}

# Create a static public IP for each of the worker nodes.
resource "azurerm_public_ip" "workers" {
  count               = var.worker_count
  name                = "${var.resource_name_prefix}-worker${count.index}-ip"
  resource_group_name = local.rg_name
  location            = local.location
  allocation_method   = "Static"
}

# Create a NIC for each of the worker nodes.
resource "azurerm_network_interface" "workers" {
  count               = var.worker_count
  name                = "${var.resource_name_prefix}-worker${count.index}-nic"
  location            = local.location
  resource_group_name = local.rg_name

  enable_accelerated_networking = true

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.internal.id
    public_ip_address_id          = azurerm_public_ip.workers[count.index].id
    private_ip_address_allocation = "Dynamic"
  }
}

# Associate each of the worker nodes' NIC with the network security group.
resource "azurerm_network_interface_security_group_association" "workers" {
  count                     = var.worker_count
  network_interface_id      = azurerm_network_interface.workers[count.index].id
  network_security_group_id = azurerm_network_security_group.nsg.id
}

# Create the manager VM.
# Add a login user that can SSH to the VM using the first supplied SSH key.
resource "azurerm_linux_virtual_machine" "manager" {
  name                = "${var.resource_name_prefix}-manager"
  resource_group_name = local.rg_name
  location            = local.location
  size                = var.vm_sku
  computer_name       = "manager"
  admin_username      = var.admin_username
  custom_data         = base64encode(module.cloud_init_config.cloud_init_data)

  disable_password_authentication = true

  network_interface_ids = [
    azurerm_network_interface.manager.id,
  ]

  dynamic "admin_ssh_key" {
    for_each = local.ssh_keys
    content {
      username   = var.admin_username
      public_key = admin_ssh_key.value
    }
  }

  os_disk {
    storage_account_type = var.os_disk_type
    caching              = var.os_disk_caching
    disk_size_gb         = var.os_disk_size_gb
  }

  source_image_reference {
    publisher = var.vm_image.publisher
    offer     = var.vm_image.offer
    sku       = var.vm_image.sku
    version   = var.vm_image.version
  }
}

# Create and attach managed disks to the manager VM.
resource "azurerm_managed_disk" "manager_managed_disk" {
  count                = var.managed_disk_configuration != null ? var.managed_disk_configuration.disk_count : 0
  name                 = format("%s_disk%02d", azurerm_linux_virtual_machine.manager.name, count.index)
  resource_group_name  = local.rg_name
  location             = local.location
  storage_account_type = var.managed_disk_configuration.storage_account_type
  disk_size_gb         = var.managed_disk_configuration.disk_size_gb
  create_option        = "Empty"
}

resource "azurerm_virtual_machine_data_disk_attachment" "manager_managed_disk_attachment" {
  count              = var.managed_disk_configuration != null ? var.managed_disk_configuration.disk_count : 0
  managed_disk_id    = azurerm_managed_disk.manager_managed_disk[count.index].id
  virtual_machine_id = azurerm_linux_virtual_machine.manager.id
  lun                = 10 + count.index
  caching            = "ReadOnly"
}

# Wait for cloud-init to complete on the manager VM.
# This is done here rather than in the VM resource because the cloud-init script
# waits for managed disks to be attached (if used), but the managed disks cannot
# be attached until the VM is created, so we'd have a deadlock.
resource "null_resource" "wait_for_manager_cloud_init" {
  provisioner "remote-exec" {
    inline = local.ready_script
    connection {
      type = "ssh"
      user = azurerm_linux_virtual_machine.manager.admin_username
      host = azurerm_linux_virtual_machine.manager.public_ip_address
    }
  }

  depends_on = [
    azurerm_virtual_machine_data_disk_attachment.manager_managed_disk_attachment
  ]
}

# Create the worker VMs.
# Add a login user that can SSH to the VM using the first supplied SSH key.
resource "azurerm_linux_virtual_machine" "workers" {
  count               = var.worker_count
  name                = "${var.resource_name_prefix}-worker${count.index}"
  resource_group_name = local.rg_name
  location            = local.location
  size                = var.vm_sku
  computer_name       = "worker${count.index}"
  admin_username      = var.admin_username
  custom_data         = base64encode(module.cloud_init_config.cloud_init_data)

  disable_password_authentication = true

  network_interface_ids = [
    azurerm_network_interface.workers[count.index].id
  ]

  dynamic "admin_ssh_key" {
    for_each = local.ssh_keys
    content {
      username   = var.admin_username
      public_key = admin_ssh_key.value
    }
  }

  os_disk {
    storage_account_type = var.os_disk_type
    caching              = var.os_disk_caching
    disk_size_gb         = var.os_disk_size_gb
  }

  source_image_reference {
    publisher = var.vm_image.publisher
    offer     = var.vm_image.offer
    sku       = var.vm_image.sku
    version   = var.vm_image.version
  }
}

# Create and attach managed disks to the worker VMs.
locals {
  worker_disks = var.managed_disk_configuration == null ? [] : flatten([
    for vm_num, vm in azurerm_linux_virtual_machine.workers : [
      for disk_num in range(var.managed_disk_configuration.disk_count) : {
        datadisk_name = format("%s_disk%02d", vm.name, disk_num)
        lun           = 10 + disk_num
        worker_num    = vm_num
      }
    ]
  ])
}

resource "azurerm_managed_disk" "worker_managed_disk" {
  count                = length(local.worker_disks)
  name                 = local.worker_disks[count.index].datadisk_name
  resource_group_name  = local.rg_name
  location             = local.location
  storage_account_type = var.managed_disk_configuration.storage_account_type
  disk_size_gb         = var.managed_disk_configuration.disk_size_gb
  create_option        = "Empty"
}

resource "azurerm_virtual_machine_data_disk_attachment" "worker_managed_disk_attachment" {
  count              = length(local.worker_disks)
  managed_disk_id    = azurerm_managed_disk.worker_managed_disk[count.index].id
  virtual_machine_id = azurerm_linux_virtual_machine.workers[local.worker_disks[count.index].worker_num].id
  lun                = local.worker_disks[count.index].lun
  caching            = "ReadOnly"
}

# Wait for cloud-init to complete on the worker VMs.
# This is done here rather than in the VM resources because the cloud-init script
# waits for managed disks to be attached (if used), but the managed disks cannot
# be attached until the VMs are created, so we'd have a deadlock.
resource "null_resource" "wait_for_workers_cloud_init" {
  count = length(azurerm_linux_virtual_machine.workers)
  provisioner "remote-exec" {
    inline = local.ready_script
    connection {
      type = "ssh"
      user = azurerm_linux_virtual_machine.workers[count.index].admin_username
      host = azurerm_linux_virtual_machine.workers[count.index].public_ip_address
    }
  }

  depends_on = [
    azurerm_virtual_machine_data_disk_attachment.worker_managed_disk_attachment
  ]
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

  os_type = local.os_type

  software_root = var.software_root
  upload_host   = local.manager_ip
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

  depends_on = [
    null_resource.wait_for_manager_cloud_init
  ]
}

#
# This module uploads any local tarballs to the manager VM and
# stores them on the NFS share.
#
module "upload_software" {
  source = "../modules/upload-software"

  local_sources_dir = var.local_sources_dir
  upload_dir        = var.software_root
  upload_host       = local.manager_ip

  depends_on = [
    null_resource.wait_for_manager_cloud_init
  ]
}

#
# This section performs final configuration of the Accumulo cluster.
#
module "configure_nodes" {
  source = "../modules/configure-nodes"

  software_root = var.software_root
  upload_host   = local.manager_ip

  accumulo_instance_name = module.config_files.accumulo_instance_name
  accumulo_root_password = module.config_files.accumulo_root_password

  depends_on = [
    module.upload_software,
    module.config_files,
    null_resource.wait_for_workers_cloud_init
  ]
}

##############################
# Outputs                    #
##############################
output "manager_ip" {
  value       = local.manager_ip
  description = "The public IP address of the manager VM."
}

output "worker_ips" {
  value       = local.worker_ips
  description = "The public IP addresses of the worker VMs."
}

output "accumulo_root_password" {
  value       = module.config_files.accumulo_root_password
  description = "The user-supplied or automatically generated Accumulo root user password."
}
