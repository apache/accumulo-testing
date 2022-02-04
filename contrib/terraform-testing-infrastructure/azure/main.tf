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
      version = "~> 2.91.0"
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

  # Save the public/private IP addresses of the VMs to pass to sub-modules.
  manager_ip         = azurerm_linux_virtual_machine.manager.public_ip_address
  worker_ips         = azurerm_linux_virtual_machine.workers.*.public_ip_address
  manager_private_ip = azurerm_linux_virtual_machine.manager.private_ip_address
  worker_private_ips = azurerm_linux_virtual_machine.workers.*.private_ip_address

  # This script is run on all node to ensure a "ready" state.
  # Ready means ready to continue provisioning.
  ready_script = [
    "echo Waiting for cloud init to complete...",
    "cloud-init status --wait > /dev/null",
    "cloud-init status --long"
  ]
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
  resource_group_name = azurerm_resource_group.rg[0].name
  location            = azurerm_resource_group.rg[0].location
  address_space       = var.network_address_space
}

# Create a subnet for this cluster. Give storage a service endpoint
# so that we'll be able to create an NFS share.
resource "azurerm_subnet" "internal" {
  name                 = "${var.resource_name_prefix}-subnet"
  resource_group_name  = azurerm_resource_group.rg[0].name
  virtual_network_name = azurerm_virtual_network.accumulo_vnet.name
  address_prefixes     = var.subnet_address_prefixes
}

# Create a Network Security Group that only allows SSH (22)
# traffic from the internet and denies everything else.
resource "azurerm_network_security_group" "nsg" {
  name                = "${var.resource_name_prefix}-nsg"
  location            = azurerm_resource_group.rg[0].location
  resource_group_name = azurerm_resource_group.rg[0].name

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
  os_type              = local.os_type
}

# Create a static public IP address for the manager node.
resource "azurerm_public_ip" "manager" {
  name                = "${var.resource_name_prefix}-manager-ip"
  resource_group_name = azurerm_resource_group.rg[0].name
  location            = azurerm_resource_group.rg[0].location
  allocation_method   = "Static"
}

# Create a NIC for the manager node.
resource "azurerm_network_interface" "manager" {
  name                = "${var.resource_name_prefix}-manager-nic"
  location            = azurerm_resource_group.rg[0].location
  resource_group_name = azurerm_resource_group.rg[0].name

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
  resource_group_name = azurerm_resource_group.rg[0].name
  location            = azurerm_resource_group.rg[0].location
  allocation_method   = "Static"
}

# Create a NIC for each of the worker nodes.
resource "azurerm_network_interface" "workers" {
  count               = var.worker_count
  name                = "${var.resource_name_prefix}-worker${count.index}-nic"
  location            = azurerm_resource_group.rg[0].location
  resource_group_name = azurerm_resource_group.rg[0].name

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
  resource_group_name = azurerm_resource_group.rg[0].name
  location            = azurerm_resource_group.rg[0].location
  size                = var.vm_sku
  computer_name       = "manager"
  admin_username      = var.admin_username
  custom_data         = base64encode(module.cloud_init_config.cloud_init_data)

  disable_password_authentication = true

  network_interface_ids = [
    azurerm_network_interface.manager.id,
  ]

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.authorized_ssh_keys[0]
  }

  os_disk {
    storage_account_type = "Standard_LRS"
    caching              = "ReadWrite"
  }

  source_image_reference {
    publisher = var.vm_image.publisher
    offer     = var.vm_image.offer
    sku       = var.vm_image.sku
    version   = var.vm_image.version
  }

  provisioner "remote-exec" {
    inline = local.ready_script
    connection {
      type = "ssh"
      user = self.admin_username
      host = self.public_ip_address
    }
  }
}

# Create the worker VMs.
# Add a login user that can SSH to the VM using the first supplied SSH key.
resource "azurerm_linux_virtual_machine" "workers" {
  count               = var.worker_count
  name                = "${var.resource_name_prefix}-worker${count.index}"
  resource_group_name = azurerm_resource_group.rg[0].name
  location            = azurerm_resource_group.rg[0].location
  size                = var.vm_sku
  computer_name       = "worker${count.index}"
  admin_username      = var.admin_username
  custom_data         = base64encode(module.cloud_init_config.cloud_init_data)

  disable_password_authentication = true

  network_interface_ids = [
    azurerm_network_interface.workers[count.index].id
  ]

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.authorized_ssh_keys[0]
  }

  os_disk {
    storage_account_type = "Standard_LRS"
    caching              = "ReadWrite"
  }

  source_image_reference {
    publisher = var.vm_image.publisher
    offer     = var.vm_image.offer
    sku       = var.vm_image.sku
    version   = var.vm_image.version
  }

  provisioner "remote-exec" {
    inline = local.ready_script
    connection {
      type = "ssh"
      user = self.admin_username
      host = self.public_ip_address
    }
  }
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

  os_type = local.os_type

  software_root = var.software_root
  upload_ip     = local.manager_ip
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

##############################
# Outputs                    #
##############################
output "manager_ip" {
  value = local.manager_ip
}

output "worker_ips" {
  value = local.worker_ips
}
