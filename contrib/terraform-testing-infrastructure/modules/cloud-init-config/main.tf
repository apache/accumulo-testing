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
variable "os_type" {
  default = "centos"
  type    = string
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
  ssh_keys = concat(var.authorized_ssh_keys, [tls_private_key.hadoop.public_key_openssh])
  cloud_init_script = templatefile("${path.module}/templates/cloud-init.tpl", {
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
}

output "cloud_init_data" {
  value = data.cloudinit_config.cfg.rendered
}


