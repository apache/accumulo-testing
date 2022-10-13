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

variable "os_type" {
  default = "centos"
}
variable "software_root" {}
variable "upload_host" {}
variable "manager_ip" {}
variable "worker_ips" {}

variable "zookeeper_dir" {}
variable "hadoop_dir" {}
variable "accumulo_dir" {}

variable "maven_version" {}
variable "zookeeper_version" {}
variable "hadoop_version" {}
variable "accumulo_version" {}

variable "accumulo_repo" {}
variable "accumulo_branch_name" {}
variable "accumulo_testing_repo" {}
variable "accumulo_testing_branch_name" {}

variable "accumulo_instance_name" {
  default  = "accumulo-testing"
  type     = string
  nullable = false
}
variable "accumulo_root_password" {
  default  = ""
  type     = string
  nullable = false
}


locals {
  conf_dir      = "${path.module}/conf"
  files_dir     = "${path.module}/files"
  templates_dir = "${path.module}/templates"

  java_home        = var.os_type == "ubuntu" ? "/usr/lib/jvm/java-11-openjdk-amd64" : "/usr/lib/jvm/java-11-openjdk"
  accumulo_root_pw = coalesce(var.accumulo_root_password, random_string.accumulo_root_password.result)

  template_vars = {
    manager_ip                   = var.manager_ip
    worker_ips                   = var.worker_ips
    java_home                    = local.java_home
    accumulo_branch_name         = var.accumulo_branch_name
    accumulo_dir                 = var.accumulo_dir
    accumulo_repo                = var.accumulo_repo
    accumulo_testing_repo        = var.accumulo_testing_repo
    accumulo_testing_branch_name = var.accumulo_testing_branch_name
    accumulo_version             = var.accumulo_version
    software_root                = var.software_root
    hadoop_dir                   = var.hadoop_dir
    hadoop_version               = var.hadoop_version
    maven_version                = var.maven_version
    zookeeper_dir                = var.zookeeper_dir
    zookeeper_version            = var.zookeeper_version
    accumulo_instance_name       = var.accumulo_instance_name
    accumulo_root_password       = local.accumulo_root_pw,
    accumulo_instance_secret     = random_string.accumulo_instance_secret.result
  }
}

resource "random_string" "accumulo_root_password" {
  length  = 12
  special = false
}

resource "random_string" "accumulo_instance_secret" {
  length  = 12
  special = false
}

resource "local_file" "etc-hosts" {
  filename        = "${local.conf_dir}/hosts"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/hosts.tftpl", local.template_vars)
}

resource "local_file" "etc-genders" {
  filename        = "${local.conf_dir}/genders"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/genders.tftpl", local.template_vars)
}

resource "local_file" "zookeeper-config" {
  filename        = "${local.conf_dir}/zoo.cfg"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/zoo.cfg.tftpl", local.template_vars)
}

resource "local_file" "hadoop-core-config" {
  filename        = "${local.conf_dir}/core-site.xml"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/core-site.xml.tftpl", local.template_vars)
}

resource "local_file" "hadoop-hdfs-config" {
  filename        = "${local.conf_dir}/hdfs-site.xml"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/hdfs-site.xml.tftpl", local.template_vars)
}

resource "local_file" "hadoop-yarn-config" {
  filename        = "${local.conf_dir}/yarn-site.xml"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/yarn-site.xml.tftpl", local.template_vars)
}

resource "local_file" "accumulo-cluster-config" {
  filename        = "${local.conf_dir}/cluster.yaml"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/cluster.yaml.tftpl", local.template_vars)
}

resource "local_file" "accumulo-properties-config" {
  filename        = "${local.conf_dir}/accumulo.properties"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/accumulo-properties.tftpl", local.template_vars)
}

resource "local_file" "accumulo-client-properties-config" {
  filename        = "${local.conf_dir}/accumulo-client.properties"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/accumulo-client-properties.tftpl", local.template_vars)
}

resource "local_file" "telegraf-config" {
  filename        = "${local.conf_dir}/telegraf.conf"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/telegraf.conf.tftpl", local.template_vars)
}

resource "local_file" "namenode-systemd" {
  filename        = "${local.conf_dir}/hadoop-namenode.service"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/hadoop-namenode.service.tftpl", local.template_vars)
}

resource "local_file" "datanode-systemd" {
  filename        = "${local.conf_dir}/hadoop-datanode.service"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/hadoop-datanode.service.tftpl", local.template_vars)
}

resource "local_file" "resourcemanager-systemd" {
  filename        = "${local.conf_dir}/yarn-resourcemanager.service"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/yarn-resourcemanager.service.tftpl", local.template_vars)
}

resource "local_file" "nodemanager-systemd" {
  filename        = "${local.conf_dir}/yarn-nodemanager.service"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/yarn-nodemanager.service.tftpl", local.template_vars)
}

resource "local_file" "zookeeper-systemd" {
  filename        = "${local.conf_dir}/zookeeper.service"
  file_permission = "644"
  content         = templatefile("${local.templates_dir}/zookeeper.service.tftpl", local.template_vars)
}

resource "local_file" "hadoop-bash-profile" {
  filename        = "${local.conf_dir}/hadoop_bash_profile"
  file_permission = "600"
  content         = templatefile("${local.templates_dir}/hadoop_bash_profile.tftpl", local.template_vars)
}

resource "local_file" "hadoop-bashrc" {
  filename        = "${local.conf_dir}/hadoop_bashrc"
  file_permission = "600"
  content         = templatefile("${local.templates_dir}/hadoop_bashrc.tftpl", local.template_vars)
}

resource "local_file" "install-software" {
  filename        = "${local.conf_dir}/install_sw.sh"
  file_permission = "755"
  content         = templatefile("${local.templates_dir}/install_sw.sh.tftpl", local.template_vars)
}

resource "local_file" "initialize-hadoop" {
  filename        = "${local.conf_dir}/initialize-hadoop.sh"
  file_permission = "755"
  content         = templatefile("${local.templates_dir}/initialize_hadoop.sh.tftpl", local.template_vars)
}

resource "local_file" "initialize-accumulo" {
  filename        = "${local.conf_dir}/initialize-accumulo.sh"
  file_permission = "755"
  content         = templatefile("${local.templates_dir}/initialize_accumulo.sh.tftpl", local.template_vars)
}

resource "null_resource" "upload_config_files" {
  depends_on = [
    local_file.etc-hosts,
    local_file.etc-genders,
    local_file.zookeeper-config,
    local_file.hadoop-core-config,
    local_file.hadoop-hdfs-config,
    local_file.hadoop-yarn-config,
    local_file.accumulo-cluster-config,
    local_file.accumulo-properties-config,
    local_file.accumulo-client-properties-config,
    local_file.telegraf-config,
    local_file.namenode-systemd,
    local_file.datanode-systemd,
    local_file.resourcemanager-systemd,
    local_file.nodemanager-systemd,
    local_file.zookeeper-systemd,
    local_file.hadoop-bash-profile,
    local_file.hadoop-bashrc,
    local_file.install-software,
    local_file.initialize-hadoop,
    local_file.initialize-accumulo
  ]
  connection {
    type = "ssh"
    host = var.upload_host
    user = "hadoop"
  }
  provisioner "remote-exec" {
    inline = [
      "mkdir -p ${var.software_root}/grafana/dashboards"
    ]
  }
  provisioner "file" {
    source      = local.conf_dir
    destination = var.software_root
  }
  provisioner "file" {
    source      = "${local.files_dir}/grafana_dashboards/"
    destination = "${var.software_root}/grafana/dashboards/"
  }
}

output "conf_dir" {
  value = local.conf_dir
}

output "accumulo_instance_name" {
  value = var.accumulo_instance_name
}

output "accumulo_root_password" {
  value = local.accumulo_root_pw
}
