variable "os_type" {
  default = "centos"
}
variable "software_root" {}
variable "upload_ip" {}
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

locals {
  conf_dir      = "${path.module}/conf"
  files_dir     = "${path.module}/files"
  templates_dir = "${path.module}/templates"

  java_home = var.os_type == "ubuntu" ? "/usr/lib/jvm/java-11-openjdk-amd64" : "/usr/lib/jvm/java-11-openjdk"
}

resource "local_file" "etc-hosts" {
  filename        = "${local.conf_dir}/hosts"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/hosts.tpl",
    {
      manager_ip = var.manager_ip
      worker_ips = var.worker_ips
    }
  )
}

resource "local_file" "etc-genders" {
  filename        = "${local.conf_dir}/genders"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/genders.tpl",
    {
      manager_ip = var.manager_ip
      worker_ips = var.worker_ips
    }
  )
}

resource "local_file" "zookeeper-config" {
  filename        = "${local.conf_dir}/zoo.cfg"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/zoo.cfg.tpl",
    {
      zookeeper_dir = var.zookeeper_dir
    }
  )
}

resource "local_file" "hadoop-core-config" {
  filename        = "${local.conf_dir}/core-site.xml"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/core-site.xml.tpl",
    {
      manager_ip = var.manager_ip
      hadoop_dir = var.hadoop_dir
    }
  )
}

resource "local_file" "hadoop-hdfs-config" {
  filename        = "${local.conf_dir}/hdfs-site.xml"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/hdfs-site.xml.tpl",
    {
      manager_ip = var.manager_ip
      hadoop_dir = var.hadoop_dir
    }
  )
}

resource "local_file" "accumulo-cluster-config" {
  filename        = "${local.conf_dir}/cluster.yaml"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/cluster.yaml.tpl",
    {
      manager_ip = var.manager_ip
      worker_ips = var.worker_ips
    }
  )
}

resource "local_file" "accumulo-properties-config" {
  filename        = "${local.conf_dir}/accumulo.properties"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/accumulo-properties.tpl",
    {
      manager_ip = var.manager_ip
    }
  )
}

resource "local_file" "telegraf-config" {
  filename        = "${local.conf_dir}/telegraf.conf"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/telegraf.conf.tpl",
    {
      manager_ip = var.manager_ip
    }
  )
}

resource "local_file" "namenode-systemd" {
  filename        = "${local.conf_dir}/hadoop-namenode.service"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/hadoop-namenode.service.tpl",
    {
      java_home      = local.java_home
      software_root  = var.software_root
      hadoop_dir     = var.hadoop_dir
      hadoop_version = var.hadoop_version
    }
  )
}

resource "local_file" "datanode-systemd" {
  filename        = "${local.conf_dir}/hadoop-datanode.service"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/hadoop-datanode.service.tpl",
    {
      java_home      = local.java_home
      software_root  = var.software_root
      hadoop_dir     = var.hadoop_dir
      hadoop_version = var.hadoop_version
    }
  )
}

resource "local_file" "zookeeper-systemd" {
  filename        = "${local.conf_dir}/zookeeper.service"
  file_permission = "644"
  content = templatefile("${local.templates_dir}/zookeeper.service.tpl",
    {
      java_home         = local.java_home
      software_root     = var.software_root
      zookeeper_dir     = var.zookeeper_dir
      zookeeper_version = var.zookeeper_version
    }
  )
}

resource "local_file" "hadoop-bash-profile" {
  filename        = "${local.conf_dir}/hadoop_bash_profile"
  file_permission = "600"
  content = templatefile("${local.templates_dir}/hadoop_bash_profile.tpl",
    {
    }
  )
}

resource "local_file" "hadoop-bashrc" {
  filename        = "${local.conf_dir}/hadoop_bashrc"
  file_permission = "600"
  content = templatefile("${local.templates_dir}/hadoop_bashrc.tpl",
    {
      java_home         = local.java_home
      accumulo_version  = var.accumulo_version
      accumulo_dir      = var.accumulo_dir
      software_root     = var.software_root
      hadoop_version    = var.hadoop_version
      manager_ip        = var.manager_ip
      maven_version     = var.maven_version
      zookeeper_version = var.zookeeper_version
    }
  )
}

resource "local_file" "install-software" {
  filename        = "${local.conf_dir}/install_sw.sh"
  file_permission = "755"
  content = templatefile("${local.templates_dir}/install_sw.sh.tpl",
    {
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
    }
  )
}

resource "null_resource" "upload_config_files" {
  depends_on = [
    local_file.etc-hosts,
    local_file.etc-genders,
    local_file.zookeeper-config,
    local_file.hadoop-core-config,
    local_file.hadoop-hdfs-config,
    local_file.accumulo-cluster-config,
    local_file.accumulo-properties-config,
    local_file.telegraf-config,
    local_file.namenode-systemd,
    local_file.datanode-systemd,
    local_file.zookeeper-systemd,
    local_file.hadoop-bash-profile,
    local_file.hadoop-bashrc,
    local_file.install-software
  ]
  connection {
    type = "ssh"
    host = var.upload_ip
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
    source      = "${local.files_dir}/accumulo-dashboard.json"
    destination = "${var.software_root}/grafana/dashboards/accumulo-dashboard.json"
  }
  provisioner "file" {
    source      = "${local.files_dir}/accumulo-dashboard.yaml"
    destination = "${var.software_root}/grafana/dashboards/accumulo-dashboard.yaml"
  }
}

output "conf_dir" {
  value = local.conf_dir
}
