variable "software_root" {}
variable "manager_ip" {}
variable "worker_ips" {}
variable "accumulo_dir" {}

locals {
  all_ips = concat([var.manager_ip], var.worker_ips)
}

#####################################################
# Run the install software script on the first node #
#####################################################

#
# This connects to the first node and runs the install_sw.sh script.
#
resource "null_resource" "configure_manager_node" {
  connection {
    type = "ssh"
    host = var.manager_ip
    user = "hadoop"
  }
  provisioner "remote-exec" {
    inline = [<<-EOT
      sudo bash -c 'cat ${var.software_root}/conf/hosts >> /etc/hosts'
      sudo bash -c 'cat ${var.software_root}/conf/genders >> /etc/genders'
      cp ${var.software_root}/conf/hadoop_bash_profile /home/hadoop/.bash_profile
      cp ${var.software_root}/conf/hadoop_bashrc /home/hadoop/.bashrc

      source /home/hadoop/.bash_profile

      pdcp -g worker /home/hadoop/.bashrc /home/hadoop/.bash_profile /home/hadoop/.
      pdcp -g worker ${var.software_root}/conf/hosts ${var.software_root}/conf/genders /tmp/.
      pdsh -g worker sudo bash -c 'cat /tmp/hosts >> /etc/hosts'
      pdsh -g worker sudo bash -c 'cat /tmp/genders >> /etc/genders'
      pdsh -g worker rm -f /tmp/hosts /tmp/genders

      sudo cp ${var.software_root}/conf/zookeeper.service /etc/systemd/system/zookeeper.service
      sudo cp ${var.software_root}/conf/hadoop-namenode.service /etc/systemd/system/hadoop-namenode.service
      sudo systemctl daemon-reload
      sudo systemctl enable zookeeper
      sudo systemctl enable hadoop-namenode
      sudo systemctl disable hadoop-datanode
      sudo mkdir -p /data/metrics/influxdb
      sudo mkdir -p /data/metrics/grafana
      sudo chown -R hadoop.hadoop /data/metrics
      sudo chmod 777 /data/metrics/influxdb
      bash ${var.software_root}/conf/install_sw.sh
      
      pdsh -g worker sudo cp ${var.software_root}/conf/hadoop-datanode.service /etc/systemd/system/hadoop-datanode.service
      pdsh -g worker sudo systemctl daemon-reload
      pdsh -g worker sudo systemctl enable hadoop-datanode

      sudo systemctl start zookeeper
      hdfs namenode -format
      sudo systemctl start hadoop-namenode
      pdsh -g worker 'sudo systemctl start hadoop-datanode'
      accumulo-util build-native
    EOT
    ]
  }
}
