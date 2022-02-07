variable "software_root" {}
variable "upload_host" {}
variable "accumulo_instance_name" {}
variable "accumulo_root_password" {}

#####################################################
# Run the install software script on the first node #
#####################################################

#
# This connects to the first node and runs the install_sw.sh script.
#
resource "null_resource" "configure_manager_node" {
  connection {
    type = "ssh"
    host = var.upload_host
    user = "hadoop"
  }
  provisioner "remote-exec" {
    inline = [<<-EOT
      set -eo pipefail

      # Put local bashrc/bash_profile in place and source it before doing anything else.
      cp ${var.software_root}/conf/hadoop_bash_profile /home/hadoop/.bash_profile
      cp ${var.software_root}/conf/hadoop_bashrc /home/hadoop/.bashrc
      source /home/hadoop/.bash_profile

      # Update the hosts and genders files across the cluster. This applies changes
      # locally first, then uses pdcp/pdsh to apply them across the cluster.
      /usr/local/bin/update-hosts-genders.sh ${var.software_root}/conf/hosts ${var.software_root}/conf/genders
      # Now that genders is set up properly, we can use it to copy the hadoop .bashrc and .bash_profile out.
      pdcp -g worker /home/hadoop/.bashrc /home/hadoop/.bash_profile /home/hadoop/.

      bash ${var.software_root}/conf/install_sw.sh
      bash ${var.software_root}/conf/initialize-hadoop.sh
      bash -l ${var.software_root}/conf/initialize-accumulo.sh "${var.accumulo_instance_name}" "${var.accumulo_root_password}"
    EOT
    ]
  }
}
