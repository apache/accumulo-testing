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
