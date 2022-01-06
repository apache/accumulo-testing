#cloud-config
#
# Don't log key information
#
no_ssh_fingerprints: true
groups:
  - hadoop
users:
  - default
  - name: hadoop
    homedir: /home/hadoop
    no_create_home: false
    primary_group: hadoop
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    #
    # NOTE: The ssh_authorized_keys section of the hadoop user configuration should contain
    #       the public key for every developer that is going to log into the nodes. This
    #       allows the developer to log into the nodes using the command: ssh hadoop@<ip>
    #
    ssh_authorized_keys:
%{ for key in authorized_ssh_keys ~}
      - ${key}
%{ endfor ~}
#
# yum install the following packages
#
packages:
  - gcc-c++
  - java-11-openjdk-devel
  - wget
  - yum-utils
#
# Make directories on each node
#
runcmd:
  - sudo mkdir -p ${zookeeper_dir}
  - sudo chown hadoop.hadoop ${zookeeper_dir}
  - sudo mkdir -p ${hadoop_dir}
  - sudo chown hadoop.hadoop ${hadoop_dir}
  - sudo mkdir -p ${accumulo_dir}
  - sudo chown hadoop.hadoop ${accumulo_dir}
  - sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
  - sudo yum install -y docker-ce docker-ce-cli containerd.io
  - sudo systemctl start docker
  - sudo systemctl enable docker
  - sudo usermod -aG docker hadoop
  - sudo yum install -y pdsh-mod-genders
#
# Write files to the filesystem, will be copied into place
# or invoked later
#
write_files:
  - path: /home/hadoop/ssh_config
    defer: true
    content: |
      Host *
        Compression yes
        StrictHostKeyChecking no
