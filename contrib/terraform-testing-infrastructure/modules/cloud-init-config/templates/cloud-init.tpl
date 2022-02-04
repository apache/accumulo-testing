#cloud-config
#
# Don't log key information
#
no_ssh_fingerprints: true
# Create the hadoop and docker groups
groups:
  - hadoop
  - docker
# Add default auto created user to docker group
system_info:
  default_user:
    groups: [docker]
# Create users
users:
  - default
  - name: hadoop
    homedir: /home/hadoop
    no_create_home: false
    primary_group: hadoop
    groups: docker
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

%{ if os_type == "centos" ~}
yum_repos:
  docker:
    name: Docker CE Stable - $basearch
    baseurl: https://download.docker.com/linux/centos/$releasever/$basearch/stable
    enabled: true
    gpgcheck: true
    gpgkey: https://download.docker.com/linux/centos/gpg
%{ endif ~}
%{ if os_type == "ubuntu" ~}
apt:
  sources:
    docker.list:
      source: deb [arch=amd64] https://download.docker.com/linux/ubuntu $RELEASE stable
      keyid: 9DC858229FC7DD38854AE2D88D81803C0EBFCD88
%{ endif ~}

#
# yum/apt install the following packages
#
packages:
%{ if os_type == "centos" ~}
  - epel-release
  - yum-utils
  - gcc-c++
  - java-11-openjdk-devel
  - pdsh-mod-genders
  - nfs-utils
%{ endif ~}
%{ if os_type == "ubuntu" ~}
  - net-tools
  - g++
  - openjdk-11-jdk-headless
  - pdsh
  - nfs-common
  - make
%{ endif ~}
  - docker-ce
  - docker-ce-cli
  - containerd.io
  - wget

#
# Make directories on each node
#
runcmd:
  - mkdir -p ${software_root} ${zookeeper_dir} ${hadoop_dir} ${accumulo_dir}
  - chown hadoop.hadoop ${software_root} ${zookeeper_dir} ${hadoop_dir} ${accumulo_dir}
  - systemctl enable docker
  - systemctl start docker
  - bash -c 'echo export PDSH_SSH_ARGS_APPEND=\"-o StrictHostKeyChecking=no\" > /etc/profile.d/pdsh.sh'
%{ if os_type == "ubuntu" ~}
  # Use bash instead of dash for the default shell
  - ln -s bash /bin/sh.bash
  - mv /bin/sh.bash /bin/sh
  - bash -c "echo ssh > /etc/pdsh/rcmd_default"
%{ endif ~}
  - sysctl -w vm.swappiness=0
  - sysctl -p

#
# Write files to the filesystem, will be copied into place
# or invoked later
#
write_files:
  # Increase open files limits for the Hadoop user
  - path: /etc/security/limits.conf
    append: true
    content: |
      hadoop           soft    nofile          4096
      hadoop           hard    nofile         65535
  # Set up Hadoop's SSH folder
  - path: /home/hadoop/.ssh/config
    defer: true
    owner: "hadoop:hadoop"
    permissions: '0600'
    content: |
      Host *
        Compression yes
        StrictHostKeyChecking no
  - path: /home/hadoop/.ssh/id_rsa
    defer: true
    owner: "hadoop:hadoop"
    permissions: '0600'
    content: |
      ${hadoop_private_key}
  - path: /home/hadoop/.ssh/id_rsa.pub
    defer: true
    owner: "hadoop:hadoop"
    permissions: '0644'
    content: |
      ${hadoop_public_key}
