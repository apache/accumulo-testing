<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

1. Download and Install Terraform

  wget https://releases.hashicorp.com/terraform/1.1.5/terraform_1.1.5_linux_amd64.zip
  unzip into /usr/local/bin

2. Create the Shared State

  NOTE: You only need to do this once. If you are sharing the cluster with a team,
        then only one person needs to do it and they need to share the bucket with
        the other team members.

  cd shared_state/aws
  terraform init
  terraform apply

3. Create the Configuration

  You will need to create a configuration file that includes values for the
  variables that do not have a default value. See the Variables section in
  the README. For example, you can create a file "aws.auto.tfvars" file in
  the `aws` directory with the following content (replace as appropriate):

create_route53_records = "true"
private_network = "true"
accumulo_root_password = "secret"
security_group = "sg-ABCDEF001"
route53_zone = "some.domain.com"
us_east_1b_subnet = "subnet-ABCDEF123"
us_east_1e_subnet = "subnet-ABCDEF124"
ami_owner = "000000000001"
ami_name_pattern = "MY_AMI_*"
authorized_ssh_keys = [
  "ssh-rsa .... user1",
  "ssh-rsa .... user2",
  "ssh-rsa .... user3"
]


4. Create the Resources

  NOTE: ensure that the private key corresponding to the first ssh key in 
        `authorized_ssh_keys` in the configuration above has been loaded
        into your ssh agent, or else terraform apply will fail.

  cd aws
  terraform init --backend-config=bucket=<bucket-name-goes-here>
  terraform apply

5. Accessing the cluster

  The output of the apply step above will include the IP addresses of the
  resources that were created. If created correctly, you should be able to
  ssh to the nodes using "ssh hadoop@ip". If you created DNS addresses for
  the nodes, then you should be able to ssh using those addresses also. You
  should also be able to access the web pages (see the "Accessing Web
  Pages" section of the README for ports)

