#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/bin/bash

set -eo pipefail

if [ $# -ne 2 ]; then
  echo  "usage: $0 additional_hosts_file additional_genders_file" >&2
  exit 1
fi

HOSTS_ADDITIONS=$1
GENDERS_ADDITIONS=$2

# Update the hosts file locally
sudo sed -ri '/^#+ BEGIN GENERATED HOSTS #+$/,/^#+ END GENERATED HOSTS #+$/d' /etc/hosts
cat "$HOSTS_ADDITIONS" | sudo tee -a /etc/hosts > /dev/null

# Update the genders file locally
[[ -f /etc/genders ]] && sudo sed -ri '/^#+ BEGIN GENERATED GENDERS #+$/,/^#+ END GENERATED GENDERS #+$/d' /etc/genders
cat "$GENDERS_ADDITIONS" | sudo tee -a /etc/genders > /dev/null
echo "Check genders file validity..."
nodeattr -k

# Now copy hosts updates to the workers and apply
TMPFILE=/tmp/hosts$$
pdcp -g worker "$HOSTS_ADDITIONS" $TMPFILE
pdsh -S -g worker 'sudo sed -ri '"'"'/^#+ BEGIN GENERATED HOSTS #+$/,/^#+ END GENERATED HOSTS #+$/d'"'"' /etc/hosts'
pdsh -S -g worker 'cat '$TMPFILE' | sudo tee -a /etc/hosts > /dev/null && rm -f $TMPFILE'

# Copy genders updates to the workers and apply
pdcp -g worker /etc/genders /tmp/genders
pdsh -S -g worker "sudo cp /tmp/genders /etc/genders && rm /tmp/genders"
