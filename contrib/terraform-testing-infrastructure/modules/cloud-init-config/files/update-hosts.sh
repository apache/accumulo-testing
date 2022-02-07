#!/bin/bash

set -eo pipefail

if [ $# -ne 1 ]; then
  echo  "usage: $0 additional_host_file" >&2
  exit 1
fi

HOSTS_ADDITIONS=$1
TMPFILE=/tmp/hosts$$
sudo sed -ri '/^#+ BEGIN GENERATED HOSTS #+$/,/^#+ END GENERATED HOSTS #+$/d' /etc/hosts
cat $1 | sudo tee -a /etc/hosts > /dev/null

pdcp -g worker $HOSTS_ADDITIONS $TMPFILE
pdsh -S -g worker 'sudo sed -ri '"'"'/^#+ BEGIN GENERATED HOSTS #+$/,/^#+ END GENERATED HOSTS #+$/d'"'"' /etc/hosts'
pdsh -S -g worker 'cat '$TMPFILE' | sudo tee -a /etc/hosts > /dev/null && rm -f $TMPFILE'
