#!/bin/bash

set -e

if [ $# -ne 1 ]; then
  echo  "usage: $0 additional_host_file" >&2
  exit 1
fi

HOSTS_ADDITIONS=$1
TMPFILE=/tmp/hosts$$
pdcp -a $HOSTS_ADDITIONS $TMPFILE
pdsh -S -a 'sudo sed -ri '"'"'/^#+ BEGIN GENERATED HOSTS #+$/,/^#+ END GENERATED HOSTS #+$/d'"'"' /etc/hosts'
pdsh -S -a 'cat '$TMPFILE' | sudo tee -a /etc/hosts > /dev/null && rm -f $TMPFILE'
