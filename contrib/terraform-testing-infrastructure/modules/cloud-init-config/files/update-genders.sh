#!/bin/bash

set -e

if [ $# -ne 1 ]; then
  echo  "usage: $0 additional_genders_file" >&2
  exit 1
fi

GENDERS_ADDITIONS=$1
[[ -f /etc/genders ]] && sudo sed -ri '/^#+ BEGIN GENERATED GENDERS #+$/,/^#+ END GENERATED GENDERS #+$/d' /etc/genders
cat $GENDERS_ADDITIONS | sudo tee -a /etc/genders > /dev/null

pdcp -g worker /etc/genders /tmp/genders
pdsh -S -g worker "sudo cp /tmp/genders /etc/genders && rm /tmp/genders"
