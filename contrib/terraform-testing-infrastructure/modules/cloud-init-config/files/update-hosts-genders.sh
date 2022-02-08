#!/bin/bash

set -eo pipefail

if [ $# -ne 2 ]; then
  echo  "usage: $0 additional_hosts_file additional_genders_file" >&2
  exit 1
fi

HOSTS_ADDITIONS=$1
GENDERS_ADDITIONS=$2

begin_hosts_marker="##### BEGIN GENERATED HOSTS #####"
end_hosts_marker="##### END GENERATED HOSTS #####"
begin_genders_marker="##### BEGIN GENERATED GENDERS #####"
end_genders_marker="##### END GENERATED GENDERS #####"

# Update the hosts file locally
# Wrap the supplied host additions with markers that we'll use to strip it back out.
TMPHOSTS=/tmp/hosts$$
cat > $TMPHOSTS <<EOF
$begin_hosts_marker
##### DO NOT EDIT THIS SECTION #####
$(<"$HOSTS_ADDITIONS")
$end_hosts_marker
EOF
# Strip out any previously applied hosts additiona, and then tack the new ones on to the end of /etc/hosts.
sudo sed -ri '/^'"$begin_hosts_marker"'$/,/^'"$end_hosts_marker"'$/d' /etc/hosts
cat "$TMPHOSTS" | sudo tee -a /etc/hosts > /dev/null

# Update the genders file locally
TMPGENDERS=/tmp/genders$$
cat > $TMPGENDERS <<EOF
$begin_genders_marker
$(<"$GENDERS_ADDITIONS")
$end_genders_marker
EOF
[[ -f /etc/genders ]] && sudo sed -ri '/^'"$begin_genders_marker"'$/,/^'"$end_genders_marker"'$/d' /etc/genders
cat "$TMPGENDERS" | sudo tee -a /etc/genders > /dev/null
echo "Check genders file validity..."
nodeattr -k

# Now copy hosts updates to the workers and apply
pdcp -g worker $TMPHOSTS $TMPHOSTS
pdsh -S -g worker 'sudo sed -ri '"'"'/^'"$begin_hosts_marker"'$/,/^'"$end_hosts_marker"'$/d'"'"' /etc/hosts'
pdsh -S -g worker 'cat '$TMPHOSTS' | sudo tee -a /etc/hosts > /dev/null && rm -f $TMPHOSTS'
rm -f $TMPHOSTS

# Copy genders updates to the workers and apply
pdcp -g worker $TMPGENDERS $TMPGENDERS
pdsh -S -g worker "sudo cp $TMPGENDERS /etc/genders && rm -f $TMPGENDERS"
rm -f $TMPGENDERS
