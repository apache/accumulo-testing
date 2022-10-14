#! /usr/bin/env bash
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

[ $# -eq 3 ] || {
  echo "usage: $0 disk_count mount_point user.group"
  exit 1
}

diskCount=$1
mountPoint=$2
owner=$3

until [[ $(ls -1 /dev/disk/azure/scsi1/ | wc -l) == "$diskCount" ]]; do
  echo "Waiting for $diskCount disks to be attached..."
  sleep 10
done

VG_GROUP_NAME=storage_vg
LG_GROUP_NAME=storage_lv

DISK_PATH="/dev/disk/azure/scsi1"
declare -a REAL_PATH_ARR

for i in $(ls ${DISK_PATH} 2>/dev/null); do
  REAL_PATH=$(realpath ${DISK_PATH}/${i} | tr '\n' ' ')
  REAL_PATH_ARR+=($REAL_PATH)
done

RAID_DEVICE_LIST=$(echo "${REAL_PATH_ARR[@]}" | sort)
RAID_DEVICES_COUNT=$(echo "${#REAL_PATH_ARR[@]}")
pvcreate ${RAID_DEVICE_LIST}
vgcreate -s 4M ${VG_GROUP_NAME} ${RAID_DEVICE_LIST}
lvcreate -n $LG_GROUP_NAME -l 100%FREE -i ${RAID_DEVICES_COUNT} ${VG_GROUP_NAME}
mkfs.xfs -K -f /dev/${VG_GROUP_NAME}/${LG_GROUP_NAME}
mkdir -p ${mountPoint}
printf "/dev/${VG_GROUP_NAME}/${LG_GROUP_NAME}\t${mountPoint}\tauto\tdefaults,noatime\t0\t2\n" >>/etc/fstab
mount --target ${mountPoint}
chown ${owner} ${mountPoint}
