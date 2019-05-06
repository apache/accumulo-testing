#! /usr/bin/env bash

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


# This script test upgrading from Accumulo 1.9 to 2.0. This script is
# not self verifying, its output must be inspected for correctness.

if [[ $# != 1 ]] ; then
  BASENAME=$(basename "$0")
  echo "Usage: $BASENAME clean|dirty"
  exit -1
fi

# Dir of local git repo containing Accumulo
ACCUMULO_DIR=~/git/accumulo
# Dir where Uno is located
UNO_DIR=~/git/uno
# HDFS dir were bulk import files are generated
BULK=/tmp/upt

# This script assumes Uno is configured to build Accumulo from
# $ACCUMULO_DIR when 'uno fetch accumulo' is run.

cd $ACCUMULO_DIR
git checkout 1.9
git clean -xfd
cd $UNO_DIR
./bin/uno fetch accumulo
./bin/uno setup accumulo
(
  # Run in a subshell because following sets ENV vars for older Accumulo.
  # Without subshell, ENV vars would cause problems later for newer Accumulo.
  eval "$(./bin/uno env)"

  hadoop fs -ls /accumulo/version


  hadoop fs -rmr "$BULK"
  hadoop fs -mkdir -p "$BULK/fail"
  accumulo org.apache.accumulo.test.TestIngest -i uno -u root -p secret --rfile $BULK/bulk/test --timestamp 1 --size 50 --random 56 --rows 200000 --start 200000 --cols 1

  accumulo org.apache.accumulo.test.TestIngest -i uno -u root -p secret --timestamp 1 --size 50 --random 56 --rows 200000 --start 0 --cols 1  --createTable --splits 10

  accumulo shell -u root -p secret <<EOF
   table test_ingest
   importdirectory $BULK/bulk $BULK/fail false
   createtable foo
   config -t foo -s table.compaction.major.ratio=2
   insert r1 f1 q1 v1
   flush -t foo -w
   scan -t accumulo.metadata -c file
   insert r1 f1 q2 v2
   insert r2 f1 q1 v3
EOF
)

if [[ $1 == dirty ]]; then
	pkill -9 -f accumulo\\.start
else
  (
    eval "$(./bin/uno env)"
    "$ACCUMULO_HOME/bin/stop-all.sh"
  )
fi

cd $ACCUMULO_DIR
git checkout master
git clean -xfd
cd $UNO_DIR
./bin/uno fetch accumulo
./bin/uno install accumulo --no-deps
./install/accumulo*/bin/accumulo-cluster start
(
  eval "$(./bin/uno env)"
  hadoop fs -ls /accumulo/version
  accumulo shell -u root -p secret <<EOF
    config -t foo -f table.compaction.major.ratio
    scan -t foo -np
    scan -t accumulo.metadata -c file
    compact -t foo -w
    scan -t foo -np
    scan -t accumulo.metadata -c file
EOF

  accumulo org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 1 --random 56 --rows 400000 --start 0 --cols 1
  accumulo shell -u root -p secret -e "compact -t test_ingest -w"
  accumulo org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 1 --random 56 --rows 400000 --start 0 --cols 1
  accumulo org.apache.accumulo.test.TestIngest --timestamp 2 --size 50 --random 57 --rows 500000 --start 0 --cols 1

  pkill -9 -f accumulo\\.start
  accumulo-cluster start

  accumulo org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 2 --random 57 --rows 500000 --start 0 --cols 1

  accumulo-cluster stop
  accumulo-cluster start

  accumulo org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 2 --random 57 --rows 500000 --start 0 --cols 1
)
