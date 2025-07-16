#!/bin/bash
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


if [ "$#" != "2" ]; then
  echo "Usage $0 <tablename> BAD_ITER|BAD_TABLET|BAD_SERVICE|NORMAL"
  exit 1
fi

#accumulo-testing directory
ATD=../..

table=$1
test_type=$2

$ATD/bin/cingest createtable -o test.ci.common.accumulo.table=$table

# setup a compaction time iterator and point the tablet to compaction service w/ external compactors
accumulo shell -u root -p secret <<EOF
config -t $table -s table.iterator.majc.validate=100,org.apache.accumulo.testing.continuous.ValidatingIterator
config -t $table -s table.compaction.dispatcher.opts.service=cs1
EOF

case $test_type in
BAD_ITER) accumulo shell -u root -p secret <<EOF
config -t $table -s table.iterator.majc.validate=100,org.apache.accumulo.testing.continuous.MissingIter
EOF
  ;;
BAD_TABLET)
  # write a little bit of data
  $ATD/bin/cingest ingest -o test.ci.common.accumulo.table=$table -o test.ci.ingest.client.entries=100000
  # corrupt a checksum in an entry, this should cause the ValidatingIterator to fail when compacting that tablet
  $ATD/bin/cingest corrupt -o test.ci.common.accumulo.table=$table -o test.ci.ingest.row.min=1000000000000000
  accumulo shell -u root -p secret -e "flush -t $table"
  ;;
BAD_SERVICE) accumulo shell -u root -p secret <<EOF
config -t $table -s table.compaction.dispatcher.opts.service=cs2
EOF
  ;;
NORMAL) ;;
*)
    echo "Usage $0 <tablename> BAD_ITER|BAD_TABLET|BAD_SERVICE|NORMAL"
    exit 1
esac

mkdir -p logs

if [ "$test_type" == "NORMAL" ]; then
  # start unlimited ingest into the table
  $ATD/bin/cingest ingest -o test.ci.ingest.bulk.workdir=/ci_bulk -o test.ci.common.accumulo.table=$table &> logs/bulk-$table.log &
else
  # limit the amount of data written since tablets can not compact
  # would not need to do this if the table.file.pause property existed in 2.1
  $ATD/bin/cingest ingest -o test.ci.ingest.bulk.workdir=/ci_bulk -o test.ci.common.accumulo.table=$table -o test.ci.ingest.client.entries=10000000 &> logs/bulk-$table.log &
fi

