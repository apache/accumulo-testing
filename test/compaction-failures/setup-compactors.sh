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


# configure compaction services
accumulo shell -u root -p secret <<EOF
config -s tserver.compaction.major.service.cs1.planner=org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner
config -s tserver.compaction.major.service.cs1.planner.opts.executors=[{"name":"small","type":"external","maxSize":"128M","queue":"small"},{"name":"large","type":"external","queue":"large"}]
config -s tserver.compaction.major.service.cs2.planner=org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner
config -s tserver.compaction.major.service.cs2.planner.opts.executors=[{"name":"small","type":"external","maxSize":"128M","queue":"emptysmall"},{"name":"large","type":"external","queue":"emptylarge"}]
EOF

mkdir -p logs

pkill -f "Main compactor"
pkill -f "Main compaction-coord"

ACCUMULO_SERVICE_INSTANCE=coord accumulo compaction-coordinator  &> logs/coord.out &

# get the absolute path of the the accumulo test non shaded test jar
TEST_JAR=$(readlink -f $(ls ../../target/accumulo-testing-[0-9].*jar))

# start 4 compactors with the test iterator on their classpath
for i in $(seq 1 4); do
	CLASSPATH=$TEST_JAR ACCUMULO_SERVICE_INSTANCE=compactor_small_$i accumulo compactor -q small &> logs/compactor-small-$i.out &
done

# start four compactors that do not have the test jar on their classpath.  Since
# every table configures an iterator w/ the test jar, every compaction on these
# compactors should fail
for i in $(seq 5 8); do
	ACCUMULO_SERVICE_INSTANCE=compactor_small_$i accumulo compactor -q small &> logs/compactor-small-$i.out &
done

# start 4 compactors for the large group w/ the test iterator on the classpath
for i in $(seq 1 4); do
	CLASSPATH=$TEST_JAR ACCUMULO_SERVICE_INSTANCE=compactor_large_$i accumulo compactor -q large &> logs/compactor-large-$i.out &
done

# start 4 compactors for the large group that are missing the iterator used by the test table
for i in $(seq 5 8); do
	ACCUMULO_SERVICE_INSTANCE=compactor_large_$i accumulo compactor -q large &> logs/compactor-large-$i.out &
done