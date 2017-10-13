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

# General
# =======
test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/path/to/hadoop
# Versions set below will be what is included in the shaded jar
export ACCUMULO_VERSION=`accumulo version`
export HADOOP_VERSION=`hadoop version | head -n1 | awk '{print $2}'`
export ZOOKEEPER_VERSION=3.4.9
# Make sure Hadoop configuration directory is on the classpath
export CLASSPATH=$HADOOP_PREFIX/etc/hadoop

# Agitator
# ========
# Agitator needs know where Accumulo is installed
test -z "$ACCUMULO_HOME" && export ACCUMULO_HOME=/path/to/accumulo
# Accumulo user
AGTR_ACCUMULO_USER=$(whoami)
# Time (in minutes) between killing Accumulo masters
AGTR_MASTER_KILL_SLEEP_TIME=60
AGTR_MASTER_RESTART_SLEEP_TIME=2
# Time (in minutes) between killing Accumulo tservers
AGTR_TSERVER_KILL_SLEEP_TIME=20
AGTR_TSERVER_RESTART_SLEEP_TIME=10
# Min and max number of Accumulo tservers that the agitator will kill at once
AGTR_TSERVER_MIN_KILL=1
AGTR_TSERVER_MAX_KILL=1
# Amount of time (in minutes) the agitator should sleep before killing datanodes
AGTR_DATANODE_KILL_SLEEP_TIME=20
# Amount of time (in minutes) the agitator should wait before restarting datanodes
AGTR_DATANODE_RESTART_SLEEP_TIME=10
# Min and max number of datanodes the agitator will kill at once
AGTR_DATANODE_MIN_KILL=1
AGTR_DATANODE_MAX_KILL=1
# HDFS agitation
AGTR_HDFS_USER=$(whoami)
AGTR_HDFS=false
AGTR_HDFS_SLEEP_TIME=10
AGTR_HDFS_SUPERUSER=hdfs
AGTR_HDFS_COMMAND="${HADOOP_PREFIX:-/usr/lib/hadoop}/share/hadoop/hdfs/bin/hdfs"
AGTR_HDFS_SUDO=$(which sudo)
