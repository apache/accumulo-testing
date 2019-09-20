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

# General
# =======

## Hadoop installation
export HADOOP_HOME="${HADOOP_HOME:-/path/to/hadoop}"
## Accumulo installation
export ACCUMULO_HOME="${ACCUMULO_HOME:-/path/to/accumulo}"
## Path to Accumulo client properties
export ACCUMULO_CLIENT_PROPS="$ACCUMULO_HOME/conf/accumulo-client.properties"

if [ ! -d "$ACCUMULO_HOME" ]; then
  echo "$ACCUMULO_HOME does not exist. Make sure to set ACCUMULO_HOME"
  exit 1
fi
if [ ! -d "$HADOOP_HOME" ]; then
  echo "$HADOOP_HOME does not exist. Make sure to set HADOOP_HOME"
  exit 1
fi

# Add HADOOP conf directory to CLASSPATH, if set (for defaultFS in core-site.xml, etc.)
if [[ -n $HADOOP_CONF_DIR ]]; then
  if [[ -z $CLASSPATH ]]; then
    export CLASSPATH="$HADOOP_CONF_DIR"
  else
    export CLASSPATH="$CLASSPATH:$HADOOP_CONF_DIR"
  fi
fi

# Configuration
# =============
conf_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export TEST_PROPS="${conf_dir}/accumulo-testing.properties"
if [ ! -f "$TEST_PROPS" ]; then
  echo "Please create and edit accumulo-testing.properties in $conf_dir"
  exit 1
fi
export TEST_LOG4J="${conf_dir}/log4j.properties"
if [ ! -f "$TEST_LOG4J" ]; then
  echo "Could not find logj4.properties in $conf_dir"
  exit 1
fi

# Shaded test jar
# ===============
# Versions set below will be what is included in the shaded jar
ACCUMULO_VERSION="$("$ACCUMULO_HOME"/bin/accumulo version)"; export ACCUMULO_VERSION
HADOOP_VERSION="$(hadoop version | head -n1 | awk '{print $2}')"; export HADOOP_VERSION
export ZOOKEEPER_VERSION=3.4.14
# Path to shaded test jar
at_home=$( cd "$( dirname "$conf_dir" )" && pwd )
export TEST_JAR_PATH="${at_home}/target/accumulo-testing-shaded.jar"
if [[ ! -f "$TEST_JAR_PATH" ]]; then
  echo "Building $TEST_JAR_PATH"
  cd "${at_home}" || exit 1
  mvn clean package -P create-shade-jar -D skipTests -D accumulo.version="$ACCUMULO_VERSION" -D hadoop.version="$HADOOP_VERSION" -D zookeeper.version="$ZOOKEEPER_VERSION"
fi

# Hadoop jars
# ===========
export HADOOP_API_JAR="${at_home}"/target/dependency/hadoop-client-api.jar
export HADOOP_RUNTIME_JAR="${at_home}"/target/dependency/hadoop-client-runtime.jar
if [[ ! -f "$HADOOP_API_JAR" || ! -f "$HADOOP_RUNTIME_JAR" ]]; then
  mvn dependency:copy-dependencies -Dmdep.stripVersion=true -DincludeArtifactIds=hadoop-client-api,hadoop-client-runtime -Dhadoop.version="$HADOOP_VERSION"
fi

# Agitator
# ========
# Accumulo user
AGTR_ACCUMULO_USER=$(whoami); export AGTR_ACCUMULO_USER
# Time (in minutes) between killing Accumulo masters
export AGTR_MASTER_KILL_SLEEP_TIME=60
export AGTR_MASTER_RESTART_SLEEP_TIME=2
# Time (in minutes) between killing Accumulo tservers
export AGTR_TSERVER_KILL_SLEEP_TIME=20
export AGTR_TSERVER_RESTART_SLEEP_TIME=10
# Min and max number of Accumulo tservers that the agitator will kill at once
export AGTR_TSERVER_MIN_KILL=1
export AGTR_TSERVER_MAX_KILL=1
# Amount of time (in minutes) the agitator should sleep before killing datanodes
export AGTR_DATANODE_KILL_SLEEP_TIME=20
# Amount of time (in minutes) the agitator should wait before restarting datanodes
export AGTR_DATANODE_RESTART_SLEEP_TIME=10
# Min and max number of datanodes the agitator will kill at once
export AGTR_DATANODE_MIN_KILL=1
export AGTR_DATANODE_MAX_KILL=1
# HDFS agitation
AGTR_HDFS_USER=$(whoami); export AGTR_HDFS_USER
export AGTR_HDFS=false
export AGTR_HDFS_SLEEP_TIME=10
export AGTR_HDFS_SUPERUSER=hdfs
export AGTR_HDFS_COMMAND="${HADOOP_PREFIX:-/usr/lib/hadoop}/share/hadoop/hdfs/bin/hdfs"
AGTR_HDFS_SUDO=$(command -v sudo); export AGTR_HDFS_SUDO
