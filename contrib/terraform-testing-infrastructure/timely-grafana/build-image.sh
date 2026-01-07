#! /bin/bash
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

HADOOP_VERSION=${1:-"3.3.6"}
MVN_REPO=$2
MVN_DIR=$3

rm -rf build_output
mkdir build_output

if [ ! -d "${MVN_REPO}" ]; then
  echo "Maven repository directory was not supplied"
  exit 1
fi
rsync -a ${MVN_REPO} build_output/repo

if [ -z "${MVN_DIR}" ]; then
  echo "Maven directory not supplied"
  exit 1
fi
rsync -a ${MVN_DIR} build_output/maven

docker build --build-arg HADOOP_VERSION=$HADOOP_VERSION -t timely-grafana .

id=$(docker create timely-grafana)
docker cp "$id":/opt/timely/collectd-timely-plugin.jar build_output/.
docker cp "$id":/opt/timely/lib-accumulo build_output/.
docker rm -v "$id"
