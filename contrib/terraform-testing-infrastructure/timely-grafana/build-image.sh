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

MVN_VERSION=$1
MVN_DOWNLOAD_URL=$2

if [ -z "${MVN_VERSION}" ]; then
  echo "Maven version parameter not supplied"
  exit 1
fi

if [ -z "${MVN_DOWNLOAD_URL}" ]; then
  echo "Maven download url parameter not supplied"
  exit 1
fi

docker build --build-arg VERSION="$MVN_VERSION" --build-arg URL="$MVN_DOWNLOAD_URL" -t timely-grafana .

rm -rf build_output
mkdir build_output
id=$(docker create timely-grafana)
docker cp "$id":/opt/timely/collectd-timely-plugin.jar build_output/.
docker cp "$id":/opt/timely/lib-accumulo build_output/.
docker rm -v "$id"
