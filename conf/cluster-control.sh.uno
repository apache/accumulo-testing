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

# internal functions just for uno cluster control

UNO_HOME=/home/ubuntu/git/uno
UNO=$UNO_HOME/bin/uno

function get_ah {
  $UNO env | grep ACCUMULO_HOME | sed 's/export ACCUMULO_HOME=//' | sed 's/"//g'
}

# functions required for accumulo testing cluster control

function get_hadoop_client {
  echo "$($UNO env | grep HADOOP_HOME | sed 's/export HADOOP_HOME=//' | sed 's/"//g')/share/hadoop/client/*"
}


function get_version {
  case $1 in
    ACCUMULO)
      (
        # run following in sub shell so it does not pollute
        if [[ -f "$UNO_HOME"/conf/uno-local.conf ]]; then
          . "$UNO_HOME"/conf/uno-local.conf
        else
          . "$UNO_HOME"/conf/uno.conf
        fi
        echo "$ACCUMULO_VERSION"
      )
      ;;
    HADOOP)
      (
        # run following in sub shell so it does not pollute
        if [[ -f "$UNO_HOME"/conf/uno-local.conf ]]; then
          . "$UNO_HOME"/conf/uno-local.conf
        else
          . "$UNO_HOME"/conf/uno.conf
        fi
        echo "$HADOOP_VERSION"
      )
      ;;
    ZOOKEEPER)
      (
        # run following in sub shell so it does not pollute
        if [[ -f "$UNO_HOME"/conf/uno-local.conf ]]; then
          . "$UNO_HOME"/conf/uno-local.conf
        else
          . "$UNO_HOME"/conf/uno.conf
        fi
        echo "$ZOOKEEPER_VERSION"
      )
      ;;
    *)
      return 1
  esac
}

function start_cluster {
  $UNO install accumulo
  $UNO run zookeeper
  $UNO run hadoop
}

function setup_accumulo {
  $UNO setup accumulo --no-deps
}

function get_config_file {
  local ah; ah=$(get_ah)
  cp "$ah/conf/$1" "$2"
}

function put_config_file {
  local ah; ah=$(get_ah)
  cp "$1" "$ah/conf"
}

function put_server_code {
  local ah; ah=$(get_ah)
  cp "$1" "$ah/lib/ext"
}

function start_accumulo {
  $UNO stop accumulo --no-deps &> /dev/null
  $UNO start accumulo --no-deps &> /dev/null
}

function stop_cluster {
  $UNO kill
}
