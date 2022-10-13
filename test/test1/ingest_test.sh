#! /usr/bin/env bash

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

../../../bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 1000000 --start 0 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 1000000 --start 1000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 1000000 --start 2000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 1000000 --start 3000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 1000000 --start 4000000 --cols 1 &
