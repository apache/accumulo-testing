<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

Some scripts for testing different compaction failure scenarios.

```bash
# start compactors, half of which will always fail any compaction because they are missing an iterator class
./setup-compactors.sh
# starting ingest into table ci1
./start-ingest.sh ci1 NORMAL
# starting ingest into table ci2 with a non-existent compaction iterator configured, all compactions should fail on this table
./start-ingest.sh ci2 BAD_ITER
# starting ingest into table ci3 with a compaction service that has no compactors running, no compactions should ever run for this table
./start-ingest.sh ci3 BAD_SERVICE
# starting ingest into table ci4, corrupting data in a single tablet such that that tablet can never compact
./start-ingest.sh ci4 BAD_TABLET
```

While test are running can use the following to monitor files per tablet on a table.

```
$ accumulo jshell
Preparing JShell for Apache Accumulo

Use 'client' to interact with Accumulo

|  Welcome to JShell -- Version 17.0.15
|  For an introduction type: /help intro

jshell> /open count-file-per-tablet.jshell

jshell> CFPT.printStats(client, "ci1", 3000)
  0 secs min:20 avg:30.37 max:35
  3 secs min:20 avg:30.28 max:35
```
