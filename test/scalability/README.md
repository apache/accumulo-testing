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

# Apache Accumulo Scalability Tests

The scalability test framework needs to be configured for your Accumulo
instance by performing the following steps.

WARNING: Each scalability test rewrites your `conf/tservers` file and reinitializes
your Accumulo instance. Do not run these tests on a cluster holding essential
data.

1. Make sure you have both `ACCUMULO_HOME` and `HADOOP_HOME` set in your
   `$ACCUMULO_CONF_DIR/accumulo-env.sh`
2. Edit the 'site.conf' file in the `conf` directory containing settings
   needed by test nodes to connect to Accumulo, and to guide the tests.
   ```bash
   vim conf/site.conf
   ```
3. Edit the 'Ingest.conf' file in the `conf` directory containing performance
   settings for the Ingest test. (This test is currently the only scalability
   test available.)
   ```bash
   vim conf/Ingest.conf
   ```
4. Run the tests.
   Each test has a unique ID (e.g., "Ingest") which correlates with its test
   code in `org.apache.accumulo.test.scalability.tests.<ID>`.
   This ID correlates with a config file: `conf/<ID>.conf`.
   To run the test, specify its ID to the run.py script.
   ```bash
   nohup ./run.py Ingest > test1.log 2>&1 &
   ```
   A timestamped directory will be created, and results are placed in it as each
   test completes.

