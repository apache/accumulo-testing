/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.healthprobe;

import org.apache.accumulo.testing.cli.ClientOpts;

import com.beust.jcommander.Parameter;

class MonitorOpts extends ClientOpts {
  @Parameter(names = {"-t", "--table"}, description = "table to use")
  String tableName = "";

  @Parameter(names = "--isolate",
      description = "true to turn on scan isolation, false to turn off. default is false.")
  boolean isolate = false;

  @Parameter(names = "--num-iterations", description = "number of scan iterations")
  int scan_iterations = 1024;

  @Parameter(names = "--continuous",
      description = "continuously scan the table. note that this overrides --num-iterations")
  boolean continuous = false;

  @Parameter(names = "--scan-batch-size", description = "scanner batch size")
  int batch_size = 5;

  @Parameter(names = "--scanner-sleep-ms", description = "scanner sleep interval in ms")
  int sleep_ms = 60000;

  @Parameter(names = "--num-of-rows-per-iteration",
      description = "Number of rows scanned together in one iteration of scanner consumption")
  long distance = 10l;

  @Parameter(names = "--auth", description = "Provide a auth that can access all/most rows")
  String auth = "";
}
