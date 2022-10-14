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
package org.apache.accumulo.testing.gcs;

import org.apache.accumulo.testing.TestEnv;

public class GcsEnv extends TestEnv {

  public GcsEnv(String[] args) {
    super(args);
  }

  public String getTableName() {
    return testProps.getProperty("test.gcs.table", "gcs");
  }

  public int getMaxBuckets() {
    return Integer.parseInt(testProps.getProperty("test.gcs.maxBuckets", "100000"));
  }

  public int getInitialTablets() {
    return Integer.parseInt(testProps.getProperty("test.gcs.tablets", "10"));
  }

  public int getMaxWork() {
    return Integer.parseInt(testProps.getProperty("test.gcs.maxWork", "100000000"));
  }

  public int getMaxActiveWork() {
    return Integer.parseInt(testProps.getProperty("test.gcs.maxActiveWork", "10000"));
  }

  public int getBatchSize() {
    return Integer.parseInt(testProps.getProperty("test.gcs.batchSize", "100000"));
  }
}
