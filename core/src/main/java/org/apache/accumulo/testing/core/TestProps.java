/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.testing.core;

public class TestProps {

  private static final String PREFIX = "test.";
  private static final String RANDOMWALK = PREFIX + "randomwalk.";
  private static final String COMMON = PREFIX + "common.";

  /** Common properties **/
  // Zookeeper connection string
  public static final String ZOOKEEPERS = COMMON + "zookeepers";
  // Accumulo instance name
  public static final String ACCUMULO_INSTANCE = COMMON + "accumulo.instance";
  // Accumulo username
  public static final String ACCUMULO_USERNAME = COMMON + "accumulo.username";
  // Accumulo password
  public static final String ACCUMULO_PASSWORD = COMMON + "accumulo.password";
  // Accumulo keytab
  public static final String ACCUMULO_KEYTAB = COMMON + "accumulo.keytab";
  // Memory (in MB) given to each YARN container
  public static final String YARN_CONTAINER_MEMORY_MB = COMMON + "yarn.container.memory.mb";
  // Number of cores given to each YARN container
  public static final String YARN_CONTAINER_CORES = COMMON + "yarn.container.cores";


  /** Random walk properties **/
  // Number of random walker (if running in YARN)
  public static final String RW_NUM_WALKERS = RANDOMWALK + "num.walkers";
  // Max memory for multi-table batch writer
  public static final String RW_BW_MAX_MEM = RANDOMWALK + "bw.max.mem";
  // Max latency in milliseconds for multi-table batch writer
  public static final String RW_BW_MAX_LATENCY = RANDOMWALK + "bw.max.latency";
  // Number of write thread for multi-table batch writer
  public static final String RW_BW_NUM_THREADS = RANDOMWALK + "bw.num.threads";

}
