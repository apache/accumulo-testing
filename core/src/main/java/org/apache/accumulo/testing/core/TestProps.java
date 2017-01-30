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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TestProps {

  private static final String PREFIX = "test.";
  private static final String COMMON = PREFIX + "common.";
  private static final String CI = PREFIX + "ci.";
  private static final String CI_COMMON = CI + "common.";
  private static final String CI_INGEST = CI + "ingest.";
  private static final String CI_WALKER = CI + "walker.";
  private static final String CI_BW = CI + "batch.walker.";
  private static final String CI_SCANNER = CI + "scanner.";
  private static final String CI_VERIFY = CI + "verify.";

  /** Common properties **/
  // Zookeeper connection string
  public static final String ZOOKEEPERS = COMMON + "zookeepers";
  // Accumulo instance name
  public static final String ACCUMULO_INSTANCE = COMMON + "accumulo.instance";
  // Accumulo username
  public static final String ACCUMULO_USERNAME = COMMON + "accumulo.username";
  // Accumulo password
  public static final String ACCUMULO_PASSWORD = COMMON + "accumulo.password";
  // Max memory (in bytes) each batch writer will use to buffer writes
  public static final String ACCUMULO_BW_MAX_MEM_BYTES = COMMON + "accumulo.bw.max.memory.bytes";
  // Max the maximum time (in ms) each batch writer will buffer data
  public static final String ACCUMULO_BW_MAX_LATENCY_MS = COMMON + "accumulo.bw.max.latency.ms";
  // Number of threads each batch writer will use to write data
  public static final String ACCUMULO_BW_NUM_THREADS = COMMON + "accumulo.bw.num.threads";
  // Number of thread for each batch scanner
  public static final String ACCUMULO_BS_NUM_THREADS = COMMON + "accumulo.bw.num.threads";
  // Number of key/value entries to pull during scan
  public static final String ACCUMULO_SCANNER_BATCH_SIZE = COMMON + "accumulo.scanner.batch.size";
  // Accumulo keytab
  public static final String ACCUMULO_KEYTAB = COMMON + "accumulo.keytab";
  // HDFS root path. Should match 'fs.defaultFS' property in Hadoop's core-site.xml
  public static final String HDFS_ROOT = COMMON + "hdfs.root";
  // YARN resource manager hostname. Should match 'yarn.resourcemanager.hostname' property in
  // Hadoop's yarn-site.xml
  public static final String YARN_RESOURCE_MANAGER = COMMON + "yarn.resource.manager";
  // Memory (in MB) given to each YARN container
  public static final String YARN_CONTAINER_MEMORY_MB = COMMON + "yarn.container.memory.mb";
  // Number of cores given to each YARN container
  public static final String YARN_CONTAINER_CORES = COMMON + "yarn.container.cores";

  /** Continuous ingest test properties **/
  /** Common **/
  // Accumulo table used by continuous ingest tests
  public static final String CI_COMMON_ACCUMULO_TABLE = CI_COMMON + "accumulo.table";
  // Number of tablets that should exist in Accumulo table when created
  public static final String CI_COMMON_ACCUMULO_NUM_TABLETS = CI_COMMON + "accumulo.num.tablets";
  // Optional authorizations (in CSV format) that if specified will be
  // randomly selected by scanners
  // and walkers
  public static final String CI_COMMON_AUTHS = CI_COMMON + "auths";

  /** Ingest **/
  // Number of entries each ingest client should write
  public static final String CI_INGEST_CLIENT_ENTRIES = CI_INGEST + "client.entries";
  // Minimum random row to generate
  public static final String CI_INGEST_ROW_MIN = CI_INGEST + "row.min";
  // Maximum random row to generate
  public static final String CI_INGEST_ROW_MAX = CI_INGEST + "row.max";
  // Maximum number of random column families to generate
  public static final String CI_INGEST_MAX_CF = CI_INGEST + "max.cf";
  // Maximum number of random column qualifiers to generate
  public static final String CI_INGEST_MAX_CQ = CI_INGEST + "max.cq";
  // Optional visibilities (in CSV format) that if specified will be randomly
  // selected by ingesters for
  // each linked list
  public static final String CI_INGEST_VISIBILITIES = CI_INGEST + "visibilities";
  // Checksums will be generated during ingest if set to true
  public static final String CI_INGEST_CHECKSUM = CI_INGEST + "checksum";

  /** Batch Walker **/
  // Sleep time between batch scans (in ms)
  public static final String CI_BW_SLEEP_MS = CI_BW + "sleep.ms";
  // Scan batch size
  public static final String CI_BW_BATCH_SIZE = CI_BW + "batch.size";

  /** Walker **/
  // Sleep time between scans (in ms)
  public static final String CI_WALKER_SLEEP_MS = CI_WALKER + "sleep.ms";

  /** Scanner **/
  // Sleep time between scans (in ms)
  public static final String CI_SCANNER_SLEEP_MS = CI_SCANNER + "sleep.ms";
  // Scanner entries
  public static final String CI_SCANNER_ENTRIES = CI_SCANNER + "entries";

  /** Verify **/
  // Maximum number of mapreduce mappers
  public static final String CI_VERIFY_MAX_MAPS = CI_VERIFY + "max.maps";
  // Number of mapreduce reducers
  public static final String CI_VERIFY_REDUCERS = CI_VERIFY + "reducers";
  // Perform the verification directly on the files while the table is
  // offline"
  public static final String CI_VERIFY_SCAN_OFFLINE = CI_VERIFY + "scan.offline";
  // Comma separated list of auths to use for verify
  public static final String CI_VERIFY_AUTHS = CI_VERIFY + "auths";
  // Location in HDFS to store output
  public static final String CI_VERIFY_OUTPUT_DIR = CI_VERIFY + "output.dir";

  public static Properties loadFromFile(String propsFilePath) throws IOException {
    return loadFromStream(new FileInputStream(propsFilePath));
  }

  public static Properties loadFromStream(FileInputStream fis) throws IOException {
    Properties props = new Properties();
    props.load(fis);
    fis.close();
    return props;
  }
}
