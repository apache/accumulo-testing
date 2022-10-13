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
package org.apache.accumulo.testing;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProps {

  private static final Logger LOG = LoggerFactory.getLogger(TestProps.class);

  private static final String PREFIX = "test.";
  private static final String COMMON = PREFIX + "common.";
  private static final String CI = PREFIX + "ci.";
  private static final String CI_COMMON = CI + "common.";
  private static final String CI_INGEST = CI + "ingest.";
  private static final String CI_WALKER = CI + "walker.";
  private static final String CI_BW = CI + "batch.walker.";
  private static final String CI_SCANNER = CI + "scanner.";
  private static final String CI_VERIFY = CI + "verify.";
  private static final String CI_BULK = CI + "bulk.";
  public static final String TERASORT = PREFIX + "terasort.";
  public static final String ROWHASH = PREFIX + "rowhash.";

  /** Common properties **/
  // HDFS root path. Should match 'fs.defaultFS' property in Hadoop's core-site.xml
  public static final String HDFS_ROOT = COMMON + "hdfs.root";
  // YARN resource manager hostname. Should match 'yarn.resourcemanager.hostname' property in
  // Hadoop's yarn-site.xml
  public static final String YARN_RESOURCE_MANAGER = COMMON + "yarn.resource.manager";
  // Memory (in MB) given to each YARN container
  public static final String YARN_CONTAINER_MEMORY_MB = COMMON + "yarn.container.memory.mb";
  // Number of cores given to each YARN container
  public static final String YARN_CONTAINER_CORES = COMMON + "yarn.container.cores";
  // Name of metadata table
  public static final String METADATA_TABLE_NAME = "accumulo.metadata";
  // Name of replication table
  public static final String REPLICATION_TABLE_NAME = "accumulo.replication";

  /** Continuous ingest test properties **/
  /** Common **/
  // Accumulo table used by continuous ingest tests
  public static final String CI_COMMON_ACCUMULO_TABLE = CI_COMMON + "accumulo.table";
  // props to set on the table when created
  public static final String CI_COMMON_ACCUMULO_TABLE_PROPS = CI_COMMON + "accumulo.table.props";
  // Number of tablets that should exist in Accumulo table when created
  public static final String CI_COMMON_ACCUMULO_NUM_TABLETS = CI_COMMON + "accumulo.num.tablets";
  // Optional authorizations (in CSV format) that if specified will be
  // randomly selected by scanners and walkers
  public static final String CI_COMMON_AUTHS = CI_COMMON + "auths";
  // Tserver props to set when a table is created
  public static final String CI_COMMON_ACCUMULO_SERVER_PROPS = CI_COMMON + "accumulo.server.props";

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
  // Enables periodic pausing of ingest
  public static final String CI_INGEST_PAUSE_ENABLED = CI_INGEST + "pause.enabled";
  // Minimum wait between ingest pauses (in seconds)
  public static final String CI_INGEST_PAUSE_WAIT_MIN = CI_INGEST + "pause.wait.min";
  // Maximum wait between ingest pauses (in seconds)
  public static final String CI_INGEST_PAUSE_WAIT_MAX = CI_INGEST + "pause.wait.max";
  // Minimum pause duration (in seconds)
  public static final String CI_INGEST_PAUSE_DURATION_MIN = CI_INGEST + "pause.duration.min";
  // Maximum pause duration (in seconds)
  public static final String CI_INGEST_PAUSE_DURATION_MAX = CI_INGEST + "pause.duration.max";
  // Amount of data to write before flushing. Pause checks are only done after flush.
  public static final String CI_INGEST_FLUSH_ENTRIES = CI_INGEST + "entries.flush";
  // The probability (between 0.0 and 1.0) that a set of entries will be deleted during continuous
  // ingest
  public static final String CI_INGEST_DELETE_PROBABILITY = CI_INGEST + "delete.probability";

  /** Batch Walker **/
  // Sleep time between batch scans (in ms)
  public static final String CI_BW_SLEEP_MS = CI_BW + "sleep.ms";
  // Scan batch size
  public static final String CI_BW_BATCH_SIZE = CI_BW + "batch.size";
  // Perform the scan using the configured consistency level
  public static final String CI_BW_CONSISTENCY_LEVEL = CI_BW + "consistency.level";

  /** Walker **/
  // Sleep time between scans (in ms)
  public static final String CI_WALKER_SLEEP_MS = CI_WALKER + "sleep.ms";
  // Perform the scan using the configured consistency level
  public static final String CI_WALKER_CONSISTENCY_LEVEL = CI_WALKER + "consistency.level";

  /** Scanner **/
  // Sleep time between scans (in ms)
  public static final String CI_SCANNER_SLEEP_MS = CI_SCANNER + "sleep.ms";
  // Scanner entries
  public static final String CI_SCANNER_ENTRIES = CI_SCANNER + "entries";
  // Perform the scan using the configured consistency level
  public static final String CI_SCANNER_CONSISTENCY_LEVEL = CI_SCANNER + "consistency.level";

  /** Verify **/
  // Maximum number of mapreduce mappers
  public static final String CI_VERIFY_MAX_MAPS = CI_VERIFY + "max.maps";
  // Number of mapreduce reducers
  public static final String CI_VERIFY_REDUCERS = CI_VERIFY + "reducers";
  // Perform the verify using the configured consistency level
  public static final String CI_VERIFY_CONSISTENCY_LEVEL = CI_VERIFY + "scan.consistency.level";
  // Perform the verification directly on the files while the table is
  // offline"
  public static final String CI_VERIFY_SCAN_OFFLINE = CI_VERIFY + "scan.offline";
  // Comma separated list of auths to use for verify
  public static final String CI_VERIFY_AUTHS = CI_VERIFY + "auths";
  // Location in HDFS to store output
  public static final String CI_VERIFY_OUTPUT_DIR = CI_VERIFY + "output.dir";

  /** Bulk **/
  public static final String CI_BULK_MAP_TASK = CI_BULK + "map.task";
  public static final String CI_BULK_MAP_NODES = CI_BULK + "map.nodes";
  public static final String CI_BULK_REDUCERS = CI_BULK + "reducers.max";

  /** TeraSort **/
  public static final String TERASORT_TABLE = TERASORT + "table";
  public static final String TERASORT_NUM_ROWS = TERASORT + "num.rows";
  public static final String TERASORT_MIN_KEYSIZE = TERASORT + "min.keysize";
  public static final String TERASORT_MAX_KEYSIZE = TERASORT + "max.keysize";
  public static final String TERASORT_MIN_VALUESIZE = TERASORT + "min.valuesize";
  public static final String TERASORT_MAX_VALUESIZE = TERASORT + "max.valuesize";
  public static final String TERASORT_NUM_SPLITS = TERASORT + "num.splits";

  /** RowHash **/
  public static final String ROWHASH_INPUT_TABLE = ROWHASH + "input.table";
  public static final String ROWHASH_OUTPUT_TABLE = ROWHASH + "output.table";
  public static final String ROWHASH_COLUMN = ROWHASH + "column";

  public static Properties loadFromFile(String propsFilePath) {
    try {
      return loadFromStream(new FileInputStream(propsFilePath));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Properties loadFromStream(FileInputStream fis) throws IOException {
    Properties props = new Properties();
    props.load(fis);
    fis.close();
    return props;
  }

  public static ConsistencyLevel getScanConsistencyLevel(String property) {
    ConsistencyLevel cl = ConsistencyLevel.IMMEDIATE;
    if (!StringUtils.isEmpty(property)) {
      try {
        cl = ConsistencyLevel.valueOf(property.toUpperCase());
      } catch (Exception e) {
        LOG.warn("Error setting consistency level to {}, using IMMEDIATE", property.toUpperCase());
        cl = ConsistencyLevel.IMMEDIATE;
      }
    }
    return cl;
  }
}
