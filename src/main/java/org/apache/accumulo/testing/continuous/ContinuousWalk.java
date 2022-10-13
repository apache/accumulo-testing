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
package org.apache.accumulo.testing.continuous;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousWalk {
  private static final Logger log = LoggerFactory.getLogger(ContinuousWalk.class);

  static class BadChecksumException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    BadChecksumException(String msg) {
      super(msg);
    }

  }

  public static void main(String[] args) throws Exception {

    try (ContinuousEnv env = new ContinuousEnv(args)) {

      AccumuloClient client = env.getAccumuloClient();

      ArrayList<Value> values = new ArrayList<>();

      int sleepTime = Integer.parseInt(env.getTestProperty(TestProps.CI_WALKER_SLEEP_MS));
      ConsistencyLevel cl = TestProps
          .getScanConsistencyLevel(env.getTestProperty(TestProps.CI_WALKER_CONSISTENCY_LEVEL));

      while (true) {
        try (Scanner scanner = ContinuousUtil.createScanner(client, env.getAccumuloTableName(),
            env.getRandomAuthorizations())) {
          scanner.setConsistencyLevel(cl);
          String row = findAStartRow(env.getRowMin(), env.getRowMax(), scanner, env.getRandom());

          while (row != null) {

            values.clear();

            long t1 = System.currentTimeMillis();

            scanner.setRange(new Range(new Text(row)));
            for (Entry<Key,Value> entry : scanner) {
              validate(entry.getKey(), entry.getValue());
              values.add(entry.getValue());
            }

            long t2 = System.currentTimeMillis();

            log.debug("SRQ {} {} {} {}", t1, row, (t2 - t1), values.size());

            if (values.size() > 0) {
              row = getPrevRow(values.get(env.getRandom().nextInt(values.size())));
            } else {
              log.debug("MIS {} {}", t1, row);
              log.debug("MIS {} {}", t1, row);
              row = null;
            }

            if (sleepTime > 0)
              Thread.sleep(sleepTime);
          }

          if (sleepTime > 0)
            Thread.sleep(sleepTime);
        }
      }
    }
  }

  private static String findAStartRow(long min, long max, Scanner scanner, Random r) {

    byte[] scanStart = ContinuousIngest.genRow(min, max, r);
    scanner.setRange(new Range(new Text(scanStart), null));
    scanner.setBatchSize(100);

    int count = 0;
    String pr = null;

    long t1 = System.currentTimeMillis();

    for (Entry<Key,Value> entry : scanner) {
      validate(entry.getKey(), entry.getValue());
      pr = getPrevRow(entry.getValue());
      count++;
      if (pr != null)
        break;
    }

    long t2 = System.currentTimeMillis();

    System.out.printf("FSR %d %s %d %d%n", t1, new String(scanStart, UTF_8), (t2 - t1), count);

    return pr;
  }

  static int getPrevRowOffset(byte[] val) {
    if (val.length == 0)
      throw new IllegalArgumentException();
    if (val[53] != ':')
      throw new IllegalArgumentException(new String(val, UTF_8));

    // prev row starts at 54
    if (val[54] != ':') {
      if (val[54 + 16] != ':')
        throw new IllegalArgumentException(new String(val, UTF_8));
      return 54;
    }

    return -1;
  }

  private static String getPrevRow(Value value) {

    byte[] val = value.get();
    int offset = getPrevRowOffset(val);
    if (offset > 0) {
      return new String(val, offset, 16, UTF_8);
    }

    return null;
  }

  private static int getChecksumOffset(byte[] val) {
    if (val[val.length - 1] != ':') {
      if (val[val.length - 9] != ':')
        throw new IllegalArgumentException(new String(val, UTF_8));
      return val.length - 8;
    }

    return -1;
  }

  static void validate(Key key, Value value) throws BadChecksumException {
    int ckOff = getChecksumOffset(value.get());
    if (ckOff < 0)
      return;

    long storedCksum = Long.parseLong(new String(value.get(), ckOff, 8, UTF_8), 16);

    CRC32 cksum = new CRC32();

    cksum.update(key.getRowData().toArray());
    cksum.update(key.getColumnFamilyData().toArray());
    cksum.update(key.getColumnQualifierData().toArray());
    cksum.update(key.getColumnVisibilityData().toArray());
    cksum.update(value.get(), 0, ckOff);

    if (cksum.getValue() != storedCksum) {
      throw new BadChecksumException("Checksum invalid " + key + " " + value);
    }
  }
}
