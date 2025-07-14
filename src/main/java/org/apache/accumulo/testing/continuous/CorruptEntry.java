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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

public class CorruptEntry {
  public static void main(String[] args) throws Exception {
    try (ContinuousEnv env = new ContinuousEnv(args);
        AccumuloClient client = env.getAccumuloClient();
        var scanner = client.createScanner(env.getAccumuloTableName());
        var writer = client.createBatchWriter(env.getAccumuloTableName())) {
      final long rowMin = env.getRowMin();

      var startRow = ContinuousIngest.genRow(rowMin);
      new Range(new Text(startRow), null);
      scanner.setRange(new Range(new Text(startRow), null));
      var iter = scanner.iterator();
      if (iter.hasNext()) {
        var entry = iter.next();
        byte[] val = entry.getValue().get();
        int offset = ContinuousWalk.getChecksumOffset(val);
        if (offset >= 0) {
          for (int i = 0; i < 8; i++) {
            if (val[i + offset] == 'f' || val[i + offset] == 'F') {
              // in the case of an f hex char, set the hex char to 0
              val[i + offset] = '0';

            } else {
              // increment the hex char in the checcksum
              val[i + offset]++;
            }
          }
          Key key = entry.getKey();
          Mutation m = new Mutation(key.getRow());
          m.at().family(key.getColumnFamily()).qualifier(key.getColumnQualifier())
              .visibility(key.getColumnVisibility()).put(val);
          writer.addMutation(m);
          writer.flush();
          System.out.println("Corrupted checksum value on key " + key);
        }
      } else {
        System.out.println("No entry found after " + new String(startRow, UTF_8));
      }
    }
  }
}
