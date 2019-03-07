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

package org.apache.accumulo.testing.performance.util;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.FastFormat;

public class TestData {

  private static final byte[] EMPTY = new byte[0];

  public static byte[] row(long r) {
    return FastFormat.toZeroPaddedString(r, 16, 16, EMPTY);
  }

  public static byte[] fam(int f) {
    return FastFormat.toZeroPaddedString(f, 8, 16, EMPTY);
  }

  public static byte[] qual(int q) {
    return FastFormat.toZeroPaddedString(q, 8, 16, EMPTY);
  }

  public static byte[] val(long v) {
    return FastFormat.toZeroPaddedString(v, 9, 16, EMPTY);
  }

  public static void generate(AccumuloClient client, String tableName, int rows, int fams,
      int quals) throws Exception {
    try (BatchWriter writer = client.createBatchWriter(tableName)) {
      int v = 0;
      for (int r = 0; r < rows; r++) {
        Mutation m = new Mutation(row(r));
        for (int f = 0; f < fams; f++) {
          byte[] fam = fam(f);
          for (int q = 0; q < quals; q++) {
            byte[] qual = qual(q);
            byte[] val = val(v++);
            m.put(fam, qual, val);
          }
        }
        writer.addMutation(m);
      }
    }
  }
}
