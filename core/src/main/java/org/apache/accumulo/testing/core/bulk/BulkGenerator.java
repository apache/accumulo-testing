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
package org.apache.accumulo.testing.core.bulk;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.hadoop.io.Text;

import java.util.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/*
 * Create 1 million random numbers that point to the previous 1 million (if there is a previous)
 * Write the 1 million to a file in hdfs in directory A.
 * Check the size of the hdfs directory where files are output to. If its too large then wait.
 *
 * */
public class BulkGenerator {

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static List<ColumnVisibility> visibilities;

  private static ColumnVisibility getVisibility(Random rand) {
    return visibilities.get(rand.nextInt(visibilities.size()));
  }

  public static void main(String[] args) throws Exception {
    // parameters needed
    if (args.length != 1) {
      System.err.println("Usage: BulkLoading <propsPath>");
      System.exit(-1);
    }

    Properties props = TestProps.loadFromFile(args[0]);

    String vis = props.getProperty(TestProps.BL_INGEST_VISIBILITIES);
    if (vis == null) {
      visibilities = Collections.singletonList(new ColumnVisibility());
    } else {
      visibilities = new ArrayList<>();
      for (String v : vis.split(",")) {
        visibilities.add(new ColumnVisibility(v.trim()));
      }
    }

    BulkEnv env = new BulkEnv(props);

    long rowMin = env.getRowMin();
    long rowMax = env.getRowMax();
    if (rowMin < 0 || rowMax < 0 || rowMax <= rowMin) {
      throw new IllegalArgumentException("bad min and max");
    }

    Connector conn = env.getAccumuloConnector();
    String tableName = env.getAccumuloTableName();
    if (!conn.tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, "Consult the README and create the table before starting ingest.");
    }

    while (true) {
      // run generators
    }

  }

  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, ColumnVisibility cv, byte[] ingestInstanceId, long count, byte[] prevRow,
      boolean checksum) {
    // Adler32 is supposed to be faster, but according to wikipedia is not
    // good for small data.... so used CRC32 instead
    CRC32 cksum = null;

    byte[] rowString = genRow(rowLong);

    byte[] cfString = FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
    byte[] cqString = FastFormat.toZeroPaddedString(cqInt, 4, 16, EMPTY_BYTES);

    if (checksum) {
      cksum = new CRC32();
      cksum.update(rowString);
      cksum.update(cfString);
      cksum.update(cqString);
      cksum.update(cv.getExpression());
    }

    Mutation m = new Mutation(new Text(rowString));

    m.put(new Text(cfString), new Text(cqString), cv, createValue(ingestInstanceId, count, prevRow, cksum));
    return m;
  }

  public static long genLong(long min, long max, Random r) {
    return ((r.nextLong() & 0x7fffffffffffffffL) % (max - min)) + min;
  }

  static byte[] genRow(long min, long max, Random r) {
    return genRow(genLong(min, max, r));
  }

  static byte[] genRow(long rowLong) {
    return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
  }

  private static Value createValue(byte[] ingestInstanceId, long count, byte[] prevRow, Checksum cksum) {
    int dataLen = ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + 3;
    if (cksum != null)
      dataLen += 8;
    byte val[] = new byte[dataLen];
    System.arraycopy(ingestInstanceId, 0, val, 0, ingestInstanceId.length);
    int index = ingestInstanceId.length;
    val[index++] = ':';
    int added = FastFormat.toZeroPaddedString(val, index, count, 16, 16, EMPTY_BYTES);
    if (added != 16)
      throw new RuntimeException(" " + added);
    index += 16;
    val[index++] = ':';
    if (prevRow != null) {
      System.arraycopy(prevRow, 0, val, index, prevRow.length);
      index += prevRow.length;
    }

    val[index++] = ':';

    if (cksum != null) {
      cksum.update(val, 0, index);
      cksum.getValue();
      FastFormat.toZeroPaddedString(val, index, cksum.getValue(), 8, 16, EMPTY_BYTES);
    }

    // System.out.println("val "+new String(val));
    return new Value(val);
  }
}
