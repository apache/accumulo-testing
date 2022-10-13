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
package org.apache.accumulo.testing.randomwalk.concurrent;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BulkImport extends Test {

  public static class RFileBatchWriter implements BatchWriter {

    RFileWriter writer;

    public RFileBatchWriter(Configuration conf, FileSystem fs, String file) throws IOException {
      writer = RFile.newWriter().to(file).withFileSystem(fs).build();
      writer.startDefaultLocalityGroup();
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
      List<ColumnUpdate> updates = m.getUpdates();
      for (ColumnUpdate cu : updates) {
        Key key = new Key(m.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
            cu.getColumnVisibility(), 42, false, false);
        Value val = new Value(cu.getValue(), false);

        try {
          writer.append(key, val);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
      for (Mutation mutation : iterable)
        addMutation(mutation);
    }

    @Override
    public void flush() throws MutationsRejectedException {}

    @Override
    public void close() throws MutationsRejectedException {
      try {
        writer.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    String tableName = state.getRandomTableName();
    Random rand = state.getRandom();

    FileSystem fs = FileSystem.get(env.getHadoopConfiguration());

    String bulkDir = env.getHdfsRoot() + "/tmp/concurrent_bulk/b_"
        + String.format("%016x", rand.nextLong() & 0x7fffffffffffffffL);

    fs.mkdirs(new Path(bulkDir));
    fs.mkdirs(new Path(bulkDir + "_f"));

    try {
      try (BatchWriter bw = new RFileBatchWriter(env.getHadoopConfiguration(), fs,
          bulkDir + "/file01.rf")) {
        TreeSet<Long> rows = new TreeSet<>();
        int numRows = rand.nextInt(100000);
        for (int i = 0; i < numRows; i++) {
          rows.add(rand.nextLong() & 0x7fffffffffffffffL);
        }

        for (Long row : rows) {
          Mutation m = new Mutation(String.format("%016x", row));
          long val = rand.nextLong() & 0x7fffffffffffffffL;
          for (int j = 0; j < 10; j++) {
            m.put("cf", "cq" + j, new Value(String.format("%016x", val).getBytes(UTF_8)));
          }

          bw.addMutation(m);
        }
      }

      client.tableOperations().importDirectory(bulkDir).to(tableName).tableTime(rand.nextBoolean())
          .load();

      log.debug("BulkImported to " + tableName);
    } catch (TableNotFoundException e) {
      log.debug("BulkImport " + tableName + " failed, doesnt exist");
    } catch (TableOfflineException toe) {
      log.debug("BulkImport " + tableName + " failed, offline");
    } finally {
      fs.delete(new Path(bulkDir), true);
      fs.delete(new Path(bulkDir + "_f"), true);
    }

  }
}
