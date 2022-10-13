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
package org.apache.accumulo.testing.randomwalk.shard;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

public class BulkInsert extends Test {

  static class SeqfileBatchWriter implements BatchWriter {

    SequenceFile.Writer writer;

    SeqfileBatchWriter(Configuration conf, FileSystem fs, String file) throws IOException {
      writer = SequenceFile.createWriter(conf,
          SequenceFile.Writer.file(fs.makeQualified(new Path(file))),
          SequenceFile.Writer.keyClass(Key.class), SequenceFile.Writer.valueClass(Value.class));
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
      List<ColumnUpdate> updates = m.getUpdates();
      for (ColumnUpdate cu : updates) {
        Key key = new Key(m.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
            cu.getColumnVisibility(), Long.MAX_VALUE, false, false);
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

    String indexTableName = state.getString("indexTableName");
    String dataTableName = state.getString("docTableName");
    int numPartitions = state.getInteger("numPartitions");
    Random rand = state.getRandom();
    long nextDocID = state.getLong("nextDocID");

    int minInsert = Integer.parseInt(props.getProperty("minInsert"));
    int maxInsert = Integer.parseInt(props.getProperty("maxInsert"));
    int numToInsert = rand.nextInt(maxInsert - minInsert) + minInsert;

    int maxSplits = Integer.parseInt(props.getProperty("maxSplits"));

    Configuration conf = env.getHadoopConfiguration();
    FileSystem fs = FileSystem.get(conf);

    String rootDir = "/tmp/shard_bulk/" + dataTableName;

    fs.mkdirs(new Path(rootDir));

    try (BatchWriter dataWriter = new SeqfileBatchWriter(conf, fs, rootDir + "/data.seq");
        BatchWriter indexWriter = new SeqfileBatchWriter(conf, fs, rootDir + "/index.seq")) {

      for (int i = 0; i < numToInsert; i++) {
        String docID = Insert.insertRandomDocument(nextDocID++, dataWriter, indexWriter,
            numPartitions, rand);
        log.debug("Bulk inserting document " + docID);
      }

      state.set("nextDocID", nextDocID);
    }

    sort(env, fs, dataTableName, rootDir + "/data.seq", rootDir + "/data_bulk",
        rootDir + "/data_work", maxSplits);
    sort(env, fs, indexTableName, rootDir + "/index.seq", rootDir + "/index_bulk",
        rootDir + "/index_work", maxSplits);

    bulkImport(fs, env, dataTableName, rootDir, "data");
    bulkImport(fs, env, indexTableName, rootDir, "index");

    fs.delete(new Path(rootDir), true);
  }

  @SuppressWarnings("deprecation")
  private void bulkImport(FileSystem fs, RandWalkEnv env, String tableName, String rootDir,
      String prefix) throws Exception {
    while (true) {
      String bulkDir = rootDir + "/" + prefix + "_bulk";
      String failDir = rootDir + "/" + prefix + "_failure";
      Path failPath = new Path(failDir);
      fs.delete(failPath, true);
      fs.mkdirs(failPath);
      env.getAccumuloClient().tableOperations().importDirectory(tableName, bulkDir, failDir, true);

      FileStatus[] failures = fs.listStatus(failPath);

      if (failures == null || failures.length == 0)
        break;

      log.warn("Failed to bulk import some files, retrying ");

      for (FileStatus failure : failures) {
        if (!failure.getPath().getName().endsWith(".seq"))
          fs.rename(failure.getPath(), new Path(new Path(bulkDir), failure.getPath().getName()));
        else
          log.debug("Ignoring " + failure.getPath());
      }
      sleepUninterruptibly(3, TimeUnit.SECONDS);

    }
  }

  private void sort(RandWalkEnv env, FileSystem fs, String tableName, String seqFile,
      String outputDir, String workDir, int maxSplits) throws Exception {

    try (FSDataOutputStream fsDataOutputStream = fs.create(new Path(workDir + "/splits.txt"));
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fsDataOutputStream);
        PrintStream printStream = new PrintStream(bufferedOutputStream, false, UTF_8)) {

      AccumuloClient client = env.getAccumuloClient();

      Collection<Text> splits = client.tableOperations().listSplits(tableName, maxSplits);
      for (Text split : splits)
        printStream.println(Base64.getEncoder().encodeToString(split.copyBytes()));

      SortTool sortTool = new SortTool(seqFile, outputDir, workDir + "/splits.txt", splits);

      if (ToolRunner.run(env.getHadoopConfiguration(), sortTool, new String[0]) != 0) {
        throw new Exception("Failed to run map/red verify");
      }

    }
  }

}
