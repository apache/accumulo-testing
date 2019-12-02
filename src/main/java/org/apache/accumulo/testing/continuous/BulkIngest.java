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

package org.apache.accumulo.testing.continuous;

import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.hadoop.mapreduce.partition.KeyRangePartitioner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk import a million random key value pairs. Same format as ContinuousIngest and can be verified
 * by running ContinuousVerify.
 */
public class BulkIngest extends Configured implements Tool {

  public static final Logger log = LoggerFactory.getLogger(BulkIngest.class);

  @Override
  public int run(String[] args) throws Exception {
    String ingestInstanceId = UUID.randomUUID().toString();
    String bulkDir = args[0];

    Job job = Job.getInstance(getConf());
    job.setJobName("BulkIngest_" + ingestInstanceId);
    job.setJarByClass(BulkIngest.class);
    // very important to prevent guava conflicts
    job.getConfiguration().set("mapreduce.job.classloader", "true");
    FileSystem fs = FileSystem.get(URI.create(bulkDir), job.getConfiguration());

    log.info(String.format("UUID %d %s", System.currentTimeMillis(), ingestInstanceId));

    job.setInputFormatClass(ContinuousInputFormat.class);

    // map the generated random longs to key values
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(Value.class);

    // remove bulk dir from args
    args = Arrays.asList(args).subList(1, 3).toArray(new String[2]);

    try (ContinuousEnv env = new ContinuousEnv(args)) {
      fs.mkdirs(fs.makeQualified(new Path(bulkDir)));

      // output RFiles for the import
      job.setOutputFormatClass(AccumuloFileOutputFormat.class);
      AccumuloFileOutputFormat.configure()
          .outputPath(fs.makeQualified(new Path(bulkDir + "/files"))).store(job);

      ContinuousInputFormat.configure(job.getConfiguration(), ingestInstanceId, env);

      String tableName = env.getAccumuloTableName();

      // create splits file for KeyRangePartitioner
      String splitsFile = bulkDir + "/splits.txt";
      try (AccumuloClient client = env.getAccumuloClient()) {

        // make sure splits file is closed before continuing
        try (PrintStream out = new PrintStream(
            new BufferedOutputStream(fs.create(fs.makeQualified(new Path(splitsFile)))))) {
          Collection<Text> splits = client.tableOperations().listSplits(tableName,
              env.getBulkReducers() - 1);
          for (Text split : splits) {
            out.println(Base64.getEncoder().encodeToString(split.copyBytes()));
          }
          job.setNumReduceTasks(splits.size() + 1);
        }

        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, fs.makeQualified(new Path(splitsFile)).toString());

        job.waitForCompletion(true);
        boolean success = job.isSuccessful();

        return success ? 0 : 1;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new BulkIngest(), args);
    System.exit(ret);
  }
}
