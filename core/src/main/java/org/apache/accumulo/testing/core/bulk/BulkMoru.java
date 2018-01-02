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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map only job that reads a table created by bulk loading and creates doubly linked list. This map reduce job tests the ability of a map only job to
 * read and write to accumulo at the same time. This map reduce job mutates the table in such a way that it should not create any undefined nodes.
 */
public class BulkMoru extends Configured implements Tool {
  private static final String PREFIX = BulkMoru.class.getSimpleName() + ".";
  private static final String MAX_CQ = PREFIX + "MAX_CQ";
  private static final String MAX_CF = PREFIX + "MAX_CF";
  private static final String MAX = PREFIX + "MAX";
  private static final String MIN = PREFIX + "MIN";
  private static final String BL_ID = PREFIX + "BL_ID";

  enum Counts {
    SELF_READ
  }

  public static class CMapper extends Mapper<Key,Value,Text,Mutation> {

    private short max_cf;
    private short max_cq;
    private Random random;
    private String ingestInstanceId;
    private byte[] iiId;
    private long count;

    private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();

    @Override
    public void setup(Context context) {
      int max_cf = context.getConfiguration().getInt(MAX_CF, -1);
      int max_cq = context.getConfiguration().getInt(MAX_CQ, -1);

      if (max_cf > Short.MAX_VALUE || max_cq > Short.MAX_VALUE)
        throw new IllegalArgumentException();

      this.max_cf = (short) max_cf;
      this.max_cq = (short) max_cq;

      random = new Random();
      ingestInstanceId = context.getConfiguration().get(BL_ID);
      iiId = ingestInstanceId.getBytes(UTF_8);

      count = 0;
    }

    @Override
    public void map(Key key, Value data, Context context) throws IOException, InterruptedException {

      BulkWalk.validate(key, data);

      if (WritableComparator.compareBytes(iiId, 0, iiId.length, data.get(), 0, iiId.length) != 0) {
        // only rewrite data not written by this M/R job
        byte[] val = data.get();

        int offset = BulkWalk.getPrevRowOffset(val);
        if (offset > 0) {
          long rowLong = Long.parseLong(new String(val, offset, 16, UTF_8), 16);
          Mutation m = BulkIngest.genMutation(rowLong, random.nextInt(max_cf), random.nextInt(max_cq), EMPTY_VIS, iiId, count++, key.getRowData()
              .toArray(), true);
          context.write(null, m);
        }

      } else {
        context.getCounter(Counts.SELF_READ).increment(1L);
      }
    }
  }

  @Override
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, AccumuloSecurityException {

    Properties props = TestProps.loadFromFile(args[0]);
    BulkEnv env = new BulkEnv(props);

    Job job = Job.getInstance(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());

    job.setInputFormatClass(AccumuloInputFormat.class);

    AccumuloInputFormat.setConnectorInfo(job, env.getAccumuloUserName(), env.getToken());
    AccumuloInputFormat.setInputTableName(job, env.getAccumuloTableName());
    AccumuloInputFormat.setZooKeeperInstance(job, env.getClientConfiguration());

    int maxMaps = Integer.parseInt(props.getProperty(TestProps.BL_VERIFY_MAX_MAPS));

    // set up ranges
    try {
      Set<Range> ranges = env.getAccumuloConnector().tableOperations().splitRangeByTablets(env.getAccumuloTableName(), new Range(), maxMaps);
      AccumuloInputFormat.setRanges(job, ranges);
      AccumuloInputFormat.setAutoAdjustRanges(job, false);
    } catch (Exception e) {
      throw new IOException(e);
    }

    job.setMapperClass(CMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setBatchWriterOptions(job, env.getBatchWriterConfig());
    AccumuloOutputFormat.setConnectorInfo(job, env.getAccumuloUserName(), env.getToken());
    AccumuloOutputFormat.setCreateTables(job, true);
    AccumuloOutputFormat.setDefaultTableName(job, env.getAccumuloTableName());
    AccumuloOutputFormat.setZooKeeperInstance(job, env.getClientConfiguration());

    Configuration conf = job.getConfiguration();
    conf.setLong(MIN, env.getRowMin());
    conf.setLong(MAX, env.getRowMax());
    conf.setInt(MAX_CF, env.getMaxColF());
    conf.setInt(MAX_CQ, env.getMaxColQ());
    conf.set(BL_ID, UUID.randomUUID().toString());

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    BulkEnv env = new BulkEnv(TestProps.loadFromFile(args[0]));
    int res = ToolRunner.run(env.getHadoopConfiguration(), new BulkMoru(), args);
    if (res != 0)
      System.exit(res);
  }
}
