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

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map only job that reads a table created by continuous ingest and creates doubly linked list.
 * This map reduce job tests the ability of a map only job to read and write to accumulo at the same
 * time. This map reduce job mutates the table in such a way that it should not create any undefined
 * nodes.
 */
public class ContinuousMoru extends Configured implements Tool {
  private static final String PREFIX = ContinuousMoru.class.getSimpleName() + ".";
  private static final String MAX_CQ = PREFIX + "MAX_CQ";
  private static final String MAX_CF = PREFIX + "MAX_CF";
  private static final String MAX = PREFIX + "MAX";
  private static final String MIN = PREFIX + "MIN";
  private static final String CI_ID = PREFIX + "CI_ID";

  enum Counts {
    SELF_READ
  }

  public static class CMapper extends Mapper<Key,Value,Text,Mutation> {

    private short max_cf;
    private short max_cq;
    private Random random;
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
      final String ingestInstanceId = context.getConfiguration().get(CI_ID);
      iiId = ingestInstanceId.getBytes(UTF_8);

      count = 0;
    }

    @Override
    public void map(Key key, Value data, Context context) throws IOException, InterruptedException {

      ContinuousWalk.validate(key, data);

      if (WritableComparator.compareBytes(iiId, 0, iiId.length, data.get(), 0, iiId.length) != 0) {
        // only rewrite data not written by this M/R job
        byte[] val = data.get();

        int offset = ContinuousWalk.getPrevRowOffset(val);
        if (offset > 0) {
          long rowLong = Long.parseLong(new String(val, offset, 16, UTF_8), 16);
          Mutation m = ContinuousIngest.genMutation(rowLong, random.nextInt(max_cf),
              random.nextInt(max_cq), EMPTY_VIS, iiId, count++, key.getRowData().toArray(), true);
          context.write(null, m);
        }

      } else {
        context.getCounter(Counts.SELF_READ).increment(1L);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    try (ContinuousEnv env = new ContinuousEnv(args)) {

      Job job = Job.getInstance(getConf(),
          this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      job.setInputFormatClass(AccumuloInputFormat.class);

      int maxMaps = Integer.parseInt(env.getTestProperty(TestProps.CI_VERIFY_MAX_MAPS));
      Set<Range> ranges = env.getAccumuloClient().tableOperations()
          .splitRangeByTablets(env.getAccumuloTableName(), new Range(), maxMaps);

      AccumuloInputFormat.configure().clientProperties(env.getClientProps())
          .table(env.getAccumuloTableName()).ranges(ranges).autoAdjustRanges(false).store(job);

      job.setMapperClass(CMapper.class);
      job.setNumReduceTasks(0);
      job.setOutputFormatClass(AccumuloOutputFormat.class);

      AccumuloOutputFormat.configure().clientProperties(env.getClientProps()).createTables(true)
          .defaultTable(env.getAccumuloTableName()).store(job);

      Configuration conf = job.getConfiguration();
      conf.setLong(MIN, env.getRowMin());
      conf.setLong(MAX, env.getRowMax());
      conf.setInt(MAX_CF, env.getMaxColF());
      conf.setInt(MAX_CQ, env.getMaxColQ());
      conf.set(CI_ID, UUID.randomUUID().toString());
      conf.set("mapreduce.job.classloader", "true");

      job.waitForCompletion(true);
      return job.isSuccessful() ? 0 : 1;
    }
  }

  public static void main(String[] args) throws Exception {
    try (ContinuousEnv env = new ContinuousEnv(args)) {
      int res = ToolRunner.run(env.getHadoopConfiguration(), new ContinuousMoru(), args);
      if (res != 0)
        System.exit(res);
    }
  }
}
