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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.accumulo.testing.core.bulk.BulkWalk.BadChecksumException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A map reduce job that verifies a table created by continuous ingest. It verifies that all referenced nodes are defined.
 */
public class BulkVerify extends Configured implements Tool {

    public static final VLongWritable DEF = new VLongWritable(-1);

    public static class CMapper extends Mapper<Key, Value, LongWritable, VLongWritable> {

        private static final Logger log = LoggerFactory.getLogger(CMapper.class);
        private LongWritable row = new LongWritable();
        private LongWritable ref = new LongWritable();
        private VLongWritable vrow = new VLongWritable();

        private long corrupt = 0;

        @Override
        public void map(Key key, Value data, Context context) throws IOException, InterruptedException {
            long r = Long.parseLong(key.getRow().toString(), 16);
            if (r < 0)
                throw new IllegalArgumentException();

            try {
                BulkWalk.validate(key, data);
            } catch (BadChecksumException bce) {
                context.getCounter(Counts.CORRUPT).increment(1L);
                if (corrupt < 1000) {
                    log.error("Bad checksum : " + key);
                } else if (corrupt == 1000) {
                    System.out.println("Too many bad checksums, not printing anymore!");
                }
                corrupt++;
                return;
            }

            row.set(r);

            context.write(row, DEF);
            byte[] val = data.get();

            int offset = BulkWalk.getPrevRowOffset(val);
            if (offset > 0) {
                ref.set(Long.parseLong(new String(val, offset, 16, UTF_8), 16));
                vrow.set(r);
                context.write(ref, vrow);
            }
        }
    }

    public enum Counts {
        UNREFERENCED, UNDEFINED, REFERENCED, CORRUPT
    }

    public static class CReducer extends Reducer<LongWritable, VLongWritable, Text, Text> {
        private ArrayList<Long> refs = new ArrayList<>();

        @Override
        public void reduce(LongWritable key, Iterable<VLongWritable> values, Context context) throws IOException, InterruptedException {

            int defCount = 0;

            refs.clear();
            for (VLongWritable type : values) {
                if (type.get() == -1) {
                    defCount++;
                } else {
                    refs.add(type.get());
                }
            }

            if (defCount == 0 && refs.size() > 0) {
                StringBuilder sb = new StringBuilder();
                String comma = "";
                for (Long ref : refs) {
                    sb.append(comma);
                    comma = ",";
                    sb.append(new String(BulkIngest.genRow(ref), UTF_8));
                }

                context.write(new Text(BulkIngest.genRow(key.get())), new Text(sb.toString()));
                context.getCounter(Counts.UNDEFINED).increment(1L);

            } else if (defCount > 0 && refs.size() == 0) {
                context.getCounter(Counts.UNREFERENCED).increment(1L);
            } else {
                context.getCounter(Counts.REFERENCED).increment(1L);
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Properties props = TestProps.loadFromFile(args[0]);
        BulkEnv env = new BulkEnv(props);

        Job job = Job.getInstance(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(AccumuloInputFormat.class);

        boolean scanOffline = Boolean.parseBoolean(props.getProperty(TestProps.BL_VERIFY_SCAN_OFFLINE));
        String tableName = env.getAccumuloTableName();
        int maxMaps = Integer.parseInt(props.getProperty(TestProps.BL_VERIFY_MAX_MAPS));
        int reducers = Integer.parseInt(props.getProperty(TestProps.BL_VERIFY_REDUCERS));
        String outputDir = props.getProperty(TestProps.BL_VERIFY_OUTPUT_DIR);

        Set<Range> ranges;
        String clone = "";
        Connector conn = env.getAccumuloConnector();

        if (scanOffline) {
            Random random = new Random();
            clone = tableName + "_" + String.format("%016x", (random.nextLong() & 0x7fffffffffffffffL));
            conn.tableOperations().clone(tableName, clone, true, new HashMap<>(), new HashSet<>());
            ranges = conn.tableOperations().splitRangeByTablets(tableName, new Range(), maxMaps);
            conn.tableOperations().offline(clone);
            AccumuloInputFormat.setInputTableName(job, clone);
            AccumuloInputFormat.setOfflineTableScan(job, true);
        } else {
            ranges = conn.tableOperations().splitRangeByTablets(tableName, new Range(), maxMaps);
            AccumuloInputFormat.setInputTableName(job, tableName);
        }

        AccumuloInputFormat.setRanges(job, ranges);
        AccumuloInputFormat.setAutoAdjustRanges(job, false);
        AccumuloInputFormat.setConnectorInfo(job, env.getAccumuloUserName(), env.getToken());
        AccumuloInputFormat.setZooKeeperInstance(job, env.getClientConfiguration());

        job.setMapperClass(CMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(VLongWritable.class);

        job.setReducerClass(CReducer.class);
        job.setNumReduceTasks(reducers);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", scanOffline);

        TextOutputFormat.setOutputPath(job, new Path(outputDir));

        job.waitForCompletion(true);

        if (scanOffline) {
            conn.tableOperations().delete(clone);
        }
        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        BulkEnv env = new BulkEnv(TestProps.loadFromFile(args[0]));

        int res = ToolRunner.run(env.getHadoopConfiguration(), new BulkVerify(), args);
        if (res != 0)
            System.exit(res);
    }
}
