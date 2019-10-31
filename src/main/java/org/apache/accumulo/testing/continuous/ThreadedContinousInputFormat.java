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

import static org.apache.accumulo.testing.continuous.ContinuousIngest.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import com.google.common.collect.Maps;

/**
 * Purpose and Justification: Defines an asynchronous input format that parallelizes the creation of
 * the data. This aims to move the bar from a data generation + I/O ( spill ) problem to the right
 * so that if we are able to optimize the spill path we ultimately minimize the impact of data
 * generation.
 *
 * Note that this class leverages BulkKey versus Key. This was not merged into ContinousInputformat
 * as we want to keep the original tested capabilities in the repo while also offering the ability
 * to have no additional thread management. Since this class has an ExecutorService this may not be
 * totally useful for all architectures where input formats may be used ( such as in Spark ). As a
 * result ContinuousInputFormat still exists.
 */
public class ThreadedContinousInputFormat extends InputFormat<BulkKey,Value> {

  static final String PROP_ASYNC = "mrbulk.async.threads";
  static final String PROP_ASYNC_KEYS = "mrbulk.async.keys";

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    int numTask = jobContext.getConfiguration().getInt(ContinuousInputFormat.PROP_MAP_TASK, 1);
    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < numTask; i++) {
      splits.add(new ContinuousInputFormat.RandomSplit());
    }
    return splits;
  }

  public static void configure(Configuration conf, String uuid, ContinuousEnv env) {
    conf.set(ContinuousInputFormat.PROP_UUID, uuid);
    conf.setInt(ContinuousInputFormat.PROP_MAP_TASK, env.getBulkMapTask());
    conf.setLong(ContinuousInputFormat.PROP_MAP_NODES, env.getBulkMapNodes());
    conf.setLong(ContinuousInputFormat.PROP_ROW_MIN, env.getRowMin());
    conf.setLong(ContinuousInputFormat.PROP_ROW_MAX, env.getRowMax());
    conf.setInt(ContinuousInputFormat.PROP_FAM_MAX, env.getMaxColF());
    conf.setInt(ContinuousInputFormat.PROP_QUAL_MAX, env.getMaxColQ());
    conf.setBoolean(ContinuousInputFormat.PROP_CHECKSUM,
        Boolean.parseBoolean(env.getTestProperty(TestProps.CI_INGEST_CHECKSUM)));
    conf.set(ContinuousInputFormat.PROP_VIS, env.getTestProperty(TestProps.CI_INGEST_VISIBILITIES));
  }

  @Override
  public RecordReader<BulkKey,Value> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) {
    return new RecordReader<BulkKey,Value>() {
      long numNodes;
      // need not be atomic since we're gathering data in a single thread
      long nodeCount;
      final LongAdder generatedCount = new LongAdder();
      final AtomicBoolean running = new AtomicBoolean(false);
      private Random random;

      private byte[] uuid;

      long minRow;
      long maxRow;
      int maxFam;
      int maxQual;
      List<ColumnVisibility> visibilities;
      boolean checksum;

      int threads = 0;
      int queuedKeys = 100;

      ExecutorService service = null;

      BlockingQueue<Map.Entry<BulkKey,Value>> queue;

      long timestamp = System.currentTimeMillis();

      BulkKey currKey;
      Value currValue;

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext job) {

        Configuration jobConfig = job.getConfiguration();

        numNodes = jobConfig.getLong(ContinuousInputFormat.PROP_MAP_NODES, 1000000);
        uuid = jobConfig.get(ContinuousInputFormat.PROP_UUID).getBytes(StandardCharsets.UTF_8);

        minRow = jobConfig.getLong(ContinuousInputFormat.PROP_ROW_MIN, 0);
        maxRow = jobConfig.getLong(ContinuousInputFormat.PROP_ROW_MAX, Long.MAX_VALUE);
        maxFam = jobConfig.getInt(ContinuousInputFormat.PROP_FAM_MAX, Short.MAX_VALUE);
        maxQual = jobConfig.getInt(ContinuousInputFormat.PROP_QUAL_MAX, Short.MAX_VALUE);
        checksum = jobConfig.getBoolean(ContinuousInputFormat.PROP_CHECKSUM, false);

        threads = jobConfig.getInt(PROP_ASYNC, 5);
        queuedKeys = jobConfig.getInt(PROP_ASYNC_KEYS, 1000);
        queue = new ArrayBlockingQueue<>(queuedKeys);
        checksum = job.getConfiguration().getBoolean(ContinuousInputFormat.PROP_CHECKSUM, false);
        visibilities = ContinuousIngest
            .parseVisibilities(job.getConfiguration().get(ContinuousInputFormat.PROP_VIS));

        service = Executors.newFixedThreadPool(threads);

        random = new Random(new SecureRandom().nextLong());

        nodeCount = 0;
      }

      private BulkKey genKey(CRC32 cksum) {

        byte[] row = genRow(genLong(minRow, maxRow, random));
        byte[] fam = genCol(random.nextInt(maxFam));
        byte[] qual = genCol(random.nextInt(maxQual));
        byte[] cv = visibilities.get(random.nextInt(visibilities.size())).flatten();

        if (cksum != null) {
          cksum.update(row);
          cksum.update(fam);
          cksum.update(qual);
          cksum.update(cv);
        }

        return new BulkKey(row, fam, qual, cv, timestamp, false);
      }

      private byte[] createValue(byte[] ingestInstanceId, byte[] prevRow, Checksum cksum) {
        return ContinuousIngest.createValue(ingestInstanceId, nodeCount, prevRow, cksum);
      }

      @Override
      public boolean nextKeyValue() {
        currKey = null;
        if (nodeCount < numNodes) {
          if (!running.get()) {
            running.set(true);
            // start up lazily
            IntStream.range(0, threads).forEach(x -> {
              Runnable generator = () -> {
                BulkKey prevKey;
                CRC32 cksum = checksum ? new CRC32() : null;
                while (generatedCount.longValue() < numNodes && running.get()) {
                  try {
                    final BulkKey key = genKey(cksum);
                    final Value value = new Value(
                        createValue(uuid, key.getRowData().toArray(), cksum));
                    while (!queue.offer(Maps.immutableEntry(key, value), 1, TimeUnit.SECONDS)) {
                      if (!running.get())
                        return;
                    }
                    generatedCount.add(1);
                  } catch (Throwable e) {
                    break;
                  }
                }
              };
              service.submit(generator);
            });
          }
          while (null == currKey) {
            Map.Entry<BulkKey,Value> kv = null;
            try {
              kv = queue.poll(1, TimeUnit.SECONDS);
              if (null != kv) {
                currKey = kv.getKey();
                currValue = kv.getValue();
                nodeCount++;
                break;
              }
            } catch (InterruptedException e) {
              // spurious interruptions can occur
            }

          }
          return true;
        } else {
          return false;
        }
      }

      @Override
      public BulkKey getCurrentKey() {
        return currKey;
      }

      @Override
      public Value getCurrentValue() {
        return currValue;
      }

      @Override
      public float getProgress() {
        return nodeCount * 1.0f / numNodes;
      }

      @Override
      public void close() throws IOException {
        running.set(false);
        service.shutdownNow();
      }
    };
  }
}
