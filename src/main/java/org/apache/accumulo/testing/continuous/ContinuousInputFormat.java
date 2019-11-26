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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Generates a continuous ingest linked list per map reduce split. Each linked list is of
 * configurable length.
 */
public class ContinuousInputFormat extends InputFormat<BulkKey,Value> {

  static class RandomSplit extends InputSplit implements Writable {
    @Override
    public void write(DataOutput dataOutput) {}

    @Override
    public void readFields(DataInput dataInput) {}

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    int numTask = jobContext.getConfiguration().getInt(ContinousInputOptions.PROP_MAP_TASK, 1);
    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < numTask; i++) {
      splits.add(new RandomSplit());
    }
    return splits;
  }

  @Override
  public RecordReader<BulkKey,Value> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) {
    return new ContinousRecordReader();
  }
}
