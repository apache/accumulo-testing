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

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.*;

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
public class AsyncContinousInputFormat extends ContinuousInputFormat {
  @Override
  public RecordReader<TestKey,Value> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) {
    return new AsyncRecordReader();
  }
}
