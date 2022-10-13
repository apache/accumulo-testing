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
package org.apache.accumulo.testing.randomwalk.bulk;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.hadoop.io.Text;

public class Split extends SelectiveBulkTest {

  @Override
  protected void runLater(State state, RandWalkEnv env) throws Exception {
    Random rand = state.getRandom();
    int count = rand.nextInt(20);
    SortedSet<Text> splits = Stream.generate(rand::nextLong).map(l -> l & 0x7fffffffffffffffL)
        .map(l -> l % BulkPlusOne.LOTS).map(l -> String.format(BulkPlusOne.FMT, l)).map(Text::new)
        .limit(count).collect(Collectors.toCollection(TreeSet::new));
    log.info("splitting {}", splits);
    env.getAccumuloClient().tableOperations().addSplits(Setup.getTableName(), splits);
    log.info("split for " + splits + " finished");
  }

}
