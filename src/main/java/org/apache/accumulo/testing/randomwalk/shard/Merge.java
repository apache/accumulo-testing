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

import java.util.Collection;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Merge extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String indexTableName = state.getString("indexTableName");

    Collection<Text> splits = env.getAccumuloClient().tableOperations().listSplits(indexTableName);
    SortedSet<Text> splitSet = new TreeSet<>(splits);
    log.debug("merging " + indexTableName);
    env.getAccumuloClient().tableOperations().merge(indexTableName, null, null);
    org.apache.accumulo.core.util.Merge merge = new org.apache.accumulo.core.util.Merge();
    merge.mergomatic(env.getAccumuloClient(), indexTableName, null, null, 256 * 1024 * 1024, true);
    splits = env.getAccumuloClient().tableOperations().listSplits(indexTableName);
    if (splits.size() > splitSet.size()) {
      // throw an exception so that test will die and no further changes to table will occur...
      // this way table is left as is for debugging.
      throw new Exception(
          "There are more tablets after a merge: " + splits.size() + " was " + splitSet.size());
    }
  }

}
