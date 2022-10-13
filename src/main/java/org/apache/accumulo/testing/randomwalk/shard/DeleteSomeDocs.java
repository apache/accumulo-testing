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

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

//a test created to test the batch deleter
public class DeleteSomeDocs extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    // delete documents where the document id matches a given pattern
    // from doc and index table using the batch deleter

    Random rand = state.getRandom();
    String indexTableName = state.getString("indexTableName");
    String dataTableName = state.getString("docTableName");

    List<String> patterns = props.keySet().stream().filter(key -> key instanceof String)
        .map(key -> (String) key).filter(key -> key.startsWith("pattern")).map(props::getProperty)
        .collect(Collectors.toList());

    String pattern = patterns.get(rand.nextInt(patterns.size()));
    BatchWriterConfig bwc = new BatchWriterConfig();
    IteratorSetting iterSettings = new IteratorSetting(100, RegExFilter.class);
    try (BatchDeleter ibd = env.getAccumuloClient().createBatchDeleter(indexTableName,
        Authorizations.EMPTY, 8, bwc)) {
      ibd.setRanges(Collections.singletonList(new Range()));

      RegExFilter.setRegexs(iterSettings, null, null, pattern, null, false);

      ibd.addScanIterator(iterSettings);

      ibd.delete();

    }

    try (BatchDeleter dbd = env.getAccumuloClient().createBatchDeleter(dataTableName,
        Authorizations.EMPTY, 8, bwc)) {
      dbd.setRanges(Collections.singletonList(new Range()));

      iterSettings = new IteratorSetting(100, RegExFilter.class);
      RegExFilter.setRegexs(iterSettings, pattern, null, null, null, false);

      dbd.addScanIterator(iterSettings);

      dbd.delete();

    }

    log.debug("Deleted documents w/ id matching '" + pattern + "'");
  }
}
