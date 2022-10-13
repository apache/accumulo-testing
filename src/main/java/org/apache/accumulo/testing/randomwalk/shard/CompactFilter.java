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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

/**
 * Test deleting documents by using a compaction filter iterator
 */
public class CompactFilter extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String indexTableName = state.getString("indexTableName");
    String docTableName = state.getString("docTableName");
    Random rand = state.getRandom();

    String deleteChar = Integer.toHexString(rand.nextInt(16)) + "";
    String regex = "^[0-9a-f][" + deleteChar + "].*";

    ArrayList<IteratorSetting> documentFilters = new ArrayList<>();

    IteratorSetting is = new IteratorSetting(21, "ii", RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    RegExFilter.setNegate(is, true);
    documentFilters.add(is);

    long t1 = System.currentTimeMillis();
    env.getAccumuloClient().tableOperations().compact(docTableName, null, null, documentFilters,
        true, true);
    long t2 = System.currentTimeMillis();
    long t3 = t2 - t1;

    ArrayList<IteratorSetting> indexFilters = new ArrayList<>();

    is = new IteratorSetting(21, RegExFilter.class);
    RegExFilter.setRegexs(is, null, null, regex, null, false);
    RegExFilter.setNegate(is, true);
    indexFilters.add(is);

    t1 = System.currentTimeMillis();
    env.getAccumuloClient().tableOperations().compact(indexTableName, null, null, indexFilters,
        true, true);
    t2 = System.currentTimeMillis();

    log.debug(
        "Filtered documents using compaction iterators " + regex + " " + (t3) + " " + (t2 - t1));

    try (BatchScanner bscanner = env.getAccumuloClient().createBatchScanner(docTableName,
        new Authorizations(), 10)) {

      List<Range> ranges = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        ranges.add(Range.prefix(new Text(Integer.toHexString(i) + "" + deleteChar)));
      }

      bscanner.setRanges(ranges);
      Iterator<Entry<Key,Value>> iter = bscanner.iterator();

      if (iter.hasNext()) {
        throw new Exception("Saw unexpected document " + iter.next().getKey());
      }

    }
  }

}
