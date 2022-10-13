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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Grep extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    // pick a few randoms words... grep for those words and search the index
    // ensure both return the same set of documents

    String indexTableName = state.getString("indexTableName");
    String dataTableName = state.getString("docTableName");
    Random rand = state.getRandom();

    Text[] words = new Text[rand.nextInt(4) + 2];

    for (int i = 0; i < words.length; i++) {
      words[i] = new Text(Insert.generateRandomWord(rand));
    }

    HashSet<Text> documentsFoundInIndex = new HashSet<>();

    try (BatchScanner bs = env.getAccumuloClient().createBatchScanner(indexTableName,
        Authorizations.EMPTY, 16)) {
      IteratorSetting ii = new IteratorSetting(20, "ii", IntersectingIterator.class.getName());
      IntersectingIterator.setColumnFamilies(ii, words);
      bs.addScanIterator(ii);
      bs.setRanges(Collections.singleton(new Range()));

      for (Entry<Key,Value> entry : bs) {
        documentsFoundInIndex.add(entry.getKey().getColumnQualifier());
      }

    }

    HashSet<Text> documentsFoundByGrep = new HashSet<>();

    try (BatchScanner bs = env.getAccumuloClient().createBatchScanner(dataTableName,
        Authorizations.EMPTY, 16)) {

      for (int i = 0; i < words.length; i++) {
        IteratorSetting more = new IteratorSetting(20 + i, "ii" + i, RegExFilter.class);
        RegExFilter.setRegexs(more, null, null, null, "(^|(.*\\s))" + words[i] + "($|(\\s.*))",
            false);
        bs.addScanIterator(more);
      }

      bs.setRanges(Collections.singleton(new Range()));

      for (Entry<Key,Value> entry : bs) {
        documentsFoundByGrep.add(entry.getKey().getRow());
      }

    }

    if (!documentsFoundInIndex.equals(documentsFoundByGrep)) {
      throw new Exception("Set of documents found not equal for words " + Arrays.toString(words)
          + " " + documentsFoundInIndex + " " + documentsFoundByGrep);
    }

    log.debug(
        "Grep and index agree " + Arrays.toString(words) + " " + documentsFoundInIndex.size());

  }

}
