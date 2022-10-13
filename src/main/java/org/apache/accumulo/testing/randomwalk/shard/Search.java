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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Search extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String indexTableName = state.getString("indexTableName");
    String dataTableName = state.getString("docTableName");

    Random rand = state.getRandom();

    Entry<Key,Value> entry = findRandomDocument(env, dataTableName, rand);
    if (entry == null)
      return;

    Text docID = entry.getKey().getRow();
    String doc = entry.getValue().toString();

    String[] tokens = doc.split("\\W+");
    int numSearchTerms = rand.nextInt(4) + 2;

    Set<String> searchTerms = Stream.generate(() -> tokens[rand.nextInt(tokens.length)])
        .limit(numSearchTerms).collect(Collectors.toSet());

    Text[] columns = searchTerms.stream().map(Text::new).toArray(Text[]::new);

    log.debug("Looking up terms " + searchTerms + " expect to find " + docID);

    try (BatchScanner bs = env.getAccumuloClient().createBatchScanner(indexTableName,
        Authorizations.EMPTY, 10)) {

      IteratorSetting ii = new IteratorSetting(20, "ii", IntersectingIterator.class);
      IntersectingIterator.setColumnFamilies(ii, columns);
      bs.addScanIterator(ii);
      bs.setRanges(Collections.singleton(new Range()));

      boolean sawDocID = bs.stream().map(Entry::getKey).map(Key::getColumnQualifier)
          .anyMatch(cq -> cq.equals(docID));

      if (!sawDocID)
        throw new Exception("Did not see doc " + docID + " in index.  terms:" + searchTerms + " "
            + indexTableName + " " + dataTableName);
    }

  }

  static Entry<Key,Value> findRandomDocument(RandWalkEnv env, String dataTableName, Random rand)
      throws Exception {
    try (Scanner scanner = env.getAccumuloClient().createScanner(dataTableName,
        Authorizations.EMPTY)) {
      scanner.setBatchSize(1);
      scanner.setRange(new Range(Integer.toString(rand.nextInt(0xfffffff), 16), null));
      var entry = scanner.stream().findAny();
      return entry.orElse(null);
    }
  }

}
