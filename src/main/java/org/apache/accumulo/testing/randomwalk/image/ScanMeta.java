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
package org.apache.accumulo.testing.randomwalk.image;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class ScanMeta extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {

    // scan just the metadata of the images table to find N hashes... use
    // the batch scanner to lookup those N hashes in the index table
    // this scan will test locality groups....

    String indexTableName = state.getString("indexTableName");
    String imageTableName = state.getString("imageTableName");

    String uuid = UUID.randomUUID().toString();

    AccumuloClient client = env.getAccumuloClient();

    try (Scanner imageScanner = client.createScanner(imageTableName, new Authorizations())) {

      imageScanner.setRange(new Range(new Text(uuid), null));
      imageScanner.fetchColumn(Write.META_COLUMN_FAMILY, Write.SHA1_COLUMN_QUALIFIER);

      int minScan = Integer.parseInt(props.getProperty("minScan"));
      int maxScan = Integer.parseInt(props.getProperty("maxScan"));

      int numToScan = env.getRandom().nextInt(maxScan - minScan) + minScan;

      Map<Text,Text> hashes = imageScanner.stream().limit(numToScan).collect(Collectors
          .toMap(entry -> new Text(entry.getValue().get()), entry -> entry.getKey().getRow()));

      log.debug("Found " + hashes.size() + " hashes starting at " + uuid);

      if (hashes.isEmpty()) {
        return;
      }

      // use batch scanner to verify all of these exist in index
      try (BatchScanner indexScanner = client.createBatchScanner(indexTableName,
          Authorizations.EMPTY, 3)) {
        List<Range> ranges = hashes.keySet().stream().map(Range::new).collect(Collectors.toList());

        indexScanner.setRanges(ranges);

        Map<Text,Text> hashes2 = indexScanner.stream().collect(Collectors
            .toMap(entry -> entry.getKey().getRow(), entry -> new Text(entry.getValue().get())));

        log.debug("Looked up " + ranges.size() + " ranges, found " + hashes2.size());

        if (!hashes.equals(hashes2)) {
          log.error("uuids from doc table : " + hashes.values());
          log.error("uuids from index     : " + hashes2.values());
          throw new Exception(
              "Mismatch between document table and index " + indexTableName + " " + imageTableName);
        }
      }
    }
  }
}
