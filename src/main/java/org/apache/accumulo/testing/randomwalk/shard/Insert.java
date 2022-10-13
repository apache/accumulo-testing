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

import java.util.HashSet;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class Insert extends Test {

  static final int NUM_WORDS = 100000;
  static final int MIN_WORDS_PER_DOC = 10;
  static final int MAX_WORDS_PER_DOC = 3000;

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String indexTableName = state.getString("indexTableName");
    String dataTableName = state.getString("docTableName");
    int numPartitions = state.getInteger("numPartitions");
    long nextDocID = state.getLong("nextDocID");
    Random rand = state.getRandom();

    BatchWriter dataWriter = env.getMultiTableBatchWriter().getBatchWriter(dataTableName);
    BatchWriter indexWriter = env.getMultiTableBatchWriter().getBatchWriter(indexTableName);

    String docID = insertRandomDocument(nextDocID++, dataWriter, indexWriter, numPartitions, rand);

    log.debug("Inserted document " + docID);

    state.set("nextDocID", nextDocID);
  }

  static String insertRandomDocument(long did, BatchWriter dataWriter, BatchWriter indexWriter,
      int numPartitions, Random rand) throws Exception {
    String doc = createDocument(rand);

    String docID = new StringBuilder(String.format("%016x", did)).reverse().toString();

    saveDocument(dataWriter, docID, doc);
    indexDocument(indexWriter, doc, docID, numPartitions);

    return docID;
  }

  static void saveDocument(BatchWriter bw, String docID, String doc) throws Exception {

    Mutation m = new Mutation(docID);
    m.put("doc", "", doc);

    bw.addMutation(m);
  }

  static String createDocument(Random rand) {
    StringBuilder sb = new StringBuilder();

    int numWords = rand.nextInt(MAX_WORDS_PER_DOC - MIN_WORDS_PER_DOC) + MIN_WORDS_PER_DOC;

    for (int i = 0; i < numWords; i++) {
      String word = generateRandomWord(rand);

      if (i > 0)
        sb.append(" ");

      sb.append(word);
    }

    return sb.toString();
  }

  static String generateRandomWord(Random rand) {
    return Integer.toString(rand.nextInt(NUM_WORDS), Character.MAX_RADIX);
  }

  static String genPartition(int partition) {
    return String.format("%06x", Math.abs(partition));
  }

  static void indexDocument(BatchWriter bw, String doc, String docId, int numPartitions)
      throws Exception {
    indexDocument(bw, doc, docId, numPartitions, false);
  }

  static void unindexDocument(BatchWriter bw, String doc, String docId, int numPartitions)
      throws Exception {
    indexDocument(bw, doc, docId, numPartitions, true);
  }

  static void indexDocument(BatchWriter bw, String doc, String docId, int numPartitions,
      boolean delete) throws Exception {

    String[] tokens = doc.split("\\W+");

    String partition = genPartition(doc.hashCode() % numPartitions);

    Mutation m = new Mutation(partition);

    HashSet<String> tokensSeen = new HashSet<>();

    for (String token : tokens) {
      token = token.toLowerCase();

      if (!tokensSeen.contains(token)) {
        tokensSeen.add(token);
        if (delete)
          m.putDelete(token, docId);
        else
          m.put(token, docId, new Value(new byte[0]));
      }
    }

    if (m.size() > 0)
      bw.addMutation(m);
  }

}
