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
package org.apache.accumulo.testing.gcs;

import java.util.Random;
import java.util.TreeSet;
import java.util.function.Consumer;

public class Collector {

  Persistence persistence;
  private int batchSize;

  public Collector(GcsEnv gcsEnv) {
    this.persistence = new Persistence(gcsEnv);
    this.batchSize = gcsEnv.getBatchSize();
  }

  public static void main(String[] args) throws Exception {
    new Collector(new GcsEnv(args)).run();
  }

  public static <T> void forEachBatch(Iterable<T> iter, int batchSize,
      Consumer<TreeSet<T>> batchProcessor) {
    TreeSet<T> batch = new TreeSet<>();

    for (T element : iter) {
      if (batch.size() >= batchSize) {
        batchProcessor.accept(batch);
        batch.clear();
      }
      batch.add(element);
    }

    if (!batch.isEmpty()) {
      batchProcessor.accept(batch);
    }
  }

  public void run() throws Exception {
    Random rand = new Random();

    while (true) {
      forEachBatch(persistence.candidates(), batchSize, batch -> collect(batch));
      Thread.sleep(13000);

      if (rand.nextInt(10) == 0) {
        persistence.flushTable();
      }
    }
  }

  private void collect(TreeSet<Candidate> candidates) {

    int initialSize = candidates.size();

    for (GroupRef gr : persistence.groupRefs()) {
      candidates.subSet(new Candidate(gr.clientId, gr.groupId, 0),
          new Candidate(gr.clientId, gr.groupId + 1, 0)).clear();
    }

    for (ItemRef ir : persistence.itemRefs()) {
      candidates.remove(new Candidate(ir.clientId, ir.groupId, ir.itemId));
    }

    System.out.println("Deleting " + candidates.size() + " of " + initialSize);

    for (Candidate c : candidates) {
      persistence.delete(c.item());
    }
    persistence.flush();

    for (Candidate c : candidates) {
      persistence.delete(c);
    }
    persistence.flush();
  }
}
