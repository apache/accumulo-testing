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
package org.apache.accumulo.testing.gcs;

import static org.apache.accumulo.testing.gcs.Collector.forEachBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class Verifier {

  Persistence persistence;
  private final int batchSize;

  public Verifier(GcsEnv gcsEnv) {
    this.persistence = new Persistence(gcsEnv);
    this.batchSize = gcsEnv.getBatchSize();
  }

  public static void main(String[] args) {
    new Verifier(new GcsEnv(args)).run();
  }

  private void run() {
    forEachBatch(persistence.items(ItemState.REFERENCED), batchSize, batch -> checkItems(batch));
    forEachBatch(persistence.itemRefs(), batchSize, batch -> checkItemRefs(batch));
  }

  // Ensure there is an item for each item ref
  private void checkItemRefs(TreeSet<ItemRef> itemRefs) {
    Map<Item,List<ItemRef>> refMap = new HashMap<>();

    itemRefs.forEach(ir -> {
      refMap.computeIfAbsent(ir.item(), i -> new ArrayList<>()).add(ir);
    });

    persistence.items(ItemState.NEW, ItemState.REFERENCED).forEach(item -> refMap.remove(item));

    if (refMap.size() > 0) {
      System.err.println("References without items : ");
      refMap.values().stream().flatMap(List::stream).forEach(ir -> System.err.println("\t" + ir));
    }

    System.out.printf("Checked %,d item refs\n", itemRefs.size());
  }

  // Ensure all items are referenced by something.
  private void checkItems(TreeSet<Item> items) {

    int initialSize = items.size();

    persistence.itemRefs().forEach(ir -> items.remove(ir.item()));

    persistence.candidates().forEach(c -> items.remove(new Item(c)));

    persistence.groupRefs()
        .forEach(gr -> items
            .subSet(new Item(gr.clientId, gr.groupId, 0), new Item(gr.clientId, gr.groupId + 1, 0))
            .clear());

    if (items.size() > 0) {
      System.err.println("Unreferenced items : ");
      items.forEach(i -> System.err.println("\t" + i));
    }

    System.out.printf("Checked %,d items\n", initialSize);
  }
}
