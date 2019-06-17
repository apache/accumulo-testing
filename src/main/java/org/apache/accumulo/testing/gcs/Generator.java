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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;

public class Generator {

  private long clientId;
  private long nextGroupId;
  private long nextItemId;

  private final int maxBuckets;

  // The total number of work chains to execute
  private final int maxWork;

  // The max number of work chains that should be active at any one time.
  private final int maxActiveWork;

  Random rand = new Random();

  private Persistence persistence;

  public Generator(GcsEnv gcsEnv) {
    this.persistence = new Persistence(gcsEnv);
    this.maxBuckets = gcsEnv.getMaxBuckets();

    this.maxWork = gcsEnv.getMaxWork();
    this.maxActiveWork = gcsEnv.getMaxActiveWork();
  }

  private void run() {
    // This holds future work to do.
    List<Queue<Mutator>> allActions = new ArrayList<>();

    int workCreated = 0;

    clientId = Math.abs(rand.nextLong());

    while (workCreated == 0 || !allActions.isEmpty()) {
      while (workCreated < maxWork && allActions.size() < maxActiveWork) {
        allActions.add(createWork());
        workCreated++;
      }

      int index = rand.nextInt(allActions.size());

      Queue<Mutator> queue = allActions.get(index);

      int numToRun = Math.max(1, rand.nextInt(queue.size()));

      // By selecting a random queue of work do to do and taking a random number of steps off the
      // queue we are randomly interleaving unrelated work over time.
      for (int i = 0; i < numToRun; i++) {
        queue.remove().run(persistence);
      }

      if (queue.isEmpty()) {
        allActions.set(index, allActions.get(allActions.size() - 1));
        allActions.remove(allActions.size() - 1);
      }
    }
  }

  private Queue<Mutator> createWork() {

    switch (rand.nextInt(2)) {
      case 0:
        return createGroupWork();
      case 1:
        return createSimpleWork();
      default:
        throw new IllegalStateException();
    }

  }

  private Queue<Mutator> createSimpleWork() {
    long groupId = nextGroupId++;
    int bucket = rand.nextInt(maxBuckets);

    ArrayDeque<Mutator> work = new ArrayDeque<>();

    List<Item> newItems = new ArrayList<>();
    int numItems = rand.nextInt(10) + 1;
    for (int i = 0; i < numItems; i++) {
      newItems.add(new Item(clientId, groupId, nextItemId++));
    }

    // copy because lambda will execute later
    List<Item> itemsToAdd = new ArrayList<>(newItems);
    work.add(p -> {
      for (Item item : itemsToAdd) {
        p.save(item, ItemState.NEW);
      }
      p.flush();
    });

    List<ItemRef> referencedItems = new ArrayList<>();

    while (!newItems.isEmpty() || !referencedItems.isEmpty()) {
      if (newItems.isEmpty()) {
        int size = referencedItems.size();
        List<ItemRef> subList = referencedItems.subList(rand.nextInt(size), size);
        List<ItemRef> refsToDelete = new ArrayList<>(subList);
        subList.clear();

        work.add(p -> {
          for (ItemRef ir : refsToDelete) {
            p.save(new Candidate(ir.clientId, ir.groupId, ir.itemId));
          }
          p.flush();
        });

        work.add(p -> {
          p.delete(refsToDelete);
          p.flush();
        });
      } else if (referencedItems.isEmpty()) {
        int size = newItems.size();
        List<Item> subList = newItems.subList(rand.nextInt(size), size);
        List<Item> itemsToRef = new ArrayList<>(subList);
        subList.clear();

        List<ItemRef> refsToAdd = new ArrayList<>();
        itemsToRef.forEach(item -> refsToAdd.add(new ItemRef(bucket, item)));
        referencedItems.addAll(refsToAdd);

        work.add(p -> {
          p.save(refsToAdd);
          p.flush();
        });

        work.add(p -> {
          for (Item item : itemsToRef) {
            p.save(item, ItemState.REFERENCED);
          }
          p.flush();
        });

      } else {
        int size = referencedItems.size();
        List<ItemRef> subList = referencedItems.subList(rand.nextInt(size), size);
        List<ItemRef> refsToDelete = new ArrayList<>(subList);
        subList.clear();

        Item itemToRef = newItems.remove(newItems.size() - 1);
        referencedItems.add(new ItemRef(bucket, itemToRef));

        work.add(p -> {
          for (ItemRef ir : refsToDelete) {
            p.save(new Candidate(ir.clientId, ir.groupId, ir.itemId));
          }
          p.flush();
        });

        work.add(p -> {
          p.replace(refsToDelete, new ItemRef(bucket, itemToRef));
          p.flush();
        });

        work.add(p -> {
          p.save(itemToRef, ItemState.REFERENCED);
          p.flush();
        });
      }
    }

    return work;
  }

  private Queue<Mutator> createGroupWork() {
    long groupId = nextGroupId++;

    long items[] = new long[rand.nextInt(10) + 1];
    for (int i = 0; i < items.length; i++) {
      items[i] = nextItemId++;
    }

    ArrayDeque<Mutator> work = new ArrayDeque<>();

    work.add(p -> {
      p.save(new GroupRef(clientId, groupId));
      p.flush();
    });

    work.add(p -> {
      for (long itemId : items) {
        p.save(new Item(clientId, groupId, itemId), ItemState.REFERENCED);
      }
      p.flush();
    });

    List<ItemRef> refsToAdd = new ArrayList<>();
    List<ItemRef> refsToDel = new ArrayList<>();

    for (long itemId : items) {
      for (int i = 0; i < rand.nextInt(3) + 1; i++) {
        int bucket = rand.nextInt(maxBuckets);
        refsToAdd.add(new ItemRef(bucket, clientId, groupId, itemId));
      }
    }

    Collections.shuffle(refsToAdd, rand);

    boolean deletedGroupRef = false;

    while (!refsToAdd.isEmpty() || !refsToDel.isEmpty() || !deletedGroupRef) {
      if (!refsToAdd.isEmpty() && rand.nextBoolean()) {
        ItemRef ref = refsToAdd.remove(refsToAdd.size() - 1);
        refsToDel.add(ref);
        work.add(p -> {
          p.save(ref);
          p.flush();
        });
      }

      if (refsToAdd.isEmpty() && !deletedGroupRef && rand.nextBoolean()) {
        work.add(p -> {
          p.delete(new GroupRef(clientId, groupId));
          p.flush();
        });
        deletedGroupRef = true;
      }

      if (!refsToDel.isEmpty() && rand.nextBoolean()) {
        ItemRef ref = refsToDel.remove(refsToDel.size() - 1);
        work.add(p -> {
          p.save(new Candidate(clientId, groupId, ref.itemId));
          p.delete(ref);
          p.flush();
        });
      }
    }

    return work;
  }

  public static void main(String[] args) {
    new Generator(new GcsEnv(args)).run();
  }
}
