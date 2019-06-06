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

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;

public class Persistence {

  private BatchWriter writer;
  private String table;
  private AccumuloClient client;

  Persistence(GcsEnv env) {
    this.client = env.getAccumuloClient();
    this.table = env.getTableName();
    try {
      this.writer = client.createBatchWriter(table);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  static String toHex(int i) {
    return Strings.padStart(Integer.toHexString(i), 8, '0');
  }

  static String toHex(long l) {
    return Strings.padStart(Long.toHexString(l), 16, '0');
  }

  private void write(Mutation m) {
    try {
      writer.addMutation(m);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  private String toHex(long l1, long l2, long l3) {
    return toHex(l1) + ":" + toHex(l2) + ":" + toHex(l3);
  }

  private String toHexWithHash(long l1, long l2) {
    int hc = Hashing.murmur3_32().newHasher().putLong(l1).putLong(l2).hash().asInt();
    return toHex(hc) + ":" + toHex(l1) + ":" + toHex(l2);
  }

  private String toHexWithHash(long l1, long l2, long l3) {
    int hc = Hashing.murmur3_32().newHasher().putLong(l1).putLong(l2).putLong(l3).hash().asInt();
    return toHex(hc) + ":" + toHex(l1) + ":" + toHex(l2) + ":" + toHex(l3);
  }

  void save(Item item, ItemState state) {
    Mutation m = new Mutation("I:" + toHexWithHash(item.clientId, item.groupId));
    m.put("item", toHex(item.itemId), state.name());
    write(m);
  }

  void save(ItemRef itemRef) {
    Mutation m = new Mutation("R:" + toHex(itemRef.bucket));
    m.put("ref", toHex(itemRef.clientId, itemRef.groupId, itemRef.itemId), "");
    write(m);
  }

  public void save(Collection<ItemRef> refsToAdd) {
    Map<Integer,Mutation> mutations = new HashMap<>();

    for (ItemRef itemRef : refsToAdd) {
      Mutation m = mutations.computeIfAbsent(itemRef.bucket,
          bucket -> new Mutation("R:" + toHex(bucket)));
      m.put("ref", toHex(itemRef.clientId, itemRef.groupId, itemRef.itemId), "");
    }

    mutations.values().forEach(m -> write(m));

  }

  public void replace(Collection<ItemRef> refsToDelete, ItemRef refToAdd) {
    Mutation m = new Mutation("R:" + toHex(refToAdd.bucket));
    m.put("ref", toHex(refToAdd.clientId, refToAdd.groupId, refToAdd.itemId), "");

    for (ItemRef ir : refsToDelete) {
      Preconditions.checkArgument(refToAdd.bucket == ir.bucket);
      m.putDelete("ref", toHex(ir.clientId, ir.groupId, ir.itemId));
    }

    write(m);
  }

  void save(GroupRef groupRef) {
    Mutation m = new Mutation("G:" + toHexWithHash(groupRef.clientId, groupRef.groupId));
    m.put("", "", "");
    write(m);
  }

  void save(Candidate c) {
    Mutation m = new Mutation("C:" + toHexWithHash(c.clientId, c.groupId, c.itemId));
    m.put("", "", "");
    write(m);
  }

  public void flush() {
    try {
      writer.flush();
    } catch (MutationsRejectedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void delete(ItemRef itemRef) {
    Mutation m = new Mutation("R:" + toHex(itemRef.bucket));
    m.putDelete("ref", toHex(itemRef.clientId, itemRef.groupId, itemRef.itemId));
    write(m);
  }

  public void delete(Collection<ItemRef> refsToDelete) {
    Map<Integer,Mutation> mutations = new HashMap<>();

    for (ItemRef itemRef : refsToDelete) {
      Mutation m = mutations.computeIfAbsent(itemRef.bucket,
          bucket -> new Mutation("R:" + toHex(bucket)));
      m.putDelete("ref", toHex(itemRef.clientId, itemRef.groupId, itemRef.itemId));
    }

    mutations.values().forEach(m -> write(m));
  }

  public void delete(GroupRef groupRef) {
    Mutation m = new Mutation("G:" + toHexWithHash(groupRef.clientId, groupRef.groupId));
    m.putDelete("", "");
    write(m);
  }

  public void delete(Item item) {
    Mutation m = new Mutation("I:" + toHexWithHash(item.clientId, item.groupId));
    m.putDelete("item", toHex(item.itemId));
    write(m);
  }

  public void delete(Candidate c) {
    Mutation m = new Mutation("C:" + toHexWithHash(c.clientId, c.groupId, c.itemId));
    m.putDelete("", "");
    write(m);
  }

  private Scanner createScanner(String prefix) {
    Scanner scanner;
    try {
      scanner = new IsolatedScanner(client.createScanner(table));
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      throw new RuntimeException(e);
    }
    scanner.setRange(Range.prefix(prefix));

    return scanner;
  }

  private <T> Iterable<T> transformRange(String prefix, Function<Key,T> func) {
    return Iterables.transform(createScanner(prefix), entry -> func.apply(entry.getKey()));
  }

  Iterable<Candidate> candidates() {
    return transformRange("C:", k -> {
      String row = k.getRowData().toString();
      String[] fields = row.substring(11).split(":");

      Preconditions.checkState(fields.length == 3, "Bad candidate row %s", row);

      long clientId = Long.parseLong(fields[0], 16);
      long groupId = Long.parseLong(fields[1], 16);
      long itemId = Long.parseLong(fields[2], 16);

      return new Candidate(clientId, groupId, itemId);
    });
  }

  Iterable<ItemRef> itemRefs() {
    return transformRange("R:", k -> {
      String row = k.getRowData().toString();
      String qual = k.getColumnQualifierData().toString();

      int bucket = Integer.parseInt(row.substring(2), 16);

      String[] fields = qual.split(":");

      Preconditions.checkState(fields.length == 3, "Bad item ref %s", k);

      long clientId = Long.parseLong(fields[0], 16);
      long groupId = Long.parseLong(fields[1], 16);
      long itemId = Long.parseLong(fields[2], 16);

      return new ItemRef(bucket, clientId, groupId, itemId);
    });
  }

  Iterable<GroupRef> groupRefs() {
    return transformRange("G:", k -> {
      String row = k.getRowData().toString();
      String[] fields = row.substring(11).split(":");

      Preconditions.checkState(fields.length == 2, "Bad group ref row %s", row);

      long clientId = Long.parseLong(fields[0], 16);
      long groupId = Long.parseLong(fields[1], 16);

      return new GroupRef(clientId, groupId);
    });
  }

  Iterable<Item> items(ItemState... states) {
    EnumSet<ItemState> stateSet = EnumSet.of(states[0]);
    for (int i = 1; i < states.length; i++) {
      stateSet.add(states[i]);
    }

    Iterable<Entry<Key,Value>> itemIter = Iterables.filter(createScanner("I:"),
        entry -> stateSet.contains(ItemState.valueOf(entry.getValue().toString())));

    return Iterables.transform(itemIter, entry -> {
      Key k = entry.getKey();
      String row = k.getRowData().toString();
      String qual = k.getColumnQualifierData().toString();

      String[] fields = row.substring(11).split(":");

      Preconditions.checkState(fields.length == 2, "Bad group ref row %s", row);

      long clientId = Long.parseLong(fields[0], 16);
      long groupId = Long.parseLong(fields[1], 16);
      long itemId = Long.parseLong(qual, 16);

      return new Item(clientId, groupId, itemId);
    });
  }

  private static TreeSet<Text> initialSplits(GcsEnv env) {
    TreeSet<Text> splits = new TreeSet<>();

    int tabletsPerSection = env.getInitialTablets();

    for (String prefix : new String[] {"G:", "C:", "I:", "R:"}) {
      int numSplits = tabletsPerSection - 1;

      long max;
      if (prefix.equals("R:")) {
        max = env.getMaxBuckets();
      } else {
        max = 1L << 33 - 1;
      }

      long distance = (max / tabletsPerSection) + 1;
      long split = distance;

      for (int i = 0; i < numSplits; i++) {
        String s = String.format("%08x", split);
        while (s.charAt(s.length() - 1) == '0') {
          s = s.substring(0, s.length() - 1);
        }
        splits.add(new Text(prefix + s));
        split += distance;
      }

      splits.add(new Text(prefix + "~"));
    }

    return splits;
  }

  public static void init(GcsEnv env) throws Exception {
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(initialSplits(env));
    ntc.setProperties(ImmutableMap.of("table.compaction.major.ratio", "1"));
    env.getAccumuloClient().tableOperations().create(env.getTableName(), ntc);
  }

  public void flushTable() {
    try {
      client.tableOperations().flush(table);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
  }
}
