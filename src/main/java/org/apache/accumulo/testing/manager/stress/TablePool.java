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
package org.apache.accumulo.testing.manager.stress;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;

/** Shared, locally locked table-pool state for coordinator and worker JVMs. */
final class TablePool {

  record Slot(int index, long generation, String tableName, boolean busy) {
  }

  @FunctionalInterface
  private interface LockedFunction<T> {
    T apply(List<Slot> slots) throws Exception;
  }

  private final Path stateDir;
  private final Path stateFile;
  private final Path lockFile;
  private final String namespace;
  private final String tablePrefix;
  private final int tableCount;
  private final long splitSeed;

  TablePool(Path stateDir, String namespace, String tablePrefix, int tableCount, long splitSeed) {
    this.stateDir = stateDir;
    this.stateFile = stateDir.resolve("tables.state");
    this.lockFile = stateDir.resolve("tables.lock");
    this.namespace = namespace;
    this.tablePrefix = tablePrefix;
    this.tableCount = tableCount;
    this.splitSeed = splitSeed;
  }

  void initialize(AccumuloClient client) throws Exception {
    Files.createDirectories(stateDir);
    List<Slot> slots = new ArrayList<>(tableCount);
    try {
      for (int i = 0; i < tableCount; i++) {
        String name = tableName(i, 1);
        if (client.tableOperations().exists(name)) {
          throw new IllegalStateException("Refusing to use existing table " + name);
        }
        createTable(client, name, i, 1);
        slots.add(new Slot(i, 1, name, false));
      }
      writeSlots(slots);
    } catch (Exception e) {
      for (Slot slot : slots) {
        try {
          client.tableOperations().delete(slot.tableName());
        } catch (Exception cleanupError) {
          e.addSuppressed(cleanupError);
        }
      }
      throw e;
    }
  }

  List<Slot> snapshot() throws Exception {
    return withLock(slots -> List.copyOf(slots));
  }

  int replenish(AccumuloClient client) throws Exception {
    int created = 0;
    for (int i = 0; i < tableCount; i++) {
      Slot reserved = reserve(i, true, true);
      if (reserved != null) {
        createInVacantSlot(client, reserved);
        created++;
      }
    }
    return created;
  }

  boolean delete(AccumuloClient client, int slotIndex, Runnable markSubmitted) throws Exception {
    Slot reserved = reserve(slotIndex, false, false);
    if (reserved == null) {
      return false;
    }
    try {
      markSubmitted.run();
      client.tableOperations().delete(reserved.tableName());
    } catch (TableNotFoundException e) {
      updateSlot(new Slot(reserved.index(), reserved.generation(), null, false));
      return false;
    } catch (Exception e) {
      updateSlot(new Slot(reserved.index(), reserved.generation(), reserved.tableName(), false));
      throw e;
    }
    updateSlot(new Slot(reserved.index(), reserved.generation(), null, false));
    return true;
  }

  boolean replace(AccumuloClient client, int slotIndex, Runnable markSubmitted) throws Exception {
    Slot reserved = reserve(slotIndex, false, true);
    if (reserved == null) {
      return false;
    }
    String replacementName = tableName(reserved.index(), reserved.generation());
    boolean replacementCreated = false;
    boolean oldTableDeleted = false;
    try {
      markSubmitted.run();
      createTable(client, replacementName, reserved.index(), reserved.generation());
      replacementCreated = true;
      if (reserved.tableName() != null) {
        try {
          client.tableOperations().delete(reserved.tableName());
          oldTableDeleted = true;
        } catch (TableNotFoundException e) {
          // A competing client completed the deletion first.
          oldTableDeleted = true;
        }
      }
      updateSlot(new Slot(reserved.index(), reserved.generation(), replacementName, false));
      return true;
    } catch (Exception e) {
      if (replacementCreated) {
        try {
          client.tableOperations().delete(replacementName);
        } catch (Exception cleanupError) {
          e.addSuppressed(cleanupError);
        }
      }
      String previousName = oldTableDeleted ? null : reserved.tableName();
      try {
        updateSlot(new Slot(reserved.index(), reserved.generation(), previousName, false));
      } catch (Exception updateError) {
        e.addSuppressed(updateError);
      }
      throw e;
    }
  }

  private void createInVacantSlot(AccumuloClient client, Slot reserved) throws Exception {
    String replacementName = tableName(reserved.index(), reserved.generation());
    boolean created = false;
    try {
      createTable(client, replacementName, reserved.index(), reserved.generation());
      created = true;
      updateSlot(new Slot(reserved.index(), reserved.generation(), replacementName, false));
    } catch (Exception e) {
      if (created) {
        try {
          client.tableOperations().delete(replacementName);
        } catch (Exception cleanupError) {
          e.addSuppressed(cleanupError);
        }
      }
      try {
        updateSlot(new Slot(reserved.index(), reserved.generation(), null, false));
      } catch (Exception updateError) {
        e.addSuppressed(updateError);
      }
      throw e;
    }
  }

  Slot lockTable(int preferredIndex) throws Exception {
    return reserve(preferredIndex, false, false);
  }

  void unlockTable(Slot reservation) throws Exception {
    withLock(slots -> {
      Slot current = slots.get(reservation.index());
      if (!current.busy() || current.generation() != reservation.generation()
          || !java.util.Objects.equals(current.tableName(), reservation.tableName())) {
        throw new IllegalStateException(
            "Table slot reservation changed while in use: " + reservation.index());
      }
      slots.set(reservation.index(),
          new Slot(reservation.index(), reservation.generation(), reservation.tableName(), false));
      writeSlots(slots);
      return null;
    });
  }

  private void createTable(AccumuloClient client, String tableName, int slot, long generation)
      throws Exception {
    long seed = splitSeed ^ ((long) slot * 0x9e3779b97f4a7c15L) ^ Long.rotateLeft(generation, 32);
    NewTableConfiguration config = ManagerStressRows.newTableConfiguration(new Random(seed));
    client.tableOperations().create(tableName, config);
  }

  private Slot reserve(int preferredIndex, boolean vacant, boolean incrementGeneration)
      throws Exception {
    return withLock(slots -> {
      for (int offset = 0; offset < slots.size(); offset++) {
        int index = (preferredIndex + offset) % slots.size();
        Slot current = slots.get(index);
        if (!current.busy() && (vacant == (current.tableName() == null))) {
          long generation = current.generation() + (incrementGeneration ? 1 : 0);
          Slot reserved = new Slot(index, generation, current.tableName(), true);
          slots.set(index, reserved);
          writeSlots(slots);
          return reserved;
        }
      }
      return null;
    });
  }

  private void updateSlot(Slot updated) throws Exception {
    withLock(slots -> {
      slots.set(updated.index(), updated);
      writeSlots(slots);
      return null;
    });
  }

  String tableName(int slot, long generation) {
    return namespace + "." + tablePrefix + "_" + slot + "_" + generation;
  }

  private <T> T withLock(LockedFunction<T> function) throws Exception {
    try (FileChannel channel = FileChannel.open(lockFile, CREATE, WRITE);
        FileLock ignored = channel.lock()) {
      return function.apply(readSlots());
    }
  }

  private List<Slot> readSlots() throws IOException {
    List<String> lines = Files.readAllLines(stateFile, UTF_8);
    if (lines.size() != tableCount) {
      throw new IOException("Invalid table-pool state in " + stateFile);
    }
    List<Slot> slots = new ArrayList<>(tableCount);
    for (int i = 0; i < lines.size(); i++) {
      String[] values = lines.get(i).split("\\t", -1);
      if (values.length != 3) {
        throw new IOException("Invalid table-pool state line: " + lines.get(i));
      }
      long generation = Long.parseLong(values[0]);
      String name = values[1].isEmpty() ? null : values[1];
      slots.add(new Slot(i, generation, name, Boolean.parseBoolean(values[2])));
    }
    return slots;
  }

  private void writeSlots(List<Slot> slots) throws IOException {
    Path tempFile = Files.createTempFile(stateDir, "tables", ".tmp");
    List<
        String> lines =
            slots.stream()
                .map(slot -> slot.generation() + "\t"
                    + (slot.tableName() == null ? "" : slot.tableName()) + "\t" + slot.busy())
                .toList();
    try {
      Files.write(tempFile, lines, UTF_8);
      try {
        Files.move(tempFile, stateFile, ATOMIC_MOVE, REPLACE_EXISTING);
      } catch (UnsupportedOperationException | IOException e) {
        Files.move(tempFile, stateFile, REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }
}
