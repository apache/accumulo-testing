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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;

/** Per-worker workload metrics and their atomic on-disk snapshots. */
final class ManagerStressMetrics {

  static final class Counts {
    long attempts;
    long submitted;
    long completed;
    long asyncAccepted;
    long skipped;
    long races;
    long failed;
    long outcomeUnknown;
    long totalNanos;

    OperationSnapshot snapshot() {
      return new OperationSnapshot(attempts, submitted, completed, asyncAccepted, skipped, races,
          failed, outcomeUnknown, totalNanos);
    }
  }

  record OperationSnapshot(long attempts, long submitted, long completed, long asyncAccepted,
      long skipped, long races, long failed, long outcomeUnknown, long totalNanos) {
  }

  record Snapshot(int workerId, long maintenanceCreates,
      Map<Operation,OperationSnapshot> operations) {
  }

  record RunSnapshot(int workers, long maintenanceCreates,
      Map<Operation,OperationSnapshot> operations) {
  }

  private final EnumMap<Operation,Counts> counts = new EnumMap<>(Operation.class);
  private long maintenanceCreates;

  ManagerStressMetrics() {
    for (Operation operation : Operation.values()) {
      counts.put(operation, new Counts());
    }
  }

  Counts counts(Operation operation) {
    return counts.get(operation);
  }

  void addMaintenanceCreates(long creates) {
    maintenanceCreates += creates;
  }

  long maintenanceCreates() {
    return maintenanceCreates;
  }

  void writeSnapshot(Path stateDir, int workerId) throws IOException {
    Properties properties = new Properties();
    properties.setProperty("workerId", Integer.toString(workerId));
    properties.setProperty("maintenanceCreates", Long.toString(maintenanceCreates));
    for (Operation operation : Operation.values()) {
      Counts operationCounts = counts.get(operation);
      String prefix = operation.name() + ".";
      properties.setProperty(prefix + "attempts", Long.toString(operationCounts.attempts));
      properties.setProperty(prefix + "submitted", Long.toString(operationCounts.submitted));
      properties.setProperty(prefix + "completed", Long.toString(operationCounts.completed));
      properties.setProperty(prefix + "asyncAccepted",
          Long.toString(operationCounts.asyncAccepted));
      properties.setProperty(prefix + "skipped", Long.toString(operationCounts.skipped));
      properties.setProperty(prefix + "races", Long.toString(operationCounts.races));
      properties.setProperty(prefix + "failed", Long.toString(operationCounts.failed));
      properties.setProperty(prefix + "outcomeUnknown",
          Long.toString(operationCounts.outcomeUnknown));
      properties.setProperty(prefix + "totalNanos", Long.toString(operationCounts.totalNanos));
    }

    Path target = snapshotPath(stateDir, workerId);
    Path temporary = Files.createTempFile(stateDir, "worker-metrics-", ".tmp");
    try {
      try (OutputStream output = Files.newOutputStream(temporary)) {
        properties.store(output, "ManagerStress worker metrics");
      }
      try {
        Files.move(temporary, target, ATOMIC_MOVE, REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(temporary, target, REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  static Path snapshotPath(Path stateDir, int workerId) {
    return stateDir.resolve("worker-metrics-" + workerId + ".properties");
  }

  static Snapshot readSnapshot(Path snapshotPath) throws IOException {
    Properties properties = new Properties();
    try (var input = Files.newInputStream(snapshotPath)) {
      properties.load(input);
    }
    int workerId = Integer.parseInt(properties.getProperty("workerId"));
    long maintenanceCreates = Long.parseLong(properties.getProperty("maintenanceCreates"));
    EnumMap<Operation,OperationSnapshot> operations = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      String prefix = operation.name() + ".";
      operations.put(operation, new OperationSnapshot(readLong(properties, prefix + "attempts"),
          readLong(properties, prefix + "submitted"), readLong(properties, prefix + "completed"),
          readLong(properties, prefix + "asyncAccepted"), readLong(properties, prefix + "skipped"),
          readLong(properties, prefix + "races"), readLong(properties, prefix + "failed"),
          readLong(properties, prefix + "outcomeUnknown"),
          readLong(properties, prefix + "totalNanos")));
    }
    return new Snapshot(workerId, maintenanceCreates, Map.copyOf(operations));
  }

  static RunSnapshot aggregate(Collection<Snapshot> snapshots) {
    EnumMap<Operation,Counts> totals = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      totals.put(operation, new Counts());
    }
    long maintenanceCreates = 0;
    int workers = 0;
    for (Snapshot snapshot : snapshots) {
      workers++;
      maintenanceCreates += snapshot.maintenanceCreates();
      for (Operation operation : Operation.values()) {
        OperationSnapshot current = snapshot.operations().get(operation);
        Counts total = totals.get(operation);
        total.attempts += current.attempts();
        total.submitted += current.submitted();
        total.completed += current.completed();
        total.asyncAccepted += current.asyncAccepted();
        total.skipped += current.skipped();
        total.races += current.races();
        total.failed += current.failed();
        total.outcomeUnknown += current.outcomeUnknown();
        total.totalNanos += current.totalNanos();
      }
    }
    EnumMap<Operation,OperationSnapshot> operations = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operations.put(operation, totals.get(operation).snapshot());
    }
    return new RunSnapshot(workers, maintenanceCreates, Map.copyOf(operations));
  }

  private static long readLong(Properties properties, String key) {
    return Long.parseLong(properties.getProperty(key, "0"));
  }
}
