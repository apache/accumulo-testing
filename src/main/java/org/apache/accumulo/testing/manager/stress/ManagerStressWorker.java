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

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ManagerStressWorker {

  private static final Logger log = LoggerFactory.getLogger(ManagerStressWorker.class);

  private static class SubmissionTracker {
    private final ManagerStressMetrics.Counts counts;
    private boolean submitted;

    SubmissionTracker(ManagerStressMetrics.Counts counts) {
      this.counts = counts;
    }

    void markSubmitted() {
      if (!submitted) {
        counts.submitted++;
        submitted = true;
      }
    }
  }

  private final ManagerStressOptions options;
  private final ManagerStressMetrics metrics = new ManagerStressMetrics();
  private java.nio.file.Path stateDirectory;
  private long lastMetricsSnapshotNanos;
  private Path workerBulkDir;
  private Path seedFile;
  private FileSystem fileSystem;

  ManagerStressWorker(ManagerStressOptions options) {
    this.options = options;
  }

  void run() throws Exception {
    stateDirectory = java.nio.file.Path.of(options.stateDir);
    TablePool tablePool = new TablePool(stateDirectory, options.namespace, options.tablePrefix,
        options.tables, options.seed);
    Random random = new Random(options.seed + (long) options.workerId * 0x9e3779b97f4a7c15L);

    try (AccumuloClient client = Accumulo.newClient().from(options.getClientProps()).build()) {
      if (options.bulkImportWeight > 0) {
        prepareBulkFile(random);
      }
      writeMetricsSnapshot();
      Files.createFile(stateDirectory.resolve("ready-" + options.workerId));
      long deadline = awaitStart(stateDirectory);
      log.info("Worker {} started workload", options.workerId);

      while (System.currentTimeMillis() < deadline) {
        try {
          metrics.addMaintenanceCreates(replenish(tablePool, client));
        } catch (Exception e) {
          log.warn("Worker {} could not replenish the table pool", options.workerId, e);
        }

        List<TablePool.Slot> activeSlots =
            tablePool.snapshot().stream().filter(slot -> slot.tableName() != null).toList();
        if (activeSlots.isEmpty()) {
          Thread.sleep(100);
          continue;
        }

        Operation operation = Operation.choose(options, random);
        ManagerStressMetrics.Counts operationCounts = metrics.counts(operation);
        operationCounts.attempts++;
        SubmissionTracker submission = new SubmissionTracker(operationCounts);
        long start = System.nanoTime();
        try {
          if (execute(operation, activeSlots, tablePool, client, random, submission)) {
            if (operation.isAsynchronous()) {
              operationCounts.asyncAccepted++;
            } else {
              operationCounts.completed++;
            }
          } else {
            operationCounts.skipped++;
          }
        } catch (Exception e) {
          if (isExpectedRace(e)) {
            operationCounts.races++;
            log.debug("Worker {} observed a table lifecycle race during {}", options.workerId,
                operation, e);
          } else {
            operationCounts.failed++;
            if (submission.submitted && isOutcomeUnknown(e)) {
              operationCounts.outcomeUnknown++;
            }
            log.warn("Worker {} failed {} operation", options.workerId, operation, e);
          }
        } finally {
          operationCounts.totalNanos += System.nanoTime() - start;
          writeMetricsSnapshotIfDue();
        }
      }
      try {
        metrics.addMaintenanceCreates(replenish(tablePool, client));
      } catch (Exception e) {
        log.warn("Worker {} could not restore the table pool at shutdown", options.workerId, e);
      }
    } finally {
      try {
        if (fileSystem != null) {
          try {
            if (workerBulkDir != null) {
              fileSystem.delete(workerBulkDir, true);
            }
          } catch (IOException e) {
            log.warn("Worker {} could not remove its HDFS work directory {}", options.workerId,
                workerBulkDir, e);
          } finally {
            fileSystem.close();
          }
        }
      } finally {
        report();
        writeMetricsSnapshot();
      }
    }
  }

  private int replenish(TablePool pool, AccumuloClient client) throws Exception {
    int created = pool.replenish(client);
    if (created > 0) {
      log.info("Worker {} replenished {} deleted table slot(s)", options.workerId, created);
    }
    return created;
  }

  private boolean execute(Operation operation, List<TablePool.Slot> activeSlots, TablePool pool,
      AccumuloClient client, Random random, SubmissionTracker submission) throws Exception {
    TablePool.Slot slot = activeSlots.get(random.nextInt(activeSlots.size()));
    String tableName = slot.tableName();
    return switch (operation) {
      case CREATE -> {
        if (!pool.replace(client, slot.index(), submission::markSubmitted)) {
          throw new TableNotFoundException(null, tableName,
              "all table slots are being updated by other workers");
        }
        log.debug("Worker {} created a replacement table for slot {}", options.workerId,
            slot.index());
        yield true;
      }
      case DELETE -> {
        boolean deleted = pool.delete(client, slot.index(), submission::markSubmitted);
        if (!deleted) {
          throw new TableNotFoundException(null, tableName, "table was deleted by another worker");
        }
        log.debug("Worker {} deleted table {}", options.workerId, tableName);
        yield true;
      }
      case SPLIT -> split(client, pool, slot, random, submission);
      case MERGE -> merge(client, pool, slot, random, submission);
      case AVAILABILITY -> changeAvailability(client, pool, slot, random, submission);
      case COMPACT -> {
        submission.markSubmitted();
        client.tableOperations().compact(tableName,
            new CompactionConfig().setFlush(true).setWait(false));
        log.debug("Worker {} submitted a compaction for table {}", options.workerId, tableName);
        yield true;
      }
      case BULK_IMPORT -> {
        bulkImport(client, tableName, submission);
        yield true;
      }
    };
  }

  private boolean split(AccumuloClient client, TablePool pool, TablePool.Slot selected,
      Random random, SubmissionTracker submission) throws Exception {
    TablePool.Slot reservation = pool.lockTable(selected.index());
    if (reservation == null) {
      return false;
    }
    try {
      var tableOps = client.tableOperations();
      var currentSplits = new TreeSet<>(tableOps.listSplits(reservation.tableName()));
      var newSplits = ManagerStressRows.additionalSplits(currentSplits, random);
      if (newSplits.isEmpty()) {
        log.debug("Worker {} skipped split for {} at the {}-tablet limit", options.workerId,
            reservation.tableName(), ManagerStressRows.MAX_TABLETS);
        return false;
      }
      submission.markSubmitted();
      tableOps.addSplits(reservation.tableName(), newSplits);
      log.debug("Worker {} added {} split(s) to {}", options.workerId, newSplits.size(),
          reservation.tableName());
      return true;
    } finally {
      pool.unlockTable(reservation);
    }
  }

  private boolean merge(AccumuloClient client, TablePool pool, TablePool.Slot selected,
      Random random, SubmissionTracker submission) throws Exception {
    TablePool.Slot reservation = pool.lockTable(selected.index());
    if (reservation == null) {
      return false;
    }
    try {
      var tableOps = client.tableOperations();
      var splits = tableOps.listSplits(reservation.tableName());
      int targetTablets = random.nextInt(100) + 1;
      ManagerStressRows.MergeRange range =
          ManagerStressRows.selectMergeRange(splits, targetTablets, random);
      if (range == null) {
        log.debug("Worker {} skipped merge for {} because it has fewer than two tablets",
            options.workerId, reservation.tableName());
        return false;
      }
      submission.markSubmitted();
      tableOps.merge(reservation.tableName(), range.start(), range.end());
      log.debug("Worker {} merged {} tablets in {} over ({}, {}]", options.workerId,
          range.tabletCount(), reservation.tableName(), range.start(), range.end());
      return true;
    } finally {
      pool.unlockTable(reservation);
    }
  }

  private boolean changeAvailability(AccumuloClient client, TablePool pool, TablePool.Slot selected,
      Random random, SubmissionTracker submission) throws Exception {
    TablePool.Slot reservation = pool.lockTable(selected.index());
    if (reservation == null) {
      return false;
    }
    try {
      var tableOps = client.tableOperations();
      List<Text> splits = new ArrayList<>(tableOps.listSplits(reservation.tableName()));
      List<TabletAvailability> currentAvailability =
          new ArrayList<>(Collections.nCopies(splits.size() + 1, null));
      try (var tabletInformation = tableOps.getTabletInformation(reservation.tableName(),
          List.of(RowRange.all()), TabletInformation.Field.AVAILABILITY)) {
        var iterator = tabletInformation.iterator();
        while (iterator.hasNext()) {
          var tablet = iterator.next();
          Text endRow = tablet.getTabletId().getEndRow();
          int tabletIndex =
              endRow == null ? splits.size() : Collections.binarySearch(splits, endRow);
          if (tabletIndex < 0 || tabletIndex >= currentAvailability.size()
              || currentAvailability.set(tabletIndex, tablet.getTabletAvailability()) != null) {
            throw new IllegalStateException(
                "Could not map tablet availability to split boundaries for "
                    + reservation.tableName());
          }
        }
      }
      if (currentAvailability.contains(null)) {
        throw new IllegalStateException(
            "Could not obtain availability for every tablet in " + reservation.tableName());
      }

      List<ManagerStressRows.AvailabilityChange> changes =
          ManagerStressRows.randomAvailabilityChanges(splits, currentAvailability, random);
      int changedTablets = 0;
      for (ManagerStressRows.AvailabilityChange change : changes) {
        submission.markSubmitted();
        tableOps.setTabletAvailability(reservation.tableName(), change.rowRange(),
            change.availability());
        changedTablets += change.tabletCount();
      }
      log.debug("Worker {} submitted {} availability range change(s) for {} tablet(s) in {}",
          options.workerId, changes.size(), changedTablets, reservation.tableName());
      return !changes.isEmpty();
    } finally {
      pool.unlockTable(reservation);
    }
  }

  private void bulkImport(AccumuloClient client, String tableName, SubmissionTracker submission)
      throws Exception {
    Path importDir =
        new Path(workerBulkDir, "import-" + UUID.randomUUID().toString().replace("-", ""));
    Path stagedFile = new Path(importDir, "one-entry.rf");
    fileSystem.mkdirs(importDir);
    try (FSDataInputStream in = fileSystem.open(seedFile);
        FSDataOutputStream out = fileSystem.create(stagedFile, false)) {
      IOUtils.copyBytes(in, out, new Configuration(), false);
    }
    try {
      submission.markSubmitted();
      client.tableOperations().importDirectory(importDir.toString()).to(tableName).tableTime(true)
          .load();
      log.debug("Worker {} bulk imported data into table {}", options.workerId, tableName);
    } finally {
      fileSystem.delete(importDir, true);
    }
  }

  private void prepareBulkFile(Random random) throws IOException {
    Configuration configuration = new Configuration();
    Path configuredDir = new Path(options.hdfsDir);
    fileSystem = configuredDir.getFileSystem(configuration);
    workerBulkDir = new Path(new Path(configuredDir, options.runId), "worker-" + options.workerId);
    if (!fileSystem.mkdirs(workerBulkDir) && !fileSystem.exists(workerBulkDir)) {
      throw new IOException("Could not create HDFS work directory " + workerBulkDir);
    }
    seedFile = new Path(workerBulkDir, "seed.rf");
    Key key = new Key(ManagerStressRows.randomRow(random), new Text("cf"), new Text("cq"));
    Value value = new Value(("manager-stress-" + options.runId).getBytes(UTF_8));
    try (RFileWriter writer =
        RFile.newWriter().to(seedFile.toString()).withFileSystem(fileSystem).build()) {
      writer.startDefaultLocalityGroup();
      writer.append(key, value);
    }
    log.info("Worker {} created one-entry RFile at {}", options.workerId, seedFile);
  }

  private long awaitStart(java.nio.file.Path stateDir) throws Exception {
    java.nio.file.Path startFile = stateDir.resolve("start");
    while (!Files.exists(startFile)) {
      Thread.sleep(100);
    }
    return Long.parseLong(Files.readString(startFile, UTF_8));
  }

  private boolean isExpectedRace(Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof TableNotFoundException || current instanceof TableExistsException
          || current instanceof TableOfflineException) {
        return true;
      }
    }
    return false;
  }

  private boolean isOutcomeUnknown(Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof TTransportException || current instanceof InterruptedException) {
        return true;
      }
    }
    return false;
  }

  private void writeMetricsSnapshotIfDue() {
    if (System.nanoTime() - lastMetricsSnapshotNanos >= 1_000_000_000L) {
      writeMetricsSnapshot();
    }
  }

  private void writeMetricsSnapshot() {
    try {
      metrics.writeSnapshot(stateDirectory, options.workerId);
      lastMetricsSnapshotNanos = System.nanoTime();
    } catch (IOException e) {
      log.warn("Worker {} could not write its metrics snapshot", options.workerId, e);
    }
  }

  private void report() {
    for (Operation operation : Operation.values()) {
      ManagerStressMetrics.Counts result = metrics.counts(operation);
      if (result.attempts > 0) {
        double averageMillis = (double) result.totalNanos / result.attempts / 1_000_000;
        log.info(
            "Worker {} {}: attempts={}, submitted={}, completed={}, asyncAccepted={}, skipped={}, "
                + "races={}, failed={}, outcomeUnknown={}, avgMs={}",
            options.workerId, operation, result.attempts, result.submitted, result.completed,
            result.asyncAccepted, result.skipped, result.races, result.failed,
            result.outcomeUnknown, String.format("%.3f", averageMillis));
      }
    }
    log.info("Worker {} replacement-table maintenance creates={}", options.workerId,
        metrics.maintenanceCreates());
  }
}
