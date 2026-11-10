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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts local client processes which issue Manager and Compaction Coordinator work. */
public class ManagerStress {

  private static final Logger log = LoggerFactory.getLogger(ManagerStress.class);
  private static final Duration WORKER_STARTUP_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration WORKER_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
  private static final String COMPACTION_SERVICE_NAME = "mgrstress";

  private record PreviousSystemProperty(boolean wasSet, String value) {
  }

  public static void main(String[] args) throws Exception {
    ManagerStressOptions options = new ManagerStressOptions();
    options.parseArgs(ManagerStress.class.getName(), args);
    if (options.workerId >= 0) {
      options.validate(false);
      new ManagerStressWorker(options).run();
    } else {
      options.validate(true);
      new ManagerStress(options, args).run();
    }
  }

  private final ManagerStressOptions options;
  private final String[] originalArgs;
  private final List<Process> workers = new CopyOnWriteArrayList<>();
  private final AtomicBoolean stopping = new AtomicBoolean();
  private final AtomicBoolean metricsReported = new AtomicBoolean();
  private volatile boolean namespaceCreated;
  private volatile boolean compactionServicePropertiesConfigured;
  private volatile Map<String,PreviousSystemProperty> previousCompactionServiceProperties =
      Map.of();
  private volatile Map<String,String> desiredCompactionServiceProperties = Map.of();
  private Path stateDir;
  private String runId;

  private ManagerStress(ManagerStressOptions options, String[] originalArgs) {
    this.options = options;
    this.originalArgs = originalArgs.clone();
  }

  private void run() throws Exception {
    String tablePrefix = options.tablePrefix;
    if (tablePrefix == null || tablePrefix.isBlank()) {
      tablePrefix = "mgrstress_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
      options.tablePrefix = tablePrefix;
    }
    runId = UUID.randomUUID().toString().replace("-", "");
    stateDir = Files.createTempDirectory("accumulo-manager-stress-");

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      stopWorkers();
      try {
        reportRunMetrics();
      } catch (Exception e) {
        log.error("Could not aggregate manager-stress metrics", e);
      }
      try {
        cleanupNamespace();
      } catch (Exception e) {
        log.error("Could not clean up manager-stress namespace {}", options.namespace, e);
      } finally {
        try {
          cleanupBulkImportDirectory();
        } finally {
          try {
            restoreCompactionServiceProperties();
          } catch (Exception e) {
            log.error("Could not restore compaction service system properties", e);
          } finally {
            deleteControlDirectory();
          }
        }
      }
    }, "manager-stress-shutdown"));

    try {
      try (AccumuloClient client = Accumulo.newClient().from(options.getClientProps()).build()) {
        createNamespace(client);
        configureCompactionService(client);
        new TablePool(stateDir, options.namespace, tablePrefix, options.tables, options.seed)
            .initialize(client);
      }

      log.info("Created {} stress tables with prefix {}", options.tables, tablePrefix);
      startWorkers();
      waitForWorkersReady();

      long deadline = Math.addExact(System.currentTimeMillis(), options.duration);
      Path startFile = stateDir.resolve("start");
      Path tempStartFile = stateDir.resolve("start.tmp");
      Files.writeString(tempStartFile, Long.toString(deadline), UTF_8);
      Files.move(tempStartFile, startFile, ATOMIC_MOVE);
      log.info("Started {} client processes for {} ms", options.clients, options.duration);
      awaitWorkers(deadline);
    } finally {
      stopWorkers();
      try {
        reportRunMetrics();
      } finally {
        try {
          cleanupNamespace();
        } finally {
          try {
            cleanupBulkImportDirectory();
          } finally {
            try {
              restoreCompactionServiceProperties();
            } finally {
              deleteControlDirectory();
            }
          }
        }
      }
    }
  }

  private synchronized void reportRunMetrics() {
    if (!metricsReported.compareAndSet(false, true)) {
      return;
    }
    List<ManagerStressMetrics.Snapshot> snapshots = new ArrayList<>();
    for (int workerId = 0; workerId < options.clients; workerId++) {
      Path snapshotPath = ManagerStressMetrics.snapshotPath(stateDir, workerId);
      if (Files.exists(snapshotPath)) {
        try {
          snapshots.add(ManagerStressMetrics.readSnapshot(snapshotPath));
        } catch (Exception e) {
          log.warn("Could not read metrics snapshot for worker {}", workerId, e);
        }
      } else {
        log.warn("No final metrics snapshot was available for worker {}", workerId);
      }
    }

    ManagerStressMetrics.RunSnapshot totals = ManagerStressMetrics.aggregate(snapshots);
    log.info("ManagerStress totals from {}/{} worker snapshots:", totals.workers(),
        options.clients);
    for (Operation operation : Operation.values()) {
      ManagerStressMetrics.OperationSnapshot counts = totals.operations().get(operation);
      double averageMillis =
          counts.attempts() == 0 ? 0 : (double) counts.totalNanos() / counts.attempts() / 1_000_000;
      log.info(
          "ManagerStress {}: attempts={}, submitted={}, completed={}, asyncAccepted={}, "
              + "skipped={}, races={}, failed={}, outcomeUnknown={}, avgMs={}",
          operation, counts.attempts(), counts.submitted(), counts.completed(),
          counts.asyncAccepted(), counts.skipped(), counts.races(), counts.failed(),
          counts.outcomeUnknown(), String.format("%.3f", averageMillis));
    }
    log.info("ManagerStress replacement-table maintenance creates={}", totals.maintenanceCreates());
  }

  private void createNamespace(AccumuloClient client) throws Exception {
    if (client.namespaceOperations().exists(options.namespace)) {
      throw new IllegalStateException("Refusing to use existing namespace " + options.namespace);
    }
    client.namespaceOperations().create(options.namespace);
    namespaceCreated = true;
    client.namespaceOperations().setProperty(options.namespace,
        Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", COMPACTION_SERVICE_NAME);
    log.info("Created namespace {} for manager-stress run", options.namespace);
  }

  static Map<String,String> compactionServiceProperties(String compactorResourceGroup) {
    String servicePrefix = Property.COMPACTION_SERVICE_PREFIX.getKey() + COMPACTION_SERVICE_NAME;
    String group = ResourceGroupId.of(compactorResourceGroup).canonical();
    return Map.of(servicePrefix + ".planner", RatioBasedCompactionPlanner.class.getName(),
        servicePrefix + ".planner.opts.groups", "[{\"group\":\"" + group + "\"}]");
  }

  private synchronized void configureCompactionService(AccumuloClient client) throws Exception {
    Map<String,String> desired = compactionServiceProperties(options.compactorResourceGroup);
    Map<String,PreviousSystemProperty> previous = new HashMap<>();
    desiredCompactionServiceProperties = desired;
    client.instanceOperations().modifyProperties(systemProperties -> {
      previous.clear();
      desired.forEach((key, value) -> {
        previous.put(key, new PreviousSystemProperty(systemProperties.containsKey(key),
            systemProperties.get(key)));
        systemProperties.put(key, value);
      });
      previousCompactionServiceProperties = Map.copyOf(previous);
      compactionServicePropertiesConfigured = true;
    });
    log.info("Configured compaction service {} for resource group {}", COMPACTION_SERVICE_NAME,
        options.compactorResourceGroup);
  }

  private synchronized void restoreCompactionServiceProperties() throws Exception {
    if (!compactionServicePropertiesConfigured) {
      return;
    }
    try (AccumuloClient client = Accumulo.newClient().from(options.getClientProps()).build()) {
      client.instanceOperations().modifyProperties(systemProperties -> {
        previousCompactionServiceProperties.forEach((key, previous) -> {
          if (!Objects.equals(systemProperties.get(key),
              desiredCompactionServiceProperties.get(key))) {
            log.warn(
                "System property {} changed during the ManagerStress run; preserving its value",
                key);
            return;
          }
          if (previous.wasSet()) {
            systemProperties.put(key, previous.value());
          } else {
            systemProperties.remove(key);
          }
        });
      });
      compactionServicePropertiesConfigured = false;
      log.info("Restored compaction service system properties after ManagerStress run");
    }
  }

  private synchronized void cleanupNamespace() throws Exception {
    if (!namespaceCreated) {
      return;
    }
    try (AccumuloClient client = Accumulo.newClient().from(options.getClientProps()).build()) {
      var tableOps = client.tableOperations();
      String namespacePrefix = options.namespace + ".";
      List<String> tables = tableOps.list().stream()
          .filter(tableName -> tableName.startsWith(namespacePrefix)).toList();
      Exception cleanupError = null;
      for (String table : tables) {
        try {
          tableOps.delete(table);
        } catch (Exception e) {
          if (cleanupError == null) {
            cleanupError = new IllegalStateException(
                "Could not remove all tables from manager-stress namespace " + options.namespace);
          }
          cleanupError.addSuppressed(e);
        }
      }
      if (cleanupError != null) {
        throw cleanupError;
      }
      client.namespaceOperations().delete(options.namespace);
      namespaceCreated = false;
      log.info("Deleted namespace {} after manager-stress run", options.namespace);
    }
  }

  private void cleanupBulkImportDirectory() {
    if (options.hdfsDir == null || options.hdfsDir.isBlank() || runId == null) {
      return;
    }
    try {
      deleteBulkImportRunDirectory(options.hdfsDir, runId);
      log.info("Deleted HDFS bulk-import directory for run {}", runId);
    } catch (IOException e) {
      log.warn("Could not remove HDFS bulk-import directory for run {}", runId, e);
    }
  }

  static void deleteBulkImportRunDirectory(String configuredDirectory, String runId)
      throws IOException {
    var runDirectory =
        new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path(configuredDirectory), runId);
    var configuration = new org.apache.hadoop.conf.Configuration();
    try (var fileSystem =
        org.apache.hadoop.fs.FileSystem.newInstance(runDirectory.toUri(), configuration)) {
      boolean deleted = fileSystem.delete(runDirectory, true);
      if (!deleted && fileSystem.exists(runDirectory)) {
        throw new IOException("Could not delete HDFS bulk-import directory " + runDirectory);
      }
    }
  }

  private void startWorkers() throws IOException {
    for (int i = 0; i < options.clients; i++) {
      List<String> command = new ArrayList<>();
      command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
      String logConfig = System.getProperty("log4j.configurationFile");
      if (logConfig != null) {
        command.add("-Dlog4j.configurationFile=" + logConfig);
      }
      command.add("-cp");
      command.add(System.getProperty("java.class.path"));
      command.add(ManagerStress.class.getName());
      command.addAll(workerArguments());
      command.add("--table-prefix");
      command.add(options.tablePrefix);
      command.add("--internal-worker-id");
      command.add(Integer.toString(i));
      command.add("--internal-state-dir");
      command.add(stateDir.toString());
      command.add("--internal-run-id");
      command.add(runId);

      Process process = new ProcessBuilder(command).inheritIO().start();
      workers.add(process);
      log.info("Started worker {} with pid {}", i, process.pid());
    }
  }

  private void waitForWorkersReady() throws Exception {
    long deadline = System.nanoTime() + WORKER_STARTUP_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      int ready = 0;
      for (int i = 0; i < workers.size(); i++) {
        if (Files.exists(stateDir.resolve("ready-" + i))) {
          ready++;
        } else if (!workers.get(i).isAlive()) {
          throw new IllegalStateException("Worker " + i + " exited before startup completed");
        }
      }
      if (ready == workers.size()) {
        return;
      }
      Thread.sleep(100);
    }
    throw new IllegalStateException("Timed out waiting for client workers to initialize");
  }

  private void awaitWorkers(long deadline) throws InterruptedException {
    long untilDeadline = deadline - System.currentTimeMillis();
    if (untilDeadline > 0) {
      Thread.sleep(untilDeadline);
    }
    for (int i = 0; i < workers.size(); i++) {
      Process process = workers.get(i);
      if (!process.waitFor(WORKER_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
        log.warn("Worker {} did not stop before the shutdown timeout", i);
        process.destroy();
        try {
          if (!process.waitFor(5, TimeUnit.SECONDS)) {
            process.destroyForcibly();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          process.destroyForcibly();
          throw e;
        }
      } else if (process.exitValue() != 0) {
        log.warn("Worker {} exited with status {}", i, process.exitValue());
      }
    }
  }

  private List<String> workerArguments() {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < originalArgs.length; i++) {
      String arg = originalArgs[i];
      if (arg.equals("--table-prefix") || arg.equals("--internal-worker-id")
          || arg.equals("--internal-state-dir") || arg.equals("--internal-run-id")
          || arg.startsWith("--table-prefix=") || arg.startsWith("--internal-worker-id=")
          || arg.startsWith("--internal-state-dir=") || arg.startsWith("--internal-run-id=")) {
        if (arg.contains("=")) {
          continue;
        }
        i++;
      } else {
        result.add(arg);
      }
    }
    return result;
  }

  private synchronized void stopWorkers() {
    if (!stopping.compareAndSet(false, true)) {
      return;
    }
    for (Process process : workers) {
      if (process.isAlive()) {
        process.destroy();
      }
    }
    for (Process process : workers) {
      if (process.isAlive()) {
        try {
          if (!process.waitFor(WORKER_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            process.waitFor();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          process.destroyForcibly();
        }
      }
    }
  }

  private synchronized void deleteControlDirectory() {
    Path directory = stateDir;
    stateDir = null;
    if (directory == null) {
      return;
    }
    try {
      deleteRecursivelyIfExists(directory);
    } catch (IOException e) {
      log.warn("Could not remove temporary manager-stress directory {}", directory, e);
    }
  }

  static void deleteRecursivelyIfExists(Path directory) throws IOException {
    try (var paths = Files.walk(directory)) {
      for (Path path : paths.sorted((a, b) -> b.compareTo(a)).toList()) {
        try {
          Files.deleteIfExists(path);
        } catch (NoSuchFileException ignored) {
          // The entry may have been removed by another cleanup invocation.
        }
      }
    } catch (NoSuchFileException ignored) {
      // The root directory may already have been removed.
    }
  }
}
