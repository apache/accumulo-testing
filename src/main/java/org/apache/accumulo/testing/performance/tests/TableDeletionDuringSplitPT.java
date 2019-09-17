package org.apache.accumulo.testing.performance.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;

public class TableDeletionDuringSplitPT implements PerformanceTest {

  private static final int NUM_BATCHES = 12;
  private static final int BATCH_SIZE = 8;
  private static final int MAX_THREADS = BATCH_SIZE + 2;
  private static final int NUM_TABLES = NUM_BATCHES * BATCH_SIZE;
  private static final int NUM_SPLITS = 100;
  private static final int HALF_SECOND = 500;
  private static final String BASE_TABLE_NAME = "tableDeletionDuringSplit";
  private static final String THREAD_NAME = "concurrent-api-requests";

  @Override
  public SystemConfiguration getSystemConfig() {
    return new SystemConfiguration();
  }

  @Override
  public Report runTest(final Environment env) throws Exception {
    Report.Builder reportBuilder = Report.builder().id("TableDeletionDuringSplit")
        .description("Evaluates the performance of deleting tables during split operations.")
        .parameter("num_tables", NUM_TABLES, "The number of tables that will be created/deleted.")
        .parameter("base_table_name", BASE_TABLE_NAME, "The base table name.")
        .parameter("num_splits", NUM_SPLITS,
            "The number of splits that will be added to each table.")
        .parameter("base_thread_name", THREAD_NAME, "The thread name used for the thread pool");

    AccumuloClient client = env.getClient();
    String[] tableNames = getTableNames();
    createTables(tableNames, client);
    splitAndDeleteTables(tableNames, client, reportBuilder);

    return reportBuilder.build();
  }

  private String[] getTableNames() {
    String[] names = new String[NUM_TABLES];
    for (int i = 0; i < NUM_TABLES; i++) {
      names[i] = BASE_TABLE_NAME + i;
    }
    return names;
  }

  private void createTables(final String[] tableNames, final AccumuloClient client)
      throws TableExistsException, AccumuloSecurityException, AccumuloException {
    for (String tableName : tableNames) {
      client.tableOperations().create(tableName);
    }
  }

  private void splitAndDeleteTables(final String[] tableNames, final AccumuloClient client,
      final Report.Builder reportBuilder) throws ExecutionException, InterruptedException {
    final LongAdder deletionTimes = new LongAdder();
    final AtomicInteger deletedTables = new AtomicInteger(0);
    Iterator<Runnable> iter = getTasks(tableNames, client, deletionTimes, deletedTables).iterator();
    ExecutorService pool = Executors.newFixedThreadPool(MAX_THREADS);

    List<Future<?>> results = new ArrayList<>();
    for (int batch = 0; batch < NUM_BATCHES; batch++) {
      for (int i = 0; i < BATCH_SIZE; i++) {
        results.add(pool.submit(iter.next()));
        results.add(pool.submit(iter.next()));
      }

      for (Future<?> future : results) {
        future.get();
      }
      results.clear();
    }

    List<Runnable> queued = pool.shutdownNow();

    reportBuilder.result("remaining_pending_tasks", countRemaining(iter),
        "The number of remaining pending tasks.");
    reportBuilder.result("remaining_submitted_tasks", queued.size(),
        "The number of remaining submitted tasks.");

    long totalRemainingTables = Arrays.stream(tableNames)
        .filter((name) -> client.tableOperations().exists(name)).count();
    reportBuilder.result("total_remaining_tables", totalRemainingTables,
        "The total number of unsuccessfully deleted tables.");
    Long deletionTime = deletionTimes.sum() / deletedTables.get();
    reportBuilder.result("avg_deletion_time", deletionTime,
        "The average deletion time (in ms) to delete a table.");
  }

  private List<Runnable> getTasks(final String[] tableNames, final AccumuloClient client,
      final LongAdder deletionTime, final AtomicInteger deletedTables) {
    List<Runnable> tasks = new ArrayList<>();
    final SortedSet<Text> splits = getSplits();
    for (String tableName : tableNames) {
      tasks.add(getSplitTask(tableName, client, splits));
      tasks.add(getDeletionTask(tableName, client, deletionTime, deletedTables));
    }
    return tasks;
  }

  private SortedSet<Text> getSplits() {
    SortedSet<Text> splits = new TreeSet<>();
    for (byte i = 0; i < NUM_SPLITS; i++) {
      splits.add(new Text(new byte[] {0, 0, i}));
    }
    return splits;
  }

  private Runnable getSplitTask(final String tableName, final AccumuloClient client,
      final SortedSet<Text> splits) {
    return () -> {
      try {
        client.tableOperations().addSplits(tableName, splits);
      } catch (TableNotFoundException ex) {
        // Expected, ignore.
      } catch (Exception e) {
        throw new RuntimeException(tableName, e);
      }
    };
  }

  private Runnable getDeletionTask(final String tableName, final AccumuloClient client,
      final LongAdder timeAdder, final AtomicInteger deletedTables) {
    return () -> {
      try {
        Thread.sleep(HALF_SECOND);
        long start = System.currentTimeMillis();
        client.tableOperations().delete(tableName);
        long time = System.currentTimeMillis() - start;
        timeAdder.add(time);
        deletedTables.getAndIncrement();
      } catch (Exception e) {
        throw new RuntimeException(tableName, e);
      }
    };
  }

  private int countRemaining(final Iterator<?> i) {
    int count = 0;
    while (i.hasNext()) {
      i.next();
      count++;
    }
    return count;
  }
}
