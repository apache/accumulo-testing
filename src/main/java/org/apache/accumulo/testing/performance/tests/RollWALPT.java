package org.apache.accumulo.testing.performance.tests;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;

public class RollWALPT implements PerformanceTest {

  private static final String TABLE_SMALL_WAL = "SmallRollWAL";
  private static final String TABLE_LARGE_WAL = "LargeRollWAL";
  private static final String SIZE_SMALL_WAL = "5M";
  private static final String SIZE_LARGE_WAL = "1G";
  private static final long NUM_SPLITS = 100L;
  private static final long SPLIT_DISTANCE = Long.MAX_VALUE / NUM_SPLITS;
  private static final int NUM_ENTRIES = 50_000;

  private final SecureRandom random = new SecureRandom();

  @Override
  public SystemConfiguration getSystemConfig() {
    Map<String,String> config = new HashMap<>();

    config.put(Property.TSERV_WAL_REPLICATION.getKey(), "1");
    config.put(Property.TSERV_WALOG_MAX_REFERENCED.getKey(), "100");
    config.put(Property.GC_CYCLE_START.getKey(), "1s");
    config.put(Property.GC_CYCLE_DELAY.getKey(), "1s");

    return new SystemConfiguration().setAccumuloConfig(config);
  }

  @Override
  public Report runTest(final Environment env) throws Exception {
    Report.Builder reportBuilder = Report.builder().id("rollWAL").description(
        "Evaluate the performance of ingesting a large number of entries across numerous splits given both a small and large maximum WAL size.")
        .parameter("small_wal_table", TABLE_SMALL_WAL,
            "The name of the table used for evaluating performance with a small WAL.")
        .parameter("large_wal_table", TABLE_LARGE_WAL,
            "The name of the table used for evaluating performance with a large WAL.")
        .parameter("num_splits", NUM_SPLITS,
            "The number of splits that will be added to the tables.")
        .parameter("split_distance", SPLIT_DISTANCE, "The distance between each split.")
        .parameter("num_entries", NUM_ENTRIES,
            "The number of entries that will be written to the tables.")
        .parameter("small_wal_size", SIZE_SMALL_WAL,
            "The size of the small WAL used to force many rollovers.")
        .parameter("large_wal_size", SIZE_LARGE_WAL,
            "The size of the large WAL used to avoid many rollovers");

    AccumuloClient client = env.getClient();
    final long smallWALTime = evalSmallWAL(client);
    reportBuilder.result("small_wal_write_time", smallWALTime,
        "The time (in ns) it took to write entries to the table with a small WAL of "
            + SIZE_SMALL_WAL);

    final long largeWALTime = evalLargeWAL(client);
    reportBuilder.result("large_wal_write_time", largeWALTime,
        "The time (in ns) it took to write entries to the table with a large WAL of "
            + SIZE_LARGE_WAL);
    return reportBuilder.build();
  }

  private long evalSmallWAL(final AccumuloClient client) throws AccumuloSecurityException,
      AccumuloException, TableExistsException, TableNotFoundException {
    setMaxWALSize(SIZE_SMALL_WAL, client);
    initTable(TABLE_SMALL_WAL, client);
    return getTimeToWriteEntries(TABLE_SMALL_WAL, client);
  }

  private long evalLargeWAL(final AccumuloClient client) throws AccumuloSecurityException,
      AccumuloException, TableExistsException, TableNotFoundException {
    setMaxWALSize(SIZE_LARGE_WAL, client);
    initTable(TABLE_LARGE_WAL, client);
    return getTimeToWriteEntries(TABLE_LARGE_WAL, client);
  }

  private void setMaxWALSize(final String size, final AccumuloClient client)
      throws AccumuloSecurityException, AccumuloException {
    client.instanceOperations().setProperty(Property.TSERV_WALOG_MAX_SIZE.getKey(), size);
  }

  private void initTable(final String tableName, final AccumuloClient client)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {
    client.tableOperations().create(tableName);
    client.tableOperations().addSplits(tableName, getSplits());
    client.instanceOperations().waitForBalance();
  }

  private SortedSet<Text> getSplits() {
    SortedSet<Text> splits = new TreeSet<>();
    for (long i = 0L; i < NUM_SPLITS; i++) {
      splits.add(new Text(String.format("%016x", i + SPLIT_DISTANCE)));
    }
    return splits;
  }

  private long getTimeToWriteEntries(final String tableName, final AccumuloClient client)
      throws TableNotFoundException, MutationsRejectedException {
    long start = System.nanoTime();
    writeEntries(tableName, client);
    return System.nanoTime() - start;
  }

  private void writeEntries(final String tableName, final AccumuloClient client)
      throws TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = client.createBatchWriter(tableName);

    String instanceId = UUID.randomUUID().toString();
    final ColumnVisibility cv = new ColumnVisibility();
    for (int i = 0; i < NUM_ENTRIES; i++) {
      String value = instanceId + i;
      Mutation m = genMutation(cv, value);
      bw.addMutation(m);
    }

    bw.close();
  }

  private Mutation genMutation(final ColumnVisibility colVis, final String value) {
    byte[] rowStr = toZeroPaddedString(getRandomLong(), 16);
    byte[] colFamStr = toZeroPaddedString(random.nextInt(Short.MAX_VALUE), 4);
    byte[] colQualStr = toZeroPaddedString(random.nextInt(Short.MAX_VALUE), 4);
    Mutation mutation = new Mutation(new Text(rowStr));
    mutation.put(new Text(colFamStr), new Text(colQualStr), colVis, new Value(value));
    return mutation;
  }

  private long getRandomLong() {
    return ((random.nextLong() & 0x7fffffffffffffffL) % (Long.MAX_VALUE));
  }

  private byte[] toZeroPaddedString(long num, int width) {
    return new byte[Math.max(Long.toString(num, 16).length(), width)];
  }
}
