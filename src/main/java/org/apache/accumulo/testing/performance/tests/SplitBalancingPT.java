package org.apache.accumulo.testing.performance.tests;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitBalancingPT implements PerformanceTest {

  private static final Logger LOG = LoggerFactory.getLogger(SplitBalancingPT.class);

  private static final String TABLE_NAME = "splitBalancing";
  private static final String RESERVED_PREFIX = "~";
  private static final int NUM_SPLITS = 1_000;
  private static final int MARGIN = 3;
  private static final Text TSERVER_ASSIGNED_TABLETS_COL_FAM = new Text("loc");

  @Override
  public SystemConfiguration getSystemConfig() {
    return new SystemConfiguration();
  }

  @Override
  public Report runTest(final Environment env) throws Exception {
    AccumuloClient client = env.getClient();
    client.tableOperations().create(TABLE_NAME);
    client.tableOperations().addSplits(TABLE_NAME, getSplits());
    client.instanceOperations().waitForBalance();

    int totalTabletServers = client.instanceOperations().getTabletServers().size();
    int expectedAllocation = NUM_SPLITS / totalTabletServers;
    int min = expectedAllocation - MARGIN;
    int max = expectedAllocation + MARGIN;

    Report.Builder reportBuilder = Report.builder().id("split_balancing").description(
        "Evaluate and verify that when a high number of splits are created, that the tablets are balanced equally among tablet servers.")
        .parameter("num_splits", NUM_SPLITS, "The number of splits")
        .parameter("num_tservers", totalTabletServers, "The number of tablet servers")
        .parameter("tserver_min", min,
            "The minimum number of tablets that should be assigned to a tablet server.")
        .parameter("tserver_max", max,
            "The maximum number of tablets that should be assigned to a tablet server.");

    boolean allServersBalanced = true;
    Map<String,Integer> tablets = getTablets(client);
    for (String tabletServer : tablets.keySet()) {
      int count = tablets.get(tabletServer);
      boolean balanced = count >= min && count <= max;
      allServersBalanced = allServersBalanced & balanced;

      reportBuilder.result("size_tserver_" + tabletServer, count,
          "Total tablets assigned to tablet server " + tabletServer);
    }

    return reportBuilder.build();
  }

  private SortedSet<Text> getSplits() {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < NUM_SPLITS; i++) {
      splits.add(new Text(String.valueOf(i)));
    }
    return splits;
  }

  private Map<String,Integer> getTablets(final AccumuloClient client) {
    Map<String,Integer> tablets = new HashMap<>();
    try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.fetchColumnFamily(TSERVER_ASSIGNED_TABLETS_COL_FAM);
      Range range = new Range(null, false, RESERVED_PREFIX, false);
      scanner.setRange(range);

      for (Map.Entry<Key,Value> entry : scanner) {
        String host = entry.getValue().toString();
        if (tablets.containsKey(host)) {
          tablets.put(host, tablets.get(host) + 1);
        } else {
          tablets.put(host, 1);
        }
      }
    } catch (Exception e) {
      LOG.error("Error occurred during scan:", e);
    }
    return tablets;
  }
}
