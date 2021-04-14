/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.zookeeper;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.accumulo.tserver.ZKDroppingTServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that attempts to simulate ZK dropping connections to one tserver. The use case is a cluster
 * with a rack of tservers that has trouble connecting with ZK, while the Manager and other servers
 * are fine.
 */
public class ZKDroppingIT extends ConfigurableMacBase {
  private static Logger log = LoggerFactory.getLogger(ZKDroppingIT.class);
  public static int ROWS = 5000;
  public static int SPLITS = 9;

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  // @Test
  public void test() throws Exception {
    List<ProcessReference> tProcs = new ArrayList<>(
        cluster.getProcesses().get(ServerType.TABLET_SERVER));

    Process zombie = cluster.exec(ZKDroppingTServer.class).getProcess();
    // String tableName = getUniqueNames(1)[0];
    String tableName = "test_ingest";

    FileSystem fs = getCluster().getFileSystem();
    Path root = new Path(cluster.getTemporaryPath(), getClass().getName());
    fs.deleteOnExit(root);
    Path testrf = new Path(root, "testrf");
    fs.deleteOnExit(testrf);
    var zoo = cluster.getServerContext().getZooReaderWriter();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      var tservers = client.instanceOperations().getTabletServers();
      while (tservers.size() != 2) {
        sleepUninterruptibly(5, TimeUnit.SECONDS);
        tservers = client.instanceOperations().getTabletServers();
        log.info("Tservers # = " + tservers.size());
      }
      var ntc = new NewTableConfiguration().withSplits(TestIngest.getSplitPoints(0, ROWS, SPLITS));
      client.tableOperations().create(tableName, ntc);
      FunctionalTestUtils.createRFiles(client, fs, testrf.toString(), ROWS, SPLITS, 4);

      client.tableOperations().importDirectory(testrf.toString()).to(tableName).load();

      // client.tableOperations().importDirectory(dir).to(tableName).load();
      FunctionalTestUtils.checkSplits(client, tableName, SPLITS, SPLITS + 1);
      VerifyIngest.VerifyParams params = new VerifyIngest.VerifyParams(
          cluster.getClientProperties(), tableName, ROWS);
      params.timestamp = 1;
      params.dataSize = 50;
      params.random = 56;
      params.startRow = 0;
      params.cols = 1;
      VerifyIngest.verifyIngest(client, params);

      printTabletLocations(client, tableName);

      // cluster.killProcess(ServerType.TABLET_SERVER, tProcs.get(0));

      // log.info("Killing Tserver");
      // printTabletLocations(client, tableName);

      // log.info("Wait for balance.");
      // client.instanceOperations().waitForBalance();

      // signal to the server to start dropping
      log.info("signal to the server to start dropping");
      zoo.mkdirs(ZKDroppingTServer.ZPATH_START_DROPPING);
      // zoo.mutateOrCreate(ZKDroppingTServer.ZPATH_START_DROPPING, new byte[0],
      // b -> b = "false".getBytes(StandardCharsets.UTF_8));

      log.info("Compact {}", tableName);
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      printTabletLocations(client, tableName);

      // cluster.start();

      log.info("Wait again for balance.");
      client.instanceOperations().waitForBalance();

      // ensure each tablet does not have all map files, should be ~2.5 files per tablet
      // FunctionalTestUtils.checkRFiles(client, tableName, SPLITS, SPLITS + 1, 1, 4);
    }
    sleepUninterruptibly(30, TimeUnit.SECONDS);
    // Assert.assertEquals(0, zombie.waitFor());
  }

  private void printTabletLocations(AccumuloClient client, String tableName) throws Exception {
    var locations = client.tableOperations().locate(tableName, List.of(new Range()));
    Map<String,List<TabletId>> tabletsOnServer = new HashMap<>();
    locations.groupByTablet().forEach((tid, r) -> {
      String server = locations.getTabletLocation(tid);
      tabletsOnServer.putIfAbsent(server, new ArrayList<>());
      tabletsOnServer.get(server).add(tid);
    });
    tabletsOnServer.forEach((server, tablets) -> {
      log.info("{} has {} tablets", server, tablets.size());
    });
  }
}
