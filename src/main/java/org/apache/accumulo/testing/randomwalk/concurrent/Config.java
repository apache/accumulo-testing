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
package org.apache.accumulo.testing.randomwalk.concurrent;

import java.util.Properties;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.commons.math3.random.RandomDataGenerator;

public class Config extends Test {

  private static final String LAST_SETTING = "lastSetting";

  private static final String LAST_TABLE_SETTING = "lastTableSetting";

  private static final String LAST_NAMESPACE_SETTING = "lastNamespaceSetting";

  static class Setting {
    final public Property property;
    final public long min;
    final public long max;

    public Setting(Property property, long min, long max) {
      this.property = property;
      this.min = min;
      this.max = max;
    }
  }

  static Setting s(Property property, long min, long max) {
    return new Setting(property, min, max);
  }

  @SuppressWarnings("deprecation")
  final Property TSERV_READ_AHEAD_MAXCONCURRENT_deprecated = Property.TSERV_READ_AHEAD_MAXCONCURRENT;
  // @formatter:off
  final Setting[] settings = {
			s(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, 1, 10),
			s(Property.TSERV_BULK_PROCESS_THREADS, 1, 10),
			s(Property.TSERV_BULK_RETRY, 1, 10),
			s(Property.TSERV_BULK_TIMEOUT, 10, 600),
			s(Property.TSERV_BULK_ASSIGNMENT_THREADS, 1, 10),
			s(Property.TSERV_DATACACHE_SIZE, 0, 1000000000L),
			s(Property.TSERV_INDEXCACHE_SIZE, 0, 1000000000L),
			s(Property.TSERV_CLIENT_TIMEOUT, 100, 10000),
			s(Property.TSERV_COMPACTION_SERVICE_DEFAULT_EXECUTORS, 1, 10),
			s(Property.TSERV_MAJC_DELAY, 100, 10000),
			s(Property.TSERV_COMPACTION_SERVICE_DEFAULT_MAX_OPEN, 3, 100),
			s(Property.TSERV_MINC_MAXCONCURRENT, 1, 10),
			s(Property.TSERV_DEFAULT_BLOCKSIZE, 100000, 10000000L),
			s(Property.TSERV_MAX_IDLE, 10000, 500 * 1000),
			s(Property.TSERV_MAXMEM, 1000000, 3 * 1024 * 1024 * 1024L),
			s(TSERV_READ_AHEAD_MAXCONCURRENT_deprecated, 1, 25),
			s(Property.TSERV_MIGRATE_MAXCONCURRENT, 1, 10),
			s(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX, 10000, 1024 * 1024),
			s(Property.TSERV_WAL_SORT_MAX_CONCURRENT, 1, 100),
			s(Property.TSERV_SCAN_MAX_OPENFILES, 10, 1000),
			s(Property.TSERV_THREADCHECK, 100, 10000),
			s(Property.TSERV_MINTHREADS, 1, 100),
			s(Property.TSERV_SESSION_MAXIDLE, 100, 5 * 60 * 1000),
			s(Property.TSERV_WAL_SORT_BUFFER_SIZE, 1024 * 1024, 1024 * 1024 * 1024L),
			s(Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN, 5, 100),
			s(Property.TSERV_WAL_BLOCKSIZE, 1024 * 1024,1024 * 1024 * 1024 * 10L),
			s(Property.TSERV_WORKQ_THREADS, 1, 10),
			s(Property.MANAGER_BULK_THREADPOOL_SIZE, 1, 10),
			s(Property.MANAGER_BULK_RETRIES, 1, 10),
			s(Property.MANAGER_BULK_TIMEOUT, 10, 600),
			s(Property.MANAGER_FATE_THREADPOOL_SIZE, 1, 100),
			s(Property.MANAGER_RECOVERY_DELAY, 0, 100),
			s(Property.MANAGER_LEASE_RECOVERY_WAITING_PERIOD, 0, 10),
			s(Property.MANAGER_THREADCHECK, 100, 10000),
			s(Property.MANAGER_MINTHREADS, 1, 200),};

    final Setting[] tableSettings = {
			s(Property.TABLE_MAJC_RATIO, 1, 10),
			s(Property.TABLE_SPLIT_THRESHOLD, 10 * 1024, 10L * 1024 * 1024 * 1024),
			s(Property.TABLE_MINC_COMPACT_IDLETIME, 100, 100 * 60 * 60 * 1000L),
			s(Property.TABLE_SCAN_MAXMEM, 10 * 1024, 10 * 1024 * 1024),
			s(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE, 10 * 1024, 10 * 1024 * 1024L),
			s(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX, 10 * 1024, 10 * 1024 * 1024L),
			s(Property.TABLE_FILE_REPLICATION, 0, 5),
			s(Property.TABLE_FILE_MAX, 2, 50),};
	// @formatter:on

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    // reset any previous setting
    Object lastSetting = state.getOkIfAbsent(LAST_SETTING);
    if (lastSetting != null) {
      int choice = Integer.parseInt(lastSetting.toString());
      Property property = settings[choice].property;
      log.debug("Setting " + property.getKey() + " back to " + property.getDefaultValue());
      env.getAccumuloClient().instanceOperations().setProperty(property.getKey(),
          property.getDefaultValue());
    }
    lastSetting = state.getOkIfAbsent(LAST_TABLE_SETTING);
    if (lastSetting != null) {
      String[] parts = lastSetting.toString().split(",");
      String table = parts[0];
      int choice = Integer.parseInt(parts[1]);
      Property property = tableSettings[choice].property;
      if (env.getAccumuloClient().tableOperations().exists(table)) {
        log.debug("Setting " + property.getKey() + " on " + table + " back to "
            + property.getDefaultValue());
        try {
          env.getAccumuloClient().tableOperations().setProperty(table, property.getKey(),
              property.getDefaultValue());
        } catch (AccumuloException ex) {
          if (ex.getCause() instanceof TableNotFoundException) {
            return;
          }
          throw ex;
        }
      }
    }
    lastSetting = state.getOkIfAbsent(LAST_NAMESPACE_SETTING);
    if (lastSetting != null) {
      String[] parts = lastSetting.toString().split(",");
      String namespace = parts[0];
      int choice = Integer.parseInt(parts[1]);
      Property property = tableSettings[choice].property;
      if (env.getAccumuloClient().namespaceOperations().exists(namespace)) {
        log.debug("Setting " + property.getKey() + " on " + namespace + " back to "
            + property.getDefaultValue());
        try {
          env.getAccumuloClient().namespaceOperations().setProperty(namespace, property.getKey(),
              property.getDefaultValue());
        } catch (AccumuloException ex) {
          if (ex.getCause() instanceof NamespaceNotFoundException) {
            return;
          }
          throw ex;
        }
      }
    }
    state.remove(LAST_SETTING);
    state.remove(LAST_TABLE_SETTING);
    state.remove(LAST_NAMESPACE_SETTING);
    RandomDataGenerator random = new RandomDataGenerator();
    int dice = random.nextInt(0, 2);
    if (dice == 0) {
      changeTableSetting(random, state, env, props);
    } else if (dice == 1) {
      changeNamespaceSetting(random, state, env, props);
    } else {
      changeSetting(random, state, env, props);
    }
  }

  private void changeTableSetting(RandomDataGenerator random, State state, RandWalkEnv env,
      Properties props) throws Exception {
    // pick a random property
    int choice = random.nextInt(0, tableSettings.length - 1);
    Setting setting = tableSettings[choice];

    // pick a random table
    SortedSet<String> tables = env.getAccumuloClient().tableOperations().list().tailSet("ctt")
        .headSet("ctu");
    if (tables.isEmpty())
      return;
    String table = random.nextSample(tables, 1)[0].toString();

    // generate a random value
    long newValue = random.nextLong(setting.min, setting.max);
    state.set(LAST_TABLE_SETTING, table + "," + choice);
    log.debug("Setting " + setting.property.getKey() + " on table " + table + " to " + newValue);
    try {
      env.getAccumuloClient().tableOperations().setProperty(table, setting.property.getKey(),
          "" + newValue);
    } catch (AccumuloException ex) {
      if (ex.getCause() instanceof TableNotFoundException) {
        return;
      }
      throw ex;
    }
  }

  private void changeNamespaceSetting(RandomDataGenerator random, State state, RandWalkEnv env,
      Properties props) throws Exception {
    // pick a random property
    int choice = random.nextInt(0, tableSettings.length - 1);
    Setting setting = tableSettings[choice];

    // pick a random table
    SortedSet<String> namespaces = env.getAccumuloClient().namespaceOperations().list()
        .tailSet("nspc").headSet("nspd");
    if (namespaces.isEmpty())
      return;
    String namespace = random.nextSample(namespaces, 1)[0].toString();

    // generate a random value
    long newValue = random.nextLong(setting.min, setting.max);
    state.set(LAST_NAMESPACE_SETTING, namespace + "," + choice);
    log.debug(
        "Setting " + setting.property.getKey() + " on namespace " + namespace + " to " + newValue);
    try {
      env.getAccumuloClient().namespaceOperations().setProperty(namespace,
          setting.property.getKey(), "" + newValue);
    } catch (AccumuloException ex) {
      if (ex.getCause() instanceof NamespaceNotFoundException) {
        return;
      }
      throw ex;
    }
  }

  private void changeSetting(RandomDataGenerator random, State state, RandWalkEnv env,
      Properties props) throws Exception {
    // pick a random property
    int choice = random.nextInt(0, settings.length - 1);
    Setting setting = settings[choice];
    // generate a random value
    long newValue = random.nextLong(setting.min, setting.max);
    state.set(LAST_SETTING, "" + choice);
    log.debug("Setting " + setting.property.getKey() + " to " + newValue);
    env.getAccumuloClient().instanceOperations().setProperty(setting.property.getKey(),
        "" + newValue);
  }

}
