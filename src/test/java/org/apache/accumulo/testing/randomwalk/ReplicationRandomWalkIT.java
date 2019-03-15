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
package org.apache.accumulo.testing.randomwalk;

import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_MAX_SIZE;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.testing.randomwalk.concurrent.Replication;
import org.apache.hadoop.conf.Configuration;
import org.easymock.EasyMock;
import org.junit.Test;

public class ReplicationRandomWalkIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(TSERV_NATIVEMAP_ENABLED, "false");
    cfg.setNumTservers(1);
  }

  @Test(timeout = 5 * 60 * 1000)
  public void runReplicationRandomWalkStep() throws Exception {
    Replication r = new Replication();

    RandWalkEnv env = EasyMock.createMock(RandWalkEnv.class);
    EasyMock.expect(env.getAccumuloUserName()).andReturn("root").anyTimes();
    EasyMock.expect(env.getAccumuloPassword()).andReturn(ROOT_PASSWORD).anyTimes();
    AccumuloClient client = Accumulo.newClient().from(this.getClientProperties()).build();
    EasyMock.expect(env.getAccumuloClient()).andReturn(client).anyTimes();
    EasyMock.replay(env);

    r.visit(null, env, null);
  }
}
