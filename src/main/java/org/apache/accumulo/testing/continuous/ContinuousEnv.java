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
package org.apache.accumulo.testing.continuous;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.TestEnv;
import org.apache.accumulo.testing.TestProps;

public class ContinuousEnv extends TestEnv {

  private List<Authorizations> authList;

  public ContinuousEnv(String[] args) {
    super(args);
  }

  /**
   * @return Accumulo authorizations list
   */
  private List<Authorizations> getAuthList() {
    if (authList == null) {
      String authValue = testProps.getProperty(TestProps.CI_COMMON_AUTHS);
      if (authValue == null || authValue.trim().isEmpty()) {
        authList = Collections.singletonList(Authorizations.EMPTY);
      } else {
        authList = Arrays.stream(authValue.split("\\|")).map(a -> a.split(","))
            .map(Authorizations::new).collect(Collectors.toList());
      }
    }
    return authList;
  }

  /**
   * @return random authorization
   */
  Authorizations getRandomAuthorizations() {
    return getAuthList().get(this.getRandom().nextInt(getAuthList().size()));
  }

  public long getRowMin() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_ROW_MIN));
  }

  public long getRowMax() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_ROW_MAX));
  }

  public int getMaxColF() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_MAX_CF));
  }

  public int getMaxColQ() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_MAX_CQ));
  }

  public int getBulkMapTask() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_BULK_MAP_TASK));
  }

  public long getBulkMapNodes() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_BULK_MAP_NODES));
  }

  public int getBulkReducers() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_BULK_REDUCERS));
  }

  public String getAccumuloTableName() {
    return testProps.getProperty(TestProps.CI_COMMON_ACCUMULO_TABLE);
  }
}
