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
package org.apache.accumulo.testing.performance.tests;

import org.apache.accumulo.core.spi.scan.ScanDispatch;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;

public class TimedScanDispatcher implements ScanDispatcher {

  private String quickExecutor;
  private String longExectuor;
  private long quickTime;

  @Override
  public void init(InitParameters params) {
    var options = params.getOptions();
    quickExecutor = options.get("quick.executor");
    longExectuor = options.get("long.executor");
    quickTime = Long.parseLong(params.getOptions().get("quick.time.ms"));
  }

  @Override
  public ScanDispatch dispatch(DispatchParameters params) {
    var n = params.getScanInfo().getRunTimeStats().sum() < quickTime ? quickExecutor : longExectuor;
    return ScanDispatch.builder().setExecutorName(n).build();
  }
}
