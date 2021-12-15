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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.testing.TestEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The test environment that is available for randomwalk tests. This includes configuration
 * properties that are available to any randomwalk test and facilities for creating client-side
 * objects. This class is not thread-safe.
 */
public class RandWalkEnv extends TestEnv {

  private static final Logger log = LoggerFactory.getLogger(RandWalkEnv.class);

  private MultiTableBatchWriter mtbw = null;

  public RandWalkEnv(String testPropsPath, String clientPropsPath) {
    super(testPropsPath, clientPropsPath);
  }

  /**
   * Gets a multitable batch writer. The same object is reused after the first call unless it is
   * reset.
   *
   * @return multitable batch writer
   * @throws NumberFormatException
   *           if any of the numeric batch writer configuration properties cannot be parsed
   * @throws NumberFormatException
   *           if any configuration property cannot be parsed
   */
  public MultiTableBatchWriter getMultiTableBatchWriter()
      throws AccumuloException, AccumuloSecurityException {
    if (mtbw == null) {
      mtbw = getAccumuloClient().createMultiTableBatchWriter();
    }
    return mtbw;
  }

  /**
   * Checks if a multitable batch writer has been created by this wrapper.
   *
   * @return true if multitable batch writer is already created
   */
  public boolean isMultiTableBatchWriterInitialized() {
    return mtbw != null;
  }

  /**
   * Clears the multitable batch writer previously created and remembered by this wrapper.
   */
  public void resetMultiTableBatchWriter() {
    if (mtbw == null)
      return;
    if (!mtbw.isClosed()) {
      log.warn("Setting non-closed MultiTableBatchWriter to null (leaking resources)");
    }
    mtbw = null;
  }
}
