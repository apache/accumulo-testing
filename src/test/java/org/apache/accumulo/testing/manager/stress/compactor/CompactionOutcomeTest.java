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
package org.apache.accumulo.testing.manager.stress.compactor;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompactionOutcomeTest {

  @org.junit.jupiter.api.Test
  void choosesConfiguredRelativeWeights() {
    StressCompactorOptions options = new StressCompactorOptions();
    options.successWeight = 0;
    options.failureWeight = 2;
    options.cancellationWeight = 3;

    assertEquals(CompactionOutcome.FAILURE, CompactionOutcome.choose(options, 0));
    assertEquals(CompactionOutcome.FAILURE, CompactionOutcome.choose(options, 1.99));
    assertEquals(CompactionOutcome.CANCELLATION, CompactionOutcome.choose(options, 2));
    assertEquals(CompactionOutcome.CANCELLATION, CompactionOutcome.choose(options, 4.99));
  }
}
