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
package org.apache.accumulo.testing.manager.stress;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManagerStressMetricsTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void snapshotsAreReadAndAggregatedAcrossWorkers() throws IOException {
    ManagerStressMetrics workerOne = new ManagerStressMetrics();
    ManagerStressMetrics.Counts create = workerOne.counts(Operation.CREATE);
    create.attempts = 4;
    create.submitted = 3;
    create.completed = 2;
    create.skipped = 1;
    create.failed = 1;
    create.outcomeUnknown = 1;
    create.totalNanos = 40;
    workerOne.addMaintenanceCreates(5);
    workerOne.writeSnapshot(temporaryDirectory, 0);

    ManagerStressMetrics workerTwo = new ManagerStressMetrics();
    ManagerStressMetrics.Counts compact = workerTwo.counts(Operation.COMPACT);
    compact.attempts = 2;
    compact.submitted = 2;
    compact.asyncAccepted = 2;
    compact.totalNanos = 60;
    workerTwo.addMaintenanceCreates(7);
    workerTwo.writeSnapshot(temporaryDirectory, 1);

    var first =
        ManagerStressMetrics.readSnapshot(ManagerStressMetrics.snapshotPath(temporaryDirectory, 0));
    var second =
        ManagerStressMetrics.readSnapshot(ManagerStressMetrics.snapshotPath(temporaryDirectory, 1));
    var totals = ManagerStressMetrics.aggregate(List.of(first, second));

    assertEquals(2, totals.workers());
    assertEquals(12, totals.maintenanceCreates());
    assertEquals(4, totals.operations().get(Operation.CREATE).attempts());
    assertEquals(3, totals.operations().get(Operation.CREATE).submitted());
    assertEquals(2, totals.operations().get(Operation.CREATE).completed());
    assertEquals(1, totals.operations().get(Operation.CREATE).outcomeUnknown());
    assertEquals(2, totals.operations().get(Operation.COMPACT).asyncAccepted());
  }
}
