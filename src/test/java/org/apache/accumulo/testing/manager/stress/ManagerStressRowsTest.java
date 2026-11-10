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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.RowRange;
import org.apache.hadoop.io.Text;

class ManagerStressRowsTest {

  @org.junit.jupiter.api.Test
  void createsUnhostedTablesWithBoundedInitialSplitCounts() {
    var config = ManagerStressRows.newTableConfiguration(new Random(7));

    assertEquals(TabletAvailability.UNHOSTED, config.getInitialTabletAvailability());
    assertTrue(config.getSplits().size() >= ManagerStressRows.MIN_INITIAL_SPLITS);
    assertTrue(config.getSplits().size() <= ManagerStressRows.MAX_INITIAL_SPLITS);
    assertEquals(config.getSplits().size(), new TreeSet<>(config.getSplits()).size());
    config.getSplits().forEach(split -> assertTrue(split.toString().matches("[0-9a-f]{16}")));
  }

  @org.junit.jupiter.api.Test
  void addedSplitsRespectTheThousandTabletLimit() {
    var additions = ManagerStressRows.additionalSplits(List.of(), new Random(5));
    assertFalse(additions.isEmpty());
    assertTrue(additions.size() + 1 <= ManagerStressRows.MAX_TABLETS);

    var nearlyFull = ManagerStressRows.initialSplits(998);
    var finalSplit = ManagerStressRows.additionalSplits(nearlyFull, new Random(11));
    assertEquals(1, finalSplit.size());
    assertTrue(finalSplit.stream().noneMatch(nearlyFull::contains));

    var full = ManagerStressRows.initialSplits(ManagerStressRows.MAX_INITIAL_SPLITS);
    assertTrue(ManagerStressRows.additionalSplits(full, new Random(13)).isEmpty());
  }

  @org.junit.jupiter.api.Test
  void mergeRangesUseTheAdjacentSplitBoundaries() {
    List<Text> splits = List.of(new Text("10"), new Text("20"), new Text("30"), new Text("40"));

    var first = ManagerStressRows.mergeRange(splits, 0, 2);
    assertNull(first.start());
    assertEquals(new Text("20"), first.end());

    var middle = ManagerStressRows.mergeRange(splits, 1, 2);
    assertEquals(new Text("10"), middle.start());
    assertEquals(new Text("30"), middle.end());

    var last = ManagerStressRows.mergeRange(splits, 3, 2);
    assertEquals(new Text("30"), last.start());
    assertNull(last.end());

    assertEquals(2, ManagerStressRows.selectMergeRange(splits, 1, new Random(17)).tabletCount());
    assertEquals(5, ManagerStressRows.selectMergeRange(splits, 100, new Random(19)).tabletCount());
  }

  @org.junit.jupiter.api.Test
  void availabilityRangesMapExactlyToTabletBoundaries() {
    List<Text> splits = List.of(new Text("10"), new Text("20"), new Text("30"), new Text("40"));

    assertEquals(RowRange.atMost(new Text("10")), ManagerStressRows.tabletRowRange(splits, 0, 0));
    assertEquals(RowRange.atMost(new Text("30")), ManagerStressRows.tabletRowRange(splits, 0, 2));
    assertEquals(RowRange.openClosed(new Text("10"), new Text("30")),
        ManagerStressRows.tabletRowRange(splits, 1, 2));
    assertEquals(RowRange.greaterThan(new Text("30")),
        ManagerStressRows.tabletRowRange(splits, 3, 4));
    assertEquals(RowRange.all(), ManagerStressRows.tabletRowRange(splits, 0, 4));
  }

  @org.junit.jupiter.api.Test
  void availabilityChangesSelectUniqueTabletsAndUseDifferentStates() {
    List<Text> splits = List.of(new Text("10"), new Text("20"), new Text("30"), new Text("40"));
    List<TabletAvailability> current =
        List.of(TabletAvailability.ONDEMAND, TabletAvailability.ONDEMAND,
            TabletAvailability.ONDEMAND, TabletAvailability.ONDEMAND, TabletAvailability.ONDEMAND);

    var changes = ManagerStressRows.randomAvailabilityChanges(splits, current, new Random(43));
    int selectedCount =
        changes.stream().mapToInt(ManagerStressRows.AvailabilityChange::tabletCount).sum();

    assertTrue(selectedCount >= 1);
    assertTrue(selectedCount <= current.size());
    for (int i = 0; i < changes.size(); i++) {
      var change = changes.get(i);
      assertTrue(change.availability() != TabletAvailability.ONDEMAND);
      assertEquals(ManagerStressRows.tabletRowRange(splits, change.firstTablet(),
          change.firstTablet() + change.tabletCount() - 1), change.rowRange());
      if (i > 0) {
        var previous = changes.get(i - 1);
        assertTrue(previous.firstTablet() + previous.tabletCount() <= change.firstTablet());
      }
    }
  }
}
