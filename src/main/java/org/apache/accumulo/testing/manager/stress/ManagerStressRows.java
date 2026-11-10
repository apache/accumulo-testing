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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.RowRange;
import org.apache.hadoop.io.Text;

/** Row keys and tablet ranges used by the Manager stress workload. */
final class ManagerStressRows {

  static final int MAX_TABLETS = 1000;
  static final int MIN_INITIAL_SPLITS = 10;
  static final int MAX_INITIAL_SPLITS = MAX_TABLETS - 1;

  private static final BigInteger ROW_SPACE = BigInteger.ONE.shiftLeft(64);

  record MergeRange(Text start, Text end, int tabletCount) {
  }

  record AvailabilityChange(int firstTablet, int tabletCount, TabletAvailability availability,
      RowRange rowRange) {
  }

  private ManagerStressRows() {}

  static NewTableConfiguration newTableConfiguration(Random random) {
    int splitRange = MAX_INITIAL_SPLITS - MIN_INITIAL_SPLITS + 1;
    int splitCount = MIN_INITIAL_SPLITS + random.nextInt(splitRange);
    return new NewTableConfiguration().withInitialTabletAvailability(TabletAvailability.UNHOSTED)
        .withSplits(initialSplits(splitCount));
  }

  static SortedSet<Text> initialSplits(int splitCount) {
    if (splitCount < MIN_INITIAL_SPLITS || splitCount > MAX_INITIAL_SPLITS) {
      throw new IllegalArgumentException("Initial split count must be between " + MIN_INITIAL_SPLITS
          + " and " + MAX_INITIAL_SPLITS);
    }
    TreeSet<Text> splits = new TreeSet<>();
    BigInteger denominator = BigInteger.valueOf((long) splitCount + 1);
    for (int i = 1; i <= splitCount; i++) {
      BigInteger row = ROW_SPACE.multiply(BigInteger.valueOf(i)).divide(denominator);
      splits.add(row(row));
    }
    return splits;
  }

  static Text randomRow(Random random) {
    String hex = Long.toUnsignedString(random.nextLong(), 16);
    return new Text("0".repeat(16 - hex.length()) + hex);
  }

  static SortedSet<Text> additionalSplits(Collection<Text> currentSplits, Random random) {
    TreeSet<Text> existing = new TreeSet<>(currentSplits);
    int currentTablets = existing.size() + 1;
    int available = MAX_TABLETS - currentTablets;
    TreeSet<Text> additions = new TreeSet<>();
    if (available <= 0) {
      return additions;
    }

    int requested = 1 + random.nextInt(available);
    while (additions.size() < requested) {
      Text candidate = randomRow(random);
      if (!existing.contains(candidate)) {
        additions.add(candidate);
      }
    }
    return additions;
  }

  static MergeRange selectMergeRange(Collection<Text> currentSplits, int targetTabletCount,
      Random random) {
    List<Text> splits = new ArrayList<>(new TreeSet<>(currentSplits));
    int totalTablets = splits.size() + 1;
    if (totalTablets < 2) {
      return null;
    }
    int mergeTablets = Math.max(2, Math.min(Math.min(targetTabletCount, 100), totalTablets));
    int firstTablet = random.nextInt(totalTablets - mergeTablets + 1);
    return mergeRange(splits, firstTablet, mergeTablets);
  }

  static MergeRange mergeRange(Collection<Text> currentSplits, int firstTablet, int tabletCount) {
    return mergeRange(new ArrayList<>(new TreeSet<>(currentSplits)), firstTablet, tabletCount);
  }

  static MergeRange mergeRange(List<Text> sortedSplits, int firstTablet, int tabletCount) {
    int totalTablets = sortedSplits.size() + 1;
    if (tabletCount < 2 || firstTablet < 0 || firstTablet + tabletCount > totalTablets) {
      throw new IllegalArgumentException("Invalid tablet range");
    }
    Text start = firstTablet == 0 ? null : sortedSplits.get(firstTablet - 1);
    int lastTablet = firstTablet + tabletCount - 1;
    Text end = lastTablet == sortedSplits.size() ? null : sortedSplits.get(lastTablet);
    return new MergeRange(start, end, tabletCount);
  }

  static List<AvailabilityChange> randomAvailabilityChanges(List<Text> sortedSplits,
      List<TabletAvailability> currentAvailability, Random random) {
    int totalTablets = sortedSplits.size() + 1;
    if (currentAvailability.size() != totalTablets) {
      throw new IllegalArgumentException("Availability must be supplied for every tablet");
    }

    List<Integer> tabletIndices = new ArrayList<>(totalTablets);
    for (int i = 0; i < totalTablets; i++) {
      tabletIndices.add(i);
    }
    Collections.shuffle(tabletIndices, random);
    int selectedCount = 1 + random.nextInt(totalTablets);
    tabletIndices = new ArrayList<>(tabletIndices.subList(0, selectedCount));
    tabletIndices.sort(Integer::compareTo);

    List<AvailabilityChange> changes = new ArrayList<>();
    int runStart = tabletIndices.get(0);
    int runEnd = runStart;
    TabletAvailability runAvailability =
        chooseDifferentAvailability(currentAvailability.get(runStart), random);
    for (int i = 1; i < tabletIndices.size(); i++) {
      int tablet = tabletIndices.get(i);
      TabletAvailability nextAvailability =
          chooseDifferentAvailability(currentAvailability.get(tablet), random);
      if (tablet == runEnd + 1 && nextAvailability == runAvailability) {
        runEnd = tablet;
      } else {
        changes.add(availabilityChange(sortedSplits, runStart, runEnd, runAvailability));
        runStart = tablet;
        runEnd = tablet;
        runAvailability = nextAvailability;
      }
    }
    changes.add(availabilityChange(sortedSplits, runStart, runEnd, runAvailability));
    return List.copyOf(changes);
  }

  static RowRange tabletRowRange(List<Text> sortedSplits, int firstTablet, int lastTablet) {
    int totalTablets = sortedSplits.size() + 1;
    if (firstTablet < 0 || lastTablet < firstTablet || lastTablet >= totalTablets) {
      throw new IllegalArgumentException("Invalid tablet range");
    }
    if (firstTablet == 0 && lastTablet == totalTablets - 1) {
      return RowRange.all();
    }
    if (lastTablet == totalTablets - 1) {
      return RowRange.greaterThan(sortedSplits.get(firstTablet - 1));
    }
    if (firstTablet == 0) {
      return RowRange.atMost(sortedSplits.get(lastTablet));
    }
    return RowRange.openClosed(sortedSplits.get(firstTablet - 1), sortedSplits.get(lastTablet));
  }

  private static AvailabilityChange availabilityChange(List<Text> sortedSplits, int firstTablet,
      int lastTablet, TabletAvailability availability) {
    return new AvailabilityChange(firstTablet, lastTablet - firstTablet + 1, availability,
        tabletRowRange(sortedSplits, firstTablet, lastTablet));
  }

  private static TabletAvailability chooseDifferentAvailability(TabletAvailability current,
      Random random) {
    List<TabletAvailability> candidates = new ArrayList<>();
    for (TabletAvailability availability : TabletAvailability.values()) {
      if (availability != current) {
        candidates.add(availability);
      }
    }
    return candidates.get(random.nextInt(candidates.size()));
  }

  private static Text row(BigInteger value) {
    String hex = value.toString(16);
    return new Text("0".repeat(16 - hex.length()) + hex);
  }
}
