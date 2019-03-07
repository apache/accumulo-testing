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

package org.apache.accumulo.testing.performance.impl;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.testing.performance.Result;

import com.google.common.collect.Iterables;

public class Csv {

  private static class RowId implements Comparable<RowId> {

    final Instant startTime;
    final String accumuloVersion;

    public RowId(Instant startTime, String accumuloVersion) {
      this.startTime = startTime;
      this.accumuloVersion = accumuloVersion;
    }

    @Override
    public int compareTo(RowId o) {
      int cmp = startTime.compareTo(o.startTime);
      if (cmp == 0) {
        cmp = accumuloVersion.compareTo(o.accumuloVersion);
      }
      return cmp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(startTime, accumuloVersion);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof RowId) {
        RowId orid = (RowId) o;
        return startTime.equals(orid.startTime) && accumuloVersion.equals(orid.accumuloVersion);
      }

      return false;
    }

    @Override
    public String toString() {
      return startTime + " " + accumuloVersion;
    }

  }

  public static void main(String[] args) throws Exception {
    Map<RowId,Map<String,Double>> rows = new TreeMap<>();

    for (String file : args) {
      Collection<ContextualReport> reports = Compare.readReports(file);

      Instant minStart = reports.stream().map(cr -> cr.startTime).map(Instant::parse)
          .min(Instant::compareTo).get();

      String version = Iterables
          .getOnlyElement(reports.stream().map(cr -> cr.accumuloVersion).collect(toSet()));

      Map<String,Double> row = new HashMap<>();

      for (ContextualReport report : reports) {

        String id = report.id != null ? report.id
            : report.testClass.substring(report.testClass.lastIndexOf('.') + 1);

        for (Result result : report.results) {
          if (result.purpose == Result.Purpose.COMPARISON) {
            row.put(id + "." + result.id, result.data.doubleValue());
          }
        }
      }

      rows.put(new RowId(minStart, version), row);
    }

    List<String> allCols = rows.values().stream().flatMap(row -> row.keySet().stream()).distinct()
        .sorted().collect(toList());

    // print header
    print(
        Stream.concat(Stream.of("Start Time", "Version"), allCols.stream()).collect(joining(",")));

    rows.forEach((id, row) -> {
      StringJoiner joiner = new StringJoiner(",");
      joiner.add(id.startTime.toString());
      joiner.add(id.accumuloVersion);
      for (String col : allCols) {
        joiner.add(row.getOrDefault(col, Double.valueOf(0)).toString());
      }

      print(joiner.toString());
    });

  }

  private static void print(String s) {
    System.out.println(s);
  }
}
