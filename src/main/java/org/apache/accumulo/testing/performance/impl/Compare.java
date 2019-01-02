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

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.testing.performance.Result;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;

public class Compare {

  private static class TestId {

    final String testClass;
    final String id;

    public TestId(String testClass, String id) {
      this.testClass = testClass;
      this.id = id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(testClass, id);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;

      if (obj instanceof TestId) {
        TestId other = (TestId) obj;

        return id.equals(other.id) && testClass.equals(other.testClass);
      }

      return false;
    }
  }

  public static void main(String[] args) throws Exception {
    Map<TestId,Double> oldResults = flatten(readReports(args[0]));
    Map<TestId,Double> newResults = flatten(readReports(args[1]));

    for (TestId testId : Sets.union(oldResults.keySet(), newResults.keySet())) {
      Double oldResult = oldResults.get(testId);
      Double newResult = newResults.get(testId);

      if (oldResult == null || newResult == null) {
        System.out.printf("%s %s %.2f %.2f\n", testId.testClass, testId.id, oldResult, newResult);
      } else {
        double change = (newResult - oldResult) / oldResult;
        System.out.printf("%s %s %.2f %.2f %.2f%s\n", testId.testClass, testId.id, oldResult,
            newResult, change * 100, "%");
      }
    }
  }

  static Collection<ContextualReport> readReports(String file) throws Exception {
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(file))) {
      Gson gson = new GsonBuilder().create();
      JsonStreamParser p = new JsonStreamParser(reader);
      List<ContextualReport> rl = new ArrayList<>();

      while (p.hasNext()) {
        JsonElement e = p.next();
        ContextualReport results = gson.fromJson(e, ContextualReport.class);
        rl.add(results);
      }

      return rl;
    }
  }

  private static Map<TestId,Double> flatten(Collection<ContextualReport> results) {
    HashMap<TestId,Double> flattened = new HashMap<>();

    for (ContextualReport cr : results) {
      for (Result r : cr.results) {
        if (r.purpose == Result.Purpose.COMPARISON) {
          flattened.put(new TestId(cr.testClass, r.id), r.data.doubleValue());
        }
      }
    }

    return flattened;
  }
}
