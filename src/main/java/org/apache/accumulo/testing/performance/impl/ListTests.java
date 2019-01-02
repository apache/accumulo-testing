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

import org.apache.accumulo.testing.performance.PerformanceTest;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

public class ListTests {
  public static void main(String[] args) throws Exception {

    ImmutableSet<ClassInfo> classes = ClassPath.from(ListTests.class.getClassLoader())
        .getTopLevelClasses();

    for (ClassInfo classInfo : classes) {
      if (classInfo.getName().endsWith("PT")
          && PerformanceTest.class.isAssignableFrom(classInfo.load())) {
        System.out.println(classInfo.getName());
      }
    }
  }
}
