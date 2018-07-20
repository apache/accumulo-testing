package org.apache.accumulo.testing.core.performance.impl;

import org.apache.accumulo.testing.core.performance.PerformanceTest;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

public class ListTests {
  public static void main(String[] args) throws Exception {

    ImmutableSet<ClassInfo> classes = ClassPath.from(ListTests.class.getClassLoader()).getTopLevelClasses();

    for (ClassInfo classInfo : classes) {
      if (classInfo.getName().endsWith("PT") && PerformanceTest.class.isAssignableFrom(classInfo.load())) {
        System.out.println(classInfo.getName());
      }
    }
  }
}
