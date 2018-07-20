package org.apache.accumulo.testing.core.performance;

public interface PerformanceTest {

  SystemConfiguration getConfiguration();

  Report runTest(Environment env) throws Exception;
}
