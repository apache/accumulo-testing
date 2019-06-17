package org.apache.accumulo.testing.gcs;

import org.apache.accumulo.testing.TestEnv;

public class GcsEnv extends TestEnv {

  public GcsEnv(String[] args) {
    super(args);
  }

  public String getTableName() {
    return testProps.getProperty("test.gcs.table", "gcs");
  }

  public int getMaxBuckets() {
    return Integer.parseInt(testProps.getProperty("test.gcs.maxBuckets", "100000"));
  }

  public int getInitialTablets() {
    return Integer.parseInt(testProps.getProperty("test.gcs.tablets", "10"));
  }

  public int getMaxWork() {
    return Integer.parseInt(testProps.getProperty("test.gcs.maxWork", "100000000"));
  }

  public int getMaxActiveWork() {
    return Integer.parseInt(testProps.getProperty("test.gcs.maxActiveWork", "10000"));
  }

  public int getBatchSize() {
    return Integer.parseInt(testProps.getProperty("test.gcs.batchSize", "100000"));
  }
}
