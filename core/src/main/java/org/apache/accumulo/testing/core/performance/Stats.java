package org.apache.accumulo.testing.core.performance;

public class Stats {
  public final long min;
  public final long max;
  public final long sum;
  public final double average;
  public final long count;

  public Stats(long min, long max, long sum, double average, long count) {
    this.min = min;
    this.max = max;
    this.sum = sum;
    this.average = average;
    this.count = count;
  }
}
