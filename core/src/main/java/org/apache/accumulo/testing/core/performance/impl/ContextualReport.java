package org.apache.accumulo.testing.core.performance.impl;

import java.time.Instant;

import org.apache.accumulo.testing.core.performance.Report;

public class ContextualReport extends Report {

  public final String testClass;
  public final String accumuloVersion;
  public final String startTime;
  public final String finishTime;

  public ContextualReport(String testClass, String accumuloVersion, Instant startTime, Instant finishTime, Report r) {
    super(r.id, r.description, r.results, r.parameters);
    this.testClass = testClass;
    this.accumuloVersion = accumuloVersion;
    this.startTime = startTime.toString();
    this.finishTime = finishTime.toString();

  }
}
