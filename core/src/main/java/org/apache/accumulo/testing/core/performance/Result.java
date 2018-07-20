package org.apache.accumulo.testing.core.performance;

public class Result {

  public final String id;
  public final Number data;
  public final Stats stats;
  public final String description;
  public final Purpose purpose;

  public enum Purpose {
    INFORMATIONAL, COMPARISON
  }

  public Result(String id, Number data, String description, Purpose purpose) {
    this.id = id;
    this.data = data;
    this.stats = null;
    this.description = description;
    this.purpose = purpose;
  }

  public Result(String id, Stats stats, String description, Purpose purpose) {
    this.id = id;
    this.stats = stats;
    this.data = null;
    this.description = description;
    this.purpose = purpose;
  }
}
