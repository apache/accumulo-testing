package org.apache.accumulo.testing.core.performance;

public class Parameter {

  public final String id;
  public final String data;
  public final String description;

  public Parameter(String id, String data, String description) {
    this.id = id;
    this.data = data;
    this.description = description;
  }
}
