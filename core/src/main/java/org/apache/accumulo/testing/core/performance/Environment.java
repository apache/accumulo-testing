package org.apache.accumulo.testing.core.performance;

import org.apache.accumulo.core.client.Connector;

public interface Environment {
  Connector getConnector();
}
