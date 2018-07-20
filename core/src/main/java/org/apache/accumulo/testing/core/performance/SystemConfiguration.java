package org.apache.accumulo.testing.core.performance;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class SystemConfiguration {

  private Map<String,String> accumuloSite = Collections.emptyMap();

  public SystemConfiguration setAccumuloConfig(Map<String,String> props) {
    accumuloSite = ImmutableMap.copyOf(props);
    return this;
  }

  public Map<String,String> getAccumuloSite() {
    return accumuloSite;
  }
}
