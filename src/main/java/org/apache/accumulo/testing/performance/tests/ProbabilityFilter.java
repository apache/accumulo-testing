package org.apache.accumulo.testing.performance.tests;

import java.util.Map;
import java.util.Random;
import java.util.function.BiPredicate;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class ProbabilityFilter extends YieldingFilter {
  @Override
  protected BiPredicate<Key,Value> createPredicate(Map<String,String> options) {
    double matchProbability = Double.parseDouble(options.get("probability"));
    Random rand = new Random();
    return (k, v) -> rand.nextDouble() < matchProbability;
  }
}
