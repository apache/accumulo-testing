package org.apache.accumulo.testing.performance.tests;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.function.BiPredicate;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ProbabilityFilter extends YieldingFilter {

  private double matchProbability;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.matchProbability = Double.parseDouble(options.get("probability"));
  }

  @Override
  protected BiPredicate<Key, Value> createPredicate() {
    Random rand = new Random();
    return (k,v) -> rand.nextDouble() < matchProbability;
  }
}
