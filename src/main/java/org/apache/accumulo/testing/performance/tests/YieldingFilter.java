/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.performance.tests;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiPredicate;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;

public abstract class YieldingFilter implements SortedKeyValueIterator<Key,Value> {

  private SortedKeyValueIterator<Key,Value> source;
  private BiPredicate<Key,Value> predicate;
  private YieldCallback<Key> yield;
  private long yieldTime;
  private long start;

  protected abstract BiPredicate<Key,Value> createPredicate(Map<String,String> options);

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.predicate = createPredicate(options);
    this.yieldTime = Long.parseLong(options.getOrDefault("yieldTimeMS", "100"));
    start = System.nanoTime();
  }

  protected void findTop() throws IOException {
    while (source.hasTop() && !source.getTopKey().isDeleted()
        && !predicate.test(source.getTopKey(), source.getTopValue())) {
      long duration = (System.nanoTime() - start) / 1000000;
      if (duration > yieldTime) {
        yield.yield(source.getTopKey());
        start = System.nanoTime();
        break;
      }

      source.next();
    }
  }

  @Override
  public boolean hasTop() {
    return !yield.hasYielded() && source.hasTop();
  }

  @Override
  public void next() throws IOException {
    source.next();
    findTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    findTop();
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void enableYielding(YieldCallback<Key> callback) {
    this.yield = callback;
  }
}
