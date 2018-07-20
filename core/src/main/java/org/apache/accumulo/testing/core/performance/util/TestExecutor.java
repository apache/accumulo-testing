package org.apache.accumulo.testing.core.performance.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class TestExecutor<T> implements Iterable<T>, AutoCloseable {
  private ExecutorService es;
  private List<Future<T>> futures = new ArrayList<>();

  public TestExecutor(int numThreads) {
    es = Executors.newFixedThreadPool(numThreads);
  }

  public void submit(Callable<T> task) {
    futures.add(es.submit(task));
  }

  @Override
  public void close() {
    es.shutdownNow();
  }

  public Stream<T> stream() {
    return futures.stream().map(f -> {try {
      return f.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }});
  }

  @Override
  public Iterator<T> iterator() {
    return stream().iterator();
  }
}
