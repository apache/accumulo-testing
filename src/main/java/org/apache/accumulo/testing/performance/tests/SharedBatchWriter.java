package org.apache.accumulo.testing.performance.tests;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

public class SharedBatchWriter implements BatchWriter {

  private BatchWriter bw;
  private ArrayBlockingQueue<Mutation> mQueue = new ArrayBlockingQueue<Mutation>(1000);
  private ArrayBlockingQueue<CountDownLatch> fQueue = new ArrayBlockingQueue<CountDownLatch>(1000);

  private class FlushTask implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          ArrayList<CountDownLatch> latches = new ArrayList<CountDownLatch>();
          latches.add(fQueue.take());
          /*
           * CountDownLatch cdl = fQueue.poll(1, TimeUnit.MILLISECONDS); if (cdl != null)
           * latches.add(cdl);
           */
          fQueue.drainTo(latches);

          ArrayList<Mutation> buffer = new ArrayList<Mutation>();
          mQueue.drainTo(buffer);
          bw.addMutations(buffer);
          bw.flush();

          for (CountDownLatch latch : latches) {
            latch.countDown();
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
      }

    }

  }

  public SharedBatchWriter(BatchWriter bw) {
    this.bw = bw;
    Thread thread = new Thread(new FlushTask());
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void addMutation(Mutation m) throws MutationsRejectedException {
    mQueue.add(m);
  }

  @Override
  public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() throws MutationsRejectedException {
    try {
      CountDownLatch cdl = new CountDownLatch(1);
      fQueue.put(cdl);
      cdl.await();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  public void close() throws MutationsRejectedException {
    flush();
  }

}
