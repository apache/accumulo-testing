package org.apache.accumulo.testing.continuous;

import com.google.common.collect.Maps;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

public class AsyncRecordReader extends ContinousRecordReader {

  int threads = 0;
  int queuedKeys = 100;

  ExecutorService service = null;

  BlockingQueue<Map.Entry<TestKey,Value>> queue;

  final LongAdder generatedCount = new LongAdder();

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext job) {
    super.initialize(inputSplit, job);

    threads = job.getConfiguration().getInt(ContinousInputOptions.PROP_ASYNC, 5);
    queuedKeys = job.getConfiguration().getInt(ContinousInputOptions.PROP_ASYNC_KEYS, 1000);
    queue = new ArrayBlockingQueue<>(queuedKeys);

    service = Executors.newFixedThreadPool(threads);
  }

  @Override
  public boolean nextKeyValue() {
    currKey = null;
    if (nodeCount < numNodes) {
      if (!running.get()) {
        running.set(true);
        // start up lazily
        IntStream.range(0, threads).forEach(x -> {
          Runnable generator = () -> {
            CRC32 cksum = checksum ? new CRC32() : null;
            while (generatedCount.longValue() < numNodes && running.get()) {
              try {
                final TestKey key = genKey(cksum);
                final Value value = new Value(createValue(uuid, key.getRowData().toArray(), cksum));
                while (!queue.offer(Maps.immutableEntry(key, value), 1, TimeUnit.SECONDS)) {
                  if (!running.get())
                    return;
                }
                generatedCount.add(1);
              } catch (Throwable e) {
                break;
              }
            }
          };
          service.submit(generator);
        });
      }
      while (null == currKey) {
        Map.Entry<TestKey,Value> kv = null;
        try {
          kv = queue.poll(1, TimeUnit.SECONDS);
          if (null != kv) {
            currKey = kv.getKey();
            currValue = kv.getValue();
            nodeCount++;
            break;
          }
        } catch (InterruptedException e) {
          // spurious interruptions can occur
        }
      }
      return true;
    } else {
      return false;
    }
  }
}
