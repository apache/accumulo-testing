package org.apache.accumulo.testing.continuous;

import org.apache.accumulo.hadoop.mapreduce.partition.RangePartitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Defines a partitioner for BulkKey
 */
public class BulkKeyPartitioner extends Partitioner<BulkKey,Writable> implements Configurable {
  private RangePartitioner rp = new RangePartitioner();

  public BulkKeyPartitioner() {}

  public int getPartition(BulkKey key, Writable value, int numPartitions) {
    return this.rp.getPartition(key.getKey().getRow(), value, numPartitions);
  }

  public Configuration getConf() {
    return this.rp.getConf();
  }

  public void setConf(Configuration conf) {
    this.rp.setConf(conf);
  }

  public static void setSplitFile(Job job, String file) {
    RangePartitioner.setSplitFile(job, file);
  }

  public static void setNumSubBins(Job job, int num) {
    RangePartitioner.setNumSubBins(job, num);
  }
}
