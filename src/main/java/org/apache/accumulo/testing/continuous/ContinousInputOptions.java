package org.apache.accumulo.testing.continuous;

import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.conf.Configuration;

public interface ContinousInputOptions {
  String PROP_MAP_TASK = "mrbulk.map.task";
  String PROP_UUID = "mrbulk.uuid";
  String PROP_MAP_NODES = "mrbulk.map.nodes";
  String PROP_ROW_MIN = "mrbulk.row.min";
  String PROP_ROW_MAX = "mrbulk.row.max";
  String PROP_FAM_MAX = "mrbulk.fam.max";
  String PROP_QUAL_MAX = "mrbulk.qual.max";
  String PROP_CHECKSUM = "mrbulk.checksum";
  String PROP_VIS = "mrbulk.vis";
  String PROP_ASYNC = "mrbulk.async.threads";
  String PROP_ASYNC_KEYS = "mrbulk.async.queued.keys";

  static void configure(Configuration conf, String uuid, ContinuousEnv env) {
    conf.set(ContinousInputOptions.PROP_UUID, uuid);
    conf.setInt(ContinousInputOptions.PROP_MAP_TASK, env.getBulkMapTask());
    conf.setLong(ContinousInputOptions.PROP_MAP_NODES, env.getBulkMapNodes());
    conf.setLong(ContinousInputOptions.PROP_ROW_MIN, env.getRowMin());
    conf.setLong(ContinousInputOptions.PROP_ROW_MAX, env.getRowMax());
    conf.setInt(ContinousInputOptions.PROP_FAM_MAX, env.getMaxColF());
    conf.setInt(ContinousInputOptions.PROP_QUAL_MAX, env.getMaxColQ());
    conf.setBoolean(ContinousInputOptions.PROP_CHECKSUM,
        Boolean.parseBoolean(env.getTestProperty(TestProps.CI_INGEST_CHECKSUM)));
    conf.set(ContinousInputOptions.PROP_VIS, env.getTestProperty(TestProps.CI_INGEST_VISIBILITIES));
    // set the async option to only be used in async mode
    conf.set(ContinousInputOptions.PROP_ASYNC,
        env.getTestProperty(TestProps.CI_BULK_ASYNC_THREADS));
    conf.set(ContinousInputOptions.PROP_ASYNC_KEYS,
        env.getTestProperty(TestProps.CI_BULK_ASYNC_KEYS_QUEUE));
  }
}
