package org.apache.accumulo.testing.continuous;

import static org.apache.accumulo.testing.continuous.ContinuousIngest.genCol;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genLong;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genRow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ContinousRecordReader extends RecordReader<BulkKey,Value> {
  protected long numNodes;
  protected long nodeCount;
  protected Random random;

  protected byte[] uuid;

  protected long minRow;
  protected long maxRow;
  protected int maxFam;
  protected int maxQual;
  protected List<ColumnVisibility> visibilities;
  protected boolean checksum;

  protected BulkKey prevKey;
  protected BulkKey currKey;
  protected Value currValue;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext job) {
    numNodes = job.getConfiguration().getLong(ContinousInputOptions.PROP_MAP_NODES, 1000000);
    uuid = job.getConfiguration().get(ContinousInputOptions.PROP_UUID)
        .getBytes(StandardCharsets.UTF_8);

    minRow = job.getConfiguration().getLong(ContinousInputOptions.PROP_ROW_MIN, 0);
    maxRow = job.getConfiguration().getLong(ContinousInputOptions.PROP_ROW_MAX, Long.MAX_VALUE);
    maxFam = job.getConfiguration().getInt(ContinousInputOptions.PROP_FAM_MAX, Short.MAX_VALUE);
    maxQual = job.getConfiguration().getInt(ContinousInputOptions.PROP_QUAL_MAX, Short.MAX_VALUE);
    checksum = job.getConfiguration().getBoolean(ContinousInputOptions.PROP_CHECKSUM, false);
    visibilities = ContinuousIngest
        .parseVisibilities(job.getConfiguration().get(ContinousInputOptions.PROP_VIS));

    random = new Random(new SecureRandom().nextLong());

    nodeCount = 0;
  }

  protected BulkKey genKey(CRC32 cksum) {

    byte[] row = genRow(genLong(minRow, maxRow, random));
    byte[] fam = genCol(random.nextInt(maxFam));
    byte[] qual = genCol(random.nextInt(maxQual));
    byte[] cv = visibilities.get(random.nextInt(visibilities.size())).flatten();

    if (cksum != null) {
      cksum.update(row);
      cksum.update(fam);
      cksum.update(qual);
      cksum.update(cv);
    }

    return new BulkKey(row, fam, qual, cv, Long.MAX_VALUE, false);
  }

  protected byte[] createValue(byte[] ingestInstanceId, byte[] prevRow, Checksum cksum) {
    return ContinuousIngest.createValue(ingestInstanceId, nodeCount, prevRow, cksum);
  }

  @Override
  public boolean nextKeyValue() {
    if (nodeCount < numNodes) {
      CRC32 cksum = checksum ? new CRC32() : null;
      prevKey = currKey;
      byte[] prevRow = prevKey != null ? prevKey.getRowData().toArray() : null;
      currKey = genKey(cksum);
      currValue = new Value(createValue(uuid, prevRow, cksum));

      nodeCount++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public BulkKey getCurrentKey() {
    return currKey;
  }

  @Override
  public Value getCurrentValue() {
    return currValue;
  }

  @Override
  public float getProgress() {
    return nodeCount * 1.0f / numNodes;
  }

  @Override
  public void close() throws IOException {

  }
}
