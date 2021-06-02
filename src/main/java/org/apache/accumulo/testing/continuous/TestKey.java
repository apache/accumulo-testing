package org.apache.accumulo.testing.continuous;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Supports immediate sorting via eager deser of the key object. This has the benefit of reducing
 * the amount of deserialization that may occur when sorting keys in memory
 */
public class TestKey implements WritableComparable<TestKey> {

  protected Key key = new Key();
  protected int hashCode = 31;

  static final byte[] EMPTY = {};

  protected Text row = new Text();
  protected Text cf = new Text();
  protected Text cq = new Text();
  protected Text cv = new Text();

  public TestKey() {}

  public TestKey(byte[] row, byte[] cf, byte[] cq, byte[] cv) {
    // don't copy the arrays
    this.key = new Key(row, cf, cq, cv);
    hashCode = key.hashCode();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  public Key getKey() {
    return key;
  }

  public ByteSequence getRowData() {
    return key.getRowData();
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    final int rowsize = WritableUtils.readVInt(in);
    final byte[] row = readBytes(in, rowsize);

    final int cfsize = WritableUtils.readVInt(in);
    final byte[] cf = readBytes(in, cfsize);

    final int cqsize = WritableUtils.readVInt(in);
    final byte[] cq = readBytes(in, cqsize);

    final int cvsize = WritableUtils.readVInt(in);
    final byte[] cv = readBytes(in, cvsize);

    key = new Key(row, cf, cq, cv);

    hashCode = key.hashCode();
  }

  protected static byte[] readBytes(DataInput in, int size) throws IOException {
    if (size == 0)
      return EMPTY;
    final byte[] data = new byte[size];
    in.readFully(data, 0, data.length);
    return data;
  }

  @Override
  public void write(DataOutput out) throws IOException {

    key.getRow(row);
    key.getColumnFamily(cf);
    key.getColumnQualifier(cq);
    key.getColumnVisibility(cv);

    /**
     * writeVInt for sizes < 128 ends up writing a single byte which reduces the pass through size
     * for map reduce jobs.
     */

    WritableUtils.writeVInt(out, row.getLength());
    out.write(row.getBytes(), 0, row.getLength());

    WritableUtils.writeVInt(out, cf.getLength());
    out.write(cf.getBytes(), 0, cf.getLength());

    WritableUtils.writeVInt(out, cq.getLength());
    out.write(cq.getBytes(), 0, cq.getLength());

    WritableUtils.writeVInt(out, cv.getLength());
    out.write(cv.getBytes(), 0, cv.getLength());
  }

  @Override
  public int compareTo(TestKey other) {
    return key.compareTo(other.key);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TestKey other = (TestKey) obj;
    return compareTo(other) == 0;
  }

  /** A WritableComparator optimized for TestKey objects. */
  public static class TestShortCircuitComparator extends WritableComparator {
    public TestShortCircuitComparator() {
      super(TestKey.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int o1 = s1;
      int o2 = s2;
      int[] startAndLen = {0, 0};
      // perform the comparisons in order
      for (int i = 0; i < 4; i++) {
        startAndLen[0] = o1;
        // get Text's length in bytes
        int tl1 = readVInt(b1, startAndLen);
        o1 += startAndLen[1];
        startAndLen[0] = o2;
        int tl2 = readVInt(b2, startAndLen);
        o2 += startAndLen[1];

        int result = compareBytes(b1, o1, tl1, b2, o2, tl2);
        if (result != 0) {
          return result;
        }
        o1 += tl1;
        o2 += tl2;
      }
      return 0;
    }

    /**
     * Reads a Variable Long from a byte[]. Also returns the variable int size in the second
     * position (index 1) of the startAndLen array. This allows the caller to have access to the
     * VInt size without having to call decode again.
     *
     * @param bytes
     *          payload containing variable long
     * @param startAndLen
     *          index 0 holds the offset into the byte array and position 1 is populated with the
     *          length of the bytes
     * @return the value
     */
    public static int readVInt(byte[] bytes, int[] startAndLen) {
      byte firstByte = bytes[startAndLen[0]];
      startAndLen[1] = WritableUtils.decodeVIntSize(firstByte);
      if (startAndLen[1] == 1) {
        return firstByte;
      }
      long i = 0;
      for (int idx = 0; idx < startAndLen[1] - 1; idx++) {
        byte b = bytes[startAndLen[0] + 1 + idx];
        i = i << 8;
        i = i | (b & 0xFF);
      }
      return (int) (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }

  }

  static {
    // register this comparator
    WritableComparator.define(TestKey.class, new TestShortCircuitComparator());
  }

}
