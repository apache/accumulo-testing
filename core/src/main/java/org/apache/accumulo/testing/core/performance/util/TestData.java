package org.apache.accumulo.testing.core.performance.util;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.FastFormat;

public class TestData {

  private static final byte[] EMPTY = new byte[0];

  public static byte[] row(long r) {
    return FastFormat.toZeroPaddedString(r, 16, 16, EMPTY);
  }

  public static byte[] fam(int f) {
    return FastFormat.toZeroPaddedString(f, 8, 16, EMPTY);
  }

  public static byte[] qual(int q) {
    return FastFormat.toZeroPaddedString(q, 8, 16, EMPTY);
  }

  public static byte[] val(long v) {
    return FastFormat.toZeroPaddedString(v, 9, 16, EMPTY);
  }

  public static void generate(Connector conn, String tableName, int rows, int fams, int quals) throws Exception {
    try (BatchWriter writer = conn.createBatchWriter(tableName)) {
      int v = 0;
      for (int r = 0; r < rows; r++) {
        Mutation m = new Mutation(row(r));
        for (int f = 0; f < fams; f++) {
          byte[] fam = fam(f);
          for (int q = 0; q < quals; q++) {
            byte[] qual = qual(q);
            byte[] val = val(v++);
            m.put(fam, qual, val);
          }
        }
        writer.addMutation(m);
      }
    }
  }
}
