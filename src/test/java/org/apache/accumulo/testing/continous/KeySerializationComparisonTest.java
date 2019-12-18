package org.apache.accumulo.testing.continous;

import static org.apache.accumulo.testing.continuous.ContinuousIngest.genCol;
import static org.apache.accumulo.testing.continuous.ContinuousIngest.genRow;

import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.testing.continuous.ContinuousIngest;
import org.apache.accumulo.testing.continuous.TestKey;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KeySerializationComparisonTest {

  private static List<ColumnVisibility> visibilities;
  private static final long MEGABYTE = 50 * 1024L * 1024L;
  Random random;

  @Before
  public void setup() {
    random = new Random(System.currentTimeMillis());
    visibilities = ContinuousIngest.parseVisibilities("IMATEST");
  }

  /**
   * Avoid reflection by creating a fancy function.
   * 
   * @return Test Key
   */
  protected TestKey genTestKey(long rowl, int coll) {

    byte[] row = genRow(rowl);
    byte[] col = genCol(coll);
    return new TestKey(row, col, col, col);
  }

  /**
   * Generate a full key
   * 
   * @return Test Key
   */
  protected Key genRegularKey(long rowl, int coll) {

    byte[] row = genRow(rowl);
    byte[] col = genCol(coll);
    return new Key(row, col, col, col);
  }

  @Test
  public void validateComparison() {
    TestKey.TestShortCircuitComparator comparator = new TestKey.TestShortCircuitComparator();
    for (int i = 0; i < 1000; i++) {
      byte[] a = WritableUtils.toByteArray(genTestKey(i, i));
      int adder = random.nextInt(1001) + 1 + i;
      byte[] b = WritableUtils.toByteArray(genTestKey(adder, adder));
      byte[] c = WritableUtils.toByteArray(genTestKey(i, adder));
      int comparisonResult = comparator.compare(a, 0, a.length, b, 0, b.length);
      Assert.assertTrue("Comparision should be negative, not " + comparisonResult,
          comparisonResult < 0);
      // in this case c has a larger cf
      comparisonResult = comparator.compare(a, 0, a.length, c, 0, c.length);
      Assert.assertTrue("Comparision should be negative, not " + comparisonResult,
          comparisonResult < 0);
    }

    for (int i = 0; i < 1000; i++) {
      int adder = random.nextInt(1001) + 1 + i;
      byte[] a = WritableUtils.toByteArray(genTestKey(adder, adder));
      byte[] b = WritableUtils.toByteArray(genTestKey(i, i));
      byte[] c = WritableUtils.toByteArray(genTestKey(i, adder));
      int comparisonResult = comparator.compare(a, 0, a.length, b, 0, b.length);
      Assert.assertTrue("Comparision should be positive, not " + comparisonResult,
          comparisonResult > 0);

      comparisonResult = comparator.compare(a, 0, a.length, a, 0, a.length);
      Assert.assertTrue("Comparision should be negative, not " + comparisonResult,
          comparisonResult == 0);

      // in this case a has a larger row so we should be
      comparisonResult = comparator.compare(a, 0, a.length, c, 0, c.length);
      Assert.assertTrue("Comparision should be positive, not " + comparisonResult,
          comparisonResult > 0);
    }

  }

  /**
   * Timing in unit tests is generally ill advised but we will use a slop factor in the Bulkkey vs
   * TestKey case
   */
  @Test
  public void testSmallerAndFaster() {
    long total = 0;
    int counter = 0;
    // warm up before we time.
    while (total <= MEGABYTE) {
      byte[] a = WritableUtils.toByteArray(genTestKey(counter, counter));
      counter++;
      total += a.length;
    }

    total = counter = 0;
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    while (total <= MEGABYTE) {
      byte[] a = WritableUtils.toByteArray(genTestKey(counter, counter));
      counter++;
      total += a.length;
    }
    stopWatch.stop();
    long testKeyTime = stopWatch.getTime();
    long testKeysGenerated = counter;

    total = counter = 0;
    stopWatch.reset();
    stopWatch.start();
    while (total <= MEGABYTE) {
      byte[] a = WritableUtils.toByteArray(genRegularKey(counter, counter));
      counter++;
      total += a.length;
    }
    stopWatch.stop();
    long regularKeyTime = stopWatch.getTime();
    long regularKeysGenerated = counter;

    Assert.assertTrue("It is expected that test keys generate more keys in the same space ",
        testKeysGenerated > regularKeysGenerated);

    // may not always be faster than bulk key but we should be within 10%
    Assert.assertTrue("It is expected that test keys generate more keys in the same space "
        + testKeyTime + " " + regularKeyTime, testKeyTime <= regularKeyTime);

  }

}
