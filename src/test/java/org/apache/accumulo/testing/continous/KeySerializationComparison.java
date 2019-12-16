package org.apache.accumulo.testing.continous;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.testing.continuous.ContinuousIngest;
import org.apache.accumulo.testing.continuous.TestKey;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Before;
import org.junit.Test;
import sun.jvm.hotspot.utilities.Assert;

import java.util.List;
import java.util.Random;

public class KeySerializationComparison {

    private static List<ColumnVisibility> visibilities;
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final long MEGABYTE = 10*1024L*1024L;
    Random random;

    @Before
    public void setup(){
        random = new Random(System.currentTimeMillis());
        visibilities = ContinuousIngest
                .parseVisibilities("IMATEST");
    }

    public static byte[] genCol(long cfInt) {
        return FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
    }

    public static long genLong(long min, long max, Random r) {
        return ((r.nextLong() & 0x7fffffffffffffffL) % (max - min)) + min;
    }

    static byte[] genRow(long min, long max, Random r) {
        return genRow(genLong(min, max, r));
    }

    public static byte[] genRow(long rowLong) {
        return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
    }

    /**
     * Avoid reflection by creating a fancy function.
     * @return Test Key
     */
    protected TestKey genTestKey(long rowl, long coll) {

        byte[] row = genRow(rowl);
        byte[] col = genCol(coll);
        return new TestKey(row, col, col, col);
    }

    /**
     * Generate a full key
     * @return Test Key
     */
    protected Key genRegularKey(long rowl, long coll) {

        byte[] row = genRow(rowl);
        byte[] col = genCol(coll);
        return new Key(row, col, col, col);
    }

    /**
     * Generate a full key
     * @return Test Key
     */
    protected BulkKey genBulkKey(long rowl, long coll) {

        byte[] row = genRow(rowl);
        byte[] col = genCol(coll);
        return new BulkKey(row, col, col, col, Long.MAX_VALUE, false);
    }


    @Test
    public void validateComparison()
    {
        TestKey.TestShortCircuitComparator comparator = new TestKey.TestShortCircuitComparator();
        for(int i=0; i < 1000; i++) {
            byte [] a = WritableUtils.toByteArray( genTestKey(i,i) );
            int adder = random.nextInt(1001)+1+i;
            byte [] b = WritableUtils.toByteArray( genTestKey(adder,adder));
            int comparisonResult = comparator.compare(a,0,a.length,b,0,b.length);
            Assert.that( comparisonResult < 0, "Comparision should be negative, not " + comparisonResult);
        }

        for(int i=0; i < 1000; i++) {
            int adder = random.nextInt(1001)+1+i;
            byte [] a = WritableUtils.toByteArray( genTestKey(adder,adder) );
            byte [] b = WritableUtils.toByteArray( genTestKey(i,i));
            int comparisonResult = comparator.compare(a,0,a.length,b,0,b.length);
            Assert.that( comparisonResult > 0, "Comparision should be negative, not " + comparisonResult);

            comparisonResult = comparator.compare(a,0,a.length,a,0,a.length);
            Assert.that( comparisonResult == 0, "Comparision should be negative, not " + comparisonResult);
        }
    }

    @Test
    public void testSmaller()
    {
        long total = 0;
        int counter=0;
        // warm up before we time.
        while(total <= MEGABYTE ) {
            byte [] a = WritableUtils.toByteArray( genTestKey(counter,counter) );
            counter++;
            total += a.length;
        }

        total = counter = 0;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        while(total <= MEGABYTE ) {
            byte [] a = WritableUtils.toByteArray( genTestKey(counter,counter) );
            counter++;
            total += a.length;
        }
        stopWatch.stop();
        long testKeyTime = stopWatch.getTime();
        long testKeysGenerated = counter;

        total = counter = 0;
        stopWatch.reset();
        stopWatch.start();
        while(total <= MEGABYTE ) {
            byte [] a = WritableUtils.toByteArray( genRegularKey(counter,counter) );
            counter++;
            total += a.length;
        }

        stopWatch.stop();
        long bulkKeyTime = stopWatch.getTime();
        long bulkKeysGenerated = counter;

        total = counter = 0;
        stopWatch.reset();
        stopWatch.start();
        while(total <= MEGABYTE ) {
            byte [] a = WritableUtils.toByteArray( genBulkKey(counter,counter) );
            counter++;
            total += a.length;
        }
        stopWatch.stop();
        long regularKeyTime = stopWatch.getTime();
        long regularKeysGenerated = counter;

        Assert.that(testKeysGenerated > regularKeysGenerated &&
                testKeysGenerated > bulkKeysGenerated, "It is expected that test keys generate more keys in the same space " + testKeysGenerated + " " + regularKeysGenerated + " " + bulkKeysGenerated);

        Assert.that(testKeyTime <= regularKeyTime &&
                testKeyTime <= (bulkKeyTime+(bulkKeyTime*.10)), "It is expected that test keys generate more keys in the same space " + testKeyTime + " " + regularKeyTime + " " + bulkKeyTime);

    }


}
