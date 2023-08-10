/**
 * HistogramTest.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.std.histogram.org.HdrHistogram.ConcurrentHistogram;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.zip.Deflater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * JUnit test for Histogram
 */

public class DoubleHistogramTest {
    static final long trackableValueRangeSize = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units
    static final int numberOfSignificantValueDigits = 3;
    // static final long testValueLevel = 12340;
    static final double testValueLevel = 4.0;

    public DoubleHistogramTest() {
    }

    @Test
    public void testTrackableValueRangeMustBeGreaterThanTwo() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 1, numberOfSignificantValueDigits));
    }
    @Test
    public void testNumberOfSignificantValueDigitsMustBeLessThanSix() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> {
                    DoubleHistogram histogram =
                            HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, 6);
                });
    }

    @Test
    public void testNumberOfSignificantValueDigitsMustBePositive() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> {
                    DoubleHistogram histogram =
                            HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, -1);
                });
    }

    @Test
    public void testConstructionArgumentGets() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record 1.0, and verify that the range adjust to it:
        histogram.recordValue(Math.pow(2.0, 20));
        histogram.recordValue(1.0);
        assertEquals(1.0, histogram.getCurrentLowestTrackableNonZeroValue(), 0.001);
        assertEquals(trackableValueRangeSize, histogram.getHighestToLowestValueRatio());
        assertEquals(numberOfSignificantValueDigits, histogram.getNumberOfSignificantValueDigits());

        DoubleHistogram histogram2 =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record a larger value, and verify that the range adjust to it too:
        histogram2.recordValue(2048.0 * 1024.0 * 1024.0);
        assertEquals(2048.0 * 1024.0 * 1024.0, histogram2.getCurrentLowestTrackableNonZeroValue(), 0.001);

        DoubleHistogram histogram3 =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record a value that is 1000x outside of the initially set range, which should scale us by 1/1024x:
        histogram3.recordValue(1/1000.0);
        assertEquals(1.0/1024, histogram3.getCurrentLowestTrackableNonZeroValue(), 0.001);
    }

    @Test
    public void testDataRange() {
        // A trackableValueRangeSize histigram
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0.0);  // Include a zero value to make sure things are handled right.
        assertEquals(1L, histogram.getCountAtValue(0.0));

        double topValue = 1.0;
        try {
            while (true) {
                histogram.recordValue(topValue);
                topValue *= 2.0;
            }
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
        assertEquals(1L << 33, topValue, 0.00001);
        assertEquals(1L, histogram.getCountAtValue(0.0));

        histogram = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0.0); // Include a zero value to make sure things are handled right.

        double bottomValue = 1L << 33;
        try {
            while (true) {
                histogram.recordValue(bottomValue);
                bottomValue /= 2.0;
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            System.out.println("Bottom value at exception point = " + bottomValue);
        }
        assertEquals(1.0, bottomValue, 0.00001);

        long expectedRange = 1L << (findContainingBinaryOrderOfMagnitude(trackableValueRangeSize) + 1);
        assertEquals(expectedRange, (topValue / bottomValue), 0.00001);
        assertEquals(1L, histogram.getCountAtValue(0.0));
    }

    @Test
    public void testRecordValue() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        assertEquals(1L, histogram.getCountAtValue(testValueLevel));
        assertEquals(1L, histogram.getTotalCount());
    }

    @Test
    public void testRecordValue_Overflow_ShouldThrowException() {
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class,
                () -> {
                    DoubleHistogram histogram =
                            HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
                    histogram.recordValue(trackableValueRangeSize * 3);
                    histogram.recordValue(1.0);
                });
    }

    @Test
    public void testRecordValueWithExpectedInterval() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0);
        histogram.recordValueWithExpectedInterval(testValueLevel, testValueLevel/4);
        DoubleHistogram rawHistogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        rawHistogram.recordValue(0);
        rawHistogram.recordValue(testValueLevel);
        // The raw data will not include corrected samples:
        assertEquals(1L, rawHistogram.getCountAtValue(0));
        assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 1 )/4));
        assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 2 )/4));
        assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 3 )/4));
        assertEquals(1L, rawHistogram.getCountAtValue((testValueLevel * 4 )/4));
        assertEquals(2L, rawHistogram.getTotalCount());
        // The data will include corrected samples:
        assertEquals(1L, histogram.getCountAtValue(0));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 1 )/4));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 2 )/4));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 3 )/4));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 4 )/4));
        assertEquals(5L, histogram.getTotalCount());
    }

    @Test
    public void testReset() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(10);
        histogram.recordValue(100);
        assertEquals(histogram.getMinValue(), Math.min(10.0, testValueLevel), 1.0);
        assertEquals(histogram.getMaxValue(), Math.max(100.0, testValueLevel), 1.0);
        histogram.reset();
        assertEquals(0L, histogram.getCountAtValue(testValueLevel));
        assertEquals(0L, histogram.getTotalCount());
        histogram.recordValue(20);
        histogram.recordValue(80);
        assertEquals(histogram.getMinValue(), 20.0, 1.0);
        assertEquals(histogram.getMaxValue(), 80.0, 1.0);
    }

    @Test
    public void testAdd() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        DoubleHistogram other =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);

        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 1000);
        other.recordValue(testValueLevel);
        other.recordValue(testValueLevel * 1000);
        histogram.add(other);
        assertEquals(2L, histogram.getCountAtValue(testValueLevel));
        assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
        assertEquals(4L, histogram.getTotalCount());

        DoubleHistogram biggerOther =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize * 2, numberOfSignificantValueDigits);
        biggerOther.recordValue(testValueLevel);
        biggerOther.recordValue(testValueLevel * 1000);

        // Adding the smaller histogram to the bigger one should work:
        biggerOther.add(histogram);
        assertEquals(3L, biggerOther.getCountAtValue(testValueLevel));
        assertEquals(3L, biggerOther.getCountAtValue(testValueLevel * 1000));
        assertEquals(6L, biggerOther.getTotalCount());

        // Since we are auto-sized, trying to add a larger histogram into a smaller one should work if no
        // overflowing data is there:
        try {
            // This should throw:
            histogram.add(biggerOther);
        } catch (ArrayIndexOutOfBoundsException e) {
            Assert.fail("Should not thow with out of bounds error");
        }

        // But trying to add smaller values to a larger histogram that actually uses it's range should throw an AIOOB:
        histogram.recordValue(1.0);
        other.recordValue(1.0);
        biggerOther.recordValue(trackableValueRangeSize * 8);

        try {
            // This should throw:
            biggerOther.add(histogram);
            Assert.fail("Should have thown with out of bounds error");
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
    }

    @Test
    public void testAddWithAutoResize() {
        DoubleHistogram histo1 = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 3);
        histo1.setAutoResize(true);
        histo1.recordValue(6.0);
        histo1.recordValue(1.0);
        histo1.recordValue(5.0);
        histo1.recordValue(8.0);
        histo1.recordValue(3.0);
        histo1.recordValue(7.0);
        DoubleHistogram histo2 = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 3);
        histo2.setAutoResize(true);
        histo2.recordValue(9.0);
        DoubleHistogram histo3 = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 3);
        histo3.setAutoResize(true);
        histo3.recordValue(4.0);
        histo3.recordValue(2.0);
        histo3.recordValue(10.0);

        DoubleHistogram merged = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 3);
        merged.setAutoResize(true);
        merged.add(histo1);
        merged.add(histo2);
        merged.add(histo3);

        assertEquals(merged.getTotalCount(), histo1.getTotalCount() + histo2.getTotalCount() + histo3.getTotalCount());
        assertEquals(1.0, merged.getMinValue(), 0.01);
        assertEquals(10.0, merged.getMaxValue(), 0.01);
    }

    @Test
    public void testSizeOfEquivalentValueRange() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals(1.0/1024.0, histogram.sizeOfEquivalentValueRange(1), 0.001);
        assertEquals(2, histogram.sizeOfEquivalentValueRange(2500), 0.001);
        assertEquals(4, histogram.sizeOfEquivalentValueRange(8191), 0.001);
        assertEquals(8, histogram.sizeOfEquivalentValueRange(8192), 0.001);
        assertEquals(8, histogram.sizeOfEquivalentValueRange(10000), 0.001);
    }

    @Test
    public void testLowestEquivalentValue() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals(10000, histogram.lowestEquivalentValue(10007), 0.001);
        assertEquals(10008, histogram.lowestEquivalentValue(10009), 0.001);
    }

    @Test
    public void testHighestEquivalentValue() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals(8183.99999, histogram.highestEquivalentValue(8180), 0.001);
        assertEquals(8191.99999, histogram.highestEquivalentValue(8191), 0.001);
        assertEquals(8199.99999, histogram.highestEquivalentValue(8193), 0.001);
        assertEquals(9999.99999, histogram.highestEquivalentValue(9995), 0.001);
        assertEquals(10007.99999, histogram.highestEquivalentValue(10007), 0.001);
        assertEquals(10015.99999, histogram.highestEquivalentValue(10008), 0.001);
    }

    @Test
    public void testMedianEquivalentValue() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals(4.002, histogram.medianEquivalentValue(4), 0.001);
        assertEquals(5.002, histogram.medianEquivalentValue(5), 0.001);
        assertEquals(4001, histogram.medianEquivalentValue(4000), 0.001);
        assertEquals(8002, histogram.medianEquivalentValue(8000), 0.001);
        assertEquals(10004, histogram.medianEquivalentValue(10007), 0.001);
    }

    @Test
    public void testNextNonEquivalentValue() {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, trackableValueRangeSize, numberOfSignificantValueDigits);
        Assert.assertNotSame(null, histogram);
    }

    @Test
    public void testMaxValue() {
        DoubleHistogram histogram = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 1_000_000_000, 2);
        Assert.assertNotSame(null, histogram);
        histogram.recordValue(2.5362386543);
        double maxValue = histogram.getMaxValue();
        assertEquals(maxValue, histogram.highestEquivalentValue(2.5362386543), 0);
    }

    void testDoubleHistogramSerialization(DoubleHistogram histogram) throws Exception {
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getCurrentHighestTrackableValue() - 1, histogram.getCurrentHighestTrackableValue() / 1000);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        DoubleHistogram newHistogram = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(histogram);
            Deflater compresser = new Deflater();
            compresser.setInput(bos.toByteArray());
            compresser.finish();
            byte [] compressedOutput = new byte[1024*1024];
            int compressedDataLength = compresser.deflate(compressedOutput);
            System.out.println("Serialized form of " + histogram.getClass() + " with internalHighestToLowestValueRatio = " +
                    histogram.getHighestToLowestValueRatio() + "\n and a numberOfSignificantValueDigits = " +
                    histogram.getNumberOfSignificantValueDigits() + " is " + bos.toByteArray().length +
                    " bytes long. Compressed form is " + compressedDataLength + " bytes long.");
            System.out.println("   (estimated footprint was " + histogram.getEstimatedFootprintInBytes() + " bytes)");
            bis = new ByteArrayInputStream(bos.toByteArray());
            in = new ObjectInputStream(bis);
            newHistogram = (DoubleHistogram) in.readObject();
        } finally {
            if (out != null) out.close();
            bos.close();
            if (in !=null) in.close();
            if (bis != null) bis.close();
        }
        assertNotNull(newHistogram);
        assertEqual(histogram, newHistogram);
    }

    private void assertEqual(DoubleHistogram expectedHistogram, DoubleHistogram actualHistogram) {
        assertEquals(expectedHistogram, actualHistogram);
        assertEquals(expectedHistogram.hashCode(), actualHistogram.hashCode());
        assertEquals(expectedHistogram.getCountAtValue(testValueLevel), actualHistogram.getCountAtValue(testValueLevel));
        assertEquals(expectedHistogram.getCountAtValue(testValueLevel * 10), actualHistogram.getCountAtValue(testValueLevel * 10));
        assertEquals(expectedHistogram.getTotalCount(), actualHistogram.getTotalCount());
    }


    @Test
    public void testSerialization() throws Exception {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class,trackableValueRangeSize, 3);
        testDoubleHistogramSerialization(histogram);
        histogram = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class,trackableValueRangeSize, 2);
        testDoubleHistogramSerialization(histogram);
    }

    @Test
    public void testSerializationWithInternals() throws Exception {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class,trackableValueRangeSize, 3);
        testDoubleHistogramSerialization(histogram);
        histogram = HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class,trackableValueRangeSize, 2, Histogram.class);
        testDoubleHistogramSerialization(histogram);
    }

    @Test
    public void testCopy() {
        DoubleHistogram histogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getCurrentHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copy of DoubleHistogram:");
        assertEqual(histogram, histogram.copy());
    }
    @Test
    public void testCopyInto() {
        DoubleHistogram histogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits);
        DoubleHistogram targetHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getCurrentHighestTrackableValue() - 1,
                histogram.getCurrentHighestTrackableValue() / 1000);

        System.out.println("Testing copyInto for DoubleHistogram:");
        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);

        histogram.recordValue(testValueLevel * 20);

        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);


        DoubleHistogram withConcurrentHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                ConcurrentHistogram.class);
        DoubleHistogram targetWithConcurrentHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                ConcurrentHistogram.class);
        withConcurrentHistogram.recordValue(testValueLevel);
        withConcurrentHistogram.recordValue(testValueLevel * 10);
        withConcurrentHistogram.recordValueWithExpectedInterval(withConcurrentHistogram.getCurrentHighestTrackableValue() - 1,
                withConcurrentHistogram.getCurrentHighestTrackableValue() / 1000);

        System.out.println("Testing copyInto for DoubleHistogram backed by ConcurrentHistogram:");
        withConcurrentHistogram.copyInto(targetWithConcurrentHistogram);
        assertEqual(withConcurrentHistogram, targetWithConcurrentHistogram);

        withConcurrentHistogram.recordValue(testValueLevel * 20);

        withConcurrentHistogram.copyInto(targetWithConcurrentHistogram);
        assertEqual(withConcurrentHistogram, targetWithConcurrentHistogram);
    }

    private int findContainingBinaryOrderOfMagnitude(long longNumber) {
        int pow2ceiling = 64 - Long.numberOfLeadingZeros(longNumber); // smallest power of 2 containing value
        pow2ceiling = Math.min(pow2ceiling, 62);
        return pow2ceiling;
    }

    private void genericResizeTest(DoubleHistogram h) {
        h.recordValue(0);
        h.recordValue(5);
        h.recordValue(1);
        h.recordValue(8);
        h.recordValue(9);

        assertEquals(9.0, h.getValueAtPercentile(100), 0.1d);
    }

    @Test
    public void testResize() {
        // Verify resize behvaior for various underlying internal integer histogram implementations:
        genericResizeTest(HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 2));
    }

    @Test
    public void testResizeInternals() {
        // Verify resize behvaior for various underlying internal integer histogram implementations:
        genericResizeTest(HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class, 2));
        genericResizeTest(HistogramTestUtils.constructDoubleHistogram(DoubleHistogram.class,2, ConcurrentHistogram.class));
    }
}
