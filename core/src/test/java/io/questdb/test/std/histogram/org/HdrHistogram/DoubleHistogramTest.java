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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.util.zip.Deflater;

/**
 * JUnit test for Histogram
 */
public class DoubleHistogramTest {
    static final long trackableValueRangeSize = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units
    static final int numberOfSignificantValueDigits = 3;
    // static final long testValueLevel = 12340;
    static final double testValueLevel = 4.0;

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class,
    })
    public void testTrackableValueRangeMustBeGreaterThanTwo(final Class<?> histoClass)
    {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HistogramTestUtils.constructDoubleHistogram(histoClass, 1, numberOfSignificantValueDigits));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testNumberOfSignificantValueDigitsMustBeLessThanSix(final Class<?> histoClass)
    {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> {
                    DoubleHistogram histogram =
                            HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, 6);
                });
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testNumberOfSignificantValueDigitsMustBePositive(final Class<?> histoClass) {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> {
                    DoubleHistogram histogram =
                            HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, -1);
                });
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testConstructionArgumentGets(Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record 1.0, and verify that the range adjust to it:
        histogram.recordValue(Math.pow(2.0, 20));
        histogram.recordValue(1.0);
        Assertions.assertEquals(1.0, histogram.getCurrentLowestTrackableNonZeroValue(), 0.001);
        Assertions.assertEquals(trackableValueRangeSize, histogram.getHighestToLowestValueRatio());
        Assertions.assertEquals(numberOfSignificantValueDigits, histogram.getNumberOfSignificantValueDigits());

        DoubleHistogram histogram2 =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record a larger value, and verify that the range adjust to it too:
        histogram2.recordValue(2048.0 * 1024.0 * 1024.0);
        Assertions.assertEquals(2048.0 * 1024.0 * 1024.0, histogram2.getCurrentLowestTrackableNonZeroValue(), 0.001);

        DoubleHistogram histogram3 =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record a value that is 1000x outside of the initially set range, which should scale us by 1/1024x:
        histogram3.recordValue(1/1000.0);
        Assertions.assertEquals(1.0/1024, histogram3.getCurrentLowestTrackableNonZeroValue(), 0.001);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testDataRange(Class<?> histoClass) {
        // A trackableValueRangeSize histigram
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0.0);  // Include a zero value to make sure things are handled right.
        Assertions.assertEquals(1L, histogram.getCountAtValue(0.0));

        double topValue = 1.0;
        try {
            while (true) {
                histogram.recordValue(topValue);
                topValue *= 2.0;
            }
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
        Assertions.assertEquals(1L << 33, topValue, 0.00001);
        Assertions.assertEquals(1L, histogram.getCountAtValue(0.0));

        histogram = HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
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
        Assertions.assertEquals(1.0, bottomValue, 0.00001);

        long expectedRange = 1L << (findContainingBinaryOrderOfMagnitude(trackableValueRangeSize) + 1);
        Assertions.assertEquals(expectedRange, (topValue / bottomValue), 0.00001);
        Assertions.assertEquals(1L, histogram.getCountAtValue(0.0));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testRecordValue(Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        Assertions.assertEquals(1L, histogram.getCountAtValue(testValueLevel));
        Assertions.assertEquals(1L, histogram.getTotalCount());
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class,
    })
    public void testRecordValue_Overflow_ShouldThrowException(final Class<?> histoClass) {
        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class,
                () -> {
                    DoubleHistogram histogram =
                            HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
                    histogram.recordValue(trackableValueRangeSize * 3);
                    histogram.recordValue(1.0);
                });
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testRecordValueWithExpectedInterval(Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0);
        histogram.recordValueWithExpectedInterval(testValueLevel, testValueLevel/4);
        DoubleHistogram rawHistogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        rawHistogram.recordValue(0);
        rawHistogram.recordValue(testValueLevel);
        // The raw data will not include corrected samples:
        Assertions.assertEquals(1L, rawHistogram.getCountAtValue(0));
        Assertions.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 1 )/4));
        Assertions.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 2 )/4));
        Assertions.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 3 )/4));
        Assertions.assertEquals(1L, rawHistogram.getCountAtValue((testValueLevel * 4 )/4));
        Assertions.assertEquals(2L, rawHistogram.getTotalCount());
        // The data will include corrected samples:
        Assertions.assertEquals(1L, histogram.getCountAtValue(0));
        Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 1 )/4));
        Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 2 )/4));
        Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 3 )/4));
        Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 4 )/4));
        Assertions.assertEquals(5L, histogram.getTotalCount());
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testReset(final Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(10);
        histogram.recordValue(100);
        Assertions.assertEquals(histogram.getMinValue(), Math.min(10.0, testValueLevel), 1.0);
        Assertions.assertEquals(histogram.getMaxValue(), Math.max(100.0, testValueLevel), 1.0);
        histogram.reset();
        Assertions.assertEquals(0L, histogram.getCountAtValue(testValueLevel));
        Assertions.assertEquals(0L, histogram.getTotalCount());
        histogram.recordValue(20);
        histogram.recordValue(80);
        Assertions.assertEquals(histogram.getMinValue(), 20.0, 1.0);
        Assertions.assertEquals(histogram.getMaxValue(), 80.0, 1.0);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testAdd(final Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        DoubleHistogram other =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);

        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 1000);
        other.recordValue(testValueLevel);
        other.recordValue(testValueLevel * 1000);
        histogram.add(other);
        Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel));
        Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
        Assertions.assertEquals(4L, histogram.getTotalCount());

        DoubleHistogram biggerOther =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize * 2, numberOfSignificantValueDigits);
        biggerOther.recordValue(testValueLevel);
        biggerOther.recordValue(testValueLevel * 1000);

        // Adding the smaller histogram to the bigger one should work:
        biggerOther.add(histogram);
        Assertions.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel));
        Assertions.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel * 1000));
        Assertions.assertEquals(6L, biggerOther.getTotalCount());

        // Since we are auto-sized, trying to add a larger histogram into a smaller one should work if no
        // overflowing data is there:
        try {
            // This should throw:
            histogram.add(biggerOther);
        } catch (ArrayIndexOutOfBoundsException e) {
            Assertions.fail("Should not thow with out of bounds error");
        }

        // But trying to add smaller values to a larger histogram that actually uses it's range should throw an AIOOB:
        histogram.recordValue(1.0);
        other.recordValue(1.0);
        biggerOther.recordValue(trackableValueRangeSize * 8);

        try {
            // This should throw:
            biggerOther.add(histogram);
            Assertions.fail("Should have thown with out of bounds error");
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testAddWithAutoResize(final Class<?> histoClass) {
        DoubleHistogram histo1 = HistogramTestUtils.constructDoubleHistogram(histoClass, 3);
        histo1.setAutoResize(true);
        histo1.recordValue(6.0);
        histo1.recordValue(1.0);
        histo1.recordValue(5.0);
        histo1.recordValue(8.0);
        histo1.recordValue(3.0);
        histo1.recordValue(7.0);
        DoubleHistogram histo2 = HistogramTestUtils.constructDoubleHistogram(histoClass, 3);
        histo2.setAutoResize(true);
        histo2.recordValue(9.0);
        DoubleHistogram histo3 = HistogramTestUtils.constructDoubleHistogram(histoClass, 3);
        histo3.setAutoResize(true);
        histo3.recordValue(4.0);
        histo3.recordValue(2.0);
        histo3.recordValue(10.0);

        DoubleHistogram merged = HistogramTestUtils.constructDoubleHistogram(histoClass, 3);
        merged.setAutoResize(true);
        merged.add(histo1);
        merged.add(histo2);
        merged.add(histo3);

        Assertions.assertEquals(merged.getTotalCount(), histo1.getTotalCount() + histo2.getTotalCount() + histo3.getTotalCount());
        Assertions.assertEquals(1.0, merged.getMinValue(), 0.01);
        Assertions.assertEquals(10.0, merged.getMaxValue(), 0.01);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testSizeOfEquivalentValueRange(final Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        Assertions.assertEquals(1.0/1024.0, histogram.sizeOfEquivalentValueRange(1), 0.001, "Size of equivalent range for value 1 is 1");
        Assertions.assertEquals(2, histogram.sizeOfEquivalentValueRange(2500), 0.001, "Size of equivalent range for value 2500 is 2");
        Assertions.assertEquals(4, histogram.sizeOfEquivalentValueRange(8191), 0.001, "Size of equivalent range for value 8191 is 4");
        Assertions.assertEquals(8, histogram.sizeOfEquivalentValueRange(8192), 0.001, "Size of equivalent range for value 8192 is 8");
        Assertions.assertEquals(8, histogram.sizeOfEquivalentValueRange(10000), 0.001, "Size of equivalent range for value 10000 is 8");
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testLowestEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        Assertions.assertEquals(10000, histogram.lowestEquivalentValue(10007), 0.001, "The lowest equivalent value to 10007 is 10000");
        Assertions.assertEquals(10008, histogram.lowestEquivalentValue(10009), 0.001, "The lowest equivalent value to 10009 is 10008");
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testHighestEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        Assertions.assertEquals(8183.99999, histogram.highestEquivalentValue(8180), 0.001, "The highest equivalent value to 8180 is 8183");
        Assertions.assertEquals(8191.99999, histogram.highestEquivalentValue(8191), 0.001, "The highest equivalent value to 8187 is 8191");
        Assertions.assertEquals(8199.99999, histogram.highestEquivalentValue(8193), 0.001, "The highest equivalent value to 8193 is 8199");
        Assertions.assertEquals(9999.99999, histogram.highestEquivalentValue(9995), 0.001, "The highest equivalent value to 9995 is 9999");
        Assertions.assertEquals(10007.99999, histogram.highestEquivalentValue(10007), 0.001, "The highest equivalent value to 10007 is 10007");
        Assertions.assertEquals(10015.99999, histogram.highestEquivalentValue(10008), 0.001, "The highest equivalent value to 10008 is 10015");
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testMedianEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        Assertions.assertEquals(4.002, histogram.medianEquivalentValue(4), 0.001, "The median equivalent value to 4 is 4");
        Assertions.assertEquals(5.002, histogram.medianEquivalentValue(5), 0.001, "The median equivalent value to 5 is 5");
        Assertions.assertEquals(4001, histogram.medianEquivalentValue(4000), 0.001, "The median equivalent value to 4000 is 4001");
        Assertions.assertEquals(8002, histogram.medianEquivalentValue(8000), 0.001, "The median equivalent value to 8000 is 8002");
        Assertions.assertEquals(10004, histogram.medianEquivalentValue(10007), 0.001, "The median equivalent value to 10007 is 10004");
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testNextNonEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        Assertions.assertNotSame(null, histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testMaxValue(final Class<?> histoClass) {
        DoubleHistogram histogram = HistogramTestUtils.constructDoubleHistogram(histoClass, 1_000_000_000, 2);
        Assertions.assertNotSame(null, histogram);
        histogram.recordValue(2.5362386543);
        double maxValue = histogram.getMaxValue();
        Assertions.assertEquals(maxValue, histogram.highestEquivalentValue(2.5362386543));
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
        Assertions.assertNotNull(newHistogram);
        assertEqual(histogram, newHistogram);
    }

    private void assertEqual(DoubleHistogram expectedHistogram, DoubleHistogram actualHistogram) {
        Assertions.assertEquals(expectedHistogram, actualHistogram);
        Assertions.assertEquals(expectedHistogram.hashCode(), actualHistogram.hashCode());
        Assertions.assertEquals(expectedHistogram.getCountAtValue(testValueLevel), actualHistogram.getCountAtValue(testValueLevel));
        Assertions.assertEquals(expectedHistogram.getCountAtValue(testValueLevel * 10), actualHistogram.getCountAtValue(testValueLevel * 10));
        Assertions.assertEquals(expectedHistogram.getTotalCount(), actualHistogram.getTotalCount());
    }


    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testSerialization(final Class<?> histoClass) throws Exception {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass,trackableValueRangeSize, 3);
        testDoubleHistogramSerialization(histogram);
        histogram = HistogramTestUtils.constructDoubleHistogram(histoClass,trackableValueRangeSize, 2);
        testDoubleHistogramSerialization(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class,
    })
    public void testSerializationWithInternals(final Class<?> histoClass) throws Exception {
        DoubleHistogram histogram =
                HistogramTestUtils.constructDoubleHistogram(histoClass,trackableValueRangeSize, 3);
        testDoubleHistogramSerialization(histogram);
        histogram = HistogramTestUtils.constructDoubleHistogram(histoClass,trackableValueRangeSize, 2, Histogram.class);
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

        Assert.assertEquals(9.0, h.getValueAtPercentile(100), 0.1d);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testResize(final Class<?> histoClass) {
        // Verify resize behvaior for various underlying internal integer histogram implementations:
        genericResizeTest(HistogramTestUtils.constructDoubleHistogram(histoClass, 2));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class,
    })
    public void testResizeInternals(final Class<?> histoClass) {
        // Verify resize behvaior for various underlying internal integer histogram implementations:
        genericResizeTest(HistogramTestUtils.constructDoubleHistogram(histoClass, 2));
        genericResizeTest(HistogramTestUtils.constructDoubleHistogram(histoClass,2, ConcurrentHistogram.class));
    }
}
