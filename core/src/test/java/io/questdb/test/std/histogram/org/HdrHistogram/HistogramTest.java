/**
 * HistogramTest.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.std.histogram.org.HdrHistogram.*;
import org.junit.Assume;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.Deflater;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructHistogram;

/**
 * JUnit test for Histogram
 */
@RunWith(Parameterized.class)
public class HistogramTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units
    static final int numberOfSignificantValueDigits = 3;
    static final long testValueLevel = 4;

    private final HistogramType type;
    private final Class<?> histoClass;
    enum HistogramType {Histogram, Concurrent, Atomic}
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{HistogramType.Histogram, Histogram.class},
                {HistogramType.Atomic, AtomicHistogram.class}});
    }

    public HistogramTest(HistogramType type, Class<?> histoClass) {
        this.type = type;
        this.histoClass = histoClass;
    }

    @Test
    public void testConstructionArgumentRanges() {
        boolean thrown = false;
        AbstractHistogram histogram = null;

        try {
            // This should throw:
            // histogram = new Histogram(1, numberOfSignificantValueDigits);
            histogram = constructHistogram(histoClass, 1, numberOfSignificantValueDigits);
        } catch (IllegalArgumentException e) {
            thrown = true;
        }
        Assert.assertTrue(thrown);
        Assert.assertNull(histogram);

        thrown = false;
        try {
            // This should throw:
            // histogram = new Histogram(highestTrackableValue, 6);
            histogram = constructHistogram(histoClass, highestTrackableValue, 6);
        } catch (IllegalArgumentException e) {
            thrown = true;
        }
        Assert.assertTrue(thrown);
        Assert.assertNull(histogram);

        thrown = false;
        try {
            // This should throw:
            // histogram = new Histogram(highestTrackableValue, -1);
            histogram = constructHistogram(histoClass, highestTrackableValue, -1);
        } catch (IllegalArgumentException e) {
            thrown = true;
        }
        Assert.assertTrue(thrown);
        Assert.assertNull(histogram);
    }

    @Test
    public void testUnitMagnitude0IndexCalculations() {
        // Histogram h = new Histogram(1L, 1L << 32, 3);
        AbstractHistogram h = constructHistogram(histoClass, 1L, 1L << 32, 3);
        Assert.assertEquals(2048, h.subBucketCount);
        Assert.assertEquals(0, h.unitMagnitude);
        // subBucketCount = 2^11, so 2^11 << 22 is > the max of 2^32 for 23 buckets total
        Assert.assertEquals(23, h.bucketCount);

        // first half of first bucket
        Assert.assertEquals(0, h.getBucketIndex(3));
        Assert.assertEquals(3, h.getSubBucketIndex(3, 0));

        // second half of first bucket
        Assert.assertEquals(0, h.getBucketIndex(1024 + 3));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex(1024 + 3, 0));

        // second bucket (top half)
        Assert.assertEquals(1, h.getBucketIndex(2048 + 3 * 2));
        // counting by 2s, starting at halfway through the bucket
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex(2048 + 3 * 2, 1));

        // third bucket (top half)
        Assert.assertEquals(2, h.getBucketIndex((2048 << 1) + 3 * 4));
        // counting by 4s, starting at halfway through the bucket
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex((2048 << 1) + 3 * 4, 2));

        // past last bucket -- not near Long.MAX_VALUE, so should still calculate ok.
        Assert.assertEquals(23, h.getBucketIndex((2048L << 22) + 3 * (1 << 23)));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex((2048L << 22) + 3 * (1 << 23), 23));
    }

    @Test
    public void testUnitMagnitude4IndexCalculations() {
        // Histogram h = new Histogram(1L << 12, 1L << 32, 3);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 12, 1L << 32, 3);
        Assert.assertEquals(2048, h.subBucketCount);
        Assert.assertEquals(12, h.unitMagnitude);
        // subBucketCount = 2^11. With unit magnitude shift, it's 2^23. 2^23 << 10 is > the max of 2^32 for 11 buckets
        // total
        Assert.assertEquals(11, h.bucketCount);
        long unit = 1L << 12;

        // below lowest value
        Assert.assertEquals(0, h.getBucketIndex(3));
        Assert.assertEquals(0, h.getSubBucketIndex(3, 0));

        // first half of first bucket
        Assert.assertEquals(0, h.getBucketIndex(3 * unit));
        Assert.assertEquals(3, h.getSubBucketIndex(3 * unit, 0));

        // second half of first bucket
        // subBucketHalfCount's worth of units, plus 3 more
        Assert.assertEquals(0, h.getBucketIndex(unit * (1024 + 3)));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex(unit * (1024 + 3), 0));

        // second bucket (top half), bucket scale = unit << 1.
        // Middle of bucket is (subBucketHalfCount = 2^10) of bucket scale, = unit << 11.
        // Add on 3 of bucket scale.
        Assert.assertEquals(1, h.getBucketIndex((unit << 11) + 3 * (unit << 1)));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 11) + 3 * (unit << 1), 1));

        // third bucket (top half), bucket scale = unit << 2.
        // Middle of bucket is (subBucketHalfCount = 2^10) of bucket scale, = unit << 12.
        // Add on 3 of bucket scale.
        Assert.assertEquals(2, h.getBucketIndex((unit << 12) + 3 * (unit << 2)));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 12) + 3 * (unit << 2), 2));

        // past last bucket -- not near Long.MAX_VALUE, so should still calculate ok.
        Assert.assertEquals(11, h.getBucketIndex((unit << 21) + 3 * (unit << 11)));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 21) + 3 * (unit << 11), 11));
    }

    @Test
    public void testUnitMagnitude51SubBucketMagnitude11IndexCalculations() {
        // maximum unit magnitude for this precision
        // Histogram h = new Histogram(1L << 51, Long.MAX_VALUE, 3);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 51, Long.MAX_VALUE, 3);
        Assert.assertEquals(2048, h.subBucketCount);
        Assert.assertEquals(51, h.unitMagnitude);
        // subBucketCount = 2^11. With unit magnitude shift, it's 2^62. 1 more bucket to (almost) reach 2^63.
        Assert.assertEquals(2, h.bucketCount);
        Assert.assertEquals(2, h.leadingZeroCountBase);
        long unit = 1L << 51;

        // below lowest value
        Assert.assertEquals(0, h.getBucketIndex(3));
        Assert.assertEquals(0, h.getSubBucketIndex(3, 0));

        // first half of first bucket
        Assert.assertEquals(0, h.getBucketIndex(3 * unit));
        Assert.assertEquals(3, h.getSubBucketIndex(3 * unit, 0));

        // second half of first bucket
        // subBucketHalfCount's worth of units, plus 3 more
        Assert.assertEquals(0, h.getBucketIndex(unit * (1024 + 3)));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex(unit * (1024 + 3), 0));

        // end of second half
        Assert.assertEquals(0, h.getBucketIndex(unit * 1024 + 1023 * unit));
        Assert.assertEquals(1024 + 1023, h.getSubBucketIndex(unit * 1024 + 1023 * unit, 0));

        // second bucket (top half), bucket scale = unit << 1.
        // Middle of bucket is (subBucketHalfCount = 2^10) of bucket scale, = unit << 11.
        // Add on 3 of bucket scale.
        Assert.assertEquals(1, h.getBucketIndex((unit << 11) + 3 * (unit << 1)));
        Assert.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 11) + 3 * (unit << 1), 1));

        // upper half of second bucket, last slot
        Assert.assertEquals(1, h.getBucketIndex(Long.MAX_VALUE));
        Assert.assertEquals(1024 + 1023, h.getSubBucketIndex(Long.MAX_VALUE, 1));
    }

    @Test
    public void testUnitMagnitude52SubBucketMagnitude11Throws() {
        try {
            constructHistogram(histoClass, 1L << 52, 1L << 62, 3);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Cannot represent numberOfSignificantValueDigits worth of values beyond lowestDiscernibleValue", e.getMessage());
        }
    }

    @Test
    public void testUnitMagnitude54SubBucketMagnitude8Ok() {
        // Histogram h = new Histogram(1L << 54, 1L << 62, 2);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 54, 1L << 62, 2);
        Assert.assertEquals(256, h.subBucketCount);
        Assert.assertEquals(54, h.unitMagnitude);
        // subBucketCount = 2^8. With unit magnitude shift, it's 2^62.
        Assert.assertEquals(2, h.bucketCount);

        // below lowest value
        Assert.assertEquals(0, h.getBucketIndex(3));
        Assert.assertEquals(0, h.getSubBucketIndex(3, 0));

        // upper half of second bucket, last slot
        Assert.assertEquals(1, h.getBucketIndex(Long.MAX_VALUE));
        Assert.assertEquals(128 + 127, h.getSubBucketIndex(Long.MAX_VALUE, 1));
    }

    @Test
    public void testUnitMagnitude61SubBucketMagnitude0Ok() {
        // Histogram h = new Histogram(1L << 61, 1L << 62, 0);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 61, 1L << 62, 0);
        Assert.assertEquals(2, h.subBucketCount);
        Assert.assertEquals(61, h.unitMagnitude);
        // subBucketCount = 2^1. With unit magnitude shift, it's 2^62. 1 more bucket to be > the max of 2^62.
        Assert.assertEquals(2, h.bucketCount);

        // below lowest value
        Assert.assertEquals(0, h.getBucketIndex(3));
        Assert.assertEquals(0, h.getSubBucketIndex(3, 0));

        // upper half of second bucket, last slot
        Assert.assertEquals(1, h.getBucketIndex(Long.MAX_VALUE));
        Assert.assertEquals(1, h.getSubBucketIndex(Long.MAX_VALUE, 1));
    }

    @Test
    public void testEmptyHistogram() {
        Assume.assumeTrue(type != HistogramType.Atomic);
        AbstractHistogram histogram = constructHistogram(histoClass, 3);
        long min = histogram.getMinValue();
        Assert.assertEquals(0, min);
        long max = histogram.getMaxValue();
        Assert.assertEquals(0, max);
        double mean = histogram.getMean();
        Assert.assertEquals(0, mean, 0.0000000000001D);
        double stddev = histogram.getStdDeviation();
        Assert.assertEquals(0, stddev, 0.0000000000001D);
        double pcnt = histogram.getPercentileAtOrBelowValue(0);
        Assert.assertEquals(100.0, pcnt, 0.0000000000001D);
    }

    @Test
    public void testConstructionArgumentGets() {
        AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
        Assert.assertEquals(1, histogram.getLowestDiscernibleValue());
        Assert.assertEquals(highestTrackableValue, histogram.getHighestTrackableValue());
        Assert.assertEquals(numberOfSignificantValueDigits, histogram.getNumberOfSignificantValueDigits());
        AbstractHistogram histogram2 = constructHistogram(histoClass, 1000, highestTrackableValue, numberOfSignificantValueDigits);
        Assert.assertEquals(1000, histogram2.getLowestDiscernibleValue());
        verifyMaxValue(histogram);
    }

    @Test
    public void testGetEstimatedFootprintInBytes() {
            Assume.assumeTrue(type != HistogramType.Concurrent);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            /*
             *     largestValueWithSingleUnitResolution = 2 * (10 ^ numberOfSignificantValueDigits);
             *     subBucketSize = roundedUpToNearestPowerOf2(largestValueWithSingleUnitResolution);

             *     expectedHistogramFootprintInBytes = 512 +
             *          ({primitive type size} / 2) *
             *          (log2RoundedUp((trackableValueRangeSize) / subBucketSize) + 2) *
             *          subBucketSize
             */
            long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);
            int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution) / Math.log(2));
            int subBucketSize = (int) Math.pow(2, (subBucketCountMagnitude));
            long expectedSize = 512 +
                    ((8 *
                            ((long) (
                                    Math.ceil(
                                            Math.log((double) highestTrackableValue / subBucketSize)
                                                    / Math.log(2)
                                    )
                                            + 2)) *
                            (1L << (64 - Long.numberOfLeadingZeros(2 * (long) Math.pow(10, numberOfSignificantValueDigits))))
                    ) / 2);
            Assert.assertEquals(expectedSize, histogram.getEstimatedFootprintInBytes());
            verifyMaxValue(histogram);
    }

    @Test
    public void testRecordValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            Assert.assertEquals(1L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(1L, histogram.getTotalCount());
            verifyMaxValue(histogram);
    }

    @Test
    public void testConstructionWithLargeNumbers() {
            AbstractHistogram histogram = constructHistogram(histoClass, 20000000, 100000000, 5);
            histogram.recordValue(100000000);
            histogram.recordValue(20000000);
            histogram.recordValue(30000000);
            Assert.assertTrue(histogram.valuesAreEquivalent(20000000, histogram.getValueAtPercentile(50.0)));
            Assert.assertTrue(histogram.valuesAreEquivalent(30000000, histogram.getValueAtPercentile(50.0)));
            Assert.assertTrue(histogram.valuesAreEquivalent(100000000, histogram.getValueAtPercentile(83.33)));
            Assert.assertTrue(histogram.valuesAreEquivalent(100000000, histogram.getValueAtPercentile(83.34)));
            Assert.assertTrue(histogram.valuesAreEquivalent(100000000, histogram.getValueAtPercentile(99.0)));
    }

    @Test
    public void testValueAtPercentileMatchesPercentile() {
            AbstractHistogram histogram = constructHistogram(histoClass, 1, Long.MAX_VALUE, 2);
            long[] lengths = {1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000};

            for (long length : lengths) {
                histogram.reset();
                for (long value = 1; value <= length; value++) {
                    histogram.recordValue(value);
                }

                for (long value = 1; value <= length; value = histogram.nextNonEquivalentValue(value)) {
                    double calculatedPercentile = 100.0 * ((double) value) / length;
                    long lookupValue = histogram.getValueAtPercentile(calculatedPercentile);
                    Assert.assertTrue(histogram.valuesAreEquivalent(value, lookupValue));
                }
            }
    }

    @Test
    public void testValueAtPercentileMatchesPercentileIter() {
            AbstractHistogram histogram = constructHistogram(histoClass, 1, Long.MAX_VALUE, 2);
            long[] lengths = {1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000};

            for (long length : lengths) {
                histogram.reset();
                for (long value = 1; value <= length; value++) {
                    histogram.recordValue(value);
                }

                int percentileTicksPerHalfDistance = 1000;
                for (HistogramIterationValue v : histogram.percentiles(percentileTicksPerHalfDistance)) {
                    long calculatedValue = histogram.getValueAtPercentile(v.getPercentile());
                    long iterValue = v.getValueIteratedTo();
                    Assert.assertTrue(histogram.valuesAreEquivalent(calculatedValue, iterValue));
                    Assert.assertTrue(histogram.valuesAreEquivalent(calculatedValue, iterValue));
                }
            }
    }

    @Test
    public void testRecordValueWithExpectedInterval() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValueWithExpectedInterval(testValueLevel, testValueLevel / 4);
            AbstractHistogram rawHistogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            rawHistogram.recordValue(testValueLevel);
            // The data will include corrected samples:
            Assert.assertEquals(1L, histogram.getCountAtValue((testValueLevel) / 4));
            Assert.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 2) / 4));
            Assert.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 3) / 4));
            Assert.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 4) / 4));
            Assert.assertEquals(4L, histogram.getTotalCount());
            // But the raw data will not:
            Assert.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel) / 4));
            Assert.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 2) / 4));
            Assert.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 3) / 4));
            Assert.assertEquals(1L, rawHistogram.getCountAtValue((testValueLevel * 4) / 4));
            Assert.assertEquals(1L, rawHistogram.getTotalCount());

            verifyMaxValue(histogram);
    }

    @Test
    public void testReset() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(10);
            histogram.recordValue(100);
            Assert.assertEquals(histogram.getMinValue(), Math.min(10, testValueLevel));
            Assert.assertEquals(histogram.getMaxValue(), Math.max(100, testValueLevel));
            histogram.reset();
            Assert.assertEquals(0L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(0L, histogram.getTotalCount());
            verifyMaxValue(histogram);
            histogram.recordValue(20);
            histogram.recordValue(80);
            Assert.assertEquals(histogram.getMinValue(), 20);
            Assert.assertEquals(histogram.getMaxValue(), 80);
    }

    @Test
    public void testAdd() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram other = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            other.recordValue(testValueLevel);
            other.recordValue(testValueLevel * 1000);
            histogram.add(other);
            Assert.assertEquals(2L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(4L, histogram.getTotalCount());

            AbstractHistogram biggerOther = constructHistogram(histoClass, highestTrackableValue * 2, numberOfSignificantValueDigits);
            biggerOther.recordValue(testValueLevel);
            biggerOther.recordValue(testValueLevel * 1000);
            biggerOther.recordValue(highestTrackableValue * 2);

            // Adding the smaller histogram to the bigger one should work:
            biggerOther.add(histogram);
            Assert.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel));
            Assert.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(1L, biggerOther.getCountAtValue(highestTrackableValue * 2)); // overflow smaller hist...
            Assert.assertEquals(7L, biggerOther.getTotalCount());

            // But trying to add a larger histogram into a smaller one should throw an AIOOB:
            boolean thrown = false;
            try {
                // This should throw:
                histogram.add(biggerOther);
            } catch (ArrayIndexOutOfBoundsException e) {
                thrown = true;
            }
            Assert.assertTrue(thrown);

            verifyMaxValue(histogram);
            verifyMaxValue(other);
            verifyMaxValue(biggerOther);
    }

    @Test
    public void testSubtractAfterAdd() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram other = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            other.recordValue(testValueLevel);
            other.recordValue(testValueLevel * 1000);
            histogram.add(other);
            Assert.assertEquals(2L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(4L, histogram.getTotalCount());
            histogram.add(other);
            Assert.assertEquals(3L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(3L, histogram.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(6L, histogram.getTotalCount());
            histogram.subtract(other);
            Assert.assertEquals(2L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(4L, histogram.getTotalCount());

            verifyMaxValue(histogram);
            verifyMaxValue(other);
    }

    @Test
    public void testSubtractToZeroCounts() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            Assert.assertEquals(1L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(1L, histogram.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(2L, histogram.getTotalCount());

            // Subtracting down to zero counts should work:
            histogram.subtract(histogram);
            Assert.assertEquals(0L, histogram.getCountAtValue(testValueLevel));
            Assert.assertEquals(0L, histogram.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(0L, histogram.getTotalCount());

            verifyMaxValue(histogram);
    }

    @Test
    public void testSubtractToNegativeCountsThrows() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram other = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            other.recordValueWithCount(testValueLevel, 2);
            other.recordValueWithCount(testValueLevel * 1000, 2);

            try {
                histogram.subtract(other);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                // should throw
            }

            verifyMaxValue(histogram);
            verifyMaxValue(other);
    }

    @Test
    public void testSubtractSubtrahendValuesOutsideMinuendRangeThrows() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);

            AbstractHistogram biggerOther = constructHistogram(histoClass, highestTrackableValue * 2, numberOfSignificantValueDigits);
            biggerOther.recordValue(testValueLevel);
            biggerOther.recordValue(testValueLevel * 1000);
            biggerOther.recordValue(highestTrackableValue * 2); // outside smaller histogram's range

            try {
                histogram.subtract(biggerOther);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                // should throw
            }

            verifyMaxValue(histogram);
            verifyMaxValue(biggerOther);
    }

    @Test
    public void testSubtractSubtrahendValuesInsideMinuendRangeWorks() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);

            AbstractHistogram biggerOther = constructHistogram(histoClass, highestTrackableValue * 2, numberOfSignificantValueDigits);
            biggerOther.recordValue(testValueLevel);
            biggerOther.recordValue(testValueLevel * 1000);
            biggerOther.recordValue(highestTrackableValue * 2);
            biggerOther.add(biggerOther);
            biggerOther.add(biggerOther);
            Assert.assertEquals(4L, biggerOther.getCountAtValue(testValueLevel));
            Assert.assertEquals(4L, biggerOther.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(4L, biggerOther.getCountAtValue(highestTrackableValue * 2)); // overflow smaller hist...
            Assert.assertEquals(12L, biggerOther.getTotalCount());

            // Subtracting the smaller histogram from the bigger one should work:
            biggerOther.subtract(histogram);
            Assert.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel));
            Assert.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel * 1000));
            Assert.assertEquals(4L, biggerOther.getCountAtValue(highestTrackableValue * 2)); // overflow smaller hist...
            Assert.assertEquals(10L, biggerOther.getTotalCount());

            verifyMaxValue(histogram);
            verifyMaxValue(biggerOther);
    }

    @Test
    public void testSizeOfEquivalentValueRange() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(1, histogram.sizeOfEquivalentValueRange(1));
            Assert.assertEquals(1, histogram.sizeOfEquivalentValueRange(1025));
            Assert.assertEquals(1, histogram.sizeOfEquivalentValueRange(2047));
            Assert.assertEquals(2, histogram.sizeOfEquivalentValueRange(2048));
            Assert.assertEquals(2, histogram.sizeOfEquivalentValueRange(2500));
            Assert.assertEquals(4, histogram.sizeOfEquivalentValueRange(8191));
            Assert.assertEquals(8, histogram.sizeOfEquivalentValueRange(8192));
            Assert.assertEquals(8, histogram.sizeOfEquivalentValueRange(10000));
            verifyMaxValue(histogram);
    }

    @Test
    public void testScaledSizeOfEquivalentValueRange() {
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(1024, histogram.sizeOfEquivalentValueRange(1024));
            Assert.assertEquals(2 * 1024, histogram.sizeOfEquivalentValueRange(2500 * 1024));
            Assert.assertEquals(4 * 1024, histogram.sizeOfEquivalentValueRange(8191 * 1024));
            Assert.assertEquals(8 * 1024, histogram.sizeOfEquivalentValueRange(8192 * 1024));
            Assert.assertEquals(8 * 1024, histogram.sizeOfEquivalentValueRange(10000 * 1024));
            verifyMaxValue(histogram);
    }

    @Test
    public void testLowestEquivalentValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(10000, histogram.lowestEquivalentValue(10007));
            Assert.assertEquals(10008, histogram.lowestEquivalentValue(10009));
            verifyMaxValue(histogram);
    }

    @Test
    public void testScaledLowestEquivalentValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(10000 * 1024, histogram.lowestEquivalentValue(10007 * 1024));
            Assert.assertEquals(10008 * 1024, histogram.lowestEquivalentValue(10009 * 1024));
            verifyMaxValue(histogram);
    }

    @Test
    public void testHighestEquivalentValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(8183 * 1024 + 1023, histogram.highestEquivalentValue(8180 * 1024));
            Assert.assertEquals(8191 * 1024 + 1023, histogram.highestEquivalentValue(8191 * 1024));
            Assert.assertEquals(8199 * 1024 + 1023, histogram.highestEquivalentValue(8193 * 1024));
            Assert.assertEquals(9999 * 1024 + 1023, histogram.highestEquivalentValue(9995 * 1024));
            Assert.assertEquals(10007 * 1024 + 1023, histogram.highestEquivalentValue(10007 * 1024));
            Assert.assertEquals(10015 * 1024 + 1023, histogram.highestEquivalentValue(10008 * 1024));
            verifyMaxValue(histogram);
    }

    @Test
    public void testScaledHighestEquivalentValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(8183, histogram.highestEquivalentValue(8180));
            Assert.assertEquals(8191, histogram.highestEquivalentValue(8191));
            Assert.assertEquals(8199, histogram.highestEquivalentValue(8193));
            Assert.assertEquals(9999, histogram.highestEquivalentValue(9995));
            Assert.assertEquals(10007, histogram.highestEquivalentValue(10007));
            Assert.assertEquals(10015, histogram.highestEquivalentValue(10008));
            verifyMaxValue(histogram);
    }

    @Test
    public void testMedianEquivalentValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(4, histogram.medianEquivalentValue(4));
            Assert.assertEquals(5, histogram.medianEquivalentValue(5));
            Assert.assertEquals(4001, histogram.medianEquivalentValue(4000));
            Assert.assertEquals(8002, histogram.medianEquivalentValue(8000));
            Assert.assertEquals(10004, histogram.medianEquivalentValue(10007));
            verifyMaxValue(histogram);
    }

    @Test
    public void testScaledMedianEquivalentValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertEquals(4 * 1024 + 512, histogram.medianEquivalentValue(4 * 1024));
            Assert.assertEquals(5 * 1024 + 512, histogram.medianEquivalentValue(5 * 1024));
            Assert.assertEquals(4001 * 1024, histogram.medianEquivalentValue(4000 * 1024));
            Assert.assertEquals(8002 * 1024, histogram.medianEquivalentValue(8000 * 1024));
            Assert.assertEquals(10004 * 1024, histogram.medianEquivalentValue(10007 * 1024));
            verifyMaxValue(histogram);
    }

    @Test
    public void testNextNonEquivalentValue() {
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assert.assertNotSame(null, histogram);
    }

    static void testAbstractSerialization(AbstractHistogram histogram) throws Exception {
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 255);
        if (histogram.supportsAutoResize()) {
            histogram.setAutoResize(true);
            Assert.assertTrue(histogram.isAutoResize());
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        AbstractHistogram newHistogram = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(histogram);
            Deflater compresser = new Deflater();
            compresser.setInput(bos.toByteArray());
            compresser.finish();
            byte [] compressedOutput = new byte[1024*1024];
            int compressedDataLength = compresser.deflate(compressedOutput);
            System.out.println("Serialized form of " + histogram.getClass() + " with trackableValueRangeSize = " +
                    histogram.getHighestTrackableValue() + "\n and a numberOfSignificantValueDigits = " +
                    histogram.getNumberOfSignificantValueDigits() + " is " + bos.toByteArray().length +
                    " bytes long. Compressed form is " + compressedDataLength + " bytes long.");
            System.out.println("   (estimated footprint was " + histogram.getEstimatedFootprintInBytes() + " bytes)");
            bis = new ByteArrayInputStream(bos.toByteArray());
            in = new ObjectInputStream(bis);
            newHistogram = (AbstractHistogram) in.readObject();
        } finally {
            if (out != null) out.close();
            bos.close();
            if (in !=null) in.close();
            if (bis != null) bis.close();
        }
        Assert.assertNotNull(newHistogram);
        assertEqual(histogram, newHistogram);
        Assert.assertEquals(histogram, newHistogram);
        if (histogram.supportsAutoResize()) {
            Assert.assertTrue(histogram.isAutoResize());
        }
        Assert.assertEquals(newHistogram.isAutoResize(), histogram.isAutoResize());
        Assert.assertEquals(histogram.hashCode(), newHistogram.hashCode());
        Assert.assertEquals(histogram.getNeededByteBufferCapacity(), newHistogram.copy().getNeededByteBufferCapacity());
        Assert.assertEquals(histogram.getNeededByteBufferCapacity(), newHistogram.getNeededByteBufferCapacity());
    }

    private static void assertEqual(AbstractHistogram expectedHistogram, AbstractHistogram actualHistogram) {
        Assert.assertEquals(expectedHistogram, actualHistogram);
        Assert.assertEquals(expectedHistogram.getCountAtValue(testValueLevel), actualHistogram.getCountAtValue(testValueLevel));
        Assert.assertEquals(expectedHistogram.getCountAtValue(testValueLevel * 10), actualHistogram.getCountAtValue(testValueLevel * 10));
        Assert.assertEquals(expectedHistogram.getTotalCount(), actualHistogram.getTotalCount());
        verifyMaxValue(expectedHistogram);
        verifyMaxValue(actualHistogram);
    }

    @RunWith(Parameterized.class)
    public static class HistogramCsvTest {

        private final HistogramType type;
        private final String hist;
        private final int dig;
        enum HistogramType {Histogram3, Histogram2, Concurrent3, Concurrent2, Atomic3, Atomic2}
        @Parameterized.Parameters(name = "{0}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {{HistogramType.Histogram3, "Histogram", 3},
                    {HistogramType.Histogram2, "Histogram", 2},
                    {HistogramType.Concurrent3, "ConcurrentHistogram", 3},
                    {HistogramType.Concurrent2, "ConcurrentHistogram", 2},
                    {HistogramType.Atomic3, "AtomicHistogram", 3},
                    {HistogramType.Atomic2, "AtomicHistogram", 2}});
        }

        public HistogramCsvTest(HistogramType type, String hist, int dig) {
            this.type = type;
            this.hist = hist;
            this.dig = dig;
        }

        @Test
        public void testSerialization() throws Exception {
            Class<?> histoClass = Class.forName("io.questdb.std.histogram.org.HdrHistogram." + hist);
            int digits = dig;

            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, digits);
            testAbstractSerialization(histogram);
        }

    }

    @Test
    public void testCopy() {
        AbstractHistogram histogram =
                constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 31000);
        assertEqual(histogram, histogram.copy());
    }

    @Test
    public void testScaledCopy() {
        AbstractHistogram histogram =
                constructHistogram(histoClass,1000, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copy of scaled Histogram:");
        assertEqual(histogram, histogram.copy());
    }

    @Test
    public void testCopyInto() {
        AbstractHistogram histogram =
                constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
        AbstractHistogram targetHistogram =
                constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copyInto for Histogram:");
        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);

        histogram.recordValue(testValueLevel * 20);

        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);
    }

    @Test
    public void testScaledCopyInto() {
        AbstractHistogram histogram =
                constructHistogram(histoClass, 1000, highestTrackableValue, numberOfSignificantValueDigits);
        AbstractHistogram targetHistogram =
                constructHistogram(histoClass, 1000, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copyInto for scaled Histogram:");
        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);

        histogram.recordValue(testValueLevel * 20);

        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);
    }

    public static void verifyMaxValue(AbstractHistogram histogram) {
        long computedMaxValue = 0;
        for (int i = 0; i < histogram.countsArrayLength; i++) {
            if (histogram.getCountAtIndex(i) > 0) {
                computedMaxValue = histogram.valueFromIndex(i);
            }
        }
        computedMaxValue = (computedMaxValue == 0) ? 0 : histogram.highestEquivalentValue(computedMaxValue);
        Assert.assertEquals(computedMaxValue, histogram.getMaxValue());
    }

}
