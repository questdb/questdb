/**
 * HistogramTest.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.std.histogram.org.HdrHistogram.*;
import org.junit.Assert;

import java.io.*;
import java.util.zip.Deflater;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructHistogram;

/**
 * JUnit test for Histogram
 */
public class HistogramTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units
    static final int numberOfSignificantValueDigits = 3;
    static final long testValueLevel = 4;

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testConstructionArgumentRanges(Class<?> histoClass) {
        boolean thrown = false;
        AbstractHistogram histogram = null;

        try {
            // This should throw:
            // histogram = new Histogram(1, numberOfSignificantValueDigits);
            histogram = constructHistogram(histoClass, 1, numberOfSignificantValueDigits);
        } catch (IllegalArgumentException e) {
            thrown = true;
        }
        Assertions.assertTrue(thrown);
        Assertions.assertNull(histogram);

        thrown = false;
        try {
            // This should throw:
            // histogram = new Histogram(highestTrackableValue, 6);
            histogram = constructHistogram(histoClass, highestTrackableValue, 6);
        } catch (IllegalArgumentException e) {
            thrown = true;
        }
        Assertions.assertTrue(thrown);
        Assertions.assertNull(histogram);

        thrown = false;
        try {
            // This should throw:
            // histogram = new Histogram(highestTrackableValue, -1);
            histogram = constructHistogram(histoClass, highestTrackableValue, -1);
        } catch (IllegalArgumentException e) {
            thrown = true;
        }
        Assertions.assertTrue(thrown);
        Assertions.assertNull(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testUnitMagnitude0IndexCalculations(Class<?> histoClass) {
        // Histogram h = new Histogram(1L, 1L << 32, 3);
        AbstractHistogram h = constructHistogram(histoClass, 1L, 1L << 32, 3);
        Assertions.assertEquals(2048, h.subBucketCount);
        Assertions.assertEquals(0, h.unitMagnitude);
        // subBucketCount = 2^11, so 2^11 << 22 is > the max of 2^32 for 23 buckets total
        Assertions.assertEquals(23, h.bucketCount);

        // first half of first bucket
        Assertions.assertEquals(0, h.getBucketIndex(3));
        Assertions.assertEquals(3, h.getSubBucketIndex(3, 0));

        // second half of first bucket
        Assertions.assertEquals(0, h.getBucketIndex(1024 + 3));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex(1024 + 3, 0));

        // second bucket (top half)
        Assertions.assertEquals(1, h.getBucketIndex(2048 + 3 * 2));
        // counting by 2s, starting at halfway through the bucket
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex(2048 + 3 * 2, 1));

        // third bucket (top half)
        Assertions.assertEquals(2, h.getBucketIndex((2048 << 1) + 3 * 4));
        // counting by 4s, starting at halfway through the bucket
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex((2048 << 1) + 3 * 4, 2));

        // past last bucket -- not near Long.MAX_VALUE, so should still calculate ok.
        Assertions.assertEquals(23, h.getBucketIndex((2048L << 22) + 3 * (1 << 23)));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex((2048L << 22) + 3 * (1 << 23), 23));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testUnitMagnitude4IndexCalculations(Class<?> histoClass) {
        // Histogram h = new Histogram(1L << 12, 1L << 32, 3);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 12, 1L << 32, 3);
        Assertions.assertEquals(2048, h.subBucketCount);
        Assertions.assertEquals(12, h.unitMagnitude);
        // subBucketCount = 2^11. With unit magnitude shift, it's 2^23. 2^23 << 10 is > the max of 2^32 for 11 buckets
        // total
        Assertions.assertEquals(11, h.bucketCount);
        long unit = 1L << 12;

        // below lowest value
        Assertions.assertEquals(0, h.getBucketIndex(3));
        Assertions.assertEquals(0, h.getSubBucketIndex(3, 0));

        // first half of first bucket
        Assertions.assertEquals(0, h.getBucketIndex(3 * unit));
        Assertions.assertEquals(3, h.getSubBucketIndex(3 * unit, 0));

        // second half of first bucket
        // subBucketHalfCount's worth of units, plus 3 more
        Assertions.assertEquals(0, h.getBucketIndex(unit * (1024 + 3)));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex(unit * (1024 + 3), 0));

        // second bucket (top half), bucket scale = unit << 1.
        // Middle of bucket is (subBucketHalfCount = 2^10) of bucket scale, = unit << 11.
        // Add on 3 of bucket scale.
        Assertions.assertEquals(1, h.getBucketIndex((unit << 11) + 3 * (unit << 1)));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 11) + 3 * (unit << 1), 1));

        // third bucket (top half), bucket scale = unit << 2.
        // Middle of bucket is (subBucketHalfCount = 2^10) of bucket scale, = unit << 12.
        // Add on 3 of bucket scale.
        Assertions.assertEquals(2, h.getBucketIndex((unit << 12) + 3 * (unit << 2)));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 12) + 3 * (unit << 2), 2));

        // past last bucket -- not near Long.MAX_VALUE, so should still calculate ok.
        Assertions.assertEquals(11, h.getBucketIndex((unit << 21) + 3 * (unit << 11)));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 21) + 3 * (unit << 11), 11));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testUnitMagnitude51SubBucketMagnitude11IndexCalculations(Class<?> histoClass) {
        // maximum unit magnitude for this precision
        // Histogram h = new Histogram(1L << 51, Long.MAX_VALUE, 3);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 51, Long.MAX_VALUE, 3);
        Assertions.assertEquals(2048, h.subBucketCount);
        Assertions.assertEquals(51, h.unitMagnitude);
        // subBucketCount = 2^11. With unit magnitude shift, it's 2^62. 1 more bucket to (almost) reach 2^63.
        Assertions.assertEquals(2, h.bucketCount);
        Assertions.assertEquals(2, h.leadingZeroCountBase);
        long unit = 1L << 51;

        // below lowest value
        Assertions.assertEquals(0, h.getBucketIndex(3));
        Assertions.assertEquals(0, h.getSubBucketIndex(3, 0));

        // first half of first bucket
        Assertions.assertEquals(0, h.getBucketIndex(3 * unit));
        Assertions.assertEquals(3, h.getSubBucketIndex(3 * unit, 0));

        // second half of first bucket
        // subBucketHalfCount's worth of units, plus 3 more
        Assertions.assertEquals(0, h.getBucketIndex(unit * (1024 + 3)));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex(unit * (1024 + 3), 0));

        // end of second half
        Assertions.assertEquals(0, h.getBucketIndex(unit * 1024 + 1023 * unit));
        Assertions.assertEquals(1024 + 1023, h.getSubBucketIndex(unit * 1024 + 1023 * unit, 0));

        // second bucket (top half), bucket scale = unit << 1.
        // Middle of bucket is (subBucketHalfCount = 2^10) of bucket scale, = unit << 11.
        // Add on 3 of bucket scale.
        Assertions.assertEquals(1, h.getBucketIndex((unit << 11) + 3 * (unit << 1)));
        Assertions.assertEquals(1024 + 3, h.getSubBucketIndex((unit << 11) + 3 * (unit << 1), 1));

        // upper half of second bucket, last slot
        Assertions.assertEquals(1, h.getBucketIndex(Long.MAX_VALUE));
        Assertions.assertEquals(1024 + 1023, h.getSubBucketIndex(Long.MAX_VALUE, 1));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testUnitMagnitude52SubBucketMagnitude11Throws(Class<?> histoClass) {
        try {
            // new Histogram(1L << 52, 1L << 62, 3);
            constructHistogram(histoClass, 1L << 52, 1L << 62, 3);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals("Cannot represent numberOfSignificantValueDigits worth of values beyond lowestDiscernibleValue", e.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testUnitMagnitude54SubBucketMagnitude8Ok(Class<?> histoClass) {
        // Histogram h = new Histogram(1L << 54, 1L << 62, 2);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 54, 1L << 62, 2);
        Assertions.assertEquals(256, h.subBucketCount);
        Assertions.assertEquals(54, h.unitMagnitude);
        // subBucketCount = 2^8. With unit magnitude shift, it's 2^62.
        Assertions.assertEquals(2, h.bucketCount);

        // below lowest value
        Assertions.assertEquals(0, h.getBucketIndex(3));
        Assertions.assertEquals(0, h.getSubBucketIndex(3, 0));

        // upper half of second bucket, last slot
        Assertions.assertEquals(1, h.getBucketIndex(Long.MAX_VALUE));
        Assertions.assertEquals(128 + 127, h.getSubBucketIndex(Long.MAX_VALUE, 1));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testUnitMagnitude61SubBucketMagnitude0Ok(Class<?> histoClass) {
        // Histogram h = new Histogram(1L << 61, 1L << 62, 0);
        AbstractHistogram h = constructHistogram(histoClass, 1L << 61, 1L << 62, 0);
        Assertions.assertEquals(2, h.subBucketCount);
        Assertions.assertEquals(61, h.unitMagnitude);
        // subBucketCount = 2^1. With unit magnitude shift, it's 2^62. 1 more bucket to be > the max of 2^62.
        Assertions.assertEquals(2, h.bucketCount);

        // below lowest value
        Assertions.assertEquals(0, h.getBucketIndex(3));
        Assertions.assertEquals(0, h.getSubBucketIndex(3, 0));

        // upper half of second bucket, last slot
        Assertions.assertEquals(1, h.getBucketIndex(Long.MAX_VALUE));
        Assertions.assertEquals(1, h.getSubBucketIndex(Long.MAX_VALUE, 1));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class
    })
    public void testEmptyHistogram(Class<?> histoClass) {
        // Histogram histogram = new Histogram(3);
        AbstractHistogram histogram = constructHistogram(histoClass, 3);
        long min = histogram.getMinValue();
        Assertions.assertEquals(0, min);
        long max = histogram.getMaxValue();
        Assertions.assertEquals(0, max);
        double mean = histogram.getMean();
        Assertions.assertEquals(0, mean, 0.0000000000001D);
        double stddev = histogram.getStdDeviation();
        Assertions.assertEquals(0, stddev, 0.0000000000001D);
        double pcnt = histogram.getPercentileAtOrBelowValue(0);
        Assertions.assertEquals(100.0, pcnt, 0.0000000000001D);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testConstructionArgumentGets(Class<?> histoClass) {
        // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
        AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
        Assertions.assertEquals(1, histogram.getLowestDiscernibleValue());
        Assertions.assertEquals(highestTrackableValue, histogram.getHighestTrackableValue());
        Assertions.assertEquals(numberOfSignificantValueDigits, histogram.getNumberOfSignificantValueDigits());
        // Histogram histogram2 = new Histogram(1000, highestTrackableValue, numberOfSignificantValueDigits);
        AbstractHistogram histogram2 = constructHistogram(histoClass, 1000, highestTrackableValue, numberOfSignificantValueDigits);
        Assertions.assertEquals(1000, histogram2.getLowestDiscernibleValue());
        verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            AtomicHistogram.class
    })
    public void testGetEstimatedFootprintInBytes(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
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
            Assertions.assertEquals(expectedSize, histogram.getEstimatedFootprintInBytes());
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testRecordValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            Assertions.assertEquals(1L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(1L, histogram.getTotalCount());
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testConstructionWithLargeNumbers(Class<?> histoClass) {
            // Histogram histogram = new Histogram(20000000, 100000000, 5);
            AbstractHistogram histogram = constructHistogram(histoClass, 20000000, 100000000, 5);
            histogram.recordValue(100000000);
            histogram.recordValue(20000000);
            histogram.recordValue(30000000);
            Assertions.assertTrue(histogram.valuesAreEquivalent(20000000, histogram.getValueAtPercentile(50.0)));
            Assertions.assertTrue(histogram.valuesAreEquivalent(30000000, histogram.getValueAtPercentile(50.0)));
            Assertions.assertTrue(histogram.valuesAreEquivalent(100000000, histogram.getValueAtPercentile(83.33)));
            Assertions.assertTrue(histogram.valuesAreEquivalent(100000000, histogram.getValueAtPercentile(83.34)));
            Assertions.assertTrue(histogram.valuesAreEquivalent(100000000, histogram.getValueAtPercentile(99.0)));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testValueAtPercentileMatchesPercentile(Class<?> histoClass) {
            // Histogram histogram = new Histogram(1, Long.MAX_VALUE, 3);
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
                    Assertions.assertTrue(histogram.valuesAreEquivalent(value, lookupValue), "length:" + length + " value: " + value + " calculatedPercentile:" + calculatedPercentile +
                                    " getValueAtPercentile(" + calculatedPercentile + ") = " + lookupValue +
                                    " [should be " + value + "]");
                }
            }
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testValueAtPercentileMatchesPercentileIter(Class<?> histoClass) {
            // Histogram histogram = new Histogram(1, Long.MAX_VALUE, 3);
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
                    Assertions.assertTrue(histogram.valuesAreEquivalent(calculatedValue, iterValue), "length:" + length + " percentile: " + v.getPercentile() +
                                    " calculatedValue:" + calculatedValue + " iterValue:" + iterValue +
                                    "[should be " + calculatedValue + "]");
                    Assertions.assertTrue(histogram.valuesAreEquivalent(calculatedValue, iterValue));
                }
            }
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testRecordValueWithExpectedInterval(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValueWithExpectedInterval(testValueLevel, testValueLevel / 4);
            // Histogram rawHistogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram rawHistogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            rawHistogram.recordValue(testValueLevel);
            // The data will include corrected samples:
            Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel) / 4));
            Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 2) / 4));
            Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 3) / 4));
            Assertions.assertEquals(1L, histogram.getCountAtValue((testValueLevel * 4) / 4));
            Assertions.assertEquals(4L, histogram.getTotalCount());
            // But the raw data will not:
            Assertions.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel) / 4));
            Assertions.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 2) / 4));
            Assertions.assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 3) / 4));
            Assertions.assertEquals(1L, rawHistogram.getCountAtValue((testValueLevel * 4) / 4));
            Assertions.assertEquals(1L, rawHistogram.getTotalCount());

            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testReset(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(10);
            histogram.recordValue(100);
            Assertions.assertEquals(histogram.getMinValue(), Math.min(10, testValueLevel));
            Assertions.assertEquals(histogram.getMaxValue(), Math.max(100, testValueLevel));
            histogram.reset();
            Assertions.assertEquals(0L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(0L, histogram.getTotalCount());
            verifyMaxValue(histogram);
            histogram.recordValue(20);
            histogram.recordValue(80);
            Assertions.assertEquals(histogram.getMinValue(), 20);
            Assertions.assertEquals(histogram.getMaxValue(), 80);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testAdd(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            // Histogram other = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram other = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            other.recordValue(testValueLevel);
            other.recordValue(testValueLevel * 1000);
            histogram.add(other);
            Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(4L, histogram.getTotalCount());

            // Histogram biggerOther = new Histogram(highestTrackableValue * 2, numberOfSignificantValueDigits);
            AbstractHistogram biggerOther = constructHistogram(histoClass, highestTrackableValue * 2, numberOfSignificantValueDigits);
            biggerOther.recordValue(testValueLevel);
            biggerOther.recordValue(testValueLevel * 1000);
            biggerOther.recordValue(highestTrackableValue * 2);

            // Adding the smaller histogram to the bigger one should work:
            biggerOther.add(histogram);
            Assertions.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel));
            Assertions.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(1L, biggerOther.getCountAtValue(highestTrackableValue * 2)); // overflow smaller hist...
            Assertions.assertEquals(7L, biggerOther.getTotalCount());

            // But trying to add a larger histogram into a smaller one should throw an AIOOB:
            boolean thrown = false;
            try {
                // This should throw:
                histogram.add(biggerOther);
            } catch (ArrayIndexOutOfBoundsException e) {
                thrown = true;
            }
            Assertions.assertTrue(thrown);

            verifyMaxValue(histogram);
            verifyMaxValue(other);
            verifyMaxValue(biggerOther);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testSubtractAfterAdd(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            // Histogram other = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram other = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            other.recordValue(testValueLevel);
            other.recordValue(testValueLevel * 1000);
            histogram.add(other);
            Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(4L, histogram.getTotalCount());
            histogram.add(other);
            Assertions.assertEquals(3L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(3L, histogram.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(6L, histogram.getTotalCount());
            histogram.subtract(other);
            Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(4L, histogram.getTotalCount());

            verifyMaxValue(histogram);
            verifyMaxValue(other);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testSubtractToZeroCounts(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            Assertions.assertEquals(1L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(1L, histogram.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(2L, histogram.getTotalCount());

            // Subtracting down to zero counts should work:
            histogram.subtract(histogram);
            Assertions.assertEquals(0L, histogram.getCountAtValue(testValueLevel));
            Assertions.assertEquals(0L, histogram.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(0L, histogram.getTotalCount());

            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testSubtractToNegativeCountsThrows(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            // Histogram other = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram other = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);
            other.recordValueWithCount(testValueLevel, 2);
            other.recordValueWithCount(testValueLevel * 1000, 2);

            try {
                histogram.subtract(other);
                Assertions.fail();
            } catch (IllegalArgumentException e) {
                // should throw
            }

            verifyMaxValue(histogram);
            verifyMaxValue(other);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testSubtractSubtrahendValuesOutsideMinuendRangeThrows(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);

            // Histogram biggerOther = new Histogram(highestTrackableValue * 2, numberOfSignificantValueDigits);
            AbstractHistogram biggerOther = constructHistogram(histoClass, highestTrackableValue * 2, numberOfSignificantValueDigits);
            biggerOther.recordValue(testValueLevel);
            biggerOther.recordValue(testValueLevel * 1000);
            biggerOther.recordValue(highestTrackableValue * 2); // outside smaller histogram's range

            try {
                histogram.subtract(biggerOther);
                Assertions.fail();
            } catch (IllegalArgumentException e) {
                // should throw
            }

            verifyMaxValue(histogram);
            verifyMaxValue(biggerOther);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testSubtractSubtrahendValuesInsideMinuendRangeWorks(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.recordValue(testValueLevel);
            histogram.recordValue(testValueLevel * 1000);

            // Histogram biggerOther = new Histogram(highestTrackableValue * 2, numberOfSignificantValueDigits);
            AbstractHistogram biggerOther = constructHistogram(histoClass, highestTrackableValue * 2, numberOfSignificantValueDigits);
            biggerOther.recordValue(testValueLevel);
            biggerOther.recordValue(testValueLevel * 1000);
            biggerOther.recordValue(highestTrackableValue * 2);
            biggerOther.add(biggerOther);
            biggerOther.add(biggerOther);
            Assertions.assertEquals(4L, biggerOther.getCountAtValue(testValueLevel));
            Assertions.assertEquals(4L, biggerOther.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(4L, biggerOther.getCountAtValue(highestTrackableValue * 2)); // overflow smaller hist...
            Assertions.assertEquals(12L, biggerOther.getTotalCount());

            // Subtracting the smaller histogram from the bigger one should work:
            biggerOther.subtract(histogram);
            Assertions.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel));
            Assertions.assertEquals(3L, biggerOther.getCountAtValue(testValueLevel * 1000));
            Assertions.assertEquals(4L, biggerOther.getCountAtValue(highestTrackableValue * 2)); // overflow smaller hist...
            Assertions.assertEquals(10L, biggerOther.getTotalCount());

            verifyMaxValue(histogram);
            verifyMaxValue(biggerOther);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testSizeOfEquivalentValueRange(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(1, histogram.sizeOfEquivalentValueRange(1), "Size of equivalent range for value 1 is 1");
            Assertions.assertEquals(1, histogram.sizeOfEquivalentValueRange(1025), "Size of equivalent range for value 1025 is 1");
            Assertions.assertEquals(1, histogram.sizeOfEquivalentValueRange(2047), "Size of equivalent range for value 2047 is 1");
            Assertions.assertEquals(2, histogram.sizeOfEquivalentValueRange(2048), "Size of equivalent range for value 2048 is 2");
            Assertions.assertEquals(2, histogram.sizeOfEquivalentValueRange(2500), "Size of equivalent range for value 2500 is 2");
            Assertions.assertEquals(4, histogram.sizeOfEquivalentValueRange(8191), "Size of equivalent range for value 8191 is 4");
            Assertions.assertEquals(8, histogram.sizeOfEquivalentValueRange(8192), "Size of equivalent range for value 8192 is 8");
            Assertions.assertEquals(8, histogram.sizeOfEquivalentValueRange(10000), "Size of equivalent range for value 10000 is 8");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testScaledSizeOfEquivalentValueRange(Class<?> histoClass) {
            // Histogram histogram = new Histogram(1024, highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(1024, histogram.sizeOfEquivalentValueRange(1024), "Size of equivalent range for value 1 * 1024 is 1 * 1024");
            Assertions.assertEquals(2 * 1024, histogram.sizeOfEquivalentValueRange(2500 * 1024), "Size of equivalent range for value 2500 * 1024 is 2 * 1024");
            Assertions.assertEquals(4 * 1024, histogram.sizeOfEquivalentValueRange(8191 * 1024), "Size of equivalent range for value 8191 * 1024 is 4 * 1024");
            Assertions.assertEquals(8 * 1024, histogram.sizeOfEquivalentValueRange(8192 * 1024), "Size of equivalent range for value 8192 * 1024 is 8 * 1024");
            Assertions.assertEquals(8 * 1024, histogram.sizeOfEquivalentValueRange(10000 * 1024), "Size of equivalent range for value 10000 * 1024 is 8 * 1024");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testLowestEquivalentValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(10000, histogram.lowestEquivalentValue(10007), "The lowest equivalent value to 10007 is 10000");
            Assertions.assertEquals(10008, histogram.lowestEquivalentValue(10009), "The lowest equivalent value to 10009 is 10008");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testScaledLowestEquivalentValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(1024, highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(10000 * 1024, histogram.lowestEquivalentValue(10007 * 1024), "The lowest equivalent value to 10007 * 1024 is 10000 * 1024");
            Assertions.assertEquals(10008 * 1024, histogram.lowestEquivalentValue(10009 * 1024), "The lowest equivalent value to 10009 * 1024 is 10008 * 1024");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testHighestEquivalentValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(1024, highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(8183 * 1024 + 1023, histogram.highestEquivalentValue(8180 * 1024), "The highest equivalent value to 8180 * 1024 is 8183 * 1024 + 1023");
            Assertions.assertEquals(8191 * 1024 + 1023, histogram.highestEquivalentValue(8191 * 1024), "The highest equivalent value to 8187 * 1024 is 8191 * 1024 + 1023");
            Assertions.assertEquals(8199 * 1024 + 1023, histogram.highestEquivalentValue(8193 * 1024), "The highest equivalent value to 8193 * 1024 is 8199 * 1024 + 1023");
            Assertions.assertEquals(9999 * 1024 + 1023, histogram.highestEquivalentValue(9995 * 1024), "The highest equivalent value to 9995 * 1024 is 9999 * 1024 + 1023");
            Assertions.assertEquals(10007 * 1024 + 1023, histogram.highestEquivalentValue(10007 * 1024), "The highest equivalent value to 10007 * 1024 is 10007 * 1024 + 1023");
            Assertions.assertEquals(10015 * 1024 + 1023, histogram.highestEquivalentValue(10008 * 1024), "The highest equivalent value to 10008 * 1024 is 10015 * 1024 + 1023");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testScaledHighestEquivalentValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(8183, histogram.highestEquivalentValue(8180), "The highest equivalent value to 8180 is 8183");
            Assertions.assertEquals(8191, histogram.highestEquivalentValue(8191), "The highest equivalent value to 8187 is 8191");
            Assertions.assertEquals(8199, histogram.highestEquivalentValue(8193), "The highest equivalent value to 8193 is 8199");
            Assertions.assertEquals(9999, histogram.highestEquivalentValue(9995), "The highest equivalent value to 9995 is 9999");
            Assertions.assertEquals(10007, histogram.highestEquivalentValue(10007), "The highest equivalent value to 10007 is 10007");
            Assertions.assertEquals(10015, histogram.highestEquivalentValue(10008), "The highest equivalent value to 10008 is 10015");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testMedianEquivalentValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(4, histogram.medianEquivalentValue(4), "The median equivalent value to 4 is 4");
            Assertions.assertEquals(5, histogram.medianEquivalentValue(5), "The median equivalent value to 5 is 5");
            Assertions.assertEquals(4001, histogram.medianEquivalentValue(4000), "The median equivalent value to 4000 is 4001");
            Assertions.assertEquals(8002, histogram.medianEquivalentValue(8000), "The median equivalent value to 8000 is 8002");
            Assertions.assertEquals(10004, histogram.medianEquivalentValue(10007), "The median equivalent value to 10007 is 10004");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testScaledMedianEquivalentValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(1024, highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, 1024, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertEquals(4 * 1024 + 512, histogram.medianEquivalentValue(4 * 1024), "The median equivalent value to 4 * 1024 is 4 * 1024 + 512");
            Assertions.assertEquals(5 * 1024 + 512, histogram.medianEquivalentValue(5 * 1024), "The median equivalent value to 5 * 1024 is 5 * 1024 + 512");
            Assertions.assertEquals(4001 * 1024, histogram.medianEquivalentValue(4000 * 1024), "The median equivalent value to 4000 * 1024 is 4001 * 1024");
            Assertions.assertEquals(8002 * 1024, histogram.medianEquivalentValue(8000 * 1024), "The median equivalent value to 8000 * 1024 is 8002 * 1024");
            Assertions.assertEquals(10004 * 1024, histogram.medianEquivalentValue(10007 * 1024), "The median equivalent value to 10007 * 1024 is 10004 * 1024");
            verifyMaxValue(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testNextNonEquivalentValue(Class<?> histoClass) {
            // Histogram histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
            Assertions.assertNotSame(null, histogram);
    }

    void testAbstractSerialization(AbstractHistogram histogram) throws Exception {
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 255);
        if (histogram.supportsAutoResize()) {
            histogram.setAutoResize(true);
            Assertions.assertTrue(histogram.isAutoResize());
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
        Assertions.assertNotNull(newHistogram);
        assertEqual(histogram, newHistogram);
        Assertions.assertEquals(histogram, newHistogram);
        if (histogram.supportsAutoResize()) {
            Assertions.assertTrue(histogram.isAutoResize());
        }
        Assertions.assertEquals(newHistogram.isAutoResize(), histogram.isAutoResize());
        Assertions.assertEquals(histogram.hashCode(), newHistogram.hashCode());
        Assertions.assertEquals(histogram.getNeededByteBufferCapacity(), newHistogram.copy().getNeededByteBufferCapacity());
        Assertions.assertEquals(histogram.getNeededByteBufferCapacity(), newHistogram.getNeededByteBufferCapacity());
    }

    private void assertEqual(AbstractHistogram expectedHistogram, AbstractHistogram actualHistogram) {
        Assertions.assertEquals(expectedHistogram, actualHistogram);
        Assertions.assertEquals(expectedHistogram.getCountAtValue(testValueLevel), actualHistogram.getCountAtValue(testValueLevel));
        Assertions.assertEquals(expectedHistogram.getCountAtValue(testValueLevel * 10), actualHistogram.getCountAtValue(testValueLevel * 10));
        Assertions.assertEquals(expectedHistogram.getTotalCount(), actualHistogram.getTotalCount());
        verifyMaxValue(expectedHistogram);
        verifyMaxValue(actualHistogram);
    }

    @ParameterizedTest
    @CsvSource({
            "Histogram, 3",
            "Histogram, 2",
            "ConcurrentHistogram, 3",
            "ConcurrentHistogram, 2",
            "AtomicHistogram, 3",
            "AtomicHistogram, 2"
    })
    public void testSerialization(ArgumentsAccessor arguments) throws Exception {
        Class<?> histoClass = Class.forName("io.questdb.std.histogram.org.HdrHistogram." + arguments.getString(0));
        int digits = arguments.getInteger(1);

        AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, digits);
        testAbstractSerialization(histogram);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testCopy(Class<?> histoClass) {
        AbstractHistogram histogram =
                constructHistogram(histoClass, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 31000);
        assertEqual(histogram, histogram.copy());
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testScaledCopy(Class<?> histoClass) {
        AbstractHistogram histogram =
                constructHistogram(histoClass,1000, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copy of scaled Histogram:");
        assertEqual(histogram, histogram.copy());
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testCopyInto(Class<?> histoClass) {
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

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class,
            AtomicHistogram.class
    })
    public void testScaledCopyInto(Class<?> histoClass) {
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

    public void verifyMaxValue(AbstractHistogram histogram) {
        long computedMaxValue = 0;
        for (int i = 0; i < histogram.countsArrayLength; i++) {
            if (histogram.getCountAtIndex(i) > 0) {
                computedMaxValue = histogram.valueFromIndex(i);
            }
        }
        computedMaxValue = (computedMaxValue == 0) ? 0 : histogram.highestEquivalentValue(computedMaxValue);
        Assertions.assertEquals(computedMaxValue, histogram.getMaxValue());
    }

}
