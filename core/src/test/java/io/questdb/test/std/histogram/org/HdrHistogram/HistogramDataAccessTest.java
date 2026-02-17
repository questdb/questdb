/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/
//
// @author Gil Tene

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.cairo.CairoException;
import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue;
import io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit test for {@link io.questdb.std.histogram.org.HdrHistogram.Histogram}
 */
public class HistogramDataAccessTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // 1 hour in usec units
    static final Histogram histogram;
    static final int numberOfSignificantValueDigits = 3; // Maintain at least 3 decimal points of accuracy
    static final Histogram postCorrectedHistogram;
    static final Histogram postCorrectedScaledHistogram;
    static final Histogram rawHistogram;
    static final Histogram scaledHistogram;
    static final Histogram scaledRawHistogram;

    @Test
    public void scanLinearIteratorForAIOOB() {
        List<int[]> broken = new ArrayList<>();
        // scan iterators through a range of step sizes and recorded values, looking for AIOOB:
        for (int step = 1; step < 100; step++) {
            for (int value = 1; value < 1000; value++) {
                try {
                    recordOneValueAndDisplayLinearBuckets(value, step);
                } catch (CairoException e) {
                    broken.add(new int[]{value, step});
                }
            }
        }
        Assert.assertEquals("should not have any AIOOB iterations", 0, broken.size());
        for (int[] brk : broken) {
            System.out.println("broken: value=" + brk[0] + " step=" + brk[1]);
        }
    }

    @Test
    public void testAllValues() {
        int index = 0;
        long latestValueAtIndex;
        long totalCountToThisPoint = 0;
        long totalValueToThisPoint = 0;
        // Iterate raw data by stepping through every value that has a count recorded:
        for (HistogramIterationValue v : rawHistogram.allValues()) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 1000) {
                Assert.assertEquals("Raw allValues bucket # 0 added a count of 10000",
                        10000, countAddedInThisBucket);
            } else if (histogram.valuesAreEquivalent(v.getValueIteratedTo(), 100000000)) {
                Assert.assertEquals("Raw allValues value bucket # " + index + " added a count of 1",
                        1, countAddedInThisBucket);
            } else {
                Assert.assertEquals("Raw allValues value bucket # " + index + " added a count of 0",
                        0, countAddedInThisBucket);
            }
            latestValueAtIndex = v.getValueIteratedTo();
            totalCountToThisPoint += v.getCountAtValueIteratedTo();
            Assert.assertEquals("total Count should match", totalCountToThisPoint, v.getTotalCountToThisValue());
            totalValueToThisPoint += v.getCountAtValueIteratedTo() * latestValueAtIndex;
            Assert.assertEquals("total Value should match", totalValueToThisPoint, v.getTotalValueToThisValue());
            index++;
        }
        Assert.assertEquals("index should be equal to countsArrayLength", histogram.countsArrayLength(), index);

        index = 0;
        long totalAddedCounts = 0;
        for (HistogramIterationValue v : histogram.allValues()) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 1000) {
                Assert.assertEquals("AllValues bucket # 0 [" +
                                v.getValueIteratedFrom() + ".." + v.getValueIteratedTo() +
                                "] added a count of 10000",
                        10000, countAddedInThisBucket);
            }
            Assert.assertEquals("The count in AllValues bucket #" + index +
                            " is exactly the amount added since the last iteration ",
                    v.getCountAtValueIteratedTo(), v.getCountAddedInThisIterationStep());
            totalAddedCounts += v.getCountAddedInThisIterationStep();
            Assert.assertTrue("valueFromIndex(index) should be equal to getValueIteratedTo()",
                    histogram.valuesAreEquivalent(histogram.valueFromIndex(index), v.getValueIteratedTo()));
            index++;
        }
        Assert.assertEquals("index should be equal to countsArrayLength", histogram.countsArrayLength(), index);
        Assert.assertEquals("Total added counts should be 20000", 20000, totalAddedCounts);
    }

    @Test
    public void testGetCountAtValue() {
        Assert.assertEquals("Count of raw values at 10 msec is 0",
                0, rawHistogram.getCountBetweenValues(10000L, 10010L));
        Assert.assertEquals("Count of values at 10 msec is 0",
                1, histogram.getCountBetweenValues(10000L, 10010L));
        Assert.assertEquals("Count of raw values at 1 msec is 10,000",
                10000, rawHistogram.getCountAtValue(1000L));
        Assert.assertEquals("Count of values at 1 msec is 10,000",
                10000, histogram.getCountAtValue(1000L));
    }

    @Test
    public void testGetCountBetweenValues() {
        Assert.assertEquals("Count of raw values between 1 msec and 1 msec is 1",
                10000, rawHistogram.getCountBetweenValues(1000L, 1000L));
        Assert.assertEquals("Count of raw values between 5 msec and 150 sec is 1",
                1, rawHistogram.getCountBetweenValues(5000L, 150000000L));
        Assert.assertEquals("Count of values between 5 msec and 150 sec is 10,000",
                10000, histogram.getCountBetweenValues(5000L, 150000000L));
    }

    @Test
    public void testGetMaxValue() {
        Assert.assertTrue(
                histogram.valuesAreEquivalent(100L * 1000 * 1000,
                        histogram.getMaxValue()));
    }

    @Test
    public void testGetMean() {
        double expectedRawMean = ((10000.0 * 1000) + (1.0 * 100000000)) / 10001; /* direct avg. of raw results */
        double expectedMean = (1000.0 + 50000000.0) / 2; /* avg. 1 msec for half the time, and 50 sec for other half */
        // We expect to see the mean to be accurate to ~3 decimal points (~0.1%):
        Assert.assertEquals("Raw mean is " + expectedRawMean + " +/- 0.1%",
                expectedRawMean, rawHistogram.getMean(), expectedRawMean * 0.001);
        Assert.assertEquals("Mean is " + expectedMean + " +/- 0.1%",
                expectedMean, histogram.getMean(), expectedMean * 0.001);
    }

    @Test
    public void testGetMinValue() {
        Assert.assertTrue(
                histogram.valuesAreEquivalent(1000,
                        histogram.getMinValue()));
    }

    @Test
    public void testGetPercentileAtOrBelowValue() {
        Assert.assertEquals("Raw percentile at or below 5 msec is 99.99% +/- 0.0001",
                99.99,
                rawHistogram.getPercentileAtOrBelowValue(5000), 0.0001);
        Assert.assertEquals("Percentile at or below 5 msec is 50% +/- 0.0001%",
                50.0,
                histogram.getPercentileAtOrBelowValue(5000), 0.0001);
        Assert.assertEquals("Percentile at or below 100 sec is 100% +/- 0.0001%",
                100.0,
                histogram.getPercentileAtOrBelowValue(100000000L), 0.0001);
    }

    @Test
    public void testGetStdDeviation() {
        double expectedRawMean = ((10000.0 * 1000) + (1.0 * 100000000)) / 10001; /* direct avg. of raw results */
        double expectedRawStdDev =
                Math.sqrt(
                        ((10000.0 * Math.pow((1000.0 - expectedRawMean), 2)) +
                                Math.pow((100000000.0 - expectedRawMean), 2)) /
                                10001);

        double expectedMean = (1000.0 + 50000000.0) / 2; /* avg. 1 msec for half the time, and 50 sec for other half */
        double expectedSquareDeviationSum = 10000 * Math.pow((1000.0 - expectedMean), 2);
        for (long value = 10000; value <= 100000000; value += 10000) {
            expectedSquareDeviationSum += Math.pow((value - expectedMean), 2);
        }
        double expectedStdDev = Math.sqrt(expectedSquareDeviationSum / 20000);

        // We expect to see the standard deviations to be accurate to ~3 decimal points (~0.1%):
        Assert.assertEquals("Raw standard deviation is " + expectedRawStdDev + " +/- 0.1%",
                expectedRawStdDev, rawHistogram.getStdDeviation(), expectedRawStdDev * 0.001);
        Assert.assertEquals("Standard deviation is " + expectedStdDev + " +/- 0.1%",
                expectedStdDev, histogram.getStdDeviation(), expectedStdDev * 0.001);
    }

    @Test
    public void testGetTotalCount() {
        // The overflow value should count in the total count:
        Assert.assertEquals("Raw total count is 10,001",
                10001L, rawHistogram.getTotalCount());
        Assert.assertEquals("Total count is 20,000",
                20000L, histogram.getTotalCount());
    }

    @Test
    public void testGetValueAtPercentile() {
        Assert.assertEquals("raw 30%'ile is 1 msec +/- 0.1%",
                1000.0, (double) rawHistogram.getValueAtPercentile(30.0),
                1000.0 * 0.001);
        Assert.assertEquals("raw 99%'ile is 1 msec +/- 0.1%",
                1000.0, (double) rawHistogram.getValueAtPercentile(99.0),
                1000.0 * 0.001);
        Assert.assertEquals("raw 99.99%'ile is 1 msec +/- 0.1%",
                1000.0, (double) rawHistogram.getValueAtPercentile(99.99)
                , 1000.0 * 0.001);
        Assert.assertEquals("raw 99.999%'ile is 100 sec +/- 0.1%",
                100000000.0, (double) rawHistogram.getValueAtPercentile(99.999),
                100000000.0 * 0.001);
        Assert.assertEquals("raw 100%'ile is 100 sec +/- 0.1%",
                100000000.0, (double) rawHistogram.getValueAtPercentile(100.0),
                100000000.0 * 0.001);

        Assert.assertEquals("30%'ile is 1 msec +/- 0.1%",
                1000.0, (double) histogram.getValueAtPercentile(30.0),
                1000.0 * 0.001);
        Assert.assertEquals("50%'ile is 1 msec +/- 0.1%",
                1000.0, (double) histogram.getValueAtPercentile(50.0),
                1000.0 * 0.001);
        Assert.assertEquals("75%'ile is 50 sec +/- 0.1%",
                50000000.0, (double) histogram.getValueAtPercentile(75.0),
                50000000.0 * 0.001);
        Assert.assertEquals("90%'ile is 80 sec +/- 0.1%",
                80000000.0, (double) histogram.getValueAtPercentile(90.0),
                80000000.0 * 0.001);
        Assert.assertEquals("99%'ile is 98 sec +/- 0.1%",
                98000000.0, (double) histogram.getValueAtPercentile(99.0),
                98000000.0 * 0.001);
        Assert.assertEquals("99.999%'ile is 100 sec +/- 0.1%",
                100000000.0, (double) histogram.getValueAtPercentile(99.999),
                100000000.0 * 0.001);
        Assert.assertEquals("100%'ile is 100 sec +/- 0.1%",
                100000000.0, (double) histogram.getValueAtPercentile(100.0),
                100000000.0 * 0.001);
    }

    @Test
    public void testGetValueAtPercentileExamples() {
        Histogram hist = new Histogram(3600000000L, 3);
        hist.recordValue(1);
        hist.recordValue(2);

        Assert.assertEquals("50.0%'ile is 1",
                1, hist.getValueAtPercentile(50.0));
        Assert.assertEquals("50.00000000000001%'ile is 1",
                1, hist.getValueAtPercentile(50.00000000000001));
        Assert.assertEquals("50.0000000000001%'ile is 2",
                2, hist.getValueAtPercentile(50.0000000000001));

        hist.recordValue(2);
        hist.recordValue(2);
        hist.recordValue(2);

        Assert.assertEquals("25%'ile is 2",
                2, hist.getValueAtPercentile(25));
        Assert.assertEquals("30%'ile is 2",
                2, hist.getValueAtPercentile(30));

    }

    @Test
    public void testGetValueAtPercentileForLargeHistogram() {
        long largestValue = 1000000000000L;
        Histogram h = new Histogram(largestValue, 5);
        h.recordValue(largestValue);

        Assert.assertTrue(h.getValueAtPercentile(100.0) > 0);
    }

    @Test
    public void testLinearBucketValues() {
        int index = 0;
        // Note that using linear buckets should work "as expected" as long as the number of linear buckets
        // is lower than the resolution level determined by largestValueWithSingleUnitResolution
        // (2000 in this case). Above that count, some of the linear buckets can end up rounded up in size
        // (to the nearest local resolution unit level), which can result in a smaller number of buckets that
        // expected covering the range.

        // Iterate raw data using linear buckets of 100 msec each.
        for (HistogramIterationValue v : rawHistogram.linearBucketValues(100000)) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 0) {
                Assert.assertEquals("Raw Linear 100 msec bucket # 0 added a count of 10000",
                        10000, countAddedInThisBucket);
            } else if (index == 999) {
                Assert.assertEquals("Raw Linear 100 msec bucket # 999 added a count of 1",
                        1, countAddedInThisBucket);
            } else {
                Assert.assertEquals("Raw Linear 100 msec bucket # " + index + " added a count of 0",
                        0, countAddedInThisBucket);
            }
            index++;
        }
        Assert.assertEquals(1000, index);

        index = 0;
        long totalAddedCounts = 0;
        // Iterate data using linear buckets of 10 msec each.
        for (HistogramIterationValue v : histogram.linearBucketValues(10000)) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 0) {
                Assert.assertEquals("Linear 1 sec bucket # 0 [" +
                                v.getValueIteratedFrom() + ".." + v.getValueIteratedTo() +
                                "] added a count of 10000",
                        10000, countAddedInThisBucket);
            }
            // Because value resolution is low enough (3 digits) that multiple linear buckets will end up
            // residing in a single value-equivalent range, some linear buckets will have counts of 2 or
            // more, and some will have 0 (when the first bucket in the equivalent range was the one that
            // got the total count bump).
            // However, we can still verify the sum of counts added in all the buckets...
            totalAddedCounts += v.getCountAddedInThisIterationStep();
            index++;
        }
        Assert.assertEquals("There should be 10000 linear buckets of size 10000 usec between 0 and 100 sec.",
                10000, index);
        Assert.assertEquals("Total added counts should be 20000", 20000, totalAddedCounts);

        index = 0;
        totalAddedCounts = 0;
        // Iterate data using linear buckets of 1 msec each.
        for (HistogramIterationValue v : histogram.linearBucketValues(1000)) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 1) {
                Assert.assertEquals("Linear 1 sec bucket # 0 [" +
                                v.getValueIteratedFrom() + ".." + v.getValueIteratedTo() +
                                "] added a count of 10000",
                        10000, countAddedInThisBucket);
            }
            // Because value resolution is low enough (3 digits) that multiple linear buckets will end up
            // residing in a single value-equivalent range, some linear buckets will have counts of 2 or
            // more, and some will have 0 (when the first bucket in the equivalent range was the one that
            // got the total count bump).
            // However, we can still verify the sum of counts added in all the buckets...
            totalAddedCounts += v.getCountAddedInThisIterationStep();
            index++;
        }
        // You may ask "why 100007 and not 100000?" for the value below? The answer is that at this fine
        // a linear stepping resolution, the final populated sub-bucket (at 100 seconds with 3 decimal
        // point resolution) is larger than our liner stepping, and holds more than one linear 1 msec
        // step in it.
        // Since we only know we're done with linear iteration when the next iteration step will step
        // out of the last populated bucket, there is not way to tell if the iteration should stop at
        // 100000 or 100007 steps. The proper thing to do is to run to the end of the sub-bucket quanta...
        Assert.assertEquals("There should be 100007 linear buckets of size 1000 usec between 0 and 100 sec.",
                100007, index);
        Assert.assertEquals("Total added counts should be 20000", 20000, totalAddedCounts);


    }

    @Test
    public void testLinearIteratorSteps() {
        IntCountsHistogram histogram = new IntCountsHistogram(2);
        histogram.recordValue(193);
        histogram.recordValue(0);
        histogram.recordValue(1);
        histogram.recordValue(64);
        histogram.recordValue(128);
        int step = 64;
        int stepCount = 0;
        for (HistogramIterationValue itValue : histogram.linearBucketValues(step)) {
            stepCount++;
            itValue.getCountAddedInThisIterationStep();
        }
        Assert.assertEquals("should see 4 steps", 4, stepCount);
    }

    @Test
    public void testLinearIteratorVisitsBucketsWiderThanStepSizeMultipleTimes() {
        Histogram h = new Histogram(1, Long.MAX_VALUE, 3);

        h.recordValue(1);
        h.recordValue(2047);
        // bucket size 2
        h.recordValue(2048);
        h.recordValue(2049);
        h.recordValue(4095);
        // bucket size 4
        h.recordValue(4096);
        h.recordValue(4097);
        h.recordValue(4098);
        h.recordValue(4099);
        // 2nd bucket in size 4
        h.recordValue(4100);

        // sadly verbose helper class to hang on to iteration information for later comparison
        class IteratorValueSnapshot {
            private final long count;
            private final long value;

            private IteratorValueSnapshot(HistogramIterationValue iv) {
                this.value = iv.getValueIteratedTo();
                this.count = iv.getCountAddedInThisIterationStep();
            }

            private IteratorValueSnapshot(long value, long count) {
                this.value = value;
                this.count = count;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                IteratorValueSnapshot that = (IteratorValueSnapshot) o;

                if (value != that.value) {
                    return false;
                }
                return count == that.count;
            }

            @Override
            public int hashCode() {
                int result = (int) (value ^ (value >>> 32));
                result = 31 * result + (int) (count ^ (count >>> 32));
                return result;
            }

            @Override
            public String toString() {
                return "IteratorValueSnapshot{" +
                        "value=" + value +
                        ", count=" + count +
                        '}';
            }
        }

        List<IteratorValueSnapshot> snapshots = new ArrayList<>();

        for (HistogramIterationValue iv : h.linearBucketValues(1)) {
            snapshots.add(new IteratorValueSnapshot(iv));
        }

        // bucket size 1
        Assert.assertEquals(new IteratorValueSnapshot(0, 0), snapshots.get(0));
        Assert.assertEquals(new IteratorValueSnapshot(1, 1), snapshots.get(1));
        Assert.assertEquals(new IteratorValueSnapshot(2046, 0), snapshots.get(2046));
        Assert.assertEquals(new IteratorValueSnapshot(2047, 1), snapshots.get(2047));
        // bucket size 2
        Assert.assertEquals(new IteratorValueSnapshot(2048, 2), snapshots.get(2048));
        Assert.assertEquals(new IteratorValueSnapshot(2049, 0), snapshots.get(2049));
        Assert.assertEquals(new IteratorValueSnapshot(2050, 0), snapshots.get(2050));
        Assert.assertEquals(new IteratorValueSnapshot(2051, 0), snapshots.get(2051));
        Assert.assertEquals(new IteratorValueSnapshot(4094, 1), snapshots.get(4094));
        Assert.assertEquals(new IteratorValueSnapshot(4095, 0), snapshots.get(4095));
        // bucket size 4
        Assert.assertEquals(new IteratorValueSnapshot(4096, 4), snapshots.get(4096));
        Assert.assertEquals(new IteratorValueSnapshot(4097, 0), snapshots.get(4097));
        Assert.assertEquals(new IteratorValueSnapshot(4098, 0), snapshots.get(4098));
        Assert.assertEquals(new IteratorValueSnapshot(4099, 0), snapshots.get(4099));
        // also size 4, last bucket
        Assert.assertEquals(new IteratorValueSnapshot(4100, 1), snapshots.get(4100));
        Assert.assertEquals(new IteratorValueSnapshot(4101, 0), snapshots.get(4101));
        Assert.assertEquals(new IteratorValueSnapshot(4102, 0), snapshots.get(4102));
        Assert.assertEquals(new IteratorValueSnapshot(4103, 0), snapshots.get(4103));

        Assert.assertEquals(4104, snapshots.size());
    }

    @Test
    public void testLogarithmicBucketValues() {
        int index = 0;
        // Iterate raw data using logarithmic buckets starting at 10 msec.
        for (HistogramIterationValue v : rawHistogram.logarithmicBucketValues(10000, 2)) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 0) {
                Assert.assertEquals("Raw Logarithmic 10 msec bucket # 0 added a count of 10000",
                        10000, countAddedInThisBucket);
            } else if (index == 14) {
                Assert.assertEquals("Raw Logarithmic 10 msec bucket # 14 added a count of 1",
                        1, countAddedInThisBucket);
            } else {
                Assert.assertEquals("Raw Logarithmic 100 msec bucket # " + index + " added a count of 0",
                        0, countAddedInThisBucket);
            }
            index++;
        }
        Assert.assertEquals(14, index - 1);

        index = 0;
        long totalAddedCounts = 0;
        for (HistogramIterationValue v : histogram.logarithmicBucketValues(10000, 2)) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 0) {
                Assert.assertEquals("Logarithmic 10 msec bucket # 0 [" +
                                v.getValueIteratedFrom() + ".." + v.getValueIteratedTo() +
                                "] added a count of 10000",
                        10000, countAddedInThisBucket);
            }
            totalAddedCounts += v.getCountAddedInThisIterationStep();
            index++;
        }
        Assert.assertEquals("There should be 14 Logarithmic buckets of size 10000 usec between 0 and 100 sec.",
                14, index - 1);
        Assert.assertEquals("Total added counts should be 20000", 20000, totalAddedCounts);
    }

    @Test
    public void testPercentiles() {
        for (HistogramIterationValue v : histogram.percentiles(5 /* ticks per half */)) {
            Assert.assertEquals("Value at Iterated-to Percentile is the same as the matching getValueAtPercentile():\n" +
                            "getPercentileLevelIteratedTo = " + v.getPercentileLevelIteratedTo() +
                            "\ngetValueIteratedTo = " + v.getValueIteratedTo() +
                            "\ngetValueIteratedFrom = " + v.getValueIteratedFrom() +
                            "\ngetValueAtPercentile(getPercentileLevelIteratedTo()) = " +
                            histogram.getValueAtPercentile(v.getPercentileLevelIteratedTo()) +
                            "\ngetPercentile = " + v.getPercentile() +
                            "\ngetValueAtPercentile(getPercentile())" +
                            histogram.getValueAtPercentile(v.getPercentile()) +
                            "\nequivalent at getValueAtPercentile(v.getPercentileLevelIteratedTo()) = " +
                            histogram.highestEquivalentValue(histogram.getValueAtPercentile(v.getPercentileLevelIteratedTo())) +
                            "\nequivalent at getValueAtPercentile(v.getPercentile()) = " +
                            histogram.highestEquivalentValue(histogram.getValueAtPercentile(v.getPercentile())) +
                            "\nindex at v.getValueIteratedTo() = " +
                            histogram.countsArrayIndex(v.getValueIteratedTo()) +
                            "\nindex at getValueAtPercentile(v.getPercentileLevelIteratedTo()) = " +
                            histogram.countsArrayIndex(histogram.getValueAtPercentile(v.getPercentileLevelIteratedTo())) +
                            "\nindex at getValueAtPercentile(v.getPercentile()) = " +
                            histogram.countsArrayIndex(histogram.getValueAtPercentile(v.getPercentile())) +
                            "\nindex at getValueAtPercentile(v.getPercentile() - 0.0000000001) = " +
                            histogram.countsArrayIndex(histogram.getValueAtPercentile(v.getPercentile() - 0.0000000001)) +
                            "\ncount for (long)(((v.getPercentile() / 100.0) * histogram.getTotalCount()) + 0.5) = " +
                            (long) (((v.getPercentile() / 100.0) * histogram.getTotalCount()) + 0.5) +
                            "\n math for ((v.getPercentile() / 100.0) * histogram.getTotalCount()) = " +
                            ((v.getPercentile() / 100.0) * histogram.getTotalCount()) +
                            "\n math for (((v.getPercentile() / 100.0) * histogram.getTotalCount()) + 0.5) = " +
                            (((v.getPercentile() / 100.0) * histogram.getTotalCount()) + 0.5) +
                            "\n math for (long)(((v.getPercentile() / 100.0) * histogram.getTotalCount()) + 0.5) = " +
                            (long) (((v.getPercentile() / 100.0) * histogram.getTotalCount()) + 0.5) +
                            "\ncount for (long)(Math.ceil((v.getPercentile() / 100.0) * histogram.getTotalCount())) = " +
                            (long) (Math.ceil((v.getPercentile() / 100.0) * histogram.getTotalCount())) +
                            "\n math for Math.ceil((v.getPercentile() / 100.0) * histogram.getTotalCount()) = " +
                            Math.ceil((v.getPercentile() / 100.0) * histogram.getTotalCount()) +
                            "\ntotalCountToThisValue = " +
                            v.getTotalCountToThisValue() +
                            "\ncountAtValueIteratedTo = " +
                            v.getCountAtValueIteratedTo() +
                            "\ncount at index at getValueAtPercentile(v.getPercentileLevelIteratedTo()) = " +
                            histogram.getCountAtIndex(histogram.countsArrayIndex(histogram.getValueAtPercentile(v.getPercentileLevelIteratedTo()))) +
                            "\ncount at index at getValueAtPercentile(v.getPercentile()) = " +
                            histogram.getCountAtIndex(histogram.countsArrayIndex(histogram.getValueAtPercentile(v.getPercentile()))) +
                            "\n"
                    ,
                    v.getValueIteratedTo(),
                    histogram.highestEquivalentValue(histogram.getValueAtPercentile(v.getPercentile())));
        }
    }

    @Test
    public void testPreVsPostCorrectionValues() {
        // Loop both ways (one would be enough, but good practice just for fun:

        Assert.assertEquals("pre and post corrected count totals ",
                histogram.getTotalCount(), postCorrectedHistogram.getTotalCount());

        // The following comparison loops would have worked in a perfect accuracy world, but since post
        // correction is done based on the value extracted from the bucket, and the during-recording is done
        // based on the actual (not pixelized) value, there will be subtle differences due to roundoffs:

        //        for (HistogramIterationValue v : histogram.allValues()) {
        //            long preCorrectedCount = v.getCountAtValueIteratedTo();
        //            long postCorrectedCount = postCorrectedHistogram.getCountAtValue(v.getValueIteratedTo());
        //            Assert.assertEquals("pre and post corrected count at value " + v.getValueIteratedTo(),
        //                    preCorrectedCount, postCorrectedCount);
        //        }
        //
        //        for (HistogramIterationValue v : postCorrectedHistogram.allValues()) {
        //            long preCorrectedCount = v.getCountAtValueIteratedTo();
        //            long postCorrectedCount = histogram.getCountAtValue(v.getValueIteratedTo());
        //            Assert.assertEquals("pre and post corrected count at value " + v.getValueIteratedTo(),
        //                    preCorrectedCount, postCorrectedCount);
        //        }

    }

    @Test
    public void testRecordedValues() {
        int index = 0;
        // Iterate raw data by stepping through every value that has a count recorded:
        for (HistogramIterationValue v : rawHistogram.recordedValues()) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 0) {
                Assert.assertEquals("Raw recorded value bucket # 0 added a count of 10000",
                        10000, countAddedInThisBucket);
            } else {
                Assert.assertEquals("Raw recorded value bucket # " + index + " added a count of 1",
                        1, countAddedInThisBucket);
            }
            index++;
        }
        Assert.assertEquals(2, index);

        index = 0;
        long totalAddedCounts = 0;
        for (HistogramIterationValue v : histogram.recordedValues()) {
            long countAddedInThisBucket = v.getCountAddedInThisIterationStep();
            if (index == 0) {
                Assert.assertEquals("Recorded bucket # 0 [" +
                                v.getValueIteratedFrom() + ".." + v.getValueIteratedTo() +
                                "] added a count of 10000",
                        10000, countAddedInThisBucket);
            }
            Assert.assertTrue("The count in recorded bucket #" + index + " is not 0",
                    v.getCountAtValueIteratedTo() != 0);
            Assert.assertEquals("The count in recorded bucket #" + index +
                            " is exactly the amount added since the last iteration ",
                    v.getCountAtValueIteratedTo(), v.getCountAddedInThisIterationStep());
            totalAddedCounts += v.getCountAddedInThisIterationStep();
            index++;
        }
        Assert.assertEquals("Total added counts should be 20000", 20000, totalAddedCounts);
    }

    @Test
    public void testScalingEquivalence() {
        Assert.assertEquals("averages should be equivalent",
                histogram.getMean() * 512,
                scaledHistogram.getMean(), scaledHistogram.getMean() * 0.000001);
        Assert.assertEquals("total count should be the same",
                histogram.getTotalCount(),
                scaledHistogram.getTotalCount());
        Assert.assertEquals("99%'iles should be equivalent",
                scaledHistogram.highestEquivalentValue(histogram.getValueAtPercentile(99.0) * 512),
                scaledHistogram.highestEquivalentValue(scaledHistogram.getValueAtPercentile(99.0)));
        Assert.assertEquals("Max should be equivalent",
                scaledHistogram.highestEquivalentValue(histogram.getMaxValue() * 512),
                scaledHistogram.getMaxValue());
        // Same for post-corrected:
        Assert.assertEquals("averages should be equivalent",
                histogram.getMean() * 512,
                scaledHistogram.getMean(), scaledHistogram.getMean() * 0.000001);
        Assert.assertEquals("total count should be the same",
                postCorrectedHistogram.getTotalCount(),
                postCorrectedScaledHistogram.getTotalCount());
        Assert.assertEquals("99%'iles should be equivalent",
                postCorrectedHistogram.lowestEquivalentValue(postCorrectedHistogram.getValueAtPercentile(99.0)) * 512,
                postCorrectedScaledHistogram.lowestEquivalentValue(postCorrectedScaledHistogram.getValueAtPercentile(99.0)));
        Assert.assertEquals("Max should be equivalent",
                postCorrectedScaledHistogram.highestEquivalentValue(postCorrectedHistogram.getMaxValue() * 512),
                postCorrectedScaledHistogram.getMaxValue());
    }

    @Test
    public void testVerifyManualAllValuesDuplication() {
        Histogram histogram1 = histogram.copy();

        AbstractHistogram.AllValues values = histogram1.allValues();
        ArrayList<Long> ranges = new ArrayList<>();
        ArrayList<Long> counts = new ArrayList<>();
        int index = 0;
        for (HistogramIterationValue value : values) {
            if (value.getCountAddedInThisIterationStep() > 0) {
                ranges.add(value.getValueIteratedTo());
                counts.add(value.getCountAddedInThisIterationStep());
            }
            index++;
        }
        Assert.assertEquals("index should be equal to countsArrayLength", histogram1.countsArrayLength(), index);

        AbstractHistogram histogram2 = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
        for (int i = 0; i < ranges.size(); ++i) {
            histogram2.recordValueWithCount(ranges.get(i), counts.get(i));
        }

        Assert.assertEquals("Histograms should be equal", histogram1, histogram2);
    }

    private static void recordOneValueAndDisplayLinearBuckets(int value, long step) {
        IntCountsHistogram histogram = new IntCountsHistogram(2);
        histogram.recordValue(value);
        for (HistogramIterationValue itValue : histogram.linearBucketValues(step)) {
            itValue.getCountAddedInThisIterationStep();
        }
    }

    static {
        histogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
        scaledHistogram = new Histogram(1000, highestTrackableValue * 512, numberOfSignificantValueDigits);
        rawHistogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
        scaledRawHistogram = new Histogram(1000, highestTrackableValue * 512, numberOfSignificantValueDigits);
        // Log hypothetical scenario: 100 seconds of "perfect" 1msec results, sampled
        // 100 times per second (10,000 results), followed by a 100 second pause with
        // a single (100 second) recorded result. Recording is done indicating an expected
        // interval between samples of 10 msec:
        for (int i = 0; i < 10000; i++) {
            histogram.recordValueWithExpectedInterval(1000 /* 1 msec */, 10000 /* 10 msec expected interval */);
            scaledHistogram.recordValueWithExpectedInterval(1000 * 512 /* 1 msec */, 10000 * 512 /* 10 msec expected interval */);
            rawHistogram.recordValue(1000 /* 1 msec */);
            scaledRawHistogram.recordValue(1000 * 512/* 1 msec */);
        }
        histogram.recordValueWithExpectedInterval(100000000L /* 100 sec */, 10000 /* 10 msec expected interval */);
        scaledHistogram.recordValueWithExpectedInterval(100000000L * 512 /* 100 sec */, 10000 * 512 /* 10 msec expected interval */);
        rawHistogram.recordValue(100000000L /* 100 sec */);
        scaledRawHistogram.recordValue(100000000L * 512 /* 100 sec */);

        postCorrectedHistogram = rawHistogram.copyCorrectedForCoordinatedOmission(10000 /* 10 msec expected interval */);
        postCorrectedScaledHistogram = scaledRawHistogram.copyCorrectedForCoordinatedOmission(10000 * 512 /* 10 msec expected interval */);
    }
}
