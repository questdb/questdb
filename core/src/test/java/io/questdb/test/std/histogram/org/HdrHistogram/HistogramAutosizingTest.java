/**
 * HistogramTest.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.Test;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructHistogram;
import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructDoubleHistogram;

/**
 * JUnit test for org.HdrHistogram.Histogram
 */
@RunWith(Parameterized.class)
public class HistogramAutosizingTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units

    @Parameterized.Parameters(name = "{0}")
    public static Object[] data() {
        return new Object[] {Histogram.class};
    }

    @Parameter(0)
    public Class<?> c;

    @Test
    public void testHistogramAutoSizingEdges() {
        AbstractHistogram histogram = constructHistogram(c,3);
        histogram.recordValue((1L << 62) - 1);
        Assert.assertEquals(52, histogram.bucketCount);
        Assert.assertEquals(54272, histogram.countsArrayLength);
        histogram.recordValue(Long.MAX_VALUE);
        Assert.assertEquals(53, histogram.bucketCount);
        Assert.assertEquals(55296, histogram.countsArrayLength);
    }

    @Test
    public void testHistogramEqualsAfterResizing() {
        AbstractHistogram histogram = constructHistogram(c,3);
        histogram.recordValue((1L << 62) - 1);
        Assert.assertEquals(52, histogram.bucketCount);
        Assert.assertEquals(54272, histogram.countsArrayLength);
        histogram.recordValue(Long.MAX_VALUE);
        Assert.assertEquals(53, histogram.bucketCount);
        Assert.assertEquals(55296, histogram.countsArrayLength);
        histogram.reset();
        histogram.recordValue((1L << 62) - 1);

        Histogram histogram1 = new Histogram(3);
        histogram1.recordValue((1L << 62) - 1);
        Assert.assertEquals(histogram, histogram1);
    }

    @Test
    public void testHistogramAutoSizing() {
        AbstractHistogram histogram = constructHistogram(c,3);
        for (int i = 0; i < 63; i++) {
            long value = 1L << i;
            histogram.recordValue(value);
        }
        Assert.assertEquals(53, histogram.bucketCount);
        Assert.assertEquals(55296, histogram.countsArrayLength);
    }

    @Test
    public void testAutoSizingAdd() {
        AbstractHistogram histogram1 = constructHistogram(c, 2);
        AbstractHistogram histogram2 = constructHistogram(c, 2);

        histogram1.recordValue(1000L);
        histogram1.recordValue(1000000000L);

        histogram2.add(histogram1);

        Assert.assertTrue(histogram2.valuesAreEquivalent(histogram2.getMaxValue(), 1000000000L));
    }

    @Test
    public void testAutoSizingAcrossContinuousRange() {
        AbstractHistogram histogram = constructHistogram(c, 2);

        for (long i = 0; i < 10000000L; i++) {
            histogram.recordValue(i);
        }
    }

    @Test
    public void testAutoSizingAddDouble() {
        DoubleHistogram histogram1 = constructDoubleHistogram(DoubleHistogram.class,2);
        DoubleHistogram histogram2 = constructDoubleHistogram(DoubleHistogram.class,2);

        histogram1.recordValue(1000L);
        histogram1.recordValue(1000000000L);

        histogram2.add(histogram1);

        //CHECK THIS
        Assert.assertTrue(histogram2.valuesAreEquivalent(histogram2.getMaxValue(), 1000000000L));
    }

    @Test
    public void testDoubleHistogramAutoSizingUp() {
        DoubleHistogram histogram = constructDoubleHistogram(DoubleHistogram.class,2);
        for (int i = 0; i < 55; i++) {
            double value = 1L << i;
            histogram.recordValue(value);
        }
    }

    @Test
    public void testDoubleHistogramAutoSizingDown() {
        DoubleHistogram histogram = constructDoubleHistogram(DoubleHistogram.class,2);
        for (int i = 0; i < 56; i++) {
            double value = (1L << 45) * 1.0 / (1L << i);
            histogram.recordValue(value);
        }
    }

    @Test
    public void testDoubleHistogramAutoSizingEdges() {
        DoubleHistogram histogram = constructDoubleHistogram(DoubleHistogram.class,3);
        histogram.recordValue(1);
        histogram.recordValue(1L << 48);
        histogram.recordValue((1L << 52) - 1);
        Assert.assertEquals(52, histogram.integerValuesHistogram.bucketCount);
        Assert.assertEquals(54272, histogram.integerValuesHistogram.countsArrayLength);
        histogram.recordValue((1L << 53) - 1);
        Assert.assertEquals(53, histogram.integerValuesHistogram.bucketCount);
        Assert.assertEquals(55296, histogram.integerValuesHistogram.countsArrayLength);

        DoubleHistogram histogram2 = constructDoubleHistogram(DoubleHistogram.class,2);
        histogram2.recordValue(1);
        histogram2.recordValue(1L << 48);
        histogram2.recordValue((1L << 54) - 1);
        Assert.assertEquals(55, histogram2.integerValuesHistogram.bucketCount);
        Assert.assertEquals(7168, histogram2.integerValuesHistogram.countsArrayLength);
        histogram2.recordValue((1L << 55) - 1);
        Assert.assertEquals(56, histogram2.integerValuesHistogram.bucketCount);
        Assert.assertEquals(7296, histogram2.integerValuesHistogram.countsArrayLength);

        DoubleHistogram histogram3 = constructDoubleHistogram(DoubleHistogram.class,2);
        histogram3.recordValue(1E50);
        histogram3.recordValue((1L << 48) * 1E50);
        histogram3.recordValue(((1L << 54) - 1) * 1E50);
        Assert.assertEquals(55, histogram3.integerValuesHistogram.bucketCount);
        Assert.assertEquals(7168, histogram3.integerValuesHistogram.countsArrayLength);
        histogram3.recordValue(((1L << 55) - 1) * 1E50);
        Assert.assertEquals(56, histogram3.integerValuesHistogram.bucketCount);
        Assert.assertEquals(7296, histogram3.integerValuesHistogram.countsArrayLength);
    }

}
