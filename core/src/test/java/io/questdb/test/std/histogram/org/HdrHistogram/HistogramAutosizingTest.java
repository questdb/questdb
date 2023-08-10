/**
 * HistogramTest.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import io.questdb.std.histogram.org.HdrHistogram.ConcurrentHistogram;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructHistogram;
import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructDoubleHistogram;

/**
 * JUnit test for org.HdrHistogram.Histogram
 */
public class HistogramAutosizingTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class
    })
    public void testHistogramAutoSizingEdges(Class<?> c) {
        AbstractHistogram histogram = constructHistogram(c,3);
        histogram.recordValue((1L << 62) - 1);
        Assertions.assertEquals(52, histogram.bucketCount);
        Assertions.assertEquals(54272, histogram.countsArrayLength);
        histogram.recordValue(Long.MAX_VALUE);
        Assertions.assertEquals(53, histogram.bucketCount);
        Assertions.assertEquals(55296, histogram.countsArrayLength);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class
    })
    public void testHistogramEqualsAfterResizing(Class<?> c) {
        AbstractHistogram histogram = constructHistogram(c,3);
        histogram.recordValue((1L << 62) - 1);
        Assertions.assertEquals(52, histogram.bucketCount);
        Assertions.assertEquals(54272, histogram.countsArrayLength);
        histogram.recordValue(Long.MAX_VALUE);
        Assertions.assertEquals(53, histogram.bucketCount);
        Assertions.assertEquals(55296, histogram.countsArrayLength);
        histogram.reset();
        histogram.recordValue((1L << 62) - 1);
        
        Histogram histogram1 = new Histogram(3);
        histogram1.recordValue((1L << 62) - 1);
        Assertions.assertEquals(histogram, histogram1);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class
    })
    public void testHistogramAutoSizing(Class<?> c) {
        AbstractHistogram histogram = constructHistogram(c,3);
        for (int i = 0; i < 63; i++) {
            long value = 1L << i;
            histogram.recordValue(value);
        }
        Assertions.assertEquals(53, histogram.bucketCount);
        Assertions.assertEquals(55296, histogram.countsArrayLength);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class
    })
    public void testAutoSizingAdd(Class<?> c) {
        AbstractHistogram histogram1 = constructHistogram(c, 2);
        AbstractHistogram histogram2 = constructHistogram(c, 2);

        histogram1.recordValue(1000L);
        histogram1.recordValue(1000000000L);

        histogram2.add(histogram1);

        Assertions.assertTrue(histogram2.valuesAreEquivalent(histogram2.getMaxValue(), 1000000000L), "Max should be equivalent to 1000000000L");
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Histogram.class,
            ConcurrentHistogram.class
    })
    public void testAutoSizingAcrossContinuousRange(Class<?> c) {
        AbstractHistogram histogram = constructHistogram(c, 2);

        for (long i = 0; i < 10000000L; i++) {
            histogram.recordValue(i);
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testAutoSizingAddDouble(Class<?> c) {
        DoubleHistogram histogram1 = constructDoubleHistogram(c,2);
        DoubleHistogram histogram2 = constructDoubleHistogram(c,2);

        histogram1.recordValue(1000L);
        histogram1.recordValue(1000000000L);

        histogram2.add(histogram1);

        Assertions.assertTrue(histogram2.valuesAreEquivalent(histogram2.getMaxValue(), 1000000000L), "Max should be equivalent to 1000000000L");
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testDoubleHistogramAutoSizingUp(Class<?> c) {
        DoubleHistogram histogram = constructDoubleHistogram(c,2);
        for (int i = 0; i < 55; i++) {
            double value = 1L << i;
            histogram.recordValue(value);
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testDoubleHistogramAutoSizingDown(Class<?> c) {
        DoubleHistogram histogram = constructDoubleHistogram(c,2);
        for (int i = 0; i < 56; i++) {
            double value = (1L << 45) * 1.0 / (1L << i);
            histogram.recordValue(value);
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DoubleHistogram.class
    })
    public void testDoubleHistogramAutoSizingEdges(Class<?> c) {
        DoubleHistogram histogram = constructDoubleHistogram(c,3);
        histogram.recordValue(1);
        histogram.recordValue(1L << 48);
        histogram.recordValue((1L << 52) - 1);
        Assertions.assertEquals(52, histogram.integerValuesHistogram.bucketCount);
        Assertions.assertEquals(54272, histogram.integerValuesHistogram.countsArrayLength);
        histogram.recordValue((1L << 53) - 1);
        Assertions.assertEquals(53, histogram.integerValuesHistogram.bucketCount);
        Assertions.assertEquals(55296, histogram.integerValuesHistogram.countsArrayLength);

        DoubleHistogram histogram2 = constructDoubleHistogram(c,2);
        histogram2.recordValue(1);
        histogram2.recordValue(1L << 48);
        histogram2.recordValue((1L << 54) - 1);
        Assertions.assertEquals(55, histogram2.integerValuesHistogram.bucketCount);
        Assertions.assertEquals(7168, histogram2.integerValuesHistogram.countsArrayLength);
        histogram2.recordValue((1L << 55) - 1);
        Assertions.assertEquals(56, histogram2.integerValuesHistogram.bucketCount);
        Assertions.assertEquals(7296, histogram2.integerValuesHistogram.countsArrayLength);

        DoubleHistogram histogram3 = constructDoubleHistogram(c,2);
        histogram3.recordValue(1E50);
        histogram3.recordValue((1L << 48) * 1E50);
        histogram3.recordValue(((1L << 54) - 1) * 1E50);
        Assertions.assertEquals(55, histogram3.integerValuesHistogram.bucketCount);
        Assertions.assertEquals(7168, histogram3.integerValuesHistogram.countsArrayLength);
        histogram3.recordValue(((1L << 55) - 1) * 1E50);
        Assertions.assertEquals(56, histogram3.integerValuesHistogram.bucketCount);
        Assertions.assertEquals(7296, histogram3.integerValuesHistogram.countsArrayLength);
    }

}
