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

import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedHistogram;
import io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructDoubleHistogram;
import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructHistogram;

/**
 * JUnit test for {@link io.questdb.std.histogram.org.HdrHistogram.Histogram}
 */
public class HistogramAutosizingTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units

    @Test
    public void testAutoSizingAcrossContinuousRange() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            AbstractHistogram histogram = constructHistogram(histoClass, 2);

            for (long i = 0; i < 10000000L; i++) {
                histogram.recordValue(i);
            }
        }
    }

    @Test
    public void testAutoSizingAdd() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class,
                ShortCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            AbstractHistogram histogram1 = constructHistogram(histoClass, 2);
            AbstractHistogram histogram2 = constructHistogram(histoClass, 2);

            histogram1.recordValue(1000L);
            histogram1.recordValue(1000000000L);

            histogram2.add(histogram1);

            Assert.assertTrue("Max should be equivalent to 1000000000L",
                    histogram2.valuesAreEquivalent(histogram2.getMaxValue(), 1000000000L)
            );
        }
    }

    @Test
    public void testAutoSizingAddDouble() {
        Class<?>[] testClasses = new Class[]{
                DoubleHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            DoubleHistogram histogram1 = constructDoubleHistogram(histoClass, 2);
            DoubleHistogram histogram2 = constructDoubleHistogram(histoClass, 2);

            histogram1.recordValue(1000L);
            histogram1.recordValue(1000000000L);

            histogram2.add(histogram1);

            Assert.assertTrue("Max should be equivalent to 1000000000L",
                    histogram2.valuesAreEquivalent(histogram2.getMaxValue(), 1000000000L)
            );
        }
    }

    @Test
    public void testDoubleHistogramAutoSizingDown() {
        Class<?>[] testClasses = new Class[]{
                DoubleHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            DoubleHistogram histogram = constructDoubleHistogram(histoClass, 2);
            for (int i = 0; i < 56; i++) {
                double value = (1L << 45) * 1.0 / (1L << i);
                histogram.recordValue(value);
            }
        }
    }

    @Test
    public void testDoubleHistogramAutoSizingEdges() {
        Class<?>[] testClasses = new Class[]{
                DoubleHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            DoubleHistogram histogram = constructDoubleHistogram(histoClass, 3);
            histogram.recordValue(1);
            histogram.recordValue(1L << 48);
            histogram.recordValue((1L << 52) - 1);
            Assert.assertEquals(52, histogram.integerValuesHistogram().bucketCount());
            Assert.assertEquals(54272, histogram.integerValuesHistogram().countsArrayLength());
            histogram.recordValue((1L << 53) - 1);
            Assert.assertEquals(53, histogram.integerValuesHistogram().bucketCount());
            Assert.assertEquals(55296, histogram.integerValuesHistogram().countsArrayLength());

            DoubleHistogram histogram2 = constructDoubleHistogram(histoClass, 2);
            histogram2.recordValue(1);
            histogram2.recordValue(1L << 48);
            histogram2.recordValue((1L << 54) - 1);
            Assert.assertEquals(55, histogram2.integerValuesHistogram().bucketCount());
            Assert.assertEquals(7168, histogram2.integerValuesHistogram().countsArrayLength());
            histogram2.recordValue((1L << 55) - 1);
            Assert.assertEquals(56, histogram2.integerValuesHistogram().bucketCount());
            Assert.assertEquals(7296, histogram2.integerValuesHistogram().countsArrayLength());

            DoubleHistogram histogram3 = constructDoubleHistogram(histoClass, 2);
            histogram3.recordValue(1E50);
            histogram3.recordValue((1L << 48) * 1E50);
            histogram3.recordValue(((1L << 54) - 1) * 1E50);
            Assert.assertEquals(55, histogram3.integerValuesHistogram().bucketCount());
            Assert.assertEquals(7168, histogram3.integerValuesHistogram().countsArrayLength());
            histogram3.recordValue(((1L << 55) - 1) * 1E50);
            Assert.assertEquals(56, histogram3.integerValuesHistogram().bucketCount());
            Assert.assertEquals(7296, histogram3.integerValuesHistogram().countsArrayLength());
        }
    }

    @Test
    public void testDoubleHistogramAutoSizingUp() {
        Class<?>[] testClasses = new Class[]{
                DoubleHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            DoubleHistogram histogram = constructDoubleHistogram(histoClass, 2);
            for (int i = 0; i < 55; i++) {
                double value = 1L << i;
                histogram.recordValue(value);
            }
        }
    }

    @Test
    public void testHistogramAutoSizing() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class,
                ShortCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            AbstractHistogram histogram = constructHistogram(histoClass, 3);
            for (int i = 0; i < 63; i++) {
                long value = 1L << i;
                histogram.recordValue(value);
            }
            Assert.assertEquals(53, histogram.bucketCount());
            Assert.assertEquals(55296, histogram.countsArrayLength());
        }
    }

    @Test
    public void testHistogramAutoSizingEdges() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class,
                ShortCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            AbstractHistogram histogram = constructHistogram(histoClass, 3);
            histogram.recordValue((1L << 62) - 1);
            Assert.assertEquals(52, histogram.bucketCount());
            Assert.assertEquals(54272, histogram.countsArrayLength());
            histogram.recordValue(Long.MAX_VALUE);
            Assert.assertEquals(53, histogram.bucketCount());
            Assert.assertEquals(55296, histogram.countsArrayLength());
        }
    }

    @Test
    public void testHistogramEqualsAfterResizing() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class,
                ShortCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            AbstractHistogram histogram = constructHistogram(histoClass, 3);
            histogram.recordValue((1L << 62) - 1);
            Assert.assertEquals(52, histogram.bucketCount());
            Assert.assertEquals(54272, histogram.countsArrayLength());
            histogram.recordValue(Long.MAX_VALUE);
            Assert.assertEquals(53, histogram.bucketCount());
            Assert.assertEquals(55296, histogram.countsArrayLength());
            histogram.reset();
            histogram.recordValue((1L << 62) - 1);

            Histogram histogram1 = new Histogram(3);
            histogram1.recordValue((1L << 62) - 1);
            Assert.assertEquals(histogram, histogram1);
        }
    }
}
