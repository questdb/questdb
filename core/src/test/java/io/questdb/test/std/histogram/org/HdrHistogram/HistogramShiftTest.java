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
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedHistogram;
import io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructHistogram;

/**
 * JUnit test for {@link io.questdb.std.histogram.org.HdrHistogram.Histogram}
 */
public class HistogramShiftTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units

    static final Class[] histogramClassesNoAtomic = {
            Histogram.class,
            PackedHistogram.class
    };

    @Test
    public void testHistogramShift() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class,
                ShortCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            // Histogram h = new Histogram(1L, 1L << 32, 3);
            AbstractHistogram histogram = constructHistogram(histoClass, highestTrackableValue, 3);
            testShiftLowestBucket(histogram);
            testShiftNonLowestBucket(histogram);
        }
    }

    void testShiftLowestBucket(AbstractHistogram histogram) {
        for (int shiftAmount = 0; shiftAmount < 10; shiftAmount++) {
            histogram.reset();
            histogram.recordValueWithCount(0, 500);
            histogram.recordValue(2);
            histogram.recordValue(4);
            histogram.recordValue(5);
            histogram.recordValue(511);
            histogram.recordValue(512);
            histogram.recordValue(1023);
            histogram.recordValue(1024);
            histogram.recordValue(1025);

            AbstractHistogram histogram2 = histogram.copy();

            histogram2.reset();
            histogram2.recordValueWithCount(0, 500);
            histogram2.recordValue(2 << shiftAmount);
            histogram2.recordValue(4 << shiftAmount);
            histogram2.recordValue(5 << shiftAmount);
            histogram2.recordValue(511 << shiftAmount);
            histogram2.recordValue(512 << shiftAmount);
            histogram2.recordValue(1023 << shiftAmount);
            histogram2.recordValue(1024 << shiftAmount);
            histogram2.recordValue(1025 << shiftAmount);

            histogram.shiftValuesLeft(shiftAmount);

            if (!histogram.equals(histogram2)) {
                System.out.println("Not Equal for shift of " + shiftAmount);
            }
            Assert.assertEquals(histogram, histogram2);
        }
    }

    void testShiftNonLowestBucket(AbstractHistogram histogram) {
        for (int shiftAmount = 0; shiftAmount < 10; shiftAmount++) {
            histogram.reset();
            histogram.recordValueWithCount(0, 500);
            histogram.recordValue(2 << 10);
            histogram.recordValue(4 << 10);
            histogram.recordValue(5 << 10);
            histogram.recordValue(511 << 10);
            histogram.recordValue(512 << 10);
            histogram.recordValue(1023 << 10);
            histogram.recordValue(1024 << 10);
            histogram.recordValue(1025 << 10);

            AbstractHistogram origHistogram = histogram.copy();
            AbstractHistogram histogram2 = histogram.copy();

            histogram2.reset();
            histogram2.recordValueWithCount(0, 500);
            histogram2.recordValue((2 << 10) << shiftAmount);
            histogram2.recordValue((4 << 10) << shiftAmount);
            histogram2.recordValue((5 << 10) << shiftAmount);
            histogram2.recordValue((511 << 10) << shiftAmount);
            histogram2.recordValue((512 << 10) << shiftAmount);
            histogram2.recordValue((1023 << 10) << shiftAmount);
            histogram2.recordValue((1024 << 10) << shiftAmount);
            histogram2.recordValue((1025 << 10) << shiftAmount);

            histogram.shiftValuesLeft(shiftAmount);

            if (!histogram.equals(histogram2)) {
                System.out.println("Not Equal for shift of " + shiftAmount);
            }
            Assert.assertEquals(histogram, histogram2);

            histogram.shiftValuesRight(shiftAmount);

            Assert.assertEquals(histogram, origHistogram);
        }
    }
}
