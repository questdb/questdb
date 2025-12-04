/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.std.Rnd;
import io.questdb.std.histogram.org.HdrHistogram.GroupByHistogramSink;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class GroupByHistogramSinkTest extends AbstractCairoTest {

    @Test
    public void testBasicRecording() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(1, 1000, 3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(1, 1000, 3);
            offHeap.setAllocator(allocator);

            for (long i = 1; i <= 100; i++) {
                onHeap.recordValue(i);
                offHeap.recordValue(i);
            }

            Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());
            Assert.assertEquals(onHeap.getMinValue(), offHeap.getMinValue());
            Assert.assertEquals(onHeap.getMaxValue(), offHeap.getMaxValue());
            Assert.assertEquals(onHeap.getMean(), offHeap.getMean(), 0.0);
            Assert.assertEquals(onHeap.getStdDeviation(), offHeap.getStdDeviation(), 0.0);
        }
    }

    @Test
    public void testPercentiles() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(1, 3600000, 3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(1, 3600000, 3);
            offHeap.setAllocator(allocator);

            Rnd rnd1 = new Rnd();
            Rnd rnd2 = new Rnd();
            for (int i = 0; i < 10000; i++) {
                long value = rnd1.nextLong(3600000);
                onHeap.recordValue(value);

                long value2 = rnd2.nextLong(3600000);
                offHeap.recordValue(value2);
            }

            double[] percentiles = {50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99};
            for (double p : percentiles) {
                long onHeapValue = onHeap.getValueAtPercentile(p);
                long offHeapValue = offHeap.getValueAtPercentile(p);
                Assert.assertEquals(onHeapValue, offHeapValue);
            }
        }
    }

    @Test
    public void testRecordValueWithCount() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(1, 10000, 2);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(1, 10000, 2);
            offHeap.setAllocator(allocator);

            onHeap.recordValueWithCount(100, 50);
            offHeap.recordValueWithCount(100, 50);

            onHeap.recordValueWithCount(500, 100);
            offHeap.recordValueWithCount(500, 100);

            onHeap.recordValueWithCount(1000, 25);
            offHeap.recordValueWithCount(1000, 25);

            Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());
            Assert.assertEquals(onHeap.getMean(), offHeap.getMean(), 0.00);
            Assert.assertEquals(onHeap.getMinValue(), offHeap.getMinValue());
            Assert.assertEquals(onHeap.getMaxValue(), offHeap.getMaxValue());
        }
    }

    @Test
    public void testReset() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(3);
            offHeap.setAllocator(allocator);

            for (int i = 1; i <= 100; i++) {
                onHeap.recordValue(i);
                offHeap.recordValue(i);
            }

            onHeap.reset();
            offHeap.clear();

            Assert.assertEquals(0, onHeap.getTotalCount());
            Assert.assertEquals(0, offHeap.getTotalCount());

            onHeap.recordValue(42);
            offHeap.recordValue(42);

            Assert.assertEquals(1, onHeap.getTotalCount());
            Assert.assertEquals(1, offHeap.getTotalCount());
        }
    }

    @Test
    public void testAutoResize() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(3);
            offHeap.setAllocator(allocator);

            long[] values = {1, 10, 100, 1000, 10000, 100000, 1000000};
            for (long value : values) {
                onHeap.recordValue(value);
                offHeap.recordValue(value);
            }

            Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());
            Assert.assertEquals(onHeap.getMaxValue(), offHeap.getMaxValue());
            Assert.assertEquals(onHeap.getMinValue(), offHeap.getMinValue());
        }
    }

    @Test
    public void testAdd() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap1 = new Histogram(1, 10000, 2);
            Histogram onHeap2 = new Histogram(1, 10000, 2);
            GroupByHistogramSink offHeap1 = new GroupByHistogramSink(1, 10000, 2);
            GroupByHistogramSink offHeap2 = new GroupByHistogramSink(1, 10000, 2);
            offHeap1.setAllocator(allocator);
            offHeap2.setAllocator(allocator);

            for (int i = 1; i <= 50; i++) {
                onHeap1.recordValue(i);
                offHeap1.recordValue(i);
            }

            for (int i = 51; i <= 100; i++) {
                onHeap2.recordValue(i);
                offHeap2.recordValue(i);
            }

            onHeap1.add(onHeap2);
            offHeap1.add(offHeap2);

            Assert.assertEquals(onHeap1.getTotalCount(), offHeap1.getTotalCount());
            Assert.assertEquals(onHeap1.getMinValue(), offHeap1.getMinValue());
            Assert.assertEquals(onHeap1.getMaxValue(), offHeap1.getMaxValue());
            Assert.assertEquals(onHeap1.getMean(), offHeap1.getMean(), 0.0);
        }
    }

    @Test
    public void testCopy() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(1, 1000, 3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(1, 1000, 3);
            offHeap.setAllocator(allocator);

            for (int i = 1; i <= 100; i++) {
                onHeap.recordValue(i);
                offHeap.recordValue(i);
            }

            Histogram onHeapCopy = onHeap.copy();
            GroupByHistogramSink offHeapCopy = offHeap.copy();

            Assert.assertEquals(onHeapCopy.getTotalCount(), offHeapCopy.getTotalCount());
            Assert.assertEquals(onHeapCopy.getMean(), offHeapCopy.getMean(), 0.0);
            Assert.assertEquals(onHeapCopy.getMinValue(), offHeapCopy.getMinValue());
            Assert.assertEquals(onHeapCopy.getMaxValue(), offHeapCopy.getMaxValue());
        }
    }

    @Test
    public void testLargeValues() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            long highest = 3600000000L;
            Histogram onHeap = new Histogram(1, highest, 3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(1, highest, 3);
            offHeap.setAllocator(allocator);

            Rnd rnd1 = new Rnd();
            Rnd rnd2 = new Rnd();
            for (int i = 0; i < 1000; i++) {
                long value = rnd1.nextLong(highest);
                onHeap.recordValue(value);

                long value2 = rnd2.nextLong(highest);
                offHeap.recordValue(value2);
            }

            Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());

            Assert.assertEquals(onHeap.getValueAtPercentile(50), offHeap.getValueAtPercentile(50));
            Assert.assertEquals(onHeap.getValueAtPercentile(99), offHeap.getValueAtPercentile(99));
        }
    }

    @Test
    public void testGetCountAtValue() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(1, 1000, 2);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(1, 1000, 2);
            offHeap.setAllocator(allocator);

            for (int i = 0; i < 10; i++) {
                onHeap.recordValue(100);
                offHeap.recordValue(100);
            }

            long onHeapCount = onHeap.getCountAtValue(100);
            long offHeapCount = offHeap.getCountAtValue(100);

            Assert.assertEquals(onHeapCount, offHeapCount);
            Assert.assertTrue(onHeapCount > 0);
        }
    }

    @Test
    public void testEquivalentValues() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(1, 100000, 2);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(1, 100000, 2);
            offHeap.setAllocator(allocator);

            long testValue = 12345;

            long onHeapEquiv = onHeap.lowestEquivalentValue(testValue);
            long offHeapEquiv = offHeap.lowestEquivalentValue(testValue);
            Assert.assertEquals(onHeapEquiv, offHeapEquiv);

            long onHeapHighEquiv = onHeap.highestEquivalentValue(testValue);
            long offHeapHighEquiv = offHeap.highestEquivalentValue(testValue);
            Assert.assertEquals(onHeapHighEquiv, offHeapHighEquiv);
        }
    }

    @Test
    public void testEmptyHistogramQueries() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            GroupByHistogramSink histogram = new GroupByHistogramSink(1, 1000, 3);
            histogram.setAllocator(allocator);

            Assert.assertEquals(0, histogram.getTotalCount());
            Assert.assertEquals(0.0, histogram.getMean(), 0.00);
            Assert.assertEquals(0, histogram.getMinValue());
            Assert.assertEquals(0, histogram.getMaxValue());
        }
    }

    @Test
    public void testMultipleResizes() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(3);
            offHeap.setAllocator(allocator);

            long[] values = {1, 100, 10000, 1000000, 100000000};
            for (long value : values) {
                onHeap.recordValue(value);
                offHeap.recordValue(value);
            }

            Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());
            Assert.assertEquals(onHeap.getMinValue(), offHeap.getMinValue());
            Assert.assertEquals(onHeap.getMaxValue(), offHeap.getMaxValue());
        }
    }

    @Test
    public void testAddDifferentSizes() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeapSmall = new Histogram(3);
            Histogram onHeapLarge = new Histogram(3);
            GroupByHistogramSink offHeapSmall = new GroupByHistogramSink(3);
            GroupByHistogramSink offHeapLarge = new GroupByHistogramSink(3);
            offHeapSmall.setAllocator(allocator);
            offHeapLarge.setAllocator(allocator);

            onHeapSmall.recordValue(100);
            onHeapLarge.recordValue(500000);
            offHeapSmall.recordValue(100);
            offHeapLarge.recordValue(500000);

            onHeapSmall.add(onHeapLarge);
            offHeapSmall.add(offHeapLarge);

            Assert.assertEquals(onHeapSmall.getTotalCount(), offHeapSmall.getTotalCount());
            Assert.assertEquals(onHeapSmall.getMaxValue(), offHeapSmall.getMaxValue());
            Assert.assertEquals(onHeapSmall.getMean(), offHeapSmall.getMean(), 0.0);
        }
    }

    @Test
    public void testMultipleOperations() {
        try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
            Histogram onHeap = new Histogram(3);
            GroupByHistogramSink offHeap = new GroupByHistogramSink(3);
            offHeap.setAllocator(allocator);

            for (int i = 0; i < 1000; i++) {
                long value = i % 100;
                onHeap.recordValue(value);
                offHeap.recordValue(value);
            }

            Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());

            onHeap.reset();
            offHeap.clear();

            for (int i = 0; i < 100; i++) {
                onHeap.recordValue(i);
                offHeap.recordValue(i);
            }

            Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());
            Assert.assertEquals(onHeap.getMean(), offHeap.getMean(), 0.0);
        }
    }
}