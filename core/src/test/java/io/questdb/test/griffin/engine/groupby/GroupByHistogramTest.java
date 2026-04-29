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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByHistogram;
import io.questdb.std.Rnd;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GroupByHistogramTest extends AbstractCairoTest {

    @Test
    public void testBasicRecording() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap = new Histogram(1, 1000, 3);
                GroupByHistogram offHeap = new GroupByHistogram(1, 1000, 3);
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
        });
    }

    @Test
    public void testPercentiles() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap = new Histogram(1, 3600000, 3);
                GroupByHistogram offHeap = new GroupByHistogram(1, 3600000, 3);
                offHeap.setAllocator(allocator);

                Rnd rnd = TestUtils.generateRandom(LOG);
                for (int i = 0; i < 10000; i++) {
                    long value = rnd.nextLong(3600000);
                    onHeap.recordValue(value);
                    offHeap.recordValue(value);
                }

                double[] percentiles = {50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99};
                for (double p : percentiles) {
                    long onHeapValue = onHeap.getValueAtPercentile(p);
                    long offHeapValue = offHeap.getValueAtPercentile(p);
                    Assert.assertEquals(onHeapValue, offHeapValue);
                }
            }
        });
    }

    @Test
    public void testRecordValueWithCount() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap = new Histogram(1, 10000, 2);
                GroupByHistogram offHeap = new GroupByHistogram(1, 10000, 2);
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
        });
    }

    @Test
    public void testReset() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap = new Histogram(3);
                GroupByHistogram offHeap = new GroupByHistogram(3);
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
        });
    }

    @Test
    public void testAutoResize() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap = new Histogram(3);
                GroupByHistogram offHeap = new GroupByHistogram(3);
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
        });
    }

    @Test
    public void testAdd() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap1 = new Histogram(1, 10000, 2);
                Histogram onHeap2 = new Histogram(1, 10000, 2);
                GroupByHistogram offHeap1 = new GroupByHistogram(1, 10000, 2);
                GroupByHistogram offHeap2 = new GroupByHistogram(1, 10000, 2);
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
        });
    }

    @Test
    public void testLargeValues() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                long highest = 3600000000L;
                Histogram onHeap = new Histogram(1, highest, 3);
                GroupByHistogram offHeap = new GroupByHistogram(1, highest, 3);
                offHeap.setAllocator(allocator);

                Rnd rnd = TestUtils.generateRandom(LOG);
                for (int i = 0; i < 1000; i++) {
                    long value = rnd.nextLong(highest);
                    onHeap.recordValue(value);
                    offHeap.recordValue(value);
                }

                Assert.assertEquals(onHeap.getTotalCount(), offHeap.getTotalCount());

                Assert.assertEquals(onHeap.getValueAtPercentile(50), offHeap.getValueAtPercentile(50));
                Assert.assertEquals(onHeap.getValueAtPercentile(99), offHeap.getValueAtPercentile(99));
            }
        });
    }

    @Test
    public void testGetCountAtValue() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap = new Histogram(1, 1000, 2);
                GroupByHistogram offHeap = new GroupByHistogram(1, 1000, 2);
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
        });
    }

    @Test
    public void testEquivalentValues() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeap = new Histogram(1, 100000, 2);
                GroupByHistogram offHeap = new GroupByHistogram(1, 100000, 2);
                offHeap.setAllocator(allocator);

                long testValue = 12345;

                long onHeapEquiv = onHeap.lowestEquivalentValue(testValue);
                long offHeapEquiv = offHeap.lowestEquivalentValue(testValue);
                Assert.assertEquals(onHeapEquiv, offHeapEquiv);

                long onHeapHighEquiv = onHeap.highestEquivalentValue(testValue);
                long offHeapHighEquiv = offHeap.highestEquivalentValue(testValue);
                Assert.assertEquals(onHeapHighEquiv, offHeapHighEquiv);
            }
        });
    }

    @Test
    public void testEmptyHistogramQueries() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram histogram = new GroupByHistogram(1, 1000, 3);
                histogram.setAllocator(allocator);

                Assert.assertEquals(0, histogram.getTotalCount());
                Assert.assertEquals(0.0, histogram.getMean(), 0.00);
                Assert.assertEquals(0, histogram.getMinValue());
                Assert.assertEquals(0, histogram.getMaxValue());
            }
        });
    }

    @Test
    public void testAddDifferentSizes() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram onHeapSmall = new Histogram(3);
                Histogram onHeapLarge = new Histogram(3);
                GroupByHistogram offHeapSmall = new GroupByHistogram(3);
                GroupByHistogram offHeapLarge = new GroupByHistogram(3);
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
        });
    }

    @Test
    public void testRepointingToExistingOffHeapData() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram histogram = new GroupByHistogram(1, 1000, 3);
                histogram.setAllocator(allocator);

                histogram.recordValue(100);
                histogram.recordValue(200);
                histogram.recordValue(300);

                long addr1 = histogram.ptr();
                Assert.assertNotEquals(0, addr1);
                Assert.assertEquals(3, histogram.getTotalCount());

                GroupByHistogram other = new GroupByHistogram(1, 1000, 3);
                other.setAllocator(allocator);
                other.recordValue(500);
                other.recordValue(600);
                long addr2 = other.ptr();
                Assert.assertNotEquals(0, addr2);
                Assert.assertNotEquals(addr1, addr2);

                histogram.of(addr2);
                Assert.assertEquals(addr2, histogram.ptr());

                Assert.assertEquals(2, histogram.getTotalCount());
                Assert.assertEquals(500, histogram.getMinValue());
                Assert.assertEquals(600, histogram.getMaxValue());

                histogram.of(addr1);
                Assert.assertEquals(addr1, histogram.ptr());
                Assert.assertEquals(3, histogram.getTotalCount());

                histogram.of(0);
                Assert.assertEquals(0, histogram.ptr());
                Assert.assertEquals(0, histogram.getTotalCount());
            }
        });
    }

    @Test
    public void testMerge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(1, 10000, 2);
                GroupByHistogram src = new GroupByHistogram(1, 10000, 2);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                for (int i = 1; i <= 50; i++) {
                    dest.recordValue(i);
                }
                dest.setStartTimeStamp(1000);
                dest.setEndTimeStamp(2000);

                for (int i = 51; i <= 100; i++) {
                    src.recordValue(i);
                }
                src.setStartTimeStamp(1500);
                src.setEndTimeStamp(2500);

                dest.merge(src);

                Assert.assertEquals(100, dest.getTotalCount());
                Assert.assertEquals(1, dest.getMinValue());
                Assert.assertEquals(100, dest.getMaxValue());
                Assert.assertEquals(50.5, dest.getMean(), 0.0);
                Assert.assertEquals(1000, dest.getStartTimeStamp());
                Assert.assertEquals(2500, dest.getEndTimeStamp());
            }
        });
    }

    @Test
    public void testMergeEmptySource() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(1, 1000, 3);
                GroupByHistogram src = new GroupByHistogram(1, 1000, 3);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                for (int i = 1; i <= 10; i++) {
                    dest.recordValue(i);
                }

                long beforeCount = dest.getTotalCount();
                long beforeMin = dest.getMinValue();
                long beforeMax = dest.getMaxValue();

                dest.merge(src);

                Assert.assertEquals(beforeCount, dest.getTotalCount());
                Assert.assertEquals(beforeMin, dest.getMinValue());
                Assert.assertEquals(beforeMax, dest.getMaxValue());
            }
        });
    }

    @Test
    public void testMergeEmptyDestination() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(1, 1000, 3);
                GroupByHistogram src = new GroupByHistogram(1, 1000, 3);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                for (int i = 1; i <= 10; i++) {
                    src.recordValue(i);
                }

                dest.merge(src);

                Assert.assertEquals(10, dest.getTotalCount());
                Assert.assertEquals(1, dest.getMinValue());
                Assert.assertEquals(10, dest.getMaxValue());
            }
        });
    }

    @Test
    public void testMergePreservesPercentiles() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(1, 10000, 2);
                GroupByHistogram src = new GroupByHistogram(1, 10000, 2);
                Histogram combined = new Histogram(1, 10000, 2);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                Rnd rnd = TestUtils.generateRandom(LOG);
                for (int i = 0; i < 1000; i++) {
                    long val1 = rnd.nextLong(10000);
                    dest.recordValue(val1);
                    combined.recordValue(val1);

                    long val2 = rnd.nextLong(10000);
                    src.recordValue(val2);
                    combined.recordValue(val2);
                }

                dest.merge(src);

                double[] percentiles = {50.0, 75.0, 90.0, 95.0, 99.0};
                for (double p : percentiles) {
                    long mergedValue = dest.getValueAtPercentile(p);
                    long combinedValue = combined.getValueAtPercentile(p);
                    Assert.assertEquals(combinedValue, mergedValue);
                }
            }
        });
    }

    @Test
    public void testMergeWithOverlappingValues() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(1, 1000, 2);
                GroupByHistogram src = new GroupByHistogram(1, 1000, 2);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                for (int i = 0; i < 10; i++) {
                    dest.recordValue(100);
                    src.recordValue(100);
                }

                dest.merge(src);

                Assert.assertEquals(20, dest.getTotalCount());
                Assert.assertEquals(20, dest.getCountBetweenValues(100, 100));
            }
        });
    }

    @Test
    public void testMergeEmptyDestinationWithLargeValues() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(3);
                dest.setAllocator(allocator);

                GroupByHistogram src = new GroupByHistogram(3);
                src.setAllocator(allocator);
                Histogram oracle = new Histogram(3);
                long[] values = {1000000, 2000000, 3000000};
                for (long v : values) {
                    src.recordValue(v);
                    oracle.recordValue(v);
                }

                Assert.assertEquals(0, dest.getTotalCount());
                Assert.assertEquals(3, src.getTotalCount());
                Assert.assertEquals(0, dest.ptr());

                dest.merge(src);

                Assert.assertEquals(oracle.getTotalCount(), dest.getTotalCount());
                Assert.assertEquals(oracle.getMinValue(), dest.getMinValue());
                Assert.assertEquals(oracle.getMaxValue(), dest.getMaxValue());
                Assert.assertNotEquals(0, dest.ptr());
            }
        });
    }

    @Test
    public void testMergeBothEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(1, 1000, 3);
                GroupByHistogram src = new GroupByHistogram(1, 1000, 3);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                Assert.assertEquals(0, dest.getTotalCount());
                Assert.assertEquals(0, src.getTotalCount());

                dest.merge(src);

                Assert.assertEquals(0, dest.getTotalCount());
                Assert.assertEquals(0, dest.ptr());
            }
        });
    }

    @Test
    public void testMergeChain() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram h1 = new GroupByHistogram(1, 1000, 3);
                GroupByHistogram h2 = new GroupByHistogram(1, 1000, 3);
                GroupByHistogram h3 = new GroupByHistogram(1, 1000, 3);
                h1.setAllocator(allocator);
                h2.setAllocator(allocator);
                h3.setAllocator(allocator);

                h1.recordValue(10);
                h1.recordValue(20);

                h2.recordValue(30);
                h2.recordValue(40);

                h3.recordValue(50);
                h3.recordValue(60);

                h1.merge(h2);
                Assert.assertEquals(4, h1.getTotalCount());

                h1.merge(h3);
                Assert.assertEquals(6, h1.getTotalCount());
                Assert.assertEquals(10, h1.getMinValue());
                Assert.assertEquals(60, h1.getMaxValue());
            }
        });
    }

    @Test
    public void testMergeWithRepointedHistogram() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram h1 = new GroupByHistogram(1, 1000, 3);
                GroupByHistogram h2 = new GroupByHistogram(1, 1000, 3);
                GroupByHistogram flyweight = new GroupByHistogram(1, 1000, 3);
                h1.setAllocator(allocator);
                h2.setAllocator(allocator);
                flyweight.setAllocator(allocator);

                h1.recordValue(100);
                h1.recordValue(200);

                h2.recordValue(300);
                h2.recordValue(400);

                flyweight.of(h1.ptr());
                Assert.assertEquals(2, flyweight.getTotalCount());

                h2.merge(flyweight);

                Assert.assertEquals(4, h2.getTotalCount());
                Assert.assertEquals(h1.getMinValue(), h2.getMinValue());
            }
        });
    }

    @Test
    public void testMergeExtremeValueRange() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(3);
                GroupByHistogram src = new GroupByHistogram(3);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);
                Histogram oracle = new Histogram(3);

                long[] destValues = {1, 10};
                long[] srcValues = {1000000000L, 9000000000L};
                for (long v : destValues) {
                    dest.recordValue(v);
                    oracle.recordValue(v);
                }
                for (long v : srcValues) {
                    src.recordValue(v);
                    oracle.recordValue(v);
                }

                dest.merge(src);

                Assert.assertEquals(oracle.getTotalCount(), dest.getTotalCount());
                Assert.assertEquals(oracle.getMinValue(), dest.getMinValue());
                Assert.assertEquals(oracle.getMaxValue(), dest.getMaxValue());
                for (double p : new double[]{50.0, 75.0, 90.0, 99.0}) {
                    Assert.assertEquals(oracle.getValueAtPercentile(p), dest.getValueAtPercentile(p));
                }
            }
        });
    }

    @Test
    public void testMergePreservesCountsAccurately() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(1, 10000, 2);
                GroupByHistogram src = new GroupByHistogram(1, 10000, 2);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                dest.recordValueWithCount(100, 50);
                src.recordValueWithCount(100, 30);

                dest.merge(src);

                Assert.assertEquals(80, dest.getCountBetweenValues(100, 100));
                Assert.assertEquals(80, dest.getTotalCount());
            }
        });
    }

    @Test
    public void testMergeWithDifferentValueRanges() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram dest = new GroupByHistogram(2);
                GroupByHistogram src = new GroupByHistogram(2);
                dest.setAllocator(allocator);
                src.setAllocator(allocator);

                dest.recordValue(100);
                src.recordValue(1000000);

                dest.merge(src);

                Assert.assertEquals(2, dest.getTotalCount());
                Assert.assertTrue(dest.getMaxValue() >= 1000000);
            }
        });
    }

    @Test
    public void testRecordValueOutOfRangeWithoutAutoResize() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram offHeap = new GroupByHistogram(1, 1000, 3);
                offHeap.setAllocator(allocator);

                offHeap.recordValue(500);
                Assert.assertThrows(CairoException.class, () -> offHeap.recordValue(100_000));
                Assert.assertEquals(1, offHeap.getTotalCount());
            }
        });
    }

    @Test
    public void testRecordNegativeValue() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram offHeap = new GroupByHistogram(1, 1000, 3);
                offHeap.setAllocator(allocator);

                offHeap.recordValue(500);
                Assert.assertThrows(CairoException.class, () -> offHeap.recordValue(-1));
                Assert.assertEquals(1, offHeap.getTotalCount());
            }
        });
    }

    @Test
    public void testRecordValueFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram oracle = new Histogram(3);
                GroupByHistogram offHeap = new GroupByHistogram(3);
                offHeap.setAllocator(allocator);

                for (int i = 0; i < 10_000; i++) {
                    long value = rnd.nextLong(1_000_000_000L) + 1;
                    oracle.recordValue(value);
                    offHeap.recordValue(value);
                }

                assertHistogramsEqual(oracle, offHeap);
            }
        });
    }

    @Test
    public void testMergeFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                Histogram oracleDest = new Histogram(3);
                GroupByHistogram offHeapDest = new GroupByHistogram(3);
                offHeapDest.setAllocator(allocator);

                for (int h = 0; h < 5; h++) {
                    Histogram oracleSrc = new Histogram(3);
                    GroupByHistogram offHeapSrc = new GroupByHistogram(3);
                    offHeapSrc.setAllocator(allocator);

                    int count = rnd.nextInt(2000) + 100;
                    for (int i = 0; i < count; i++) {
                        long value = rnd.nextLong(1_000_000_000L) + 1;
                        oracleSrc.recordValue(value);
                        offHeapSrc.recordValue(value);
                    }

                    oracleDest.add(oracleSrc);
                    offHeapDest.merge(offHeapSrc);
                    offHeapSrc.clear();
                }

                assertHistogramsEqual(oracleDest, offHeapDest);
            }
        });
    }

    @Test
    public void testRepointingAfterResize() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                GroupByHistogram h1 = new GroupByHistogram(3);
                h1.setAllocator(allocator);

                h1.recordValue(100);
                h1.recordValue(200);
                Assert.assertEquals(2, h1.getTotalCount());

                long ptr1 = h1.ptr();
                Assert.assertNotEquals(0, ptr1);

                h1.recordValue(10000000);
                Assert.assertEquals(3, h1.getTotalCount());

                long ptr2 = h1.ptr();
                Assert.assertNotEquals(0, ptr2);

                GroupByHistogram h2 = new GroupByHistogram(3);
                h2.setAllocator(allocator);
                h2.of(ptr2);

                Assert.assertEquals(3, h2.getTotalCount());
                Assert.assertEquals(100, h2.getMinValue());
                Assert.assertTrue(h2.getMaxValue() >= 10000000);

                h2.recordValue(100000000);
                Assert.assertEquals(4, h2.getTotalCount());

                long ptr3 = h2.ptr();
                Assert.assertNotEquals(0, ptr3);

                GroupByHistogram h3 = new GroupByHistogram(3);
                h3.setAllocator(allocator);
                h3.of(ptr3);

                Assert.assertEquals(4, h3.getTotalCount());
                Assert.assertEquals(100, h3.getMinValue());
                Assert.assertTrue(h3.getMaxValue() >= 100000000);

                h3.recordValue(500);
                Assert.assertEquals(5, h3.getTotalCount());
            }
        });
    }

    @Test
    public void testRepointAndRecordFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                int numGroups = 10;
                Histogram[] oracles = new Histogram[numGroups];
                long[] ptrs = new long[numGroups];
                GroupByHistogram flyweight = new GroupByHistogram(3);
                flyweight.setAllocator(allocator);

                for (int i = 0; i < numGroups; i++) {
                    oracles[i] = new Histogram(3);
                }

                for (int i = 0; i < 50_000; i++) {
                    int group = rnd.nextInt(numGroups);
                    long value = rnd.nextLong(1_000_000_000L) + 1;

                    flyweight.of(ptrs[group]);
                    flyweight.recordValue(value);
                    ptrs[group] = flyweight.ptr();

                    oracles[group].recordValue(value);
                }

                for (int i = 0; i < numGroups; i++) {
                    flyweight.of(ptrs[i]);
                    assertHistogramsEqual(oracles[i], flyweight);
                }
            }
        });
    }

    private void assertHistogramsEqual(Histogram oracle, GroupByHistogram offHeap) {
        Assert.assertEquals(oracle.getTotalCount(), offHeap.getTotalCount());
        Assert.assertEquals(oracle.getMinValue(), offHeap.getMinValue());
        Assert.assertEquals(oracle.getMaxValue(), offHeap.getMaxValue());
        Assert.assertEquals(oracle.getMean(), offHeap.getMean(), 0.0);
        Assert.assertEquals(oracle.getStdDeviation(), offHeap.getStdDeviation(), 0.0);

        double[] percentiles = {50.0, 75.0, 90.0, 95.0, 99.0, 99.9};
        for (double p : percentiles) {
            Assert.assertEquals(oracle.getValueAtPercentile(p), offHeap.getValueAtPercentile(p));
        }
    }
}
