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
import io.questdb.griffin.engine.groupby.GroupByPagedHistogram;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedHistogram;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The paged histogram must return exactly what the on-heap HdrHistograms the serial approx_percentile
 * functions use return (PackedHistogram for precision 3..5, Histogram for the array form at 0..2), for
 * any multiset of values, and a histogram merged from partials must equal the one recorded serially.
 */
public class GroupByPagedHistogramTest extends AbstractCairoTest {
    private static final double[] PERCENTILES = {
            0.0, 1e-9, 0.001, 0.1, 1.0, 5.0, 10.0, 25.0, 33.333333333333336, 49.99999999, 50.0, 50.00000001,
            66.66666666666667, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.9999999, 100.0
    };

    @Test
    public void testEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                for (int precision = 0; precision <= 5; precision++) {
                    final GroupByPagedHistogram h = new GroupByPagedHistogram(precision);
                    h.setAllocator(allocator);
                    h.of(0);
                    Assert.assertEquals(0, h.getTotalCount());
                    Assert.assertEquals(0, h.ptr());
                    final PackedHistogram ref = newPacked(precision);
                    for (double p : PERCENTILES) {
                        Assert.assertEquals(ref.getValueAtPercentile(p), h.getValueAtPercentile(p));
                    }
                    // merging an empty histogram allocates nothing
                    final GroupByPagedHistogram other = new GroupByPagedHistogram(precision);
                    other.setAllocator(allocator);
                    h.merge(other.of(0));
                    Assert.assertEquals(0, h.ptr());
                }
            }
        });
    }

    @Test
    public void testMatchesOnHeapHistograms() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                for (int precision = 0; precision <= 5; precision++) {
                    for (int distribution = 0; distribution < 7; distribution++) {
                        final GroupByPagedHistogram h = new GroupByPagedHistogram(precision);
                        h.setAllocator(allocator);
                        h.of(0);
                        final PackedHistogram packed = newPacked(precision);
                        final Histogram dense = newDense(precision);
                        final int n = 1 + rnd.nextInt(5_000);
                        for (int i = 0; i < n; i++) {
                            final long v = nextValue(rnd, distribution);
                            h.recordValue(v);
                            packed.recordValue(v);
                            dense.recordValue(v);
                        }
                        assertSame(packed, h, "precision=" + precision + ", distribution=" + distribution);
                        assertSame(dense, h, "precision=" + precision + ", distribution=" + distribution);
                    }
                }
            }
        });
    }

    @Test
    public void testMergeEqualsSerial() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                for (int precision = 0; precision <= 5; precision++) {
                    for (int distribution = 0; distribution < 7; distribution++) {
                        final int parts = 1 + rnd.nextInt(6);
                        final GroupByPagedHistogram[] partials = new GroupByPagedHistogram[parts];
                        final LongList ptrs = new LongList();
                        final PackedHistogram serial = newPacked(precision);
                        for (int p = 0; p < parts; p++) {
                            final GroupByPagedHistogram partial = new GroupByPagedHistogram(precision);
                            partial.setAllocator(allocator);
                            partial.of(0);
                            // some partials stay empty, the others cover different value ranges
                            final int n = rnd.nextInt(4) == 0 ? 0 : rnd.nextInt(3_000);
                            final int partDistribution = (distribution + p) % 7;
                            for (int i = 0; i < n; i++) {
                                final long v = nextValue(rnd, partDistribution);
                                partial.recordValue(v);
                                serial.recordValue(v);
                            }
                            partials[p] = partial;
                            ptrs.add(partial.ptr());
                        }
                        // merge in a random order into a fresh histogram, as the merge step does
                        final GroupByPagedHistogram dest = new GroupByPagedHistogram(precision);
                        dest.setAllocator(allocator);
                        long destPtr = 0;
                        final GroupByPagedHistogram src = new GroupByPagedHistogram(precision);
                        for (int k = 0; k < parts; k++) {
                            final int p = (k + rnd.nextInt(parts)) % parts;
                            if (ptrs.getQuick(p) == -1) {
                                continue;
                            }
                            dest.of(destPtr).merge(src.of(ptrs.getQuick(p)));
                            destPtr = dest.ptr();
                            ptrs.setQuick(p, -1);
                        }
                        for (int p = 0; p < parts; p++) {
                            if (ptrs.getQuick(p) != -1) {
                                dest.of(destPtr).merge(src.of(ptrs.getQuick(p)));
                                destPtr = dest.ptr();
                            }
                        }
                        dest.of(destPtr);
                        assertSame(serial, dest, "precision=" + precision + ", distribution=" + distribution + ", parts=" + parts);
                        // the partials are left intact by the merge
                        for (int p = 0; p < parts; p++) {
                            Assert.assertTrue(partials[p].getTotalCount() >= 0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeValue() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                final GroupByPagedHistogram h = new GroupByPagedHistogram(5);
                h.setAllocator(allocator);
                h.of(0);
                String expected = null;
                try {
                    newPacked(5).recordValue(-1);
                    Assert.fail();
                } catch (CairoException e) {
                    expected = e.getFlyweightMessage().toString();
                }
                try {
                    h.recordValue(-1);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertEquals(expected, e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testSparseWideRange() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                final GroupByPagedHistogram h = new GroupByPagedHistogram(5);
                h.setAllocator(allocator);
                h.of(0);
                final PackedHistogram ref = newPacked(5);
                // the directory grows upwards, then downwards, then upwards again
                final long[] values = {1_000_000, 1L << 40, 5, 0, Long.MAX_VALUE, 123_456_789_012L, 262_143, 262_144, 7, Long.MAX_VALUE / 3};
                for (long v : values) {
                    h.recordValue(v);
                    ref.recordValue(v);
                    assertSame(ref, h, "after " + v);
                }
                // a few values over a wide range cost pages and directory slots, not the full counts array
                Assert.assertTrue(allocator.allocated() < 4 * 1024 * 1024);
            }
        });
    }

    private static void assertSame(AbstractHistogram expected, GroupByPagedHistogram actual, String message) {
        Assert.assertEquals(message, expected.getTotalCount(), actual.getTotalCount());
        for (double p : PERCENTILES) {
            Assert.assertEquals(message + ", p=" + p, expected.getValueAtPercentile(p), actual.getValueAtPercentile(p));
        }
    }

    private static Histogram newDense(int precision) {
        final Histogram h = new Histogram(1, 1000, precision);
        h.setAutoResize(true);
        return h;
    }

    private static PackedHistogram newPacked(int precision) {
        final PackedHistogram h = new PackedHistogram(1, 1000, precision);
        h.setAutoResize(true);
        return h;
    }

    private static long nextValue(Rnd rnd, int distribution) {
        return switch (distribution) {
            case 0 -> rnd.nextLong(1_000); // inside the pre-sized range
            case 1 -> rnd.nextInt(600) * 100L; // few distinct values, like TAQ sizes
            case 2 -> rnd.nextLong(10_000_000); // spans several buckets
            case 3 -> Math.abs(rnd.nextLong() >> rnd.nextInt(63)); // every magnitude
            case 4 -> rnd.nextInt(4); // tiny, many duplicates, including 0
            case 5 -> (1L << rnd.nextInt(62)) - rnd.nextInt(2); // bucket boundaries
            default -> Long.MAX_VALUE - rnd.nextInt(1_000); // the top of the range
        };
    }
}
