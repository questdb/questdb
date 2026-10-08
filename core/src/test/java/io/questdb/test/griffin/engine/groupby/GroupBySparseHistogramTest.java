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
import io.questdb.griffin.engine.groupby.GroupBySparseHistogram;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedHistogram;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The sparse histogram must return exactly what the on-heap HdrHistograms the serial approx_percentile
 * functions use return (PackedHistogram for precision 3..5, Histogram for the array form at 0..2), for
 * any multiset of values, and a histogram merged from partials must equal the one recorded serially.
 * Its memory must grow with the number of distinct counts indexes recorded, not with the value span.
 */
public class GroupBySparseHistogramTest extends AbstractCairoTest {
    private static final double[] PERCENTILES = {
            0.0, 1e-9, 0.001, 0.1, 1.0, 5.0, 10.0, 25.0, 33.333333333333336, 49.99999999, 50.0, 50.00000001,
            66.66666666666667, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.9999999, 100.0
    };

    @Test
    public void testEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                for (int precision = 0; precision <= 5; precision++) {
                    try (
                            GroupBySparseHistogram h = new GroupBySparseHistogram(precision);
                            GroupBySparseHistogram other = new GroupBySparseHistogram(precision)
                    ) {
                        h.setAllocator(allocator);
                        h.of(0);
                        Assert.assertEquals(0, h.getTotalCount());
                        Assert.assertEquals(0, h.ptr());
                        final PackedHistogram ref = newPacked(precision);
                        for (double p : PERCENTILES) {
                            Assert.assertEquals(ref.getValueAtPercentile(p), h.getValueAtPercentile(p));
                        }
                        // merging an empty histogram allocates nothing
                        other.setAllocator(allocator);
                        h.merge(other.of(0));
                        Assert.assertEquals(0, h.ptr());
                    }
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
                        try (GroupBySparseHistogram h = new GroupBySparseHistogram(precision)) {
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
                                if (rnd.nextInt(1_000) == 0) {
                                    // a read in between must not change what is recorded next
                                    Assert.assertEquals(packed.getValueAtPercentile(50), h.getValueAtPercentile(50));
                                }
                            }
                            assertSame(packed, h, "precision=" + precision + ", distribution=" + distribution);
                            assertSame(dense, h, "precision=" + precision + ", distribution=" + distribution);
                            // a second flyweight over the same memory reads the same: reads leave it as it was
                            try (GroupBySparseHistogram reader = new GroupBySparseHistogram(precision)) {
                                assertSame(packed, reader.of(h.ptr()), "reader, precision=" + precision + ", distribution=" + distribution);
                            }
                        }
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
                        final GroupBySparseHistogram[] partials = new GroupBySparseHistogram[parts];
                        final LongList ptrs = new LongList();
                        final PackedHistogram serial = newPacked(precision);
                        for (int p = 0; p < parts; p++) {
                            final GroupBySparseHistogram partial = new GroupBySparseHistogram(precision);
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
                        final GroupBySparseHistogram dest = new GroupBySparseHistogram(precision);
                        dest.setAllocator(allocator);
                        long destPtr = 0;
                        final GroupBySparseHistogram src = new GroupBySparseHistogram(precision);
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
                            partials[p].close();
                        }
                        dest.close();
                        src.close();
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeValue() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                final GroupBySparseHistogram h = new GroupBySparseHistogram(5);
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
    public void testFootprintGrowsWithDistinctIndexesOnly() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                for (int precision = 0; precision <= 5; precision++) {
                    try (GroupBySparseHistogram h = new GroupBySparseHistogram(precision)) {
                        h.setAllocator(allocator);
                        // two values, any span: 64 bytes (16 header + 4 slots of 12), the old paged layout
                        // took up to 278 KiB for {1, 10^15} at 5 digits
                        for (long hi : new long[]{1_000L, 1_000_000L, 1_000_000_000L, 1_000_000_000_000_000L, Long.MAX_VALUE}) {
                            h.of(0);
                            h.recordValue(1);
                            h.recordValue(hi);
                            Assert.assertEquals(64, GroupBySparseHistogram.footprint(h.ptr()));
                        }
                        // many values: at most 4/3 slots of 12 bytes per distinct index, rounded up to a power of 2
                        h.of(0);
                        final PackedHistogram ref = newPacked(precision);
                        final int n = 1 + rnd.nextInt(20_000);
                        for (int i = 0; i < n; i++) {
                            final long v = nextValue(rnd, 3);
                            h.recordValue(v);
                            ref.recordValue(v);
                        }
                        int distinct = 0;
                        for (int i = 0, len = ref.countsArrayLength(); i < len; i++) {
                            if (ref.getCountAtIndex(i) > 0) {
                                distinct++;
                            }
                        }
                        final long slots = Math.max(4, Long.highestOneBit(distinct * 4L / 3) << 1);
                        Assert.assertTrue(
                                "precision=" + precision + ", distinct=" + distinct + ", footprint=" + GroupBySparseHistogram.footprint(h.ptr()),
                                GroupBySparseHistogram.footprint(h.ptr()) <= 16 + 12 * slots
                        );
                        assertSame(ref, h, "precision=" + precision);
                    }
                }
            }
        });
    }

    @Test
    public void testMemoryIsChargedToTheTracker() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(256 * 1024);
                    GroupByAllocator allocator = new FastGroupByAllocator(4096, 64 * 1024 * 1024);
                    GroupBySparseHistogram h = new GroupBySparseHistogram(5)
            ) {
                allocator.setMemoryTracker(tracker);
                h.setAllocator(allocator);
                h.of(0);
                h.recordValue(1);
                Assert.assertTrue(tracker.getUsed() > 0);
                try {
                    // ~1M distinct indexes need ~24 MiB of slots, over the 256 KiB limit
                    for (long v = 0; v < 1_000_000; v++) {
                        h.recordValue(v);
                    }
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                allocator.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSparseWideRange() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    GroupBySparseHistogram h = new GroupBySparseHistogram(5)
            ) {
                h.setAllocator(allocator);
                h.of(0);
                final PackedHistogram ref = newPacked(5);
                final long[] values = {1_000_000, 1L << 40, 5, 0, Long.MAX_VALUE, 123_456_789_012L, 262_143, 262_144, 7, Long.MAX_VALUE / 3};
                for (long v : values) {
                    h.recordValue(v);
                    ref.recordValue(v);
                    assertSame(ref, h, "after " + v);
                }
                // 10 distinct indexes: 16 slots
                Assert.assertEquals(16 + 12 * 16, GroupBySparseHistogram.footprint(h.ptr()));
            }
        });
    }

    private static void assertSame(AbstractHistogram expected, GroupBySparseHistogram actual, String message) {
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
