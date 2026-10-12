/*+*****************************************************************************
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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class GroupByLongListTest extends AbstractCairoTest {

    @Test
    public void testAddList() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList dest = new GroupByLongList(2, 0L);
                dest.setAllocator(allocator);
                dest.of(0);
                GroupByLongList src = new GroupByLongList(2, 0L);
                src.setAllocator(allocator);
                src.of(0);

                dest.add(1);
                dest.add(2);
                for (int i = 10; i < 15; i++) {
                    src.add(i);
                }
                Assert.assertEquals(dest.addressOf(2), dest.appendAddress());

                // whole list, growing the destination beyond its initial capacity
                final long ptr = dest.add(src);
                Assert.assertEquals(dest.ptr(), ptr);
                assertList(dest, 1, 2, 10, 11, 12, 13, 14);

                // sub-range [lo, hi)
                dest.add(src, 1, 3);
                assertList(dest, 1, 2, 10, 11, 12, 13, 14, 11, 12);

                // empty range leaves the list intact
                dest.add(src, 4, 4);
                assertList(dest, 1, 2, 10, 11, 12, 13, 14, 11, 12);

                // the source list is not modified
                assertList(src, 10, 11, 12, 13, 14);
            }
        });
    }

    @Test
    public void testCheckCapacityNegative() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);
                try {
                    list.checkCapacity(-1);
                    Assert.fail("Should throw IllegalArgumentException for negative capacity");
                } catch (IllegalArgumentException e) {
                    TestUtils.assertContains(e.getMessage(), "Negative capacity");
                }
                Assert.assertEquals(16, list.capacity());
            }
        });
    }

    @Test
    public void testGetOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);
                list.add(42);
                Assert.assertEquals(42, list.get(0));
                try {
                    list.get(1);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for index >= size()");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectAllEqual() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                for (int i = 0; i < 100; i++) {
                    list.add(42L);
                }

                // Should complete without degenerate behavior on all-equal data
                list.quickSelect(0, 99, 50);
                Assert.assertEquals(42L, list.getQuick(50));
            }
        });
    }

    @Test
    public void testQuickSelectBoundsHiTooLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(0, 5, 2);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for hi >= size()");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectBoundsKGreaterThanHi() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(0, 3, 4);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for k > hi");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectBoundsKLessThanLo() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(2, 4, 1);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for k < lo");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectBoundsNegativeLo() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(-1, 4, 2);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for lo < 0");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectFirst() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select minimum element (k=0)
                list.quickSelect(0, 4, 0);
                Assert.assertEquals(1L, list.getQuick(0));
            }
        });
    }

    @Test
    public void testQuickSelectLast() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select maximum element (k=4)
                list.quickSelect(0, 4, 4);
                Assert.assertEquals(5L, list.getQuick(4));
            }
        });
    }

    @Test
    public void testQuickSelectMedian() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select median element (k=2)
                list.quickSelect(0, 4, 2);
                Assert.assertEquals(3L, list.getQuick(2));
            }
        });
    }

    @Test
    public void testQuickSelectMultipleBoundsHiTooLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelectMultiple(0, 5, new int[]{0, 2, 4}, 0, 3);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for hi >= size()");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectMultipleBoundsNegativeLo() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelectMultiple(-1, 4, new int[]{0, 2, 4}, 0, 3);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for lo < 0");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectMultipleFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                for (int iteration = 0; iteration < 500; iteration++) {
                    list.of(0);
                    final int size = 1 + rnd.nextInt(300);
                    // A narrow value range produces long runs of duplicates, so the three-way partition
                    // often lands requested indices inside the equal-to-pivot region.
                    final int valueRange = rnd.nextBoolean() ? 1 + rnd.nextInt(5) : Integer.MAX_VALUE;
                    final long[] expected = new long[size];
                    for (int i = 0; i < size; i++) {
                        final long v = rnd.nextInt(valueRange) - (valueRange >> 1);
                        list.add(v);
                        expected[i] = v;
                    }
                    Arrays.sort(expected);

                    // sorted, de-duplicated set of indices that is skewed towards either end now and then,
                    // so that both sides of a partition get to be the larger one
                    final int indexCount = 1 + rnd.nextInt(Math.min(size, 8));
                    final int bias = rnd.nextInt(3);
                    final int[] indices = new int[indexCount];
                    int count = 0;
                    for (int i = 0; i < indexCount; i++) {
                        int idx = rnd.nextInt(size);
                        if (bias == 1) {
                            idx = idx / 8;
                        } else if (bias == 2) {
                            idx = size - 1 - idx / 8;
                        }
                        indices[count++] = idx;
                    }
                    Arrays.sort(indices, 0, count);
                    int unique = 0;
                    for (int i = 0; i < count; i++) {
                        if (unique == 0 || indices[unique - 1] != indices[i]) {
                            indices[unique++] = indices[i];
                        }
                    }

                    list.quickSelectMultiple(0, size - 1, indices, 0, unique);
                    for (int i = 0; i < unique; i++) {
                        Assert.assertEquals(
                                "iteration " + iteration + ", index " + indices[i],
                                expected[indices[i]],
                                list.getQuick(indices[i])
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testQuickSelectMultipleThreeIndices() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select elements at indices 0, 2, 4 (min, median, max)
                list.quickSelectMultiple(0, 4, new int[]{0, 2, 4}, 0, 3);
                Assert.assertEquals(1L, list.getQuick(0));
                Assert.assertEquals(3L, list.getQuick(2));
                Assert.assertEquals(5L, list.getQuick(4));
            }
        });
    }

    @Test
    public void testSmallListSort() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                // empty and single-element lists are already sorted
                list.sort();
                Assert.assertEquals(0, list.size());
                list.add(7);
                list.sort();
                assertList(list, 7);

                list.add(-3);
                list.add(5);
                list.sort();
                assertList(list, -3, 5, 7);
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                Assert.assertEquals(0, list.size());
                Assert.assertEquals(16, list.capacity());

                final int N = 1000;

                for (int i = 0; i < N; i++) {
                    list.add(i);
                }
                Assert.assertEquals(N, list.size());
                Assert.assertTrue(list.capacity() >= N);
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, list.get(i));
                }
            }
        });
    }

    @Test
    public void testSortAsUnsigned() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                list.add(-1);
                list.add(3);
                list.add(Long.MIN_VALUE);
                list.add(0);
                list.add(Long.MAX_VALUE);
                list.sortAsUnsigned();

                // negative values have the top bit set, so they sort after all positive values
                assertList(list, 0, 3, Long.MAX_VALUE, Long.MIN_VALUE, -1);
            }
        });
    }

    @Test
    public void testSortFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                for (int iteration = 0; iteration < 400; iteration++) {
                    list.of(0);
                    // sizes on both sides of the insertion sort threshold
                    final int size = rnd.nextBoolean() ? rnd.nextInt(60) : rnd.nextInt(5_000);
                    final long[] expected = new long[size];
                    final int distribution = rnd.nextInt(6);
                    for (int i = 0; i < size; i++) {
                        final long v = switch (distribution) {
                            // distinct-ish values: the five sample points are all different
                            case 0 -> rnd.nextLong();
                            // a few distinct values: equal sample points select the single-pivot partition
                            case 1 -> rnd.nextInt(3);
                            // medium cardinality: distinct pivots with a large run of pivot-equal values
                            case 2 -> rnd.nextInt(16);
                            // already sorted
                            case 3 -> i;
                            // reverse sorted
                            case 4 -> size - i;
                            // sorted runs with a descending tail
                            default -> i < size / 2 ? i : size - i + rnd.nextInt(4);
                        };
                        list.add(v);
                        expected[i] = v;
                    }

                    list.sort();
                    Arrays.sort(expected);

                    Assert.assertEquals(size, list.size());
                    for (int i = 0; i < size; i++) {
                        Assert.assertEquals(
                                "iteration " + iteration + ", distribution " + distribution + ", index " + i,
                                expected[i],
                                list.getQuick(i)
                        );
                    }
                }
            }
        });
    }

    private static void assertList(GroupByLongList list, long... expected) {
        Assert.assertEquals(expected.length, list.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("index " + i, expected[i], list.get(i));
        }
    }
}
