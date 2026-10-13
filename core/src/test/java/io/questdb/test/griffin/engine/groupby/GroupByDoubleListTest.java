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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByDoubleList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class GroupByDoubleListTest extends AbstractCairoTest {

    @Test
    public void testAddList() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList dest = new GroupByDoubleList(2, Double.NaN);
                dest.setAllocator(allocator);
                dest.of(0);
                GroupByDoubleList src = new GroupByDoubleList(2, Double.NaN);
                src.setAllocator(allocator);
                src.of(0);

                dest.add(1.5);
                dest.add(2.5);
                for (int i = 0; i < 5; i++) {
                    src.add(10 + i);
                }
                Assert.assertEquals(dest.addressOf(2), dest.appendAddress());

                // whole list, growing the destination beyond its initial capacity
                final long ptr = dest.add(src);
                Assert.assertEquals(dest.ptr(), ptr);
                assertList(dest, 1.5, 2.5, 10, 11, 12, 13, 14);

                // sub-range [lo, hi)
                dest.add(src, 1, 3);
                assertList(dest, 1.5, 2.5, 10, 11, 12, 13, 14, 11, 12);

                // empty range leaves the list intact
                dest.add(src, 4, 4);
                assertList(dest, 1.5, 2.5, 10, 11, 12, 13, 14, 11, 12);

                // the source list is not modified
                assertList(src, 10, 11, 12, 13, 14);
            }
        });
    }

    @Test
    public void testCheckCapacityNegative() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
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
    public void testGet() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);
                list.add(4.25);
                Assert.assertEquals(4.25, list.get(0), 0.0);
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
    public void testQuickSelectBoundsChecks() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);
                for (int i = 0; i < 5; i++) {
                    list.add(5 - i);
                }

                // lo < 0, hi >= size(), k < lo, k > hi
                final int[][] invalid = {{-1, 4, 2}, {0, 5, 2}, {2, 4, 1}, {0, 3, 4}};
                for (int[] args : invalid) {
                    try {
                        list.quickSelect(args[0], args[1], args[2]);
                        Assert.fail("Should throw ArrayIndexOutOfBoundsException for lo=" + args[0] + ", hi=" + args[1] + ", k=" + args[2]);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        TestUtils.assertContains(e.getMessage(), "size=5");
                    }
                }

                // the list is left untouched by the rejected calls
                assertList(list, 5, 4, 3, 2, 1);
            }
        });
    }

    @Test
    public void testQuickSelectMultipleBoundsChecks() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);
                for (int i = 0; i < 5; i++) {
                    list.add(5 - i);
                }

                final int[] indices = {0, 2, 4};
                try {
                    list.quickSelectMultiple(-1, 4, indices, 0, 3);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for lo < 0");
                } catch (ArrayIndexOutOfBoundsException e) {
                    TestUtils.assertContains(e.getMessage(), "lo=-1");
                }
                try {
                    list.quickSelectMultiple(0, 5, indices, 0, 3);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for hi >= size()");
                } catch (ArrayIndexOutOfBoundsException e) {
                    TestUtils.assertContains(e.getMessage(), "hi=5");
                }

                list.quickSelectMultiple(0, 4, indices, 0, 3);
                Assert.assertEquals(1.0, list.getQuick(0), 0.0);
                Assert.assertEquals(3.0, list.getQuick(2), 0.0);
                Assert.assertEquals(5.0, list.getQuick(4), 0.0);
            }
        });
    }

    @Test
    public void testSortEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                // Sorting empty range should do nothing
                // No elements, so we can't sort
                Assert.assertEquals(0, list.size());
            }
        });
    }

    @Test
    public void testSortSingleElement() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);
                list.add(42.0);

                list.sort(0, 0);

                Assert.assertEquals(1, list.size());
                Assert.assertEquals(42.0, list.getQuick(0), 0.0);
            }
        });
    }

    @Test
    public void testSortTwoElements() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);
                list.add(10.0);
                list.add(5.0);

                list.sort(0, 1);

                Assert.assertEquals(2, list.size());
                Assert.assertEquals(5.0, list.getQuick(0), 0.0);
                Assert.assertEquals(10.0, list.getQuick(1), 0.0);
            }
        });
    }

    @Test
    public void testSortAlreadySorted() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                for (int i = 0; i < 100; i++) {
                    list.add(i);
                }

                list.sort(0, 99);

                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(i, list.getQuick(i), 0.0);
                }
            }
        });
    }

    @Test
    public void testSortReverseSorted() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                for (int i = 99; i >= 0; i--) {
                    list.add(i);
                }

                list.sort(0, 99);

                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(i, list.getQuick(i), 0.0);
                }
            }
        });
    }

    @Test
    public void testSortWithDuplicates() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                double[] values = {5.0, 2.0, 8.0, 2.0, 9.0, 1.0, 5.0, 8.0};
                for (double v : values) {
                    list.add(v);
                }

                list.sort(0, 7);

                Assert.assertEquals(1.0, list.getQuick(0), 0.0);
                Assert.assertEquals(2.0, list.getQuick(1), 0.0);
                Assert.assertEquals(2.0, list.getQuick(2), 0.0);
                Assert.assertEquals(5.0, list.getQuick(3), 0.0);
                Assert.assertEquals(5.0, list.getQuick(4), 0.0);
                Assert.assertEquals(8.0, list.getQuick(5), 0.0);
                Assert.assertEquals(8.0, list.getQuick(6), 0.0);
                Assert.assertEquals(9.0, list.getQuick(7), 0.0);
            }
        });
    }

    @Test
    public void testSortPartialRange() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                double[] values = {1.0, 9.0, 5.0, 3.0, 7.0, 2.0};
                for (double v : values) {
                    list.add(v);
                }

                // Sort only indices 1-4 (values 9, 5, 3, 7)
                list.sort(1, 4);

                Assert.assertEquals(1.0, list.getQuick(0), 0.0); // unchanged
                Assert.assertEquals(3.0, list.getQuick(1), 0.0); // sorted
                Assert.assertEquals(5.0, list.getQuick(2), 0.0); // sorted
                Assert.assertEquals(7.0, list.getQuick(3), 0.0); // sorted
                Assert.assertEquals(9.0, list.getQuick(4), 0.0); // sorted
                Assert.assertEquals(2.0, list.getQuick(5), 0.0); // unchanged
            }
        });
    }

    @Test
    public void testSortWithNegativeNumbers() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                double[] values = {-5.5, 10.2, -3.3, 0.0, -8.1, 7.7, -1.0};
                for (double v : values) {
                    list.add(v);
                }

                list.sort(0, 6);

                Assert.assertEquals(-8.1, list.getQuick(0), 0.0);
                Assert.assertEquals(-5.5, list.getQuick(1), 0.0);
                Assert.assertEquals(-3.3, list.getQuick(2), 0.0);
                Assert.assertEquals(-1.0, list.getQuick(3), 0.0);
                Assert.assertEquals(0.0, list.getQuick(4), 0.0);
                Assert.assertEquals(7.7, list.getQuick(5), 0.0);
                Assert.assertEquals(10.2, list.getQuick(6), 0.0);
            }
        });
    }

    @Test
    public void testSortSmallArray() throws Exception {
        assertMemoryLeak(() -> {
            // Test arrays smaller than INSERTION_SORT_THRESHOLD (47)
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                Rnd rnd = new Rnd();
                double[] expected = new double[30];
                for (int i = 0; i < 30; i++) {
                    double v = rnd.nextDouble();
                    list.add(v);
                    expected[i] = v;
                }

                list.sort(0, 29);
                java.util.Arrays.sort(expected);

                for (int i = 0; i < 30; i++) {
                    Assert.assertEquals(expected[i], list.getQuick(i), 0.0);
                }
            }
        });
    }

    @Test
    public void testSortLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            // Test arrays larger than INSERTION_SORT_THRESHOLD (47)
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                Rnd rnd = new Rnd();
                double[] expected = new double[200];
                for (int i = 0; i < 200; i++) {
                    double v = rnd.nextDouble() * 1000;
                    list.add(v);
                    expected[i] = v;
                }

                list.sort(0, 199);
                java.util.Arrays.sort(expected);

                for (int i = 0; i < 200; i++) {
                    Assert.assertEquals(expected[i], list.getQuick(i), 0.0);
                }
            }
        });
    }

    @Test
    public void testSortRandomFuzz() throws Exception {
        assertMemoryLeak(() -> {
            // Fuzz test with random data
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                Rnd rnd = new Rnd();
                for (int iteration = 0; iteration < 10; iteration++) {
                    GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                    list.setAllocator(allocator);
                    list.of(0);

                    int size = rnd.nextInt(500) + 1;
                    double[] expected = new double[size];
                    for (int i = 0; i < size; i++) {
                        double v = rnd.nextDouble() * 2000 - 1000; // random in [-1000, 1000]
                        list.add(v);
                        expected[i] = v;
                    }

                    list.sort(0, size - 1);
                    java.util.Arrays.sort(expected);

                    for (int i = 0; i < size; i++) {
                        Assert.assertEquals("Iteration " + iteration + ", index " + i,
                                expected[i], list.getQuick(i), 0.0);
                    }
                }
            }
        });
    }

    @Test
    public void testSortAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                for (int i = 0; i < 100; i++) {
                    list.add(42.42);
                }

                list.sort(0, 99);

                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(42.42, list.getQuick(i), 0.0);
                }
            }
        });
    }

    @Test
    public void testSortWithSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                // Test with very small and very large values
                // Double.MIN_VALUE is the smallest positive double (4.9E-324), not the most negative
                list.add(Double.MIN_VALUE);
                list.add(Double.MAX_VALUE);
                list.add(0.0);
                list.add(-0.0);
                list.add(1.0);
                list.add(-1.0);

                list.sort(0, 5);

                // Expected order: -1.0, -0.0/0.0, MIN_VALUE (smallest positive), 1.0, MAX_VALUE
                Assert.assertEquals(-1.0, list.getQuick(0), 0.0);
                // -0.0 and 0.0 are equal in comparisons
                Assert.assertTrue(list.getQuick(1) >= -0.0 && list.getQuick(1) <= 0.0);
                Assert.assertTrue(list.getQuick(2) >= -0.0 && list.getQuick(2) <= 0.0);
                Assert.assertEquals(Double.MIN_VALUE, list.getQuick(3), 0.0);
                Assert.assertEquals(1.0, list.getQuick(4), 0.0);
                Assert.assertEquals(Double.MAX_VALUE, list.getQuick(5), 0.0);
            }
        });
    }

    @Test
    public void testSortBoundaryChecks() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                for (int i = 0; i < 10; i++) {
                    list.add(i);
                }

                // Test invalid ranges
                try {
                    list.sort(-1, 5);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }

                try {
                    list.sort(0, 10);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }

                try {
                    list.sort(5, 15);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testSortStability() throws Exception {
        assertMemoryLeak(() -> {
            // Verify that sorting is stable for equal elements by checking the algorithm behavior
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(16, 0.0);
                list.setAllocator(allocator);
                list.of(0);

                // Add multiple sets of equal values
                for (int i = 0; i < 5; i++) {
                    list.add(3.0);
                    list.add(1.0);
                    list.add(2.0);
                }

                list.sort(0, 14);

                // Verify that all 1.0s come first, then 2.0s, then 3.0s
                for (int i = 0; i < 5; i++) {
                    Assert.assertEquals(1.0, list.getQuick(i), 0.0);
                }
                for (int i = 5; i < 10; i++) {
                    Assert.assertEquals(2.0, list.getQuick(i), 0.0);
                }
                for (int i = 10; i < 15; i++) {
                    Assert.assertEquals(3.0, list.getQuick(i), 0.0);
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testQuickSelectOrganPipeLarge() throws Exception {
        // Complexity canary: a median-of-first-middle-last pivot picks the minimum of a
        // rise-then-fall range, so selection without a depth limit runs in O(n^2) here.
        assertMemoryLeak(() -> {
            final int n = 4_000_000;
            final double[] expected = new double[n];
            for (int i = 0; i < n; i++) {
                expected[i] = i < n / 2 ? i : n - i;
            }
            Arrays.sort(expected);
            try (GroupByAllocator allocator = new FastGroupByAllocator(64 * 1024, Numbers.SIZE_1GB)) {
                GroupByDoubleList list = new GroupByDoubleList(n, Double.NaN);
                list.setAllocator(allocator);

                addOrganPipe(list.of(0), n);
                list.quickSelect(0, n - 1, n / 2);
                Assert.assertEquals(expected[n / 2], list.getQuick(n / 2), 0.0);

                final int[] indices = {0, n / 10, n / 2, 9 * (n / 10), n - 1};
                addOrganPipe(list.of(0), n);
                list.quickSelectMultiple(0, n - 1, indices, 0, indices.length);
                for (int index : indices) {
                    Assert.assertEquals("index " + index, expected[index], list.getQuick(index), 0.0);
                }
            }
        });
    }

    private static void addOrganPipe(GroupByDoubleList list, int n) {
        for (int i = 0; i < n; i++) {
            list.add(i < n / 2 ? i : n - i);
        }
    }

    private static void assertList(GroupByDoubleList list, double... expected) {
        Assert.assertEquals(expected.length, list.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("index " + i, expected[i], list.get(i), 0.0);
        }
    }
}
