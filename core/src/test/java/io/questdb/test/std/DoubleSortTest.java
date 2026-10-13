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

package io.questdb.test.std;

import io.questdb.std.DoubleSort;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DoubleSortTest {

    // The large-input tests are complexity canaries: with a median of the first, middle
    // and last elements as the pivot and no depth limit, smooth or rise-then-fall input
    // made DoubleSort take O(n^2) comparisons - many minutes at this size - while a
    // healthy sort or selection finishes in well under a second. The timeout turns a
    // complexity regression into a red failure instead of a CI hang. Some tests also
    // observe DoubleSort.getHeapSortCallCountForTesting(): selection on shapes the
    // ninther handles must not reach the heapsort fallback, aliasing shapes must.
    private static final int LARGE_SIZE = 4_000_000;
    private static final long LARGE_TEST_TIMEOUT_MS = 30_000;

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testAllEqualLarge() throws Exception {
        // three-way partitioning puts all-equal input in place in one pass
        double[] values = new double[LARGE_SIZE];
        Arrays.fill(values, 42.5);
        long heapSortCallsBefore = DoubleSort.getHeapSortCallCountForTesting();
        assertSortAndSelectMatchReference(values);
        Assert.assertEquals(heapSortCallsBefore, DoubleSort.getHeapSortCallCountForTesting());
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testDailyCosineLarge() throws Exception {
        // one smooth daily cycle with the minimum near the start, as in a temperature series
        double[] values = new double[LARGE_SIZE];
        for (int i = 0; i < LARGE_SIZE; i++) {
            values[i] = 15 - 8 * Math.cos(2 * Math.PI * (24.0 * (i + 1) / LARGE_SIZE - 3) / 24);
        }
        assertSortAndSelectMatchReference(values);
    }

    @Test
    public void testEmptyRange() throws Exception {
        assertMemoryLeak(() -> {
            final long size = 8L;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putDouble(ptr, 7.0);
                DoubleSort.sort(ptr, 0, -1);
                DoubleSort.quickSelectMultiple(ptr, 0, -1, new int[0], 0, 0);
                // a single element is a sorted range
                DoubleSort.sort(ptr, 0, 0);
                DoubleSort.quickSelect(ptr, 0, 0, 0);
                DoubleSort.quickSelectMultiple(ptr, 0, 0, new int[]{0}, 0, 1);
                Assert.assertEquals(7.0, Unsafe.getDouble(ptr), 0.0);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testMedianOfThreeKillerLarge() throws Exception {
        assertSortAndSelectMatchReference(medianOfThreeKiller(LARGE_SIZE));
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testOrganPipeLarge() throws Exception {
        final double[] values = organPipe(LARGE_SIZE);
        assertSortAndSelectMatchReference(values);
        // the ninther keeps the selection splits balanced, so selection needs no fallback
        assertMemoryLeak(() -> {
            final int n = values.length;
            final long size = 8L * n;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                copyToNative(values, ptr);
                final long heapSortCallsBefore = DoubleSort.getHeapSortCallCountForTesting();
                DoubleSort.quickSelect(ptr, 0, n - 1, n / 2);
                copyToNative(values, ptr);
                final int[] indices = {0, n / 10, n / 2, 9 * (n / 10), n - 1};
                DoubleSort.quickSelectMultiple(ptr, 0, n - 1, indices, 0, indices.length);
                Assert.assertEquals(heapSortCallsBefore, DoubleSort.getHeapSortCallCountForTesting());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRandomMatchesReference() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int iteration = 0; iteration < 300; iteration++) {
            // sizes around the insertion sort (47) and ninther (128) thresholds and beyond
            final int n = 1 + rnd.nextInt(iteration % 2 == 0 ? 300 : 3_000);
            // a narrow value range produces long runs of duplicates
            final int valueRange = rnd.nextBoolean() ? 1 + rnd.nextInt(4) : Integer.MAX_VALUE;
            final double[] values = new double[n];
            for (int i = 0; i < n; i++) {
                values[i] = rnd.nextInt(valueRange) - (valueRange >> 1) + (valueRange > 4 ? rnd.nextDouble() : 0);
            }
            assertSortAndSelectMatchReference(values);
        }
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testSawtoothFallsBackToHeapsort() throws Exception {
        // the period divides len / 8, so all nine ninther samples hit the same value and
        // pivots stay poor; the depth budget must hand such ranges to heapsort
        double[] values = new double[LARGE_SIZE];
        for (int i = 0; i < LARGE_SIZE; i++) {
            values[i] = i % 1000;
        }
        final double[] expected = values.clone();
        Arrays.sort(expected);
        final int n = values.length;
        assertMemoryLeak(() -> {
            final long size = 8L * n;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                copyToNative(values, ptr);
                long heapSortCallsBefore = DoubleSort.getHeapSortCallCountForTesting();
                DoubleSort.sort(ptr, 0, n - 1);
                Assert.assertTrue(DoubleSort.getHeapSortCallCountForTesting() > heapSortCallsBefore);
                assertNativeEquals(expected, ptr, 0, n - 1);

                copyToNative(values, ptr);
                heapSortCallsBefore = DoubleSort.getHeapSortCallCountForTesting();
                DoubleSort.quickSelect(ptr, 0, n - 1, n / 2);
                Assert.assertTrue(DoubleSort.getHeapSortCallCountForTesting() > heapSortCallsBefore);
                Assert.assertEquals(expected[n / 2], Unsafe.getDouble(ptr + 8L * (n / 2)), 0.0);

                copyToNative(values, ptr);
                heapSortCallsBefore = DoubleSort.getHeapSortCallCountForTesting();
                final int[] indices = {0, n / 10, n / 2, 9 * (n / 10), n - 1};
                DoubleSort.quickSelectMultiple(ptr, 0, n - 1, indices, 0, indices.length);
                Assert.assertTrue(DoubleSort.getHeapSortCallCountForTesting() > heapSortCallsBefore);
                for (int index : indices) {
                    Assert.assertEquals("index " + index, expected[index], Unsafe.getDouble(ptr + 8L * index), 0.0);
                }
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testSineHumpLarge() throws Exception {
        double[] values = new double[LARGE_SIZE];
        for (int i = 0; i < LARGE_SIZE; i++) {
            values[i] = Math.sin(Math.PI * (i + 1) / LARGE_SIZE);
        }
        assertSortAndSelectMatchReference(values);
    }

    @Test
    public void testSizesAroundThresholds() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int n : new int[]{1, 2, 3, 46, 47, 48, 128, 129, 130}) {
            final double[] organPipe = organPipe(n);
            assertSortAndSelectMatchReference(organPipe);
            final double[] random = new double[n];
            for (int i = 0; i < n; i++) {
                random[i] = rnd.nextDouble();
            }
            assertSortAndSelectMatchReference(random);
            // two distinct values
            final double[] twoValues = new double[n];
            for (int i = 0; i < n; i++) {
                twoValues[i] = rnd.nextBoolean() ? -1.0 : 1.0;
            }
            assertSortAndSelectMatchReference(twoValues);
        }
    }

    @Test
    public void testSpecialValues() throws Exception {
        // -0.0 and 0.0 compare equal, so the reference compares them with ==
        final double[] specials = {
                0.0, -0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE, 1.0, -1.0
        };
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int n : new int[]{specials.length, 200, 5_000}) {
            final double[] values = new double[n];
            for (int i = 0; i < n; i++) {
                values[i] = specials[rnd.nextInt(specials.length)];
            }
            assertSortAndSelectMatchReference(values);
        }
    }

    @Test
    public void testSubrangeLeavesOutsideUntouched() throws Exception {
        assertMemoryLeak(() -> {
            final int n = 1_000;
            final int lo = 100;
            final int hi = 899;
            final double[] values = organPipe(n);
            final double[] expected = values.clone();
            Arrays.sort(expected, lo, hi + 1);
            final long size = 8L * n;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                copyToNative(values, ptr);
                DoubleSort.sort(ptr, lo, hi);
                assertNativeEquals(expected, ptr, 0, n - 1);

                copyToNative(values, ptr);
                DoubleSort.quickSelect(ptr, lo, hi, 500);
                Assert.assertEquals(expected[500], Unsafe.getDouble(ptr + 8L * 500), 0.0);
                assertOutsideUntouched(values, ptr, lo, hi);

                copyToNative(values, ptr);
                final int[] indices = {lo, 300, 500, hi};
                DoubleSort.quickSelectMultiple(ptr, lo, hi, indices, 0, indices.length);
                for (int index : indices) {
                    Assert.assertEquals("index " + index, expected[index], Unsafe.getDouble(ptr + 8L * index), 0.0);
                }
                assertOutsideUntouched(values, ptr, lo, hi);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testValleyLarge() throws Exception {
        double[] values = new double[LARGE_SIZE];
        for (int i = 0; i < LARGE_SIZE; i++) {
            values[i] = Math.abs(i - LARGE_SIZE / 2);
        }
        assertSortAndSelectMatchReference(values);
    }

    private static void assertNativeEquals(double[] expected, long ptr, int lo, int hi) {
        for (int i = lo; i <= hi; i++) {
            final double actual = Unsafe.getDouble(ptr + 8L * i);
            if (expected[i] != actual) {
                Assert.fail("index " + i + ", expected " + expected[i] + ", actual " + actual);
            }
        }
    }

    private static void assertOutsideUntouched(double[] values, long ptr, int lo, int hi) {
        assertNativeEquals(values, ptr, 0, lo - 1);
        assertNativeEquals(values, ptr, hi + 1, values.length - 1);
    }

    // Checks sort, quickSelect at k = 0, n/2, n-1 and quickSelectMultiple at a spread of
    // indices against Arrays.sort, each on a fresh copy of the input. Small inputs select
    // every index.
    private static void assertSortAndSelectMatchReference(double[] values) throws Exception {
        assertMemoryLeak(() -> {
            final int n = values.length;
            final double[] expected = values.clone();
            Arrays.sort(expected);
            final long size = 8L * n;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                copyToNative(values, ptr);
                DoubleSort.sort(ptr, 0, n - 1);
                assertNativeEquals(expected, ptr, 0, n - 1);

                final int[] indices;
                if (n <= 200) {
                    indices = new int[n];
                    for (int i = 0; i < n; i++) {
                        indices[i] = i;
                    }
                } else {
                    indices = new int[]{0, n / 10, n / 2, 9 * (n / 10), n - 1};
                }

                for (int k : n <= 200 ? indices : new int[]{0, n / 2, n - 1}) {
                    copyToNative(values, ptr);
                    DoubleSort.quickSelect(ptr, 0, n - 1, k);
                    Assert.assertEquals("k " + k, expected[k], Unsafe.getDouble(ptr + 8L * k), 0.0);
                    for (int i = 0; i < n; i++) {
                        final double v = Unsafe.getDouble(ptr + 8L * i);
                        if (i < k ? v > expected[k] : v < expected[k]) {
                            Assert.fail("k " + k + ", index " + i + " is on the wrong side: " + v);
                        }
                    }
                }

                copyToNative(values, ptr);
                DoubleSort.quickSelectMultiple(ptr, 0, n - 1, indices, 0, indices.length);
                for (int index : indices) {
                    Assert.assertEquals("index " + index, expected[index], Unsafe.getDouble(ptr + 8L * index), 0.0);
                }
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void copyToNative(double[] values, long ptr) {
        for (int i = 0; i < values.length; i++) {
            Unsafe.putDouble(ptr + 8L * i, values[i]);
        }
    }

    // Musser's median-of-3-killer permutation of 1..n (n even)
    private static double[] medianOfThreeKiller(int n) {
        final int k = n / 2;
        double[] values = new double[n];
        for (int i = 1; i <= k; i++) {
            if ((i & 1) == 1) {
                values[i - 1] = i;
                values[i] = k + i;
            }
            values[k + i - 1] = 2.0 * i;
        }
        return values;
    }

    // rises 0, 1, ..., n/2 - 1, then falls n/2, ..., 1
    private static double[] organPipe(int n) {
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = i < n / 2 ? i : n - i;
        }
        return values;
    }
}
