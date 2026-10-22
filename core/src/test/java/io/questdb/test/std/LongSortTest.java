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

package io.questdb.test.std;

import io.questdb.std.LongSort;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class LongSortTest {

    // The large-input tests are complexity canaries for the native-memory selection: a
    // median-of-three pivot with no depth limit takes O(n^2) comparisons on rise-then-fall
    // input. The timeout turns a complexity regression into a red failure instead of a CI
    // hang. Some tests also observe LongSort.getHeapSortCallCountForTesting(): selection on
    // shapes the ninther handles must not reach the heapsort fallback, aliasing shapes must.
    private static final int LARGE_SIZE = 4_000_000;
    private static final long LARGE_TEST_TIMEOUT_MS = 30_000;

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testAllEqualLarge() throws Exception {
        // three-way partitioning puts all-equal input in place in one pass
        final long[] values = new long[LARGE_SIZE];
        Arrays.fill(values, 42L);
        final long heapSortCallsBefore = LongSort.getHeapSortCallCountForTesting();
        assertSelectMatchesReference(values);
        Assert.assertEquals(heapSortCallsBefore, LongSort.getHeapSortCallCountForTesting());
    }

    @Test
    public void testEmptyRange() throws Exception {
        assertMemoryLeak(() -> {
            final long size = 8L;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putLong(ptr, 7L);
                LongSort.quickSelectMultiple(ptr, 0, -1, new int[0], 0, 0);
                // a single element is a sorted range
                LongSort.quickSelect(ptr, 0, 0, 0);
                LongSort.quickSelectMultiple(ptr, 0, 0, new int[]{0}, 0, 1);
                Assert.assertEquals(7L, Unsafe.getLong(ptr));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testExtremeValues() throws Exception {
        // the full signed range, including Long.MIN_VALUE, orders as signed longs
        final long[] extremes = {
                Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE,
                9_007_199_254_740_992L, 9_007_199_254_740_993L
        };
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int n : new int[]{extremes.length, 200, 5_000}) {
            final long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                values[i] = extremes[rnd.nextInt(extremes.length)];
            }
            assertSelectMatchesReference(values);
        }
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testOrganPipeLarge() throws Exception {
        final long[] values = organPipe(LARGE_SIZE);
        // the ninther keeps the selection splits balanced, so selection needs no fallback
        final long heapSortCallsBefore = LongSort.getHeapSortCallCountForTesting();
        assertSelectMatchesReference(values);
        Assert.assertEquals(heapSortCallsBefore, LongSort.getHeapSortCallCountForTesting());
    }

    @Test
    public void testRandomMatchesReference() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int iteration = 0; iteration < 300; iteration++) {
            // sizes around the ninther (128) threshold and beyond
            final int n = 1 + rnd.nextInt(iteration % 2 == 0 ? 300 : 3_000);
            // a narrow value range produces long runs of duplicates
            final boolean isNarrow = rnd.nextBoolean();
            final long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                values[i] = isNarrow ? rnd.nextInt(4) - 2 : rnd.nextLong();
            }
            assertSelectMatchesReference(values);
        }
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testSawtoothFallsBackToHeapsort() throws Exception {
        // the period divides len / 8, so all nine ninther samples hit the same value and
        // pivots stay poor; the depth budget must hand such ranges to heapsort
        final long[] values = new long[LARGE_SIZE];
        for (int i = 0; i < LARGE_SIZE; i++) {
            values[i] = i % 1000;
        }
        final long[] expected = values.clone();
        Arrays.sort(expected);
        final int n = values.length;
        assertMemoryLeak(() -> {
            final long size = 8L * n;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                copyToNative(values, ptr);
                long heapSortCallsBefore = LongSort.getHeapSortCallCountForTesting();
                LongSort.quickSelect(ptr, 0, n - 1, n / 2);
                Assert.assertTrue(LongSort.getHeapSortCallCountForTesting() > heapSortCallsBefore);
                Assert.assertEquals(expected[n / 2], Unsafe.getLong(ptr + 8L * (n / 2)));

                copyToNative(values, ptr);
                heapSortCallsBefore = LongSort.getHeapSortCallCountForTesting();
                final int[] indices = {0, n / 10, n / 2, 9 * (n / 10), n - 1};
                LongSort.quickSelectMultiple(ptr, 0, n - 1, indices, 0, indices.length);
                Assert.assertTrue(LongSort.getHeapSortCallCountForTesting() > heapSortCallsBefore);
                for (int index : indices) {
                    Assert.assertEquals("index " + index, expected[index], Unsafe.getLong(ptr + 8L * index));
                }
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSizesAroundThresholds() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int n : new int[]{1, 2, 3, 46, 47, 48, 128, 129, 130}) {
            assertSelectMatchesReference(organPipe(n));
            final long[] random = new long[n];
            final long[] sorted = new long[n];
            final long[] reversed = new long[n];
            final long[] twoValues = new long[n];
            for (int i = 0; i < n; i++) {
                random[i] = rnd.nextLong();
                sorted[i] = i;
                reversed[i] = n - i;
                twoValues[i] = rnd.nextBoolean() ? -1L : 1L;
            }
            assertSelectMatchesReference(random);
            assertSelectMatchesReference(sorted);
            assertSelectMatchesReference(reversed);
            assertSelectMatchesReference(twoValues);
        }
    }

    @Test(timeout = LARGE_TEST_TIMEOUT_MS)
    public void testSortedAndReversedLarge() throws Exception {
        final long[] sorted = new long[LARGE_SIZE];
        final long[] reversed = new long[LARGE_SIZE];
        for (int i = 0; i < LARGE_SIZE; i++) {
            sorted[i] = i;
            reversed[i] = LARGE_SIZE - i;
        }
        final long heapSortCallsBefore = LongSort.getHeapSortCallCountForTesting();
        assertSelectMatchesReference(sorted);
        assertSelectMatchesReference(reversed);
        Assert.assertEquals(heapSortCallsBefore, LongSort.getHeapSortCallCountForTesting());
    }

    @Test
    public void testSubrangeLeavesOutsideUntouched() throws Exception {
        assertMemoryLeak(() -> {
            final int n = 1_000;
            final int lo = 100;
            final int hi = 899;
            final long[] values = organPipe(n);
            final long[] expected = values.clone();
            Arrays.sort(expected, lo, hi + 1);
            final long size = 8L * n;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                copyToNative(values, ptr);
                LongSort.quickSelect(ptr, lo, hi, 500);
                Assert.assertEquals(expected[500], Unsafe.getLong(ptr + 8L * 500));
                assertOutsideUntouched(values, ptr, lo, hi);

                copyToNative(values, ptr);
                final int[] indices = {lo, 300, 500, hi};
                LongSort.quickSelectMultiple(ptr, lo, hi, indices, 0, indices.length);
                for (int index : indices) {
                    Assert.assertEquals("index " + index, expected[index], Unsafe.getLong(ptr + 8L * index));
                }
                assertOutsideUntouched(values, ptr, lo, hi);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertNativeEquals(long[] expected, long ptr, int lo, int hi) {
        for (int i = lo; i <= hi; i++) {
            final long actual = Unsafe.getLong(ptr + 8L * i);
            if (expected[i] != actual) {
                Assert.fail("index " + i + ", expected " + expected[i] + ", actual " + actual);
            }
        }
    }

    private static void assertOutsideUntouched(long[] values, long ptr, int lo, int hi) {
        assertNativeEquals(values, ptr, 0, lo - 1);
        assertNativeEquals(values, ptr, hi + 1, values.length - 1);
    }

    // Checks quickSelect at k = 0, n/2, n-1 and quickSelectMultiple at a spread of indices
    // against Arrays.sort, each on a fresh copy of the input. Small inputs select every index.
    private static void assertSelectMatchesReference(long[] values) throws Exception {
        assertMemoryLeak(() -> {
            final int n = values.length;
            final long[] expected = values.clone();
            Arrays.sort(expected);
            final long size = 8L * n;
            final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
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
                    LongSort.quickSelect(ptr, 0, n - 1, k);
                    Assert.assertEquals("k " + k, expected[k], Unsafe.getLong(ptr + 8L * k));
                    for (int i = 0; i < n; i++) {
                        final long v = Unsafe.getLong(ptr + 8L * i);
                        if (i < k ? v > expected[k] : v < expected[k]) {
                            Assert.fail("k " + k + ", index " + i + " is on the wrong side: " + v);
                        }
                    }
                }

                copyToNative(values, ptr);
                LongSort.quickSelectMultiple(ptr, 0, n - 1, indices, 0, indices.length);
                for (int index : indices) {
                    Assert.assertEquals("index " + index, expected[index], Unsafe.getLong(ptr + 8L * index));
                }
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void copyToNative(long[] values, long ptr) {
        for (int i = 0; i < values.length; i++) {
            Unsafe.putLong(ptr + 8L * i, values[i]);
        }
    }

    // rises 0, 1, ..., n/2 - 1, then falls n/2, ..., 1
    private static long[] organPipe(int n) {
        final long[] values = new long[n];
        for (int i = 0; i < n; i++) {
            values[i] = i < n / 2 ? i : n - i;
        }
        return values;
    }
}
