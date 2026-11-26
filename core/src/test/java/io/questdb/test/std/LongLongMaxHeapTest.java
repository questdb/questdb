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

import io.questdb.std.LongLongMaxHeap;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.PriorityQueue;

public class LongLongMaxHeapTest {
    private static final Comparator<Entry> DESCENDING = (a, b) -> {
        final int keyComparison = Long.compare(b.key, a.key);
        return keyComparison != 0 ? keyComparison : Long.compare(a.value, b.value);
    };

    @Test
    public void testClearAndReuse() {
        final LongLongMaxHeap heap = new LongLongMaxHeap();
        for (int i = 0; i < 100; i++) {
            heap.push(i, 100 - i);
        }
        Assert.assertEquals(100, heap.size());

        heap.clear();
        Assert.assertTrue(heap.isEmpty());
        Assert.assertEquals(0, heap.size());

        heap.push(Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(1, heap.size());
        Assert.assertEquals(Long.MIN_VALUE, heap.peekKey());
        Assert.assertEquals(Long.MAX_VALUE, heap.peekValue());
        heap.pop();
        Assert.assertTrue(heap.isEmpty());
    }

    @Test
    public void testDuplicateEntries() {
        final LongLongMaxHeap heap = new LongLongMaxHeap();
        for (int i = 0; i < 1_000; i++) {
            heap.push(42, -7);
        }
        for (int i = 1_000; i > 0; i--) {
            Assert.assertEquals(i, heap.size());
            Assert.assertEquals(42, heap.peekKey());
            Assert.assertEquals(-7, heap.peekValue());
            heap.pop();
        }
        Assert.assertTrue(heap.isEmpty());
    }

    @Test
    public void testEveryInsertionOrder() {
        final long[] keys = {9, Long.MIN_VALUE, 1, 9, -3, 0, 9};
        final long[] values = {Long.MAX_VALUE, Long.MAX_VALUE, -1, Long.MIN_VALUE, 7, 0, 0};
        final long[] expectedKeys = {9, 9, 9, 1, 0, -3, Long.MIN_VALUE};
        final long[] expectedValues = {Long.MIN_VALUE, 0, Long.MAX_VALUE, -1, 0, 7, Long.MAX_VALUE};
        final int[] order = {0, 1, 2, 3, 4, 5, 6};
        final LongLongMaxHeap heap = new LongLongMaxHeap();

        int permutationCount = 0;
        do {
            heap.clear();
            for (int index : order) {
                heap.push(keys[index], values[index]);
            }
            for (int i = 0; i < expectedKeys.length; i++) {
                assertPop(heap, expectedKeys[i], expectedValues[i]);
            }
            Assert.assertTrue(heap.isEmpty());
            permutationCount++;
        } while (nextPermutation(order));
        Assert.assertEquals(5_040, permutationCount);
    }

    @Test
    public void testFuzzAgainstJdkPriorityQueue() {
        final LongLongMaxHeap heap = new LongLongMaxHeap();
        final PriorityQueue<Entry> oracle = new PriorityQueue<>(DESCENDING);
        int seed = 1;
        final Rnd rnd = TestUtils.generateRandom(null);
        heap.clear();
        oracle.clear();
        for (int operation = 0; operation < 50_000; operation++) {
            final int action = rnd.nextInt(100);
            if (oracle.isEmpty() || action < 62) {
                final long key = nextFuzzLong(rnd, operation);
                final long value = nextFuzzLong(rnd, operation + 3);
                heap.push(key, value);
                oracle.add(new Entry(key, value));
            } else if (action < 99) {
                assertHead(seed, operation, heap, oracle);
                heap.pop();
                oracle.remove();
            } else {
                heap.clear();
                oracle.clear();
            }
            Assert.assertEquals("size mismatch [seed=" + seed + ", operation=" + operation + ']',
                    oracle.size(), heap.size());
            if (!oracle.isEmpty()) {
                assertHead(seed, operation, heap, oracle);
            }
        }
        while (!oracle.isEmpty()) {
            assertHead(seed, 50_000, heap, oracle);
            heap.pop();
            oracle.remove();
        }
        Assert.assertTrue(heap.isEmpty());
    }

    @Test
    public void testOrdersByDescendingKeyAndAscendingValue() {
        final LongLongMaxHeap heap = new LongLongMaxHeap();
        heap.push(1, 30);
        heap.push(3, 20);
        heap.push(3, 10);
        heap.push(3, 10);
        heap.push(2, Long.MIN_VALUE);
        heap.push(Long.MIN_VALUE, 0);
        heap.push(Long.MAX_VALUE, Long.MAX_VALUE);
        heap.push(Long.MAX_VALUE, Long.MIN_VALUE);

        assertPop(heap, Long.MAX_VALUE, Long.MIN_VALUE);
        assertPop(heap, Long.MAX_VALUE, Long.MAX_VALUE);
        assertPop(heap, 3, 10);
        assertPop(heap, 3, 10);
        assertPop(heap, 3, 20);
        assertPop(heap, 2, Long.MIN_VALUE);
        assertPop(heap, 1, 30);
        assertPop(heap, Long.MIN_VALUE, 0);
        Assert.assertTrue(heap.isEmpty());
    }

    @Test
    public void testResizeAndEveryRemovalShape() {
        final LongLongMaxHeap heap = new LongLongMaxHeap();
        final int count = 100_000;
        for (int i = 0; i < count; i++) {
            heap.push(i % 17, count - i);
        }
        Assert.assertEquals(count, heap.size());

        long previousKey = Long.MAX_VALUE;
        long previousValue = Long.MIN_VALUE;
        while (!heap.isEmpty()) {
            final long key = heap.peekKey();
            final long value = heap.peekValue();
            Assert.assertTrue(key <= previousKey);
            if (key == previousKey) {
                Assert.assertTrue(value >= previousValue);
            }
            previousKey = key;
            previousValue = value;
            heap.pop();
        }
    }

    private static void assertHead(
            int seed,
            int operation,
            LongLongMaxHeap heap,
            PriorityQueue<Entry> oracle
    ) {
        final Entry expected = oracle.element();
        Assert.assertEquals("key mismatch [seed=" + seed + ", operation=" + operation + ']',
                expected.key, heap.peekKey());
        Assert.assertEquals("value mismatch [seed=" + seed + ", operation=" + operation + ']',
                expected.value, heap.peekValue());
    }

    private static void assertPop(LongLongMaxHeap heap, long expectedKey, long expectedValue) {
        Assert.assertEquals(expectedKey, heap.peekKey());
        Assert.assertEquals(expectedValue, heap.peekValue());
        heap.pop();
    }

    private static long nextFuzzLong(Rnd rnd, int operation) {
        return switch (operation & 15) {
            case 0 -> Long.MIN_VALUE;
            case 1 -> Long.MAX_VALUE;
            case 2, 3, 4, 5 -> rnd.nextLong(11) - 5;
            default -> rnd.nextLong();
        };
    }

    private static boolean nextPermutation(int[] values) {
        int pivot = values.length - 2;
        while (pivot >= 0 && values[pivot] >= values[pivot + 1]) {
            pivot--;
        }
        if (pivot < 0) {
            return false;
        }

        int successor = values.length - 1;
        while (values[successor] <= values[pivot]) {
            successor--;
        }
        final int swap = values[pivot];
        values[pivot] = values[successor];
        values[successor] = swap;
        for (int lo = pivot + 1, hi = values.length - 1; lo < hi; lo++, hi--) {
            final int value = values[lo];
            values[lo] = values[hi];
            values[hi] = value;
        }
        return true;
    }

    private static final class Entry {
        private final long key;
        private final long value;

        private Entry(long key, long value) {
            this.key = key;
            this.value = value;
        }
    }
}
