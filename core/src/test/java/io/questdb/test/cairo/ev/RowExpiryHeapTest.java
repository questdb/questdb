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

package io.questdb.test.cairo.ev;

import io.questdb.cairo.ev.RowExpiryHeap;
import io.questdb.std.Rnd;
import org.junit.Test;

import static org.junit.Assert.*;

public class RowExpiryHeapTest {

    @Test
    public void testClear() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.upsert(1, 200);
        heap.clear();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        assertEquals(Long.MAX_VALUE, heap.peekNextExpiry());
    }

    @Test
    public void testClearAndReuse() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.upsert(1, 200);
        heap.clear();

        heap.upsert(5, 50);
        assertEquals(1, heap.size());
        assertEquals(50, heap.peekNextExpiry());
        assertEquals(5, heap.peekNextPartitionIndex());
    }

    @Test
    public void testEmptyHeap() {
        RowExpiryHeap heap = new RowExpiryHeap();
        assertTrue(heap.isEmpty());
        assertEquals(0, heap.size());
        assertEquals(Long.MAX_VALUE, heap.peekNextExpiry());
        assertEquals(-1, heap.peekNextPartitionIndex());
        assertEquals(-1, heap.pollPartitionIndex());
    }

    @Test
    public void testInsertAndPeek() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.upsert(1, 50);
        heap.upsert(2, 200);

        assertEquals(3, heap.size());
        assertFalse(heap.isEmpty());
        assertEquals(50, heap.peekNextExpiry());
        assertEquals(1, heap.peekNextPartitionIndex());
    }

    @Test
    public void testManyEntriesReverseOrder() {
        RowExpiryHeap heap = new RowExpiryHeap();
        for (int i = 99; i >= 0; i--) {
            heap.upsert(i, i * 10L);
        }
        assertEquals(100, heap.size());

        for (int i = 0; i < 100; i++) {
            assertEquals(i, heap.pollPartitionIndex());
        }
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testPollReturnsMinimum() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 300);
        heap.upsert(1, 100);
        heap.upsert(2, 200);

        assertEquals(1, heap.pollPartitionIndex());
        assertEquals(2, heap.size());
        assertEquals(2, heap.pollPartitionIndex());
        assertEquals(1, heap.size());
        assertEquals(0, heap.pollPartitionIndex());
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testRandomInsertRemove() {
        RowExpiryHeap heap = new RowExpiryHeap();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 200; i++) {
            heap.upsert(i, rnd.nextPositiveLong());
        }
        assertEquals(200, heap.size());

        // Remove every other entry
        for (int i = 0; i < 200; i += 2) {
            heap.remove(i);
        }
        assertEquals(100, heap.size());

        // Poll remaining — timestamps must be non-decreasing
        long prev = Long.MIN_VALUE;
        while (!heap.isEmpty()) {
            long ts = heap.peekNextExpiry();
            assertTrue(ts >= prev);
            prev = ts;
            heap.pollPartitionIndex();
        }
    }

    @Test
    public void testRemove() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 300);
        heap.upsert(1, 100);
        heap.upsert(2, 200);

        heap.remove(1);
        assertEquals(2, heap.size());
        assertEquals(200, heap.peekNextExpiry());
        assertEquals(2, heap.peekNextPartitionIndex());
    }

    @Test
    public void testRemoveHead() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.upsert(1, 200);
        heap.upsert(2, 300);

        heap.remove(0);
        assertEquals(2, heap.size());
        assertEquals(200, heap.peekNextExpiry());
        assertEquals(1, heap.peekNextPartitionIndex());
    }

    @Test
    public void testRemoveLast() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.remove(0);
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testRemoveNonExistent() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.remove(99);
        assertEquals(1, heap.size());
    }

    @Test
    public void testSingleEntry() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(42, 12345);

        assertEquals(1, heap.size());
        assertEquals(12345, heap.peekNextExpiry());
        assertEquals(42, heap.peekNextPartitionIndex());
        assertEquals(42, heap.pollPartitionIndex());
        assertTrue(heap.isEmpty());
    }

    @Test
    public void testUpsertDecreaseTimestamp() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 300);
        heap.upsert(1, 100);
        heap.upsert(2, 200);

        heap.upsert(0, 10);
        assertEquals(10, heap.peekNextExpiry());
        assertEquals(0, heap.peekNextPartitionIndex());
    }

    @Test
    public void testUpsertIncreaseTimestamp() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.upsert(1, 200);
        heap.upsert(2, 300);

        heap.upsert(0, 500);
        assertEquals(200, heap.peekNextExpiry());
        assertEquals(1, heap.peekNextPartitionIndex());
    }

    @Test
    public void testUpsertSameTimestamp() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.upsert(1, 100);
        heap.upsert(2, 100);

        assertEquals(3, heap.size());
        assertEquals(100, heap.peekNextExpiry());
        int count = 0;
        while (!heap.isEmpty()) {
            heap.pollPartitionIndex();
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testUpsertSameValue() {
        RowExpiryHeap heap = new RowExpiryHeap();
        heap.upsert(0, 100);
        heap.upsert(0, 100);
        assertEquals(1, heap.size());
        assertEquals(100, heap.peekNextExpiry());
    }

    @Test
    public void testFuzzRandomOperations() {
        RowExpiryHeap heap = new RowExpiryHeap();
        Rnd rnd = new Rnd();
        int maxPartitions = 500;
        boolean[] present = new boolean[maxPartitions];
        int expectedSize = 0;

        for (int iter = 0; iter < 10_000; iter++) {
            int op = rnd.nextInt(4);
            switch (op) {
                case 0 -> {
                    // upsert
                    int idx = rnd.nextInt(maxPartitions);
                    long ts = rnd.nextPositiveLong();
                    heap.upsert(idx, ts);
                    if (!present[idx]) {
                        present[idx] = true;
                        expectedSize++;
                    }
                }
                case 1 -> {
                    // remove
                    int idx = rnd.nextInt(maxPartitions);
                    heap.remove(idx);
                    if (present[idx]) {
                        present[idx] = false;
                        expectedSize--;
                    }
                }
                case 2 -> {
                    // poll
                    if (!heap.isEmpty()) {
                        int polled = heap.pollPartitionIndex();
                        assertTrue(polled >= 0 && polled < maxPartitions);
                        assertTrue(present[polled]);
                        present[polled] = false;
                        expectedSize--;
                    }
                }
                case 3 -> {
                    // peek
                    if (!heap.isEmpty()) {
                        long ts = heap.peekNextExpiry();
                        assertTrue(ts > 0);
                        int idx = heap.peekNextPartitionIndex();
                        assertTrue(idx >= 0 && idx < maxPartitions);
                    }
                }
            }
            assertEquals(expectedSize, heap.size());
            assertHeapInvariant(heap);
        }
    }

    @Test
    public void testFuzzAllSameTimestamp() {
        RowExpiryHeap heap = new RowExpiryHeap();
        long ts = 42;
        for (int i = 0; i < 200; i++) {
            heap.upsert(i, ts);
        }
        assertEquals(200, heap.size());

        // All entries have same timestamp — poll should return all 200
        boolean[] seen = new boolean[200];
        while (!heap.isEmpty()) {
            assertEquals(ts, heap.peekNextExpiry());
            int idx = heap.pollPartitionIndex();
            assertFalse("duplicate polled: " + idx, seen[idx]);
            seen[idx] = true;
        }
        for (int i = 0; i < 200; i++) {
            assertTrue("missing: " + i, seen[i]);
        }
    }

    @Test
    public void testFuzzUpsertDecreasingTimestamps() {
        RowExpiryHeap heap = new RowExpiryHeap();
        Rnd rnd = new Rnd();

        // Insert 100 partitions with high timestamps
        for (int i = 0; i < 100; i++) {
            heap.upsert(i, 1_000_000 + rnd.nextInt(100_000));
        }

        // Repeatedly upsert existing partitions with decreasing timestamps
        for (int iter = 0; iter < 5000; iter++) {
            int idx = rnd.nextInt(100);
            long newTs = rnd.nextInt(1000);
            heap.upsert(idx, newTs);
            assertHeapInvariant(heap);
        }

        // Drain and verify monotonically non-decreasing order
        long prev = Long.MIN_VALUE;
        while (!heap.isEmpty()) {
            long ts = heap.peekNextExpiry();
            assertTrue("heap order violated: " + prev + " > " + ts, ts >= prev);
            prev = ts;
            heap.pollPartitionIndex();
        }
    }

    @Test
    public void testFuzzSparsePartitionIndices() {
        RowExpiryHeap heap = new RowExpiryHeap();
        Rnd rnd = new Rnd();

        // Use very sparse partition indices
        int[] indices = new int[100];
        for (int i = 0; i < 100; i++) {
            indices[i] = rnd.nextInt(100_000);
            heap.upsert(indices[i], rnd.nextPositiveLong());
        }

        // Remove half
        for (int i = 0; i < 50; i++) {
            heap.remove(indices[i]);
        }
        assertEquals(50, heap.size());

        // Drain — must still be ordered
        long prev = Long.MIN_VALUE;
        while (!heap.isEmpty()) {
            long ts = heap.peekNextExpiry();
            assertTrue(ts >= prev);
            prev = ts;
            heap.pollPartitionIndex();
        }
    }

    @Test
    public void testFuzzRemoveAll() {
        RowExpiryHeap heap = new RowExpiryHeap();
        Rnd rnd = new Rnd();

        for (int round = 0; round < 10; round++) {
            int count = 50 + rnd.nextInt(150);
            for (int i = 0; i < count; i++) {
                heap.upsert(i, rnd.nextPositiveLong());
            }
            assertEquals(count, heap.size());

            // Remove all in random order
            int[] order = new int[count];
            for (int i = 0; i < count; i++) {
                order[i] = i;
            }
            for (int i = count - 1; i > 0; i--) {
                int j = rnd.nextInt(i + 1);
                int tmp = order[i];
                order[i] = order[j];
                order[j] = tmp;
            }
            for (int i = 0; i < count; i++) {
                heap.remove(order[i]);
                assertHeapInvariant(heap);
            }
            assertTrue(heap.isEmpty());
        }
    }

    @Test
    public void testFuzzPollAndReinsert() {
        RowExpiryHeap heap = new RowExpiryHeap();
        Rnd rnd = new Rnd();

        for (int i = 0; i < 100; i++) {
            heap.upsert(i, rnd.nextPositiveLong());
        }

        // Simulate the cleanup loop pattern: poll min, do work, reinsert with new timestamp
        for (int iter = 0; iter < 10_000; iter++) {
            long peekTs = heap.peekNextExpiry();
            int idx = heap.pollPartitionIndex();
            assertEquals(99, heap.size());

            long newTs = rnd.nextPositiveLong();
            heap.upsert(idx, newTs);
            assertEquals(100, heap.size());
            assertHeapInvariant(heap);

            // Peek must return something <= old peek (we might have reinserted with lower ts)
            // or it returns the next min
            assertTrue(heap.peekNextExpiry() > 0);
        }
    }

    private void assertHeapInvariant(RowExpiryHeap heap) {
        // We can't directly access the internal arrays, but we can verify
        // that peekNextExpiry is consistent — if non-empty, peek must succeed
        if (!heap.isEmpty()) {
            assertTrue(heap.peekNextExpiry() < Long.MAX_VALUE);
            assertTrue(heap.peekNextPartitionIndex() >= 0);
        } else {
            assertEquals(0, heap.size());
            assertEquals(Long.MAX_VALUE, heap.peekNextExpiry());
            assertEquals(-1, heap.peekNextPartitionIndex());
        }
    }
}
