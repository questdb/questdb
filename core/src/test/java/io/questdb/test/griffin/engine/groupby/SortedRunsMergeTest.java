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

import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.groupby.SortedRunsMerge;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SortedRunsMerge}, the batch-descriptor sort used by the
 * parallel {@code twap} and {@code sparkline} aggregates.
 * <p>
 * Entries are 16 bytes: an 8-byte sort key at offset 0 and an 8-byte value at
 * offset 8 derived from the key, so a wrongly copied entry is caught. Batches
 * must be key-disjoint, mirroring the real callers where each batch is one page
 * frame and frames cover disjoint, contiguous row-id ranges.
 */
public class SortedRunsMergeTest extends AbstractCairoTest {

    private static final long ENTRY_STRIDE = 16;

    @Test
    public void testAppendBatchStartLazyAllocationAndGrowth() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(3)
            ) {
                mapValue.putLong(0, 0); // descPtr
                mapValue.putLong(1, 0); // descCount
                mapValue.putLong(2, 0); // descCapacity

                // First call on a single-batch group allocates the buffer and
                // seeds it with the implicit first batch plus the new one.
                SortedRunsMerge.appendBatchStart(allocator, mapValue, 0, 5);
                Assert.assertNotEquals(0, mapValue.getLong(0));
                Assert.assertEquals(2, mapValue.getLong(1));
                Assert.assertEquals(8, mapValue.getLong(2)); // DESC_INITIAL_CAPACITY
                assertDescriptors(mapValue.getLong(0), 0, 5);

                // Fill up to capacity: descriptors 3..8.
                long[] starts = {12, 20, 30, 40, 50, 60};
                for (long start : starts) {
                    SortedRunsMerge.appendBatchStart(allocator, mapValue, 0, start);
                }
                Assert.assertEquals(8, mapValue.getLong(1));
                Assert.assertEquals(8, mapValue.getLong(2));
                assertDescriptors(mapValue.getLong(0), 0, 5, 12, 20, 30, 40, 50, 60);

                // The ninth descriptor triggers a doubling growth, preserving
                // the existing descriptors.
                SortedRunsMerge.appendBatchStart(allocator, mapValue, 0, 70);
                Assert.assertEquals(9, mapValue.getLong(1));
                Assert.assertEquals(16, mapValue.getLong(2));
                assertDescriptors(mapValue.getLong(0), 0, 5, 12, 20, 30, 40, 50, 60, 70);
            }
        });
    }

    /**
     * Mirrors production usage in {@code TwapGroupByFunction} and
     * {@code SparklineGroupByFunction}: build the descriptor buffer through
     * {@link SortedRunsMerge#appendBatchStart}, letting it grow past
     * {@code DESC_INITIAL_CAPACITY = 8}, then hand the grown buffer to
     * {@link SortedRunsMerge#compactInPlace}. With 12 batches the buffer
     * doubles once to capacity 16, so {@code compactInPlace} reads the
     * descriptors out of the post-{@code realloc} pointer.
     */
    @Test
    public void testCompactInPlaceAfterDescriptorGrowth() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(3)
            ) {
                mapValue.putLong(0, 0); // descPtr
                mapValue.putLong(1, 0); // descCount
                mapValue.putLong(2, 0); // descCapacity

                final int n = 12;
                final int entriesPerBatch = 2;
                // First call seeds two descriptors (batch 0 at offset 0,
                // batch 1 at entriesPerBatch); subsequent calls append one
                // descriptor each. The 8th call triggers the doubling growth.
                SortedRunsMerge.appendBatchStart(allocator, mapValue, 0, entriesPerBatch);
                for (int i = 2; i < n; i++) {
                    SortedRunsMerge.appendBatchStart(allocator, mapValue, 0, (long) i * entriesPerBatch);
                }
                Assert.assertEquals(n, mapValue.getLong(1));
                Assert.assertEquals(16, mapValue.getLong(2));

                // Arrange the n batches in reverse key order, forcing
                // compactInPlace to sort rather than take the isAscending
                // early return.
                final long[] arrivalKeys = new long[n * entriesPerBatch];
                for (int i = 0; i < n; i++) {
                    final long k = (n - 1 - i) * 10L;
                    arrivalKeys[i * entriesPerBatch] = k;
                    arrivalKeys[i * entriesPerBatch + 1] = k + 1;
                }
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, arrivalKeys);
                long desc = mapValue.getLong(0);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, (long) n * entriesPerBatch, desc, n, ENTRY_STRIDE);

                final long[] expectedKeys = new long[n * entriesPerBatch];
                final long[] expectedDesc = new long[n];
                for (int i = 0; i < n; i++) {
                    final long k = i * 10L;
                    expectedKeys[i * entriesPerBatch] = k;
                    expectedKeys[i * entriesPerBatch + 1] = k + 1;
                    expectedDesc[i] = (long) i * entriesPerBatch;
                }
                assertEntries(entries, expectedKeys);
                assertDescriptors(desc, expectedDesc);
            }
        });
    }

    @Test
    public void testCompactInPlaceConflatedFrames() throws Exception {
        // Three frames appended out of order: [0,1], then [100,101], then
        // [50,51]. The middle batch's range sits between the others, the exact
        // case a key-decrease run scan would conflate.
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, 0, 1, 100, 101, 50, 51);
                long desc = allocDescriptors(allocator, 0, 2, 4);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, 6, desc, 3, ENTRY_STRIDE);
                assertEntries(entries, 0, 1, 50, 51, 100, 101);
                assertDescriptors(desc, 0, 2, 4);
            }
        });
    }

    /**
     * Two batches share their first key because duplicate timestamps span a
     * page-frame boundary: in a forward-ascending scan frame F's last ts can
     * equal frame F+1's first ts. Under cross-query work-stealing the slot
     * receives them out of order, so two batches land in the buffer with
     * equal firstKey but different lastKey - the wide-range one arrived
     * first, the all-duplicate one second, plus a forward frame third.
     * <p>
     * The arrival-order buffer is not globally key-monotonic: it falls
     * from 5000 at the end of batch 0 to 1000 at the start of batch 1.
     * compactInPlace must restore monotonicity by sorting by
     * (firstKey, lastKey) - the narrower batch sorts ahead of the wider
     * one, and the result concatenates into a non-decreasing key sequence.
     */
    @Test
    public void testCompactInPlaceDuplicateFirstKeyAcrossBatches() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator,
                        // batch 0 (arrival 1, scan position 1): wider range starting at 1000
                        1000, 1000, 5000,
                        // batch 1 (arrival 2, scan position 0): all-1000 narrow batch
                        1000, 1000, 1000,
                        // batch 2 (arrival 3, scan position 2): forward run from 5000
                        5000, 5000, 9000
                );
                long desc = allocDescriptors(allocator, 0, 3, 6);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, 9, desc, 3, ENTRY_STRIDE);
                assertEntries(entries,
                        1000, 1000, 1000,
                        1000, 1000, 5000,
                        5000, 5000, 9000
                );
                assertDescriptors(desc, 0, 3, 6);
            }
        });
    }

    @Test
    public void testCompactInPlaceIdempotent() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, 0, 1, 100, 101, 50, 51);
                long desc = allocDescriptors(allocator, 0, 2, 4);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, 6, desc, 3, ENTRY_STRIDE);
                // A second call sees the batches already in key order.
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, 6, desc, 3, ENTRY_STRIDE);
                assertEntries(entries, 0, 1, 50, 51, 100, 101);
                assertDescriptors(desc, 0, 2, 4);
            }
        });
    }

    /**
     * 17 batches arrive with (firstKey, lastKey) ties on every pair: a wide
     * batch [k, k, k+5] arrives before the narrow [k, k, k] that preceded
     * it in scan order. With n above the insertion-sort threshold, the
     * mergesort branch's lastKey tie-break must put narrow ahead of wide
     * so the concatenated buffer is non-decreasing.
     */
    @Test
    public void testCompactInPlaceMergeSortKeyTies() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                final int pairs = 8;
                final int n = pairs * 2 + 1;
                final int entriesPerBatch = 3;
                final long[] arrivalKeys = new long[n * entriesPerBatch];
                final long[] arrivalDesc = new long[n];
                for (int p = 0; p < pairs; p++) {
                    final long k = p * 10L;
                    final int wide = 2 * p;
                    final int wideBase = wide * entriesPerBatch;
                    arrivalKeys[wideBase] = k;
                    arrivalKeys[wideBase + 1] = k;
                    arrivalKeys[wideBase + 2] = k + 5;
                    arrivalDesc[wide] = wideBase;
                    final int narrow = 2 * p + 1;
                    final int narrowBase = narrow * entriesPerBatch;
                    arrivalKeys[narrowBase] = k;
                    arrivalKeys[narrowBase + 1] = k;
                    arrivalKeys[narrowBase + 2] = k;
                    arrivalDesc[narrow] = narrowBase;
                }
                final long singleKey = pairs * 10L;
                final int singleBase = (n - 1) * entriesPerBatch;
                arrivalKeys[singleBase] = singleKey;
                arrivalKeys[singleBase + 1] = singleKey;
                arrivalKeys[singleBase + 2] = singleKey;
                arrivalDesc[n - 1] = singleBase;

                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, arrivalKeys);
                long desc = allocDescriptors(allocator, arrivalDesc);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, (long) n * entriesPerBatch, desc, n, ENTRY_STRIDE);

                final long[] expectedKeys = new long[n * entriesPerBatch];
                int idx = 0;
                for (int p = 0; p < pairs; p++) {
                    final long k = p * 10L;
                    expectedKeys[idx++] = k;
                    expectedKeys[idx++] = k;
                    expectedKeys[idx++] = k;
                    expectedKeys[idx++] = k;
                    expectedKeys[idx++] = k;
                    expectedKeys[idx++] = k + 5;
                }
                expectedKeys[idx++] = singleKey;
                expectedKeys[idx++] = singleKey;
                expectedKeys[idx] = singleKey;
                assertEntries(entries, expectedKeys);

                final long[] expectedDesc = new long[n];
                for (int i = 0; i < n; i++) {
                    expectedDesc[i] = (long) i * entriesPerBatch;
                }
                assertDescriptors(desc, expectedDesc);
            }
        });
    }

    /**
     * 17 batches arrive in reverse key order. With n above the
     * insertion-sort threshold, {@link SortedRunsMerge#compactInPlace}
     * takes the bottom-up mergesort branch to restore ascending key order.
     */
    @Test
    public void testCompactInPlaceMergeSortReversed() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                final int n = 17;
                final long[] arrivalKeys = new long[n * 2];
                final long[] arrivalDesc = new long[n];
                for (int i = 0; i < n; i++) {
                    final long k = (n - 1 - i) * 10L;
                    arrivalKeys[i * 2] = k;
                    arrivalKeys[i * 2 + 1] = k + 1;
                    arrivalDesc[i] = i * 2L;
                }
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, arrivalKeys);
                long desc = allocDescriptors(allocator, arrivalDesc);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, n * 2L, desc, n, ENTRY_STRIDE);

                final long[] expectedKeys = new long[n * 2];
                final long[] expectedDesc = new long[n];
                for (int i = 0; i < n; i++) {
                    final long k = i * 10L;
                    expectedKeys[i * 2] = k;
                    expectedKeys[i * 2 + 1] = k + 1;
                    expectedDesc[i] = i * 2L;
                }
                assertEntries(entries, expectedKeys);
                assertDescriptors(desc, expectedDesc);
            }
        });
    }

    /**
     * 32 batches arrive in a fixed shuffled order, forcing the bottom-up
     * mergesort through all log2(32) = 5 ping-pong rounds.
     */
    @Test
    public void testCompactInPlaceMergeSortShuffled() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                final int n = 32;
                final int[] perm = {
                        17, 5, 28, 12, 0, 9, 23, 31, 6, 19, 14, 26, 2, 11, 21, 8,
                        30, 4, 16, 25, 7, 13, 29, 1, 22, 10, 27, 3, 15, 20, 24, 18
                };
                final long[] arrivalKeys = new long[n * 2];
                final long[] arrivalDesc = new long[n];
                for (int i = 0; i < n; i++) {
                    final long k = perm[i] * 10L;
                    arrivalKeys[i * 2] = k;
                    arrivalKeys[i * 2 + 1] = k + 1;
                    arrivalDesc[i] = i * 2L;
                }
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, arrivalKeys);
                long desc = allocDescriptors(allocator, arrivalDesc);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, n * 2L, desc, n, ENTRY_STRIDE);

                final long[] expectedKeys = new long[n * 2];
                final long[] expectedDesc = new long[n];
                for (int i = 0; i < n; i++) {
                    final long k = i * 10L;
                    expectedKeys[i * 2] = k;
                    expectedKeys[i * 2 + 1] = k + 1;
                    expectedDesc[i] = i * 2L;
                }
                assertEntries(entries, expectedKeys);
                assertDescriptors(desc, expectedDesc);
            }
        });
    }

    @Test
    public void testCompactInPlaceOrderedBatchesIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, 10, 20, 30, 40, 50);
                long desc = allocDescriptors(allocator, 0, 2);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, 5, desc, 2, ENTRY_STRIDE);
                assertEntries(entries, 10, 20, 30, 40, 50);
                assertDescriptors(desc, 0, 2);
            }
        });
    }

    @Test
    public void testCompactInPlaceSingleBatchIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, 10, 20, 30);
                // descPtr == 0: a single implicit batch, already a sorted run.
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, 3, 0, 0, ENTRY_STRIDE);
                assertEntries(entries, 10, 20, 30);
            }
        });
    }

    @Test
    public void testCompactInPlaceSwappedBatches() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long entries = allocEntries(allocator, 30, 40, 50, 10, 20);
                long desc = allocDescriptors(allocator, 0, 3);
                SortedRunsMerge.compactInPlace(allocator, scratch, entries, 5, desc, 2, ENTRY_STRIDE);
                assertEntries(entries, 10, 20, 30, 40, 50);
                assertDescriptors(desc, 0, 2);
            }
        });
    }

    /**
     * Two single-batch inputs share their first key (duplicate timestamp at
     * the slot boundary). Input A's range is 1000..5000 and arrives first,
     * input B's range is 1000..1000 and arrives second; the stable sort
     * preserves arrival order on first-key ties, so the merge emits A then
     * B and produces output that drops from 5000 to 1000 at the A/B
     * boundary. The (firstKey, lastKey) tie-break must place B (narrower)
     * before A so the concatenation remains non-decreasing.
     */
    @Test
    public void testCompactIntoDuplicateFirstKey() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long a = allocEntries(allocator, 1000, 1000, 5000);
                long b = allocEntries(allocator, 1000, 1000, 1000);
                long dst = allocEntries(allocator, 0, 0, 0, 0, 0, 0);
                long dstDesc = allocDescriptors(allocator, 0, 0);
                SortedRunsMerge.compactInto(
                        scratch,
                        dst, dstDesc,
                        a, 3, 0, 0,
                        b, 3, 0, 0,
                        ENTRY_STRIDE
                );
                assertEntries(dst, 1000, 1000, 1000, 1000, 1000, 5000);
                assertDescriptors(dstDesc, 0, 3);
            }
        });
    }

    @Test
    public void testCompactIntoEmptyDest() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long src = allocEntries(allocator, 5, 6, 7);
                long dst = allocEntries(allocator, 0, 0, 0);
                SortedRunsMerge.compactInto(
                        scratch,
                        dst, 0,
                        0, 0, 0, 0,
                        src, 3, 0, 0,
                        ENTRY_STRIDE
                );
                assertEntries(dst, 5, 6, 7);
            }
        });
    }

    @Test
    public void testCompactIntoInterleavedSubtrees() throws Exception {
        // Two already-merged partials whose frame ranges interleave:
        // M1 owns {[0,1], [300,301]}, M2 owns {[100,101], [200,201]}.
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long m1 = allocEntries(allocator, 0, 1, 300, 301);
                long m1Desc = allocDescriptors(allocator, 0, 2);
                long m2 = allocEntries(allocator, 100, 101, 200, 201);
                long m2Desc = allocDescriptors(allocator, 0, 2);
                long dst = allocEntries(allocator, 0, 0, 0, 0, 0, 0, 0, 0);
                long dstDesc = allocDescriptors(allocator, 0, 0, 0, 0);
                SortedRunsMerge.compactInto(
                        scratch,
                        dst, dstDesc,
                        m1, 4, m1Desc, 2,
                        m2, 4, m2Desc, 2,
                        ENTRY_STRIDE
                );
                assertEntries(dst, 0, 1, 100, 101, 200, 201, 300, 301);
                assertDescriptors(dstDesc, 0, 2, 4, 6);
            }
        });
    }

    @Test
    public void testCompactIntoMultiBatch() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long a = allocEntries(allocator, 0, 1, 40, 41);
                long aDesc = allocDescriptors(allocator, 0, 2);
                long b = allocEntries(allocator, 20, 21);
                long dst = allocEntries(allocator, 0, 0, 0, 0, 0, 0);
                long dstDesc = allocDescriptors(allocator, 0, 0, 0);
                SortedRunsMerge.compactInto(
                        scratch,
                        dst, dstDesc,
                        a, 4, aDesc, 2,
                        b, 2, 0, 0,
                        ENTRY_STRIDE
                );
                assertEntries(dst, 0, 1, 20, 21, 40, 41);
                assertDescriptors(dstDesc, 0, 2, 4);
            }
        });
    }

    @Test
    public void testCompactIntoSingleBatchEach() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration)) {
                LongList scratch = new LongList(16);
                long a = allocEntries(allocator, 30, 40);
                long b = allocEntries(allocator, 10, 20);
                long dst = allocEntries(allocator, 0, 0, 0, 0);
                long dstDesc = allocDescriptors(allocator, 0, 0);
                SortedRunsMerge.compactInto(
                        scratch,
                        dst, dstDesc,
                        a, 2, 0, 0,
                        b, 2, 0, 0,
                        ENTRY_STRIDE
                );
                assertEntries(dst, 10, 20, 30, 40);
                assertDescriptors(dstDesc, 0, 2);
            }
        });
    }

    private static long allocDescriptors(GroupByAllocator allocator, long... starts) {
        final long ptr = allocator.malloc((long) starts.length * Long.BYTES);
        for (int i = 0; i < starts.length; i++) {
            Unsafe.putLong(ptr + (long) i * Long.BYTES, starts[i]);
        }
        return ptr;
    }

    private static long allocEntries(GroupByAllocator allocator, long... keys) {
        final long ptr = allocator.malloc((long) keys.length * ENTRY_STRIDE);
        for (int i = 0; i < keys.length; i++) {
            Unsafe.putLong(ptr + i * ENTRY_STRIDE, keys[i]);
            Unsafe.putLong(ptr + i * ENTRY_STRIDE + 8, valueOf(keys[i]));
        }
        return ptr;
    }

    private static void assertDescriptors(long descPtr, long... expected) {
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("descriptor[" + i + "]", expected[i], Unsafe.getLong(descPtr + (long) i * Long.BYTES));
        }
    }

    private static void assertEntries(long entriesPtr, long... expectedKeys) {
        for (int i = 0; i < expectedKeys.length; i++) {
            Assert.assertEquals("key[" + i + "]", expectedKeys[i], Unsafe.getLong(entriesPtr + i * ENTRY_STRIDE));
            Assert.assertEquals(
                    "value[" + i + "]",
                    valueOf(expectedKeys[i]),
                    Unsafe.getLong(entriesPtr + i * ENTRY_STRIDE + 8)
            );
        }
    }

    // Deterministic value paired with each key, so a misrouted entry is caught.
    private static long valueOf(long key) {
        return key * 7 + 3;
    }
}
