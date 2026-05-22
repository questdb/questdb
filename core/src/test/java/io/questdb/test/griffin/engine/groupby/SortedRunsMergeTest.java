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
