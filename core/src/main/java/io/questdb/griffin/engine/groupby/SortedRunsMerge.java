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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.map.MapValue;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Sorts the per-group entry buffers of parallel GROUP BY aggregate functions
 * ({@code twap}, {@code sparkline}) using explicit per-frame batch descriptors.
 *
 * <h2>What this fixes</h2>
 *
 * <p>Under parallel GROUP BY the executor binds {@code GroupByFunction}
 * instances to slots, not workers, and acquires slots through
 * {@link io.questdb.griffin.engine.PerWorkerLocks}. Under cross-query work
 * stealing a single slot's buffer can therefore receive page frames in
 * non-monotonic order - see
 * <a href="https://github.com/questdb/questdb/issues/7123">#7123</a>.
 *
 * <h2>Batch model</h2>
 *
 * <p>Each {@code computeNext} loop over one page frame appends a contiguous,
 * key-sorted run of entries - a <b>batch</b>. The owning function records the
 * exact batch boundaries as it appends: a new batch starts whenever the page
 * frame changes, detected from the high bits of the row id
 * ({@code rowId >>> 44} is the frame index). Because page frames cover disjoint,
 * contiguous row-id ranges, distinct batches never overlap in key range.
 *
 * <p>The boundaries live in a per-group <b>descriptor buffer</b>: a native array
 * of {@code descCount} ascending entry offsets, where descriptor {@code i} is
 * the start offset of batch {@code i} and batch {@code i} spans
 * {@code [desc[i], desc[i+1])} (the last batch runs to {@code entryCount}). The
 * descriptor buffer is allocated lazily by {@link #appendBatchStart}: a group
 * that only ever sees one frame keeps {@code descPtr == 0} and needs no
 * descriptor buffer at all.
 *
 * <h2>Algorithm</h2>
 *
 * <p>With exact boundaries known, sorting a buffer is a permutation of whole
 * batches - no element-wise merge:
 * <ol>
 *   <li>Gather one {@code [firstKey, address, entryCount]} record per batch.</li>
 *   <li>Stable-sort the records by {@code firstKey} (a bottom-up merge sort
 *       over the small record array; {@code firstKey} is the batch's minimum
 *       key, since each batch is itself key-sorted).</li>
 *   <li>Emit the batches in sorted order with one bulk {@link Vect#memcpy} each.</li>
 * </ol>
 *
 * <p>{@link #compactInPlace} additionally returns early when the batches are
 * already in key order (the common, uncontended case), touching no memory.
 */
public final class SortedRunsMerge {

    // Initial descriptor buffer capacity, in 8-byte entries. A multi-batch
    // group is seeded with two descriptors, leaving room for six more frames
    // before the first growth.
    private static final long DESC_INITIAL_CAPACITY = 8;
    // Each gathered batch occupies 3 longs in the scratch list:
    //   [firstKey, entriesAddress, entryCount]
    private static final int RECORD_LONGS = 3;

    private SortedRunsMerge() {
    }

    /**
     * Records the start of a new batch for the group held in {@code mapValue}.
     *
     * <p>The descriptor buffer is allocated lazily: a single-batch group keeps
     * {@code descPtr == 0}. The first call on such a group allocates the buffer
     * and seeds it with two descriptors - the previously implicit first batch
     * {@code [0, ...)} and the new one starting at {@code newBatchStart}.
     * Later calls append one descriptor each, growing the buffer when full.
     *
     * @param allocator     supplies and grows the descriptor buffer
     * @param mapValue      group state
     * @param descIndex     value index of the descriptor-buffer pointer slot;
     *                      {@code descIndex + 1} holds the descriptor count and
     *                      {@code descIndex + 2} the descriptor capacity
     * @param newBatchStart entry offset at which the new batch begins
     */
    public static void appendBatchStart(GroupByAllocator allocator, MapValue mapValue, int descIndex, long newBatchStart) {
        long descPtr = mapValue.getLong(descIndex);
        if (descPtr == 0) {
            // Transition from a single implicit batch to two explicit ones.
            descPtr = allocator.malloc(DESC_INITIAL_CAPACITY * Long.BYTES);
            Unsafe.putLong(descPtr, 0);                       // batch 0 starts at offset 0
            Unsafe.putLong(descPtr + Long.BYTES, newBatchStart); // batch 1
            mapValue.putLong(descIndex, descPtr);
            mapValue.putLong(descIndex + 1, 2);
            mapValue.putLong(descIndex + 2, DESC_INITIAL_CAPACITY);
            return;
        }
        long descCount = mapValue.getLong(descIndex + 1);
        long descCapacity = mapValue.getLong(descIndex + 2);
        if (descCount == descCapacity) {
            final long newCapacity = descCapacity * 2;
            descPtr = allocator.realloc(descPtr, descCapacity * Long.BYTES, newCapacity * Long.BYTES);
            mapValue.putLong(descIndex, descPtr);
            mapValue.putLong(descIndex + 2, newCapacity);
        }
        Unsafe.putLong(descPtr + descCount * Long.BYTES, newBatchStart);
        mapValue.putLong(descIndex + 1, descCount + 1);
    }

    /**
     * Sorts an entry buffer in place so its entries are in non-decreasing key
     * order, preserving the buffer pointer.
     *
     * <p>Returns immediately when the buffer is a single batch
     * ({@code descPtr == 0}) or when the batches are already in key order - the
     * common case under low contention - touching no memory. Otherwise sorts by
     * permuting whole batches into a transient auxiliary buffer and copying the
     * result back over the caller's buffer; the descriptor buffer is rewritten
     * to match the new layout.
     *
     * @param allocator   supplies the auxiliary buffer; freed before return
     * @param scratch     reusable scratch list for batch records; cleared on entry
     * @param entriesPtr  address of the first entry
     * @param entryCount  number of entries in the buffer
     * @param descPtr     descriptor buffer address, or 0 for a single batch
     * @param descCount   number of descriptors (ignored when {@code descPtr == 0})
     * @param entryStride bytes per entry; the 8-byte sort key sits at offset 0
     */
    public static void compactInPlace(
            GroupByAllocator allocator,
            LongList scratch,
            long entriesPtr,
            long entryCount,
            long descPtr,
            long descCount,
            long entryStride
    ) {
        if (entryCount <= 1 || descPtr == 0) {
            return; // single batch - already a sorted run
        }
        assert descCount >= 2;
        scratch.clear();
        gatherBatches(scratch, entriesPtr, entryCount, descPtr, descCount, entryStride);
        final int n = scratch.size() / RECORD_LONGS;
        if (isAscending(scratch, 0, n)) {
            return; // batches already in key order - the buffer is globally sorted
        }
        final int sortedBase = sortByFirstKey(scratch, n);
        final long totalBytes = entryCount * entryStride;
        final long aux = allocator.malloc(totalBytes);
        emit(scratch, sortedBase, n, aux, descPtr, entryStride);
        Vect.memcpy(entriesPtr, aux, totalBytes);
        allocator.free(aux, totalBytes);
    }

    /**
     * Merges two entry buffers into a caller-allocated destination holding
     * {@code countA + countB} entries, sorted by key.
     *
     * <p>Each input is described by its own batch descriptors; the helper
     * gathers the batches of both, sorts them by first key, and emits each with
     * one bulk copy. Either count may be zero, in which case the matching
     * pointers are ignored. The merged batch descriptors (ascending start
     * offsets) are written to {@code dstDescPtr}; pass 0 when the merged result
     * is a single batch.
     *
     * @param scratch       reusable scratch list for batch records
     * @param dstEntriesPtr destination entry buffer, pre-allocated by the caller
     * @param dstDescPtr    destination descriptor buffer, or 0 for a single batch
     * @param entriesPtrA   first input entries; ignored if {@code countA == 0}
     * @param countA        number of entries in input A
     * @param descPtrA      input A descriptor buffer, or 0 for a single batch
     * @param descCountA    number of descriptors in input A
     * @param entriesPtrB   second input entries; ignored if {@code countB == 0}
     * @param countB        number of entries in input B
     * @param descPtrB      input B descriptor buffer, or 0 for a single batch
     * @param descCountB    number of descriptors in input B
     * @param entryStride   bytes per entry
     */
    public static void compactInto(
            LongList scratch,
            long dstEntriesPtr,
            long dstDescPtr,
            long entriesPtrA, long countA, long descPtrA, long descCountA,
            long entriesPtrB, long countB, long descPtrB, long descCountB,
            long entryStride
    ) {
        scratch.clear();
        gatherBatches(scratch, entriesPtrA, countA, descPtrA, descCountA, entryStride);
        gatherBatches(scratch, entriesPtrB, countB, descPtrB, descCountB, entryStride);
        final int n = scratch.size() / RECORD_LONGS;
        final int sortedBase = sortByFirstKey(scratch, n);
        emit(scratch, sortedBase, n, dstEntriesPtr, dstDescPtr, entryStride);
    }

    // Copies the 3-long batch record at long-offset `from` to long-offset `to`.
    private static void copyRecord(LongList scratch, int from, int to) {
        scratch.setQuick(to, scratch.getQuick(from));
        scratch.setQuick(to + 1, scratch.getQuick(from + 1));
        scratch.setQuick(to + 2, scratch.getQuick(from + 2));
    }

    // Walks batches in sorted order, copying each into the destination buffer
    // with one bulk memcpy. When dstDescPtr is non-zero, writes the running
    // start offset of every batch into the destination descriptor buffer.
    private static void emit(LongList scratch, int base, int n, long dstEntriesPtr, long dstDescPtr, long entryStride) {
        long outOffset = 0;
        for (int k = 0; k < n; k++) {
            final int rec = base + k * RECORD_LONGS;
            final long srcAddr = scratch.getQuick(rec + 1);
            final long entryCount = scratch.getQuick(rec + 2);
            Vect.memcpy(dstEntriesPtr + outOffset * entryStride, srcAddr, entryCount * entryStride);
            if (dstDescPtr != 0) {
                Unsafe.putLong(dstDescPtr + (long) k * Long.BYTES, outOffset);
            }
            outOffset += entryCount;
        }
    }

    // Appends one [firstKey, address, entryCount] record per batch. A buffer
    // with descPtr == 0 is a single implicit batch covering [0, entryCount).
    private static void gatherBatches(
            LongList scratch,
            long entriesPtr,
            long entryCount,
            long descPtr,
            long descCount,
            long entryStride
    ) {
        if (entryCount <= 0) {
            return;
        }
        if (descPtr == 0) {
            scratch.add(Unsafe.getLong(entriesPtr));
            scratch.add(entriesPtr);
            scratch.add(entryCount);
            return;
        }
        for (long i = 0; i < descCount; i++) {
            final long start = Unsafe.getLong(descPtr + i * Long.BYTES);
            final long end = (i + 1 < descCount) ? Unsafe.getLong(descPtr + (i + 1) * Long.BYTES) : entryCount;
            final long addr = entriesPtr + start * entryStride;
            scratch.add(Unsafe.getLong(addr));
            scratch.add(addr);
            scratch.add(end - start);
        }
    }

    // True when the first keys of records [base, base + n) are non-decreasing.
    // Since batches are key-disjoint and each is internally sorted, ascending
    // first keys mean the buffer is already globally sorted.
    private static boolean isAscending(LongList scratch, int base, int n) {
        if (n <= 1) {
            return true;
        }
        long prev = scratch.getQuick(base);
        for (int k = 1; k < n; k++) {
            final long firstKey = scratch.getQuick(base + k * RECORD_LONGS);
            if (firstKey < prev) {
                return false;
            }
            prev = firstKey;
        }
        return true;
    }

    // Stable merge of the record runs [lo, mid) and [mid, hi) from the srcBase
    // half of the scratch list into the dstBase half, starting at index lo.
    // On equal keys the record from the left run is taken first.
    private static void mergeRuns(LongList scratch, int srcBase, int dstBase, int lo, int mid, int hi) {
        int i = lo, j = mid, k = lo;
        while (i < mid && j < hi) {
            if (scratch.getQuick(srcBase + i * RECORD_LONGS) <= scratch.getQuick(srcBase + j * RECORD_LONGS)) {
                copyRecord(scratch, srcBase + i * RECORD_LONGS, dstBase + k * RECORD_LONGS);
                i++;
            } else {
                copyRecord(scratch, srcBase + j * RECORD_LONGS, dstBase + k * RECORD_LONGS);
                j++;
            }
            k++;
        }
        while (i < mid) {
            copyRecord(scratch, srcBase + i * RECORD_LONGS, dstBase + k * RECORD_LONGS);
            i++;
            k++;
        }
        while (j < hi) {
            copyRecord(scratch, srcBase + j * RECORD_LONGS, dstBase + k * RECORD_LONGS);
            j++;
            k++;
        }
    }

    // Stable bottom-up merge sort of the n batch records by first key. Uses the
    // upper half of the scratch list as the ping-pong area. Returns the long
    // base offset of the half holding the sorted records.
    private static int sortByFirstKey(LongList scratch, int n) {
        if (n <= 1) {
            return 0;
        }
        final int span = n * RECORD_LONGS;
        scratch.setPos(2 * span);
        int srcBase = 0;
        int dstBase = span;
        for (int width = 1; width < n; width <<= 1) {
            for (int lo = 0; lo < n; lo += width << 1) {
                final int mid = Math.min(lo + width, n);
                final int hi = Math.min(lo + (width << 1), n);
                mergeRuns(scratch, srcBase, dstBase, lo, mid, hi);
            }
            final int tmp = srcBase;
            srcBase = dstBase;
            dstBase = tmp;
        }
        return srcBase;
    }
}
