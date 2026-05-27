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
 * <p>A <b>batch</b> is a contiguous, key-sorted run of entries spanning one or
 * more consecutive page frames. The owning function tracks each row's page
 * frame from the high bits of the row id ({@code rowId >>> 44} is the frame
 * index) and records a batch boundary only when a frame is not the immediate
 * successor of the previous one - a gap or an out-of-order frame; consecutive
 * in-order frames extend the current batch. Each batch therefore spans a
 * contiguous interval of frame indices and is itself key-sorted. Because the
 * base scan is key-ascending and page frames cover disjoint, contiguous
 * row-id ranges, two distinct batches' key ranges may meet at a boundary
 * (frame F's last key equals frame F+1's first key on a duplicate-key run)
 * but never strictly overlap - the invariant the whole-batch permutation
 * below depends on.
 *
 * <p>The boundaries live in a per-group <b>descriptor buffer</b>: a native array
 * of {@code descCount} ascending entry offsets, where descriptor {@code i} is
 * the start offset of batch {@code i} and batch {@code i} spans
 * {@code [desc[i], desc[i+1])} (the last batch runs to {@code entryCount}). The
 * descriptor buffer is allocated lazily by {@link #appendBatchStart}: a group
 * whose frames arrive as a single consecutive run keeps {@code descPtr == 0}
 * and needs no descriptor buffer at all.
 *
 * <h2>Algorithm</h2>
 *
 * <p>With exact boundaries known, sorting a buffer is a permutation of whole
 * batches - no element-wise merge:
 * <ol>
 *   <li>Gather one {@code [firstKey, lastKey, address, entryCount]} record
 *       per batch. {@code firstKey} and {@code lastKey} are the batch's
 *       minimum and maximum keys, read directly from the first and last
 *       entries since each batch is itself key-sorted.</li>
 *   <li>Sort the records lexicographically by {@code (firstKey, lastKey)} via
 *       a bottom-up merge sort. The {@code lastKey} tie-break orders two
 *       batches that collide on {@code firstKey} (a duplicate-key run
 *       spanning a page-frame boundary) by scan position: the narrower
 *       all-{@code firstKey} batch came earlier in the scan and therefore
 *       sorts ahead of the wider one - a {@code firstKey}-only sort would
 *       leave them in arrival order and break the concatenation invariant.</li>
 *   <li>Emit the batches in sorted order with one bulk {@link Vect#memcpy} each.</li>
 * </ol>
 *
 * <p>{@link #compactInPlace} additionally returns early when the batches are
 * already non-overlapping in key order (the common, uncontended case),
 * touching no memory beyond the gather step.
 */
public final class SortedRunsMerge {

    // Initial descriptor buffer capacity, in 8-byte entries. A multi-batch
    // group is seeded with two descriptors, leaving room for six more frames
    // before the first growth.
    private static final long DESC_INITIAL_CAPACITY = 8;
    // Below this batch count, sortByFirstKey runs an insertion sort in-place
    // instead of the bottom-up mergesort. The reduce-phase merge over many
    // small groups visits this code path with single-digit n and is the hot
    // path in the high-cardinality scenario (see JFR profiling of the 1M-
    // groups twap benchmark).
    private static final int INSERTION_SORT_THRESHOLD = 16;
    // Each gathered batch occupies 4 longs in the scratch list:
    //   [firstKey, lastKey, entriesAddress, entryCount]
    // The lastKey tie-breaker on a (firstKey, lastKey) sort is what keeps a
    // narrow all-{@code firstKey} batch ahead of a wider batch with the same
    // firstKey when both arrived out of scan order at the same slot - the
    // duplicate-key-across-frame-boundary case a firstKey-only sort would
    // leave in arrival order.
    private static final int RECORD_LONGS = 4;

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
     * ({@code descPtr == 0}) - touching no memory at all - or when the gather
     * finds the batches already in non-overlapping key order (the common
     * case under low contention), in which case no allocator memory is
     * touched. Otherwise sorts by permuting whole batches into a transient
     * auxiliary buffer and copying the result back over the caller's
     * buffer; the descriptor buffer is rewritten to match the new layout.
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
     * gathers the batches of both, sorts them by {@code (firstKey, lastKey)},
     * and emits each with one bulk copy. Either count may be zero, in which
     * case the matching pointers are ignored. The merged batch descriptors
     * (ascending start offsets) are written to {@code dstDescPtr}; pass 0
     * when the merged result is a single batch.
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

    // Copies the 4-long batch record at long-offset `from` to long-offset `to`.
    private static void copyRecord(LongList scratch, int from, int to) {
        scratch.setQuick(to, scratch.getQuick(from));
        scratch.setQuick(to + 1, scratch.getQuick(from + 1));
        scratch.setQuick(to + 2, scratch.getQuick(from + 2));
        scratch.setQuick(to + 3, scratch.getQuick(from + 3));
    }

    // Walks batches in sorted order, copying each into the destination buffer
    // with one bulk memcpy. When dstDescPtr is non-zero, writes the running
    // start offset of every batch into the destination descriptor buffer.
    private static void emit(LongList scratch, int base, int n, long dstEntriesPtr, long dstDescPtr, long entryStride) {
        long outOffset = 0;
        for (int k = 0; k < n; k++) {
            final int rec = base + k * RECORD_LONGS;
            final long srcAddr = scratch.getQuick(rec + 2);
            final long entryCount = scratch.getQuick(rec + 3);
            Vect.memcpy(dstEntriesPtr + outOffset * entryStride, srcAddr, entryCount * entryStride);
            if (dstDescPtr != 0) {
                Unsafe.putLong(dstDescPtr + (long) k * Long.BYTES, outOffset);
            }
            outOffset += entryCount;
        }
    }

    // Appends one [firstKey, lastKey, address, entryCount] record per batch.
    // A buffer with descPtr == 0 is a single implicit batch covering
    // [0, entryCount). Hot path (JFR's top SortedRunsMerge leaf at 439
    // samples in the 1M-groups scenario), so it pre-grows the scratch list
    // once via setPos and writes via setQuick - skipping the per-add
    // capacity check that would otherwise fire four times per batch. The
    // descriptor loop also carries the previous iteration's end forward as
    // the next batch's start, halving the descriptor reads.
    //
    // The lastKey is read from the batch's last entry; it costs one extra
    // memory access per batch (the last entry's cache line, distinct from
    // the first entry's for any batch larger than a couple of entries).
    // The cost is per-batch, not per-entry, and is dwarfed by the
    // per-entry memcpy in emit on the cold (sort) path.
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
        final int base = scratch.size();
        if (descPtr == 0) {
            scratch.setPos(base + RECORD_LONGS);
            scratch.setQuick(base, Unsafe.getLong(entriesPtr));
            scratch.setQuick(base + 1, Unsafe.getLong(entriesPtr + (entryCount - 1) * entryStride));
            scratch.setQuick(base + 2, entriesPtr);
            scratch.setQuick(base + 3, entryCount);
            return;
        }
        final int n = (int) descCount;
        scratch.setPos(base + n * RECORD_LONGS);
        int write = base;
        long start = Unsafe.getLong(descPtr);
        for (int i = 0; i < n; i++) {
            final long end = (i + 1 < n) ? Unsafe.getLong(descPtr + (long) (i + 1) * Long.BYTES) : entryCount;
            final long addr = entriesPtr + start * entryStride;
            final long batchCount = end - start;
            scratch.setQuick(write, Unsafe.getLong(addr));
            scratch.setQuick(write + 1, Unsafe.getLong(addr + (batchCount - 1) * entryStride));
            scratch.setQuick(write + 2, addr);
            scratch.setQuick(write + 3, batchCount);
            write += RECORD_LONGS;
            start = end;
        }
    }

    // True when records [base, base + n) describe a globally key-sorted
    // buffer: each batch's firstKey is >= the previous batch's lastKey, so
    // concatenating the batches in their current order is non-decreasing.
    // Stronger than a firstKey-only check, which incorrectly accepts two
    // out-of-scan-order batches that tie on firstKey across a duplicate-key
    // frame boundary.
    private static boolean isAscending(LongList scratch, int base, int n) {
        if (n <= 1) {
            return true;
        }
        long prevLast = scratch.getQuick(base + 1);
        for (int k = 1; k < n; k++) {
            final int rec = base + k * RECORD_LONGS;
            final long firstKey = scratch.getQuick(rec);
            if (firstKey < prevLast) {
                return false;
            }
            prevLast = scratch.getQuick(rec + 1);
        }
        return true;
    }

    // Stable merge of the record runs [lo, mid) and [mid, hi) from the srcBase
    // half of the scratch list into the dstBase half, starting at index lo.
    // Records compare lexicographically by (firstKey, lastKey); on equal
    // composite keys the record from the left run is taken first.
    private static void mergeRuns(LongList scratch, int srcBase, int dstBase, int lo, int mid, int hi) {
        int i = lo, j = mid, k = lo;
        while (i < mid && j < hi) {
            final int recL = srcBase + i * RECORD_LONGS;
            final int recR = srcBase + j * RECORD_LONGS;
            final long firstL = scratch.getQuick(recL);
            final long firstR = scratch.getQuick(recR);
            final boolean takeLeft = firstL != firstR
                    ? firstL < firstR
                    : scratch.getQuick(recL + 1) <= scratch.getQuick(recR + 1);
            if (takeLeft) {
                copyRecord(scratch, recL, dstBase + k * RECORD_LONGS);
                i++;
            } else {
                copyRecord(scratch, recR, dstBase + k * RECORD_LONGS);
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

    // Stable bottom-up merge sort of the n batch records by (firstKey,
    // lastKey). Uses the upper half of the scratch list as the ping-pong
    // area. Returns the long base offset of the half holding the sorted
    // records.
    //
    // Small-N inputs (the common case under the merge-reduction path for many
    // small groups) take a separate insertion-sort branch. The bottom-up
    // mergesort's per-call overhead and ping-pong copies dominate for n in
    // the single digits, where JFR profiling of the 1M-group scenario showed
    // {@code mergeRuns} as the hottest leaf in the parallel GROUP BY reduce
    // path. Insertion sort runs in-place at base 0 and avoids the auxiliary
    // half entirely.
    private static int sortByFirstKey(LongList scratch, int n) {
        if (n <= 1) {
            return 0;
        }
        if (n <= INSERTION_SORT_THRESHOLD) {
            insertionSortByFirstKey(scratch, n);
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

    // In-place insertion sort by (firstKey, lastKey) over the first n records
    // of {@code scratch}. Each record occupies {@link #RECORD_LONGS}
    // consecutive longs; the first two longs are the composite sort key.
    // The lastKey tie-break orders two batches that collide on firstKey
    // (a duplicate-key run spanning a page-frame boundary): the narrower
    // all-firstKey batch's lastKey equals its firstKey, so it sorts ahead
    // of any wider batch that shares the firstKey.
    private static void insertionSortByFirstKey(LongList scratch, int n) {
        for (int i = 1; i < n; i++) {
            final int srcBase = i * RECORD_LONGS;
            final long firstKey = scratch.getQuick(srcBase);
            final long lastKey = scratch.getQuick(srcBase + 1);
            // Skip already-in-place records: prev record sorts <= ours.
            final int prevBase = srcBase - RECORD_LONGS;
            final long prevFirst = scratch.getQuick(prevBase);
            if (prevFirst < firstKey
                    || (prevFirst == firstKey && scratch.getQuick(prevBase + 1) <= lastKey)) {
                continue;
            }
            final long addr = scratch.getQuick(srcBase + 2);
            final long cnt = scratch.getQuick(srcBase + 3);
            int j = i;
            while (j > 0) {
                final int p = (j - 1) * RECORD_LONGS;
                final long pFirst = scratch.getQuick(p);
                final long pLast = scratch.getQuick(p + 1);
                if (pFirst < firstKey || (pFirst == firstKey && pLast <= lastKey)) {
                    break;
                }
                final int cur = j * RECORD_LONGS;
                scratch.setQuick(cur, pFirst);
                scratch.setQuick(cur + 1, pLast);
                scratch.setQuick(cur + 2, scratch.getQuick(p + 2));
                scratch.setQuick(cur + 3, scratch.getQuick(p + 3));
                j--;
            }
            final int dst = j * RECORD_LONGS;
            scratch.setQuick(dst, firstKey);
            scratch.setQuick(dst + 1, lastKey);
            scratch.setQuick(dst + 2, addr);
            scratch.setQuick(dst + 3, cnt);
        }
    }
}
