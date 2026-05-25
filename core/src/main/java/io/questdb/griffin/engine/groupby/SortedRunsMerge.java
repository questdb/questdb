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

import io.questdb.std.LongList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Stable mergesort for per-slot entry buffers in parallel GROUP BY aggregate
 * functions ({@code twap}, {@code sparkline}, {@code array_agg}).
 *
 * <h2>What this fixes</h2>
 *
 * <p>Under parallel GROUP BY the executor binds {@code GroupByFunction}
 * instances to slots, not workers, and acquires slots through
 * {@link io.questdb.griffin.engine.PerWorkerLocks}. Under cross-query work
 * stealing a single slot's buffer can therefore receive page frames in
 * non-monotonic order. The two-pointer merge step used by these aggregates
 * historically assumed a single sorted run per slot and silently produced
 * wrong output when that assumption was violated - see
 * <a href="https://github.com/questdb/questdb/issues/7123">#7123</a>.
 *
 * <h2>What the algorithm relies on</h2>
 *
 * <p><b>The only assumption this helper makes about the buffer is that within
 * a single page frame's contribution (one {@code computeNext} loop), entries
 * are appended in non-decreasing 8-byte-key order.</b> That holds for all
 * three callers because the executor walks each frame in rowId order. Different
 * frames can appear in any order in the buffer, can share keys, and can
 * have key ranges that interleave - the algorithm is correct for any
 * concatenation of sorted runs and does not depend on runs being key-disjoint.
 *
 * <p>An earlier design assumed runs were pairwise disjoint in key range and
 * tried to sort by permuting whole runs. That assumption was wrong:
 * {@link #discoverRuns} detects run boundaries by looking for a strict
 * key-decrease, but two frames whose ranges happen to be ascending across a
 * gap (e.g., frame [0..49] followed by frame [100..149], with frame [50..99]
 * appended later) get fused into one fake run that internally hides a missing
 * range. A general 2-way merge of sorted runs handles that case correctly;
 * run permutation would not.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Bottom-up pairwise mergesort over the natural sorted runs discovered by a
 * single scan of the buffer:
 * <ol>
 *   <li>Walk the buffer once; emit a run descriptor every time the key
 *       strictly decreases. Cost: O(n).</li>
 *   <li>If only one run was found, the buffer is already sorted - return
 *       immediately. This is the common case under low contention.</li>
 *   <li>Otherwise, allocate an auxiliary buffer and ping-pong: each round
 *       merges adjacent pairs of runs with a stable 2-way merge, halving the
 *       run count. After {@code ceil(log2(R))} rounds the buffer holds a
 *       single sorted run.</li>
 *   <li>If the final sorted output ended up in the auxiliary buffer (an odd
 *       number of rounds), copy it back over the caller's buffer so the
 *       caller's pointer stays valid.</li>
 * </ol>
 *
 * <p>Stability: the 2-way merge takes from the left run on key ties, so
 * entries from the same source row (same rowId, multiple values in
 * {@code array_agg(D[])}) keep their original element order.
 *
 * <p>Per-entry copy uses {@link Unsafe#copyMemory(long, long, long)} rather
 * than a JNI {@code Vect.memcpy} call - it is intrinsified by HotSpot for the
 * 16-byte case the callers use. Bulk tail copies use {@link Vect#memcpy}.
 */
public final class SortedRunsMerge {

    // Each run descriptor occupies 3 longs in the scratch list:
    //   [firstKey, entriesPtr, entryCount]
    private static final int RUN_DESC_LONGS = 3;

    private SortedRunsMerge() {
    }

    /**
     * In-place sort of an entry buffer.
     *
     * <p>If the buffer is already a single sorted run (the happy path under
     * low contention), returns without any byte movement. Otherwise, sorts
     * via bottom-up pairwise merge using a transient auxiliary buffer
     * allocated from {@code allocator}. The original pointer is preserved,
     * which lets read-path callers that only see {@code Record} (no
     * {@code MapValue} write access) leave their cache keys valid across the
     * sort.
     *
     * @param allocator   supplies the auxiliary buffer; freed best-effort at
     *                    the end of the call (reclaimed when the cursor
     *                    closes regardless)
     * @param runScratch  reusable {@link LongList} for run descriptors;
     *                    cleared on entry; caller owns its lifecycle
     * @param entriesPtr  address of the first entry
     * @param entryCount  number of entries in the buffer
     * @param entryStride bytes per entry; the 8-byte sort key sits at
     *                    offset 0 of each entry
     */
    public static void compactInPlace(
            GroupByAllocator allocator,
            LongList runScratch,
            long entriesPtr,
            long entryCount,
            long entryStride
    ) {
        if (entryCount <= 1) {
            return;
        }
        runScratch.clear();
        discoverRuns(runScratch, entriesPtr, entryCount, entryStride);
        int runCount = runScratch.size() / RUN_DESC_LONGS;
        if (runCount <= 1) {
            return; // already a single sorted run
        }

        final long totalBytes = entryCount * entryStride;
        final long aux = allocator.malloc(totalBytes);

        long src = entriesPtr;
        long dst = aux;
        while (runCount > 1) {
            runCount = mergePairwiseRound(runScratch, runCount, dst, entryStride);
            // Ping-pong: next round reads from current dst, writes to current src.
            final long tmp = src;
            src = dst;
            dst = tmp;
        }
        // Sorted content lives at `src` after the loop. If that is not the
        // caller's buffer, copy back so the caller's pointer keeps pointing
        // at the sorted output.
        if (src != entriesPtr) {
            Vect.memcpy(entriesPtr, src, totalBytes);
        }
        allocator.free(aux, totalBytes);
    }

    /**
     * Merge-sort two source entry buffers into a caller-allocated destination
     * holding {@code countA + countB} entries.
     *
     * <p>Equivalent to concatenating the two sources into {@code dstEntriesPtr}
     * and then calling {@link #compactInPlace}. Either count may be zero;
     * the corresponding pointer is then ignored. The destination must have
     * room for {@code (countA + countB) * entryStride} bytes.
     *
     * @param allocator     supplies the auxiliary buffer for the sort
     * @param runScratch    reusable scratch list for run descriptors
     * @param dstEntriesPtr destination, pre-allocated by caller
     * @param entriesPtrA   first source entries; ignored if {@code countA == 0}
     * @param countA        number of entries in source A
     * @param entriesPtrB   second source entries; ignored if {@code countB == 0}
     * @param countB        number of entries in source B
     * @param entryStride   bytes per entry
     */
    public static void compactInto(
            GroupByAllocator allocator,
            LongList runScratch,
            long dstEntriesPtr,
            long entriesPtrA, long countA,
            long entriesPtrB, long countB,
            long entryStride
    ) {
        if (countA > 0) {
            Vect.memcpy(dstEntriesPtr, entriesPtrA, countA * entryStride);
        }
        if (countB > 0) {
            Vect.memcpy(dstEntriesPtr + countA * entryStride, entriesPtrB, countB * entryStride);
        }
        compactInPlace(allocator, runScratch, dstEntriesPtr, countA + countB, entryStride);
    }

    // Single-scan run discovery. A new run starts whenever a key strictly
    // decreases relative to the previous entry's key. This produces a
    // partition of the buffer into sorted runs - intra-run keys are
    // non-decreasing, but the run partition is conservative: two distinct
    // page frames can be fused into one detected run if their key ranges
    // happen to ascend across the boundary. The pairwise merge in
    // mergePairwiseRound handles that case correctly.
    private static void discoverRuns(LongList out, long entriesPtr, long count, long entryStride) {
        if (count <= 0) {
            return;
        }
        long runStart = 0;
        long firstKey = Unsafe.getLong(entriesPtr);
        long prevKey = firstKey;
        for (long i = 1; i < count; i++) {
            final long key = Unsafe.getLong(entriesPtr + i * entryStride);
            if (key < prevKey) {
                out.add(firstKey);
                out.add(entriesPtr + runStart * entryStride);
                out.add(i - runStart);
                runStart = i;
                firstKey = key;
            }
            prevKey = key;
        }
        out.add(firstKey);
        out.add(entriesPtr + runStart * entryStride);
        out.add(count - runStart);
    }

    // Stable 2-way merge: writes (count1 + count2) entries to dst by
    // interleaving the two sorted runs at ptr1 and ptr2. On equal keys the
    // entry from run 1 (the left input) is written first, which preserves
    // the original intra-run order for ties - the property array_agg(D[])
    // relies on to keep multi-element rows' elements in input order.
    private static void merge2Way(
            long dst,
            long ptr1, long count1,
            long ptr2, long count2,
            long entryStride
    ) {
        long i = 0, j = 0, k = 0;
        while (i < count1 && j < count2) {
            final long key1 = Unsafe.getLong(ptr1 + i * entryStride);
            final long key2 = Unsafe.getLong(ptr2 + j * entryStride);
            if (key1 <= key2) {
                Unsafe.copyMemory(ptr1 + i * entryStride, dst + k * entryStride, entryStride);
                i++;
            } else {
                Unsafe.copyMemory(ptr2 + j * entryStride, dst + k * entryStride, entryStride);
                j++;
            }
            k++;
        }
        if (i < count1) {
            Vect.memcpy(dst + k * entryStride, ptr1 + i * entryStride, (count1 - i) * entryStride);
        }
        if (j < count2) {
            Vect.memcpy(dst + k * entryStride, ptr2 + j * entryStride, (count2 - j) * entryStride);
        }
    }

    // One round of pairwise merging. Reads run descriptors from `runs` (each
    // pointing into the current "src" buffer), merges adjacent pairs into
    // `dst`, and rewrites the descriptors in-place to point at the new runs
    // in `dst`. Returns the new run count. The descriptor write at index
    // writeIdx always lags the descriptor read at index i (writeIdx <= i),
    // so the in-place rewrite never clobbers a not-yet-read source
    // descriptor.
    private static int mergePairwiseRound(LongList runs, int runCount, long dst, long entryStride) {
        long offset = 0;
        int writeIdx = 0;
        int i = 0;
        while (i < runCount) {
            final int base1 = i * RUN_DESC_LONGS;
            final long ptr1 = runs.getQuick(base1 + 1);
            final long count1 = runs.getQuick(base1 + 2);
            final long newRunStart = dst + offset * entryStride;
            final long newCount;
            if (i + 1 < runCount) {
                final int base2 = (i + 1) * RUN_DESC_LONGS;
                final long ptr2 = runs.getQuick(base2 + 1);
                final long count2 = runs.getQuick(base2 + 2);
                merge2Way(newRunStart, ptr1, count1, ptr2, count2, entryStride);
                newCount = count1 + count2;
                i += 2;
            } else {
                // Odd one out: forward as-is into the destination.
                Vect.memcpy(newRunStart, ptr1, count1 * entryStride);
                newCount = count1;
                i += 1;
            }
            final long newFirstKey = Unsafe.getLong(newRunStart);
            final int writeBase = writeIdx * RUN_DESC_LONGS;
            runs.setQuick(writeBase, newFirstKey);
            runs.setQuick(writeBase + 1, newRunStart);
            runs.setQuick(writeBase + 2, newCount);
            offset += newCount;
            writeIdx++;
        }
        return writeIdx;
    }
}
