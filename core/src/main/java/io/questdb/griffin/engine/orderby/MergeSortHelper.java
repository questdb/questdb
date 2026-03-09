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

package io.questdb.griffin.engine.orderby;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * 1. Scans the input to detect naturally sorted runs (ascending and descending).
 * 2. Reverses descending runs in place.
 * 3. Sorts unsorted segments using native quicksort/radix sort.
 * 4. Merges all runs into a single sorted result.
 * <p>
 * This is particularly beneficial for time-series data where the input is
 * often partially sorted.
 */
public final class MergeSortHelper {
    private static final int MIN_RUN_LENGTH = 64;

    private MergeSortHelper() {
    }

    /**
     * Sorts an array of fixed-size entries in ascending order.
     *
     * @param addr    base address of the array
     * @param count   number of entries
     * @param keyType determines entry size and sort dispatch
     * @param cpyAddr auxiliary buffer address for radix sort (only for FIXED_8);
     *                also reused as merge buffer when available
     */
    public static void sort(long addr, long count, SortKeyType keyType, long cpyAddr) {
        if (count <= 1) {
            return;
        }

        int entrySize = keyType.entrySize();
        int keyLongs = keyType.keyLength() / 8;

        if (count <= MIN_RUN_LENGTH) {
            nativeSort(addr, count, keyType, cpyAddr);
            return;
        }

        int estimatedRuns = (int) Math.min(count / MIN_RUN_LENGTH + 1, Integer.MAX_VALUE);
        long[] runStarts = new long[Math.max(estimatedRuns, 16)];
        long[] runEnds = new long[runStarts.length];
        int runCount = 0;

        long i = 0;
        while (i < count) {
            long runEnd = i + 1;

            if (runEnd < count) {
                int cmp = compareEntries(addr, i, runEnd, entrySize, keyLongs);
                if (cmp <= 0) {
                    // Ascending run: extend while non-decreasing
                    while (runEnd < count && compareEntries(addr, runEnd - 1, runEnd, entrySize, keyLongs) <= 0) {
                        runEnd++;
                    }
                } else {
                    while (runEnd < count && compareEntries(addr, runEnd - 1, runEnd, entrySize, keyLongs) > 0) {
                        runEnd++;
                    }
                    reverseEntries(addr, i, runEnd, entrySize);
                }
            }

            long runLength = runEnd - i;
            if (runLength < MIN_RUN_LENGTH && runEnd < count) {
                runEnd = Math.min(i + MIN_RUN_LENGTH, count);
                nativeSort(addr + i * entrySize, runEnd - i, keyType, cpyAddr);
            }

            // Record this run
            if (runCount == runStarts.length) {
                long[] newStarts = new long[runCount * 2];
                long[] newEnds = new long[runCount * 2];
                System.arraycopy(runStarts, 0, newStarts, 0, runCount);
                System.arraycopy(runEnds, 0, newEnds, 0, runCount);
                runStarts = newStarts;
                runEnds = newEnds;
            }
            runStarts[runCount] = i;
            runEnds[runCount] = runEnd;
            runCount++;
            i = runEnd;
        }

        if (runCount == 1) {
            // Already sorted
            return;
        }

        // Merge all runs
        long mergeBufferAddr = 0;
        boolean isMergeBufferOwned = false;
        try {
            long bufferSize = count * entrySize;
            if (cpyAddr != 0) {
                mergeBufferAddr = cpyAddr;
            } else {
                mergeBufferAddr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
                isMergeBufferOwned = true;
            }
            mergeRuns(addr, mergeBufferAddr, runStarts, runEnds, runCount, entrySize, keyLongs, count);
        } finally {
            if (isMergeBufferOwned && mergeBufferAddr != 0) {
                Unsafe.free(mergeBufferAddr, count * entrySize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static int compareByAddr(long addrA, long addrB, int keyLongs) {
        for (int k = 0; k < keyLongs; k++) {
            long a = Unsafe.getUnsafe().getLong(addrA + (long) k * 8);
            long b = Unsafe.getUnsafe().getLong(addrB + (long) k * 8);
            int cmp = Long.compareUnsigned(a, b);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Merges sorted runs using iterative bottom-up merge sort.
     * After merging, the result is in the original address.
     */
    private static void mergeRuns(
            long addr,
            long bufAddr,
            long[] runStarts,
            long[] runEnds,
            int runCount,
            int entrySize,
            int keyLongs,
            long totalCount
    ) {
        long src = addr;
        long dst = bufAddr;

        while (runCount > 1) {
            int newRunCount = 0;
            for (int r = 0; r < runCount; r += 2) {
                if (r + 1 < runCount) {
                    mergeTwoRuns(src, dst, runStarts[r], runEnds[r], runStarts[r + 1], runEnds[r + 1], entrySize, keyLongs);
                    runStarts[newRunCount] = runStarts[r];
                    runEnds[newRunCount] = runEnds[r + 1];
                } else {
                    // Odd run, copy to dst
                    long startOff = runStarts[r] * entrySize;
                    long bytes = (runEnds[r] - runStarts[r]) * entrySize;
                    Unsafe.getUnsafe().copyMemory(src + startOff, dst + startOff, bytes);
                    runStarts[newRunCount] = runStarts[r];
                    runEnds[newRunCount] = runEnds[r];
                }
                newRunCount++;
            }
            runCount = newRunCount;

            // Swap src and dst
            long tmp = src;
            src = dst;
            dst = tmp;
        }

        // If result is in bufAddr, copy back to addr
        if (src != addr) {
            Unsafe.getUnsafe().copyMemory(src, addr, totalCount * entrySize);
        }
    }

    private static void mergeTwoRuns(
            long src,
            long dst,
            long start1,
            long end1,
            long start2,
            long end2,
            int entrySize,
            int keyLongs
    ) {
        long ptrI = src + start1 * entrySize;
        long ptrEnd1 = src + end1 * entrySize;
        long ptrJ = src + start2 * entrySize;
        long ptrEnd2 = src + end2 * entrySize;
        long ptrOut = dst + start1 * entrySize;

        while (ptrI < ptrEnd1 && ptrJ < ptrEnd2) {
            if (compareByAddr(ptrI, ptrJ, keyLongs) <= 0) {
                Unsafe.getUnsafe().copyMemory(ptrI, ptrOut, entrySize);
                ptrI += entrySize;
            } else {
                Unsafe.getUnsafe().copyMemory(ptrJ, ptrOut, entrySize);
                ptrJ += entrySize;
            }
            ptrOut += entrySize;
        }
        if (ptrI < ptrEnd1) {
            Unsafe.getUnsafe().copyMemory(ptrI, ptrOut, ptrEnd1 - ptrI);
        }
        if (ptrJ < ptrEnd2) {
            Unsafe.getUnsafe().copyMemory(ptrJ, ptrOut, ptrEnd2 - ptrJ);
        }
    }

    private static void nativeSort(long addr, long count, SortKeyType keyType, long cpyAddr) {
        switch (keyType) {
            case FIXED_8 -> {
                if (cpyAddr != 0) {
                    Vect.radixSortLongIndexAscInPlace(addr, count, cpyAddr);
                } else {
                    Vect.quickSortLongIndexAscInPlace(addr, count);
                }
            }
            case FIXED_16 -> Vect.sort3LongAscInPlace(addr, count);
            case FIXED_24 -> Vect.sort4LongAscInPlace(addr, count);
            case FIXED_32 -> Vect.sort5LongAscInPlace(addr, count);
            default -> throw new UnsupportedOperationException("unsupported key type: " + keyType);
        }
    }

    /**
     * Reverses entries in [from, to) in place by swapping entries from both ends.
     */
    private static void reverseEntries(long addr, long from, long to, int entrySize) {
        long lo = addr + from * entrySize;
        long hi = addr + (to - 1) * entrySize;
        while (lo < hi) {
            // Swap entries at lo and hi using 8-byte chunks
            for (int off = 0; off < entrySize; off += 8) {
                long a = Unsafe.getUnsafe().getLong(lo + off);
                long b = Unsafe.getUnsafe().getLong(hi + off);
                Unsafe.getUnsafe().putLong(lo + off, b);
                Unsafe.getUnsafe().putLong(hi + off, a);
            }
            lo += entrySize;
            hi -= entrySize;
        }
    }

    static int compareEntries(long baseAddr, long idxA, long idxB, int entrySize, int keyLongs) {
        long addrA = baseAddr + idxA * entrySize;
        long addrB = baseAddr + idxB * entrySize;
        for (int k = 0; k < keyLongs; k++) {
            long a = Unsafe.getUnsafe().getLong(addrA + (long) k * 8);
            long b = Unsafe.getUnsafe().getLong(addrB + (long) k * 8);
            int cmp = Long.compareUnsigned(a, b);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
