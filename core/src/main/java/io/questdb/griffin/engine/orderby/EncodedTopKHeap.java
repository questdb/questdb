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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Size-K binary heap over fixed-width encoded entries laid out as
 * [encoded_key][rowId]. The encoded_key matches {@link SortKeyEncoder}'s
 * output and the rowId trailer participates in comparisons as the
 * canonical tie-breaker (ascending), matching the native
 * encoded_2x..encoded_5x compare semantics.
 *
 * <p>The heap is oriented by {@code isFirstN}:
 * <ul>
 *   <li>{@code isFirstN == true}  -> max-heap; top is the current worst
 *       candidate for displacement when picking the smallest K.</li>
 *   <li>{@code isFirstN == false} -> min-heap; top is the current worst
 *       candidate when picking the largest K.</li>
 * </ul>
 *
 * <p>Storage layout: K heap slots followed by one scratch slot at index K.
 * Callers encode each input row into {@link #scratchAddr()} and then call
 * {@link #tryPush()}.
 */
final class EncodedTopKHeap implements QuietCloseable, Reopenable {
    private final DirectLongList entryMem;
    private final long parallelThreshold;
    private long capacity;
    private long count;
    private int entrySize;
    private boolean isFirstN;
    private int keyLongs;
    private int longsPerEntry;
    private int rowIdOffset;

    EncodedTopKHeap(long parallelThreshold) {
        this.parallelThreshold = parallelThreshold;
        this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true);
    }

    @Override
    public void close() {
        Misc.free(entryMem);
    }

    public long count() {
        return count;
    }

    public void finalizeSort(SqlExecutionCircuitBreaker breaker) {
        if (count > 1) {
            Vect.sortEncodedEntries(entryMem.getAddress(), count, keyLongs, parallelThreshold);
            breaker.statefulThrowExceptionIfTrippedNoThrottle();
        }
    }

    public void of(SortKeyType keyType, long capacity, boolean isFirstN) {
        this.entrySize = keyType.entrySize();
        this.rowIdOffset = keyType.rowIdOffset();
        this.keyLongs = keyType.keyLength() / Long.BYTES;
        this.longsPerEntry = entrySize / Long.BYTES;
        this.capacity = capacity;
        this.isFirstN = isFirstN;
        this.count = 0;
        entryMem.setCapacity((capacity + 1) * longsPerEntry);
        entryMem.clear();
    }

    @Override
    public void reopen() {
        entryMem.reopen();
    }

    public long scratchAddr() {
        return entryMem.getAddress() + capacity * entrySize;
    }

    public long sortedStartAddr() {
        return entryMem.getAddress() + rowIdOffset;
    }

    public void tryPush() {
        if (capacity == 0) {
            return;
        }
        final long heapBase = entryMem.getAddress();
        final long scratchAddr = heapBase + capacity * entrySize;
        if (count < capacity) {
            final long destAddr = heapBase + count * entrySize;
            copyEntry(destAddr, scratchAddr);
            siftUp(heapBase, count);
            count++;
        } else if (isScratchBetterThanTop(heapBase, scratchAddr)) {
            copyEntry(heapBase, scratchAddr);
            siftDown(heapBase, 0);
        }
    }

    private int compareEntries(long addrA, long addrB) {
        for (int w = 0; w < keyLongs; w++) {
            final long off = (long) w * Long.BYTES;
            final int c = Long.compareUnsigned(Unsafe.getLong(addrA + off), Unsafe.getLong(addrB + off));
            if (c != 0) {
                return c;
            }
        }
        return Long.compareUnsigned(
                Unsafe.getLong(addrA + rowIdOffset),
                Unsafe.getLong(addrB + rowIdOffset)
        );
    }

    private void copyEntry(long destAddr, long srcAddr) {
        for (int i = 0; i < longsPerEntry; i++) {
            final long off = (long) i * Long.BYTES;
            Unsafe.putLong(destAddr + off, Unsafe.getLong(srcAddr + off));
        }
    }

    private boolean isScratchBetterThanTop(long heapBase, long scratchAddr) {
        final int c = compareEntries(scratchAddr, heapBase);
        return isFirstN ? c < 0 : c > 0;
    }

    private void siftDown(long heapBase, long i) {
        final long n = count;
        while (true) {
            final long left = (i << 1) + 1;
            if (left >= n) {
                break;
            }
            final long right = left + 1;
            final long iAddr = heapBase + i * entrySize;
            long bestChild = left;
            long bestAddr = heapBase + left * entrySize;
            if (right < n) {
                final long rAddr = heapBase + right * entrySize;
                final int rc = compareEntries(rAddr, bestAddr);
                if (isFirstN ? rc > 0 : rc < 0) {
                    bestChild = right;
                    bestAddr = rAddr;
                }
            }
            final int c = compareEntries(bestAddr, iAddr);
            if (!(isFirstN ? c > 0 : c < 0)) {
                break;
            }
            swapEntries(iAddr, bestAddr);
            i = bestChild;
        }
    }

    private void siftUp(long heapBase, long i) {
        while (i > 0) {
            final long parent = (i - 1) >>> 1;
            final long iAddr = heapBase + i * entrySize;
            final long pAddr = heapBase + parent * entrySize;
            final int c = compareEntries(iAddr, pAddr);
            if (!(isFirstN ? c > 0 : c < 0)) {
                break;
            }
            swapEntries(iAddr, pAddr);
            i = parent;
        }
    }

    private void swapEntries(long addrA, long addrB) {
        for (int i = 0; i < longsPerEntry; i++) {
            final long off = (long) i * Long.BYTES;
            final long tmp = Unsafe.getLong(addrA + off);
            Unsafe.putLong(addrA + off, Unsafe.getLong(addrB + off));
            Unsafe.putLong(addrB + off, tmp);
        }
    }
}
