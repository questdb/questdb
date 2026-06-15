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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.Reopenable;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

/**
 * A flat native buffer of fixed-width {@code (encoded key, rowId)} entries
 * that keeps memory at O(limit) for a top-K build. Entries are appended via
 * the {@link #beginAppend()} / {@link #endAppend()} pair: the caller encodes
 * into the returned address, and {@code endAppend} drops the entry when a
 * known threshold proves it cannot enter the top-K. When the buffer reaches
 * the compaction trigger, it sorts natively, truncates to the best
 * {@code limit} entries, and tightens the threshold to the new boundary.
 */
public class EncodedTopKBuffer implements QuietCloseable, Reopenable {
    // Compacting every `limit` rows would sort tiny batches for small limits;
    // batch at least this many entries between compactions.
    private static final long MIN_COMPACTION_TRIGGER = 4096;
    private final DirectLongList entryMem;
    private final long keyCapBytes;
    private final long parallelThreshold;
    // A copy, not a buffer address: entryMem may reallocate on growth.
    private final long[] thresholdEntry = new long[SortKeyType.MAX_ENTRY_LONGS];
    private final long valueCapBytes;
    private long compactionTrigger;
    private long count;
    private int entrySize;
    // Unsigned-max sentinel rejects nothing; compact() publishes the leading
    // threshold word here for FIRST N selections.
    private long fastRejectKeyThreshold = -1L;
    private boolean hasThreshold;
    private boolean isFirstN;
    private int keyLongs;
    private long limit;
    private int longsPerEntry;
    private long maxEntries;
    private long maxEntryMemBytes;

    public EncodedTopKBuffer(CairoConfiguration configuration) {
        this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true);
        this.keyCapBytes = configuration.getSqlSortKeyMaxBytes();
        this.valueCapBytes = configuration.getSqlSortLightValueMaxBytes();
        this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
    }

    /**
     * Returns the address to encode the next entry into; {@link #endAppend()}
     * must follow before the next call.
     */
    public long beginAppend() {
        if (count >= maxEntries) {
            SortKeyEncoder.throwSortHeapOverflow(maxEntryMemBytes);
        }
        entryMem.ensureCapacity(longsPerEntry);
        return entryMem.getAppendAddress();
    }

    public void clear() {
        count = 0;
        hasThreshold = false;
        fastRejectKeyThreshold = -1L;
        entryMem.clear();
    }

    @Override
    public void close() {
        Misc.free(entryMem);
    }

    /**
     * Accepts or rejects the entry written at the {@link #beginAppend()}
     * address; a rejected entry's slot is overwritten by the next candidate.
     */
    public void endAppend() {
        if (hasThreshold && isBeyondThreshold(entryMem.getAppendAddress())) {
            return;
        }
        entryMem.skip(longsPerEntry);
        count++;
        if (count == compactionTrigger) {
            compact();
        }
    }

    /**
     * Single-compare pre-encode rejection: a key whose leading word is strictly
     * beyond the threshold's leading word cannot enter a FIRST N top-K regardless
     * of tie-break words, so the caller can skip the entry write entirely. Keys
     * equal on the leading word still go through the full {@link #endAppend()}
     * compare. Never rejects for LAST N or before the first compaction.
     */
    public boolean fastRejectsKey(long key) {
        return Long.compareUnsigned(key, fastRejectKeyThreshold) > 0;
    }

    public long getAddress() {
        return entryMem.getAddress();
    }

    public long getCount() {
        return count;
    }

    /**
     * Appends the other buffer's entries through the threshold filter; both
     * buffers must share the entry layout and selection.
     */
    public void mergeFrom(EncodedTopKBuffer other) {
        for (long addr = other.entryMem.getAddress(), hi = addr + other.count * entrySize; addr < hi; addr += entrySize) {
            if (fastRejectsKey(Unsafe.getLong(addr))) {
                continue;
            }
            Vect.memcpy(beginAppend(), addr, entrySize);
            endAppend();
        }
    }

    public void of(SortKeyType keyType, boolean isFirstN, long limit) {
        this.isFirstN = isFirstN;
        this.limit = limit;
        entrySize = keyType.entrySize();
        keyLongs = keyType.keyLength() / Long.BYTES;
        longsPerEntry = entrySize / Long.BYTES;
        maxEntries = SortKeyEncoder.maxEntries(keyCapBytes, valueCapBytes, keyType);
        maxEntryMemBytes = maxEntries * entrySize;
        // The trigger stays within maxEntries so compaction fires before the overflow check can.
        compactionTrigger = limit > 0 && limit < maxEntries
                ? Math.min(Math.max(limit << 1, MIN_COMPACTION_TRIGGER), maxEntries)
                : Long.MAX_VALUE;
        clear();
    }

    public void presize(long entries) {
        if (entries > maxEntries) {
            SortKeyEncoder.throwSortHeapOverflow(maxEntryMemBytes);
        }
        entryMem.setCapacity(entries * longsPerEntry);
    }

    @Override
    public void reopen() {
        entryMem.reopen();
    }

    // Bind before reopen() so the entry buffer's alloc/free is charged to the
    // active workload's per-query counter. Null when no per-query limit applies.
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        entryMem.setMemoryTracker(tracker);
    }

    public void sort() {
        if (count > 1) {
            Vect.sortEncodedEntries(entryMem.getAddress(), count, keyLongs, parallelThreshold);
        }
    }

    /**
     * Sorts the buffer, keeps the {@code limit} entries the selection retains,
     * and captures the boundary entry so that subsequent rows can be rejected
     * with a key compare instead of growing the buffer. This bounds build memory
     * at O(limit) instead of O(scanned rows).
     */
    private void compact() {
        sort();
        if (!isFirstN) {
            // LAST N keeps the tail of the sorted buffer.
            Vect.memmove(entryMem.getAddress(), entryMem.getAddress() + (count - limit) * entrySize, limit * entrySize);
        }
        count = limit;
        entryMem.setPos(limit * longsPerEntry);
        final long boundaryAddr = entryMem.getAddress() + (isFirstN ? limit - 1 : 0) * entrySize;
        for (int i = 0; i < longsPerEntry; i++) {
            thresholdEntry[i] = Unsafe.getLong(boundaryAddr + 8L * i);
        }
        hasThreshold = true;
        if (isFirstN) {
            fastRejectKeyThreshold = thresholdEntry[0];
        }
    }

    // Entries are unique by their trailing rowId word, so word-wise unsigned
    // comparison is a strict total order matching Vect.sortEncodedEntries.
    private boolean isBeyondThreshold(long entryAddr) {
        for (int i = 0; i < longsPerEntry; i++) {
            final int cmp = Long.compareUnsigned(Unsafe.getLong(entryAddr + 8L * i), thresholdEntry[i]);
            if (cmp != 0) {
                return isFirstN ? cmp > 0 : cmp < 0;
            }
        }
        return true;
    }
}
