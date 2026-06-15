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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * A flat native buffer of {@code (encoded key, rowId)} entries that keeps
 * memory at O(limit) for a top-K build. Entries are appended via the
 * {@link #beginAppend()} / {@link #endAppend()} pair: the caller encodes
 * into the returned address, and {@code endAppend} drops the entry when a
 * known threshold proves it cannot enter the top-K. When the buffer reaches
 * the compaction trigger, it sorts natively, truncates to the best
 * {@code limit} entries, and tightens the threshold to the new boundary.
 * <p>
 * Variable-length keys hold a 16-byte prefix inline and spill the full key
 * bytes into a key heap ({@link #getKeyHeap()}); the entry references them
 * by heap offset. Compaction rewrites the heap to the surviving entries'
 * keys, and {@code endAppend} reclaims a rejected entry's key bytes, so the
 * heap stays bounded by the live entries.
 */
public class EncodedTopKBuffer implements QuietCloseable, Reopenable {
    // Pre-size cap: presizing the full trigger for huge limits would allocate
    // gigabytes upfront; beyond this the buffer falls back to doubling growth.
    private static final long MAX_PRESIZE_ENTRIES = 1024 * 1024;
    // Compacting every `limit` rows would sort tiny batches for small limits;
    // batch at least this many entries between compactions.
    private static final long MIN_COMPACTION_TRIGGER = 4096;
    // Variable entry layout: (k1, k2, len, heap offset, rowId) - encoded_var in ooo.cpp.
    private static final long VAR_ENTRY_KEY_OFFSET = 24;
    private static final long VAR_ENTRY_LEN_OFFSET = 16;
    private static final long VAR_ENTRY_ROWID_OFFSET = 32;
    private static final long VAR_PREFIX6_MASK = 0xFFFFFFFFFFFFFF00L;
    private final DirectLongList entryMem;
    private final long keyCapBytes;
    private final long keyHeapPageSize;
    private final long parallelThreshold;
    // A copy, not a buffer address: entryMem may reallocate on growth. For
    // variable keys the words are (k1, k2, len, stale offset, rowId); the
    // boundary's key tail lives in thresholdTailMem.
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
    private boolean isVariable;
    private MemoryCARW keyHeap;
    private int keyLongs;
    private long limit;
    private int longsPerEntry;
    private long maxEntries;
    private long maxEntryMemBytes;
    // Compaction double-buffer: survivors' key bytes pack here, then copy back
    // so the encoder's keyHeap reference stays valid.
    private MemoryCARW scratchHeap;
    private DirectLongList thresholdTailMem;

    public EncodedTopKBuffer(CairoConfiguration configuration, boolean parallelSort) {
        this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true);
        this.keyCapBytes = configuration.getSqlSortKeyMaxBytes();
        this.valueCapBytes = configuration.getSqlSortLightValueMaxBytes();
        this.parallelThreshold = parallelSort ? configuration.getSqlSortEncodedParallelThreshold() : Long.MAX_VALUE;
        this.keyHeapPageSize = configuration.getSqlSortKeyPageSize();
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
        if (keyHeap != null) {
            keyHeap.jumpTo(0);
        }
    }

    @Override
    public void close() {
        Misc.free(entryMem);
        keyHeap = Misc.free(keyHeap);
        scratchHeap = Misc.free(scratchHeap);
        thresholdTailMem = Misc.free(thresholdTailMem);
    }

    /**
     * Accepts or rejects the entry written at the {@link #beginAppend()}
     * address; a rejected entry's slot is overwritten by the next candidate,
     * and for variable keys its key bytes are reclaimed from the heap.
     */
    public void endAppend() {
        final long entryAddr = entryMem.getAppendAddress();
        if (hasThreshold && isBeyondThreshold(entryAddr)) {
            if (isVariable) {
                // The key field still holds the heap offset the encode started at.
                keyHeap.jumpTo(Unsafe.getLong(entryAddr + VAR_ENTRY_KEY_OFFSET));
            }
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

    /**
     * Pre-encode reject for a variable-length key given only its leading word, total
     * length and rowId - no key-heap write. Mirrors {@link #isVarBeyondThreshold(long)}
     * for the cases it can settle without the second word or the heap tail:
     * <ul>
     *   <li>leading words differ -> decided on the leading word alone;</li>
     *   <li>leading words tie and either key fits the leading word ({@code len <= 8})
     *       -> the leading word is the whole key, so the tie is a true value tie and
     *       the rowId tie-break settles it;</li>
     *   <li>leading words tie and both keys spill past the leading word -> returns
     *       false so the caller encodes in full and {@link #endAppend()} compares the
     *       second word and heap tail.</li>
     * </ul>
     * Rejecting ties here is what keeps a low-cardinality ORDER BY (e.g. a column
     * that is mostly one value) from encoding and appending every tied row.
     */
    public boolean fastRejectsVarEntry(long k1, long len, long rowId) {
        if (!hasThreshold) {
            return false;
        }
        int cmp = Long.compareUnsigned(k1, thresholdEntry[0]);
        if (cmp == 0) {
            final long thresholdLen = thresholdEntry[2];
            if (len > Long.BYTES && thresholdLen > Long.BYTES) {
                // Both keys extend past the leading word; the full compare needs the
                // second word and heap tail, so let the full encode + endAppend decide.
                return false;
            }
            cmp = Long.compare(len, thresholdLen);
            if (cmp == 0) {
                cmp = Long.compareUnsigned(rowId, thresholdEntry[4]);
            }
        }
        if (cmp == 0) {
            return true;
        }
        return isFirstN ? cmp > 0 : cmp < 0;
    }

    /**
     * Six-byte-prefix variant of {@link #fastRejectsVarEntry(long, long, long)} for the
     * split-VARCHAR batch path, where {@code k1} carries only the six inline-prefix value
     * bytes. Masking the seventh value byte off both sides keeps the compare consistent;
     * a six-byte tie between two keys that both spill past the prefix defers to the full
     * encode, exactly as the eight-byte tie does in the base method.
     */
    public boolean fastRejectsVarEntryPrefix6(long k1, long len, long rowId) {
        if (!hasThreshold) {
            return false;
        }
        int cmp = Long.compareUnsigned(k1 & VAR_PREFIX6_MASK, thresholdEntry[0] & VAR_PREFIX6_MASK);
        if (cmp == 0) {
            final long thresholdLen = thresholdEntry[2];
            if (len > Long.BYTES && thresholdLen > Long.BYTES) {
                return false;
            }
            cmp = Long.compare(len, thresholdLen);
            if (cmp == 0) {
                cmp = Long.compareUnsigned(rowId, thresholdEntry[4]);
            }
        }
        if (cmp == 0) {
            return true;
        }
        return isFirstN ? cmp > 0 : cmp < 0;
    }

    public long getAddress() {
        return entryMem.getAddress();
    }

    public long getCount() {
        return count;
    }

    /**
     * The variable-length key heap for this buffer, or null for fixed-width
     * keys. The encoder writes key bytes here; entries reference it by offset.
     */
    public MemoryCARW getKeyHeap() {
        return keyHeap;
    }

    /**
     * Appends the other buffer's entries through the threshold filter; both
     * buffers must share the entry layout and selection.
     */
    public void mergeFrom(EncodedTopKBuffer other) {
        if (isVariable) {
            mergeVarFrom(other);
            return;
        }
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
        this.isVariable = keyType.isVariable();
        entrySize = keyType.entrySize();
        keyLongs = keyType.keyLength() / Long.BYTES;
        longsPerEntry = entrySize / Long.BYTES;
        maxEntries = SortKeyEncoder.maxEntries(keyCapBytes, valueCapBytes, keyType);
        maxEntryMemBytes = maxEntries * entrySize;
        if (isVariable && keyHeap == null) {
            keyHeap = newKeyHeap();
        }

        compactionTrigger = limit > 0 && limit < maxEntries
                ? Math.min(Math.max(limit << 1, MIN_COMPACTION_TRIGGER), maxEntries)
                : Long.MAX_VALUE;
        if (compactionTrigger != Long.MAX_VALUE) {
            entryMem.setCapacity(Math.min(compactionTrigger, MAX_PRESIZE_ENTRIES) * longsPerEntry);
        }
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

    /**
     * Final, single-shot sort. For variable keys the native sort rebases each
     * entry's heap offset to an absolute pointer, so a second sort over the
     * same entries would corrupt them; only {@link #compact()} may sort again,
     * as its heap rewrite restores the offsets.
     */
    public void sort() {
        if (count > 1) {
            if (isVariable) {
                final long heapAddr = keyHeap.getAppendOffset() == 0 ? 0 : keyHeap.addressOf(0);
                Vect.sortEncodedVarEntries(entryMem.getAddress(), count, heapAddr, parallelThreshold);
            } else {
                Vect.sortEncodedEntries(entryMem.getAddress(), count, keyLongs, parallelThreshold);
            }
        }
    }

    // memcmp over [aAddr, aAddr+len) vs [bAddr, bAddr+len).
    private static int compareMemory(long aAddr, long bAddr, long len) {
        long i = 0;
        for (; i + 8 <= len; i += 8) {
            final long a = Long.reverseBytes(Unsafe.getLong(aAddr + i));
            final long b = Long.reverseBytes(Unsafe.getLong(bAddr + i));
            if (a != b) {
                return Long.compareUnsigned(a, b);
            }
        }
        for (; i < len; i++) {
            final int a = Unsafe.getByte(aAddr + i) & 0xFF;
            final int b = Unsafe.getByte(bAddr + i) & 0xFF;
            if (a != b) {
                return Integer.compare(a, b);
            }
        }
        return 0;
    }

    /**
     * Copies the boundary entry's key tail into its own buffer: the heap
     * compacts and grows under further appends, so the threshold cannot
     * reference it.
     */
    private void captureThresholdTail() {
        final long tailLen = thresholdEntry[2] - SortKeyEncoder.KEY_PREFIX_BYTES;
        if (tailLen <= 0) {
            return;
        }
        final long needLongs = (tailLen + 7) >> 3;
        if (thresholdTailMem == null) {
            thresholdTailMem = new DirectLongList(Math.max(needLongs, 16), MemoryTag.NATIVE_DEFAULT, false);
        } else if (thresholdTailMem.getCapacity() < needLongs) {
            thresholdTailMem.setCapacity(needLongs);
        }
        Vect.memcpy(
                thresholdTailMem.getAddress(),
                keyHeap.addressOf(thresholdEntry[3] + SortKeyEncoder.KEY_PREFIX_BYTES),
                tailLen
        );
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
        if (isVariable) {
            compactVarHeap();
        }
        final long boundaryAddr = entryMem.getAddress() + (isFirstN ? limit - 1 : 0) * entrySize;
        for (int i = 0; i < longsPerEntry; i++) {
            thresholdEntry[i] = Unsafe.getLong(boundaryAddr + 8L * i);
        }
        if (isVariable) {
            captureThresholdTail();
        }
        hasThreshold = true;
        if (isFirstN) {
            fastRejectKeyThreshold = thresholdEntry[0];
        }
    }

    /**
     * Packs the surviving entries' key bytes into the scratch heap, then bulk
     * copies them back so the encoder's keyHeap reference stays valid. Restores
     * each entry's key field from the absolute pointer the sort left to a fresh
     * heap offset.
     */
    private void compactVarHeap() {
        if (scratchHeap == null) {
            scratchHeap = newKeyHeap();
        }
        scratchHeap.jumpTo(0);
        final long entriesAddr = entryMem.getAddress();
        for (long i = 0; i < count; i++) {
            final long addr = entriesAddr + i * entrySize;
            final long len = Unsafe.getLong(addr + VAR_ENTRY_LEN_OFFSET);
            if (len > SortKeyEncoder.KEY_PREFIX_BYTES) {
                final long srcPtr = Unsafe.getLong(addr + VAR_ENTRY_KEY_OFFSET);
                final long dstOffset = scratchHeap.getAppendOffset();
                Vect.memcpy(scratchHeap.appendAddressFor(len), srcPtr, len);
                Unsafe.putLong(addr + VAR_ENTRY_KEY_OFFSET, dstOffset);
            } else {
                // Keys within the prefix have no heap bytes; the field is never read.
                Unsafe.putLong(addr + VAR_ENTRY_KEY_OFFSET, 0);
            }
        }
        final long packedBytes = scratchHeap.getAppendOffset();
        keyHeap.jumpTo(0);
        if (packedBytes > 0) {
            Vect.memcpy(keyHeap.appendAddressFor(packedBytes), scratchHeap.addressOf(0), packedBytes);
        }
    }

    private boolean isBeyondThreshold(long entryAddr) {
        if (isVariable) {
            return isVarBeyondThreshold(entryAddr);
        }
        // Entries are unique by their trailing rowId word, so word-wise unsigned
        // comparison is a strict total order matching Vect.sortEncodedEntries.
        for (int i = 0; i < longsPerEntry; i++) {
            final int cmp = Long.compareUnsigned(Unsafe.getLong(entryAddr + 8L * i), thresholdEntry[i]);
            if (cmp != 0) {
                return isFirstN ? cmp > 0 : cmp < 0;
            }
        }
        return true;
    }

    // Mirrors encoded_var::operator< in ooo.cpp: prefix words, key tail bytes,
    // length, rowId. The candidate's key field still holds a heap offset.
    private boolean isVarBeyondThreshold(long entryAddr) {
        int cmp = Long.compareUnsigned(Unsafe.getLong(entryAddr), thresholdEntry[0]);
        if (cmp == 0) {
            cmp = Long.compareUnsigned(Unsafe.getLong(entryAddr + 8), thresholdEntry[1]);
        }
        if (cmp == 0) {
            final long len = Unsafe.getLong(entryAddr + VAR_ENTRY_LEN_OFFSET);
            final long thresholdLen = thresholdEntry[2];
            final long minLen = Math.min(len, thresholdLen);
            if (minLen > SortKeyEncoder.KEY_PREFIX_BYTES) {
                final long tailAddr = keyHeap.addressOf(Unsafe.getLong(entryAddr + VAR_ENTRY_KEY_OFFSET))
                        + SortKeyEncoder.KEY_PREFIX_BYTES;
                cmp = compareMemory(tailAddr, thresholdTailMem.getAddress(), minLen - SortKeyEncoder.KEY_PREFIX_BYTES);
            }
            if (cmp == 0) {
                cmp = Long.compare(len, thresholdLen);
            }
            if (cmp == 0) {
                cmp = Long.compareUnsigned(Unsafe.getLong(entryAddr + VAR_ENTRY_ROWID_OFFSET), thresholdEntry[4]);
            }
        }
        if (cmp == 0) {
            return true;
        }
        return isFirstN ? cmp > 0 : cmp < 0;
    }

    // Per-entry copy through the threshold filter: rejected entries cost one
    // leading-word compare and copy no key bytes; endAppend reclaims the heap
    // bytes of entries the full compare rejects.
    private void mergeVarFrom(EncodedTopKBuffer other) {
        final long otherHeapAddr = other.keyHeap.getAppendOffset() == 0 ? 0 : other.keyHeap.addressOf(0);
        for (long addr = other.entryMem.getAddress(), hi = addr + other.count * entrySize; addr < hi; addr += entrySize) {
            if (fastRejectsKey(Unsafe.getLong(addr))) {
                continue;
            }
            final long dst = beginAppend();
            Vect.memcpy(dst, addr, entrySize);
            final long dstOffset = keyHeap.getAppendOffset();
            final long len = Unsafe.getLong(addr + VAR_ENTRY_LEN_OFFSET);
            if (len > SortKeyEncoder.KEY_PREFIX_BYTES) {
                Vect.memcpy(
                        keyHeap.appendAddressFor(len),
                        otherHeapAddr + Unsafe.getLong(addr + VAR_ENTRY_KEY_OFFSET),
                        len
                );
            }
            Unsafe.putLong(dst + VAR_ENTRY_KEY_OFFSET, dstOffset);
            endAppend();
        }
    }

    private MemoryCARW newKeyHeap() {
        final long budget = Math.min(keyCapBytes, SortKeyEncoder.MAX_ENTRY_HEAP_BYTES);
        return new MemoryCARWImpl(
                keyHeapPageSize,
                (int) Math.min(Integer.MAX_VALUE, budget / keyHeapPageSize + 1),
                MemoryTag.NATIVE_DEFAULT,
                PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath()
        );
    }
}
