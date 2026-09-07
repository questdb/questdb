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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.Reopenable;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.Nullable;

/**
 * Off-heap symbol dictionary backing int keys with UTF-16 byte payloads.
 * <p>
 * Storage layout: a flat native byte buffer of back-to-back entries. Each entry
 * is a 4-byte little-endian length prefix followed by {@code 2 * length} bytes
 * of UTF-16 chars (matching QuestDB's on-disk STRING format). A sentinel length
 * of {@code -1} represents a stored null value.
 * <p>
 * The byte buffer grows on demand via {@link Unsafe#realloc}, by half each time
 * or to a caller's stated size. Realloc may move the base pointer, so any
 * {@link DirectString} returned by {@link #valueOf} becomes invalid on the next
 * mutating call ({@link #put}, {@link #intern}, {@link #clear}, {@link #close}).
 * <p>
 * Two insertion modes are supported and must not be mixed on the same instance:
 * <ul>
 *   <li>Externally-keyed: {@link #put(int, CharSequence)} accepts caller-assigned
 *       ints. Used when keys come from WAL symbol diffs.</li>
 *   <li>Self-assigned: {@link #intern(CharSequence)} returns an existing key if
 *       the value has been seen, otherwise assigns the next sequential key. Used
 *       as a drop-in replacement for {@code ObjList&lt;String&gt;.indexOf}-based
 *       symbol tables.</li>
 * </ul>
 * Both modes are fully off-heap. {@link #intern} maintains a {@link ValueToKeyMap};
 * bounded lookup on externally assigned keys lazily builds an
 * independent {@link ValueToKeyMap}. Both slot tables store only
 * {@code (hash, symbolKey)} pairs and defer byte-wise equality checks to the
 * primary buffer, on a hash match only - no heap-side copy of the symbol string
 * is kept. Intern-mode keys are dense, so their forward half is a flat offset
 * array indexed by key; put-mode keys go through a hash map.
 */
public class DirectSymbolMap implements Mutable, QuietCloseable, Reopenable {
    private static final int BULK_REVERSE_INDEX_DIRECT_INSERT_THRESHOLD = 100_000;
    private static final int NO_ENTRY_KEY = Integer.MIN_VALUE;
    private static final int NO_ENTRY_VALUE = -1;

    private final long initialBufCapacity;
    private final int initialMapCapacity;
    private final DirectIntIntHashMap keyToOffset;
    private ValueToKeyMap explicitValueToKey;
    private final int memoryTag;
    private final DirectString reusableView = new DirectString();
    // Per-workload native memory tracker, bound by the owner at workload start. Null when
    // only global accounting applies, in which case every Unsafe.{malloc,realloc,free} call
    // below degrades to the global-only overload.
    private @Nullable MemoryTracker memoryTracker;
    private long bufCapacity;
    private long bufPtr;
    private long bufSize;
    // Intern mode keys are dense [0, size), so the forward key -> offset half is a flat
    // int array indexed by key rather than a hash table: one sequential write per intern,
    // one direct read per valueOf, and 4 bytes per entry instead of 16.
    private int denseOffsetsCapacity;
    private long denseOffsetsPtr;
    private int denseSize;
    private long bulkReverseIndexSortedEntries;
    // (hash, key) pairs appended by appendForRestore and not yet in the reverse index;
    // buildReverseIndex inserts medium dictionaries directly and uses a native radix sort
    // to fill larger ones sequentially. Kept closed between loads so a dictionary that
    // never bulk-loads holds no scratch.
    private final DirectLongList pendingReverse;
    private long explicitRebuildScannedEntries;
    private boolean explicitValueToKeyDirty;
    private ValueToKeyMap valueToKey;

    public DirectSymbolMap(long initialBufCapacity, int initialMapCapacity, int memoryTag) {
        this.initialBufCapacity = Math.max(64L, initialBufCapacity);
        this.initialMapCapacity = Math.max(8, initialMapCapacity);
        this.memoryTag = memoryTag;
        this.bufPtr = Unsafe.malloc(this.initialBufCapacity, memoryTag, memoryTracker);
        this.bufCapacity = this.initialBufCapacity;
        this.bufSize = 0L;
        this.pendingReverse = new DirectLongList(2L * this.initialMapCapacity, memoryTag, true);
        try {
            this.keyToOffset = new DirectIntIntHashMap(
                    this.initialMapCapacity,
                    0.5,
                    NO_ENTRY_KEY,
                    NO_ENTRY_VALUE,
                    memoryTag
            );
        } catch (Throwable th) {
            // The keyToOffset allocation can throw (OOM) after the primary buffer
            // was malloc'd. A ctor that throws leaves no reachable instance to
            // close(), so free the buffer here or bufPtr leaks.
            Unsafe.free(bufPtr, bufCapacity, memoryTag, memoryTracker);
            bufPtr = 0;
            bufCapacity = 0;
            throw th;
        }
    }

    @Override
    public void clear() {
        keyToOffset.clear();
        if (explicitValueToKey != null) {
            explicitValueToKey.clear();
        }
        explicitValueToKeyDirty = false;
        if (valueToKey != null) {
            valueToKey.clear();
        }
        denseSize = 0;
        pendingReverse.close();
        bufSize = 0L;
    }

    @Override
    public void close() {
        keyToOffset.close();
        if (denseOffsetsPtr != 0) {
            Unsafe.free(denseOffsetsPtr, 4L * denseOffsetsCapacity, memoryTag, memoryTracker);
            denseOffsetsPtr = 0;
            denseOffsetsCapacity = 0;
            denseSize = 0;
        }
        if (explicitValueToKey != null) {
            explicitValueToKey.close();
            explicitValueToKey = null;
        }
        if (valueToKey != null) {
            valueToKey.close();
            valueToKey = null;
        }
        pendingReverse.close();
        if (bufPtr != 0) {
            Unsafe.free(bufPtr, bufCapacity, memoryTag, memoryTracker);
            bufPtr = 0;
            bufCapacity = 0;
            bufSize = 0;
        }
        // The blocks are gone, so the tracker that charged them carries no debt for this map.
        // Dropping the reference keeps a later free - one that runs after a pooled tracker was
        // recycled by another workload - on the global counter, where it cannot corrupt someone
        // else's total. See DirectIntIntHashMap.close(), which does the same for its directory.
        memoryTracker = null;
    }

    /**
     * Copies every (key, value) entry from {@code source} into this map. Prior
     * contents are discarded. Expects source keys to be dense {@code [0, size)},
     * matching {@link #intern} output. Preserves source's key assignment.
     */
    public void copyFrom(DirectSymbolMap source) {
        clear();
        int n = source.size();
        if (source.valueToKey != null) {
            // Re-intern so the reverse index is rebuilt natively with the correct
            // (offsetInBuf, symbolKey) pairs pointing at this instance's buffer.
            ensureValueToKeyMap();
            for (int key = 0; key < n; key++) {
                intern(source.valueOf(key));
            }
        } else {
            for (int key = 0; key < n; key++) {
                put(key, source.valueOf(key));
            }
        }
    }

    /**
     * Cumulative number of {@code keyToOffset} slots scanned by full rebuilds of the
     * explicit (put-mode) reverse index over this instance's lifetime. Diagnostic hook
     * for tests: a bounded {@link #keyOf(CharSequence, int, int)} that no longer triggers
     * a rebuild leaves this unchanged, so interleaved put/lookup work stays linear.
     */
    public long getExplicitRebuildScannedEntries() {
        return explicitRebuildScannedEntries;
    }

    /**
     * Cumulative number of bulk-restore entries sent through the native radix-sort path.
     * Diagnostic hook for tests: medium dictionaries insert directly, while larger ones
     * retain the cache-friendly sorted fill.
     */
    public long getBulkReverseIndexSortedEntries() {
        return bulkReverseIndexSortedEntries;
    }

    /**
     * Bytes the forward {@code key -> value} half currently holds: the payload buffer
     * plus the key-to-offset directory. This is the half a durable format has to write
     * out; {@link #getReverseMemoryBytes()} is the accelerator that can be rebuilt.
     */
    public long getForwardMemoryBytes() {
        return bufCapacity + 8L * keyToOffset.capacity() + 4L * denseOffsetsCapacity;
    }

    /**
     * Pre-sizes the intern-mode structures for {@code entryCount} entries carrying
     * {@code payloadBytes} of buffer in total, so a bulk load - a checkpoint restore that
     * knows both up front - pays one allocation per structure instead of a doubling cascade
     * of reallocs and reverse-index rehashes.
     */
    public void ensureCapacity(int entryCount, long payloadBytes) {
        // Exact rather than rounded: a restore states the size once and never grows past
        // it, so a power-of-two round-up would be a third of the buffer left unused.
        if (payloadBytes > bufCapacity) {
            growBuffer(payloadBytes);
        }
        if (entryCount > denseOffsetsCapacity) {
            growDense(entryCount);
        }
        ensureValueToKeyMap();
        valueToKey.ensureCapacity(entryCount);
    }

    /**
     * Bytes the reverse {@code value -> key} indexes currently hold. Zero until a lookup
     * builds one, which is why it is reported apart from the forward half.
     */
    public long getReverseMemoryBytes() {
        long bytes = 0;
        if (valueToKey != null) {
            bytes += (long) ValueToKeyMap.SLOT_BYTES * valueToKey.capacity();
        }
        if (explicitValueToKey != null) {
            bytes += (long) ValueToKeyMap.SLOT_BYTES * explicitValueToKey.capacity();
        }
        return bytes;
    }

    /**
     * Returns an existing key if {@code value} has already been interned on this
     * map, otherwise appends a new entry with a sequentially assigned key and
     * returns it. Must not be mixed with {@link #put(int, CharSequence)} on the
     * same instance. Does not accept {@code null} - callers that must store a
     * null value use {@link #put(int, CharSequence)}.
     */
    public int intern(CharSequence value) {
        // The reverse index cannot key a null (hashOf/matches assume a
        // non-negative length); reject to fail with a clear message, not an NPE.
        if (value == null) {
            throw CairoException.nonCritical().put("DirectSymbolMap.intern does not accept null values; use put()");
        }
        ensureValueToKeyMap();
        if (pendingReverse != null && pendingReverse.size() > 0) {
            // A bulk load appended entries the reverse index does not hold yet; probing
            // now would miss them and hand the same string a second id.
            throw CairoException.critical(0).put("DirectSymbolMap reverse index is pending a bulk build; call buildReverseIndex() first");
        }
        final int h = hashOf(value);
        long idx = valueToKey.keyIndex(value, h);
        if (idx < 0) {
            return valueToKey.valueAt(idx);
        }
        final int key = appendDense(value);
        valueToKey.insertAt(idx, h, key);
        return key;
    }

    /**
     * Appends the UTF-8 bytes at {@code [addr, addr + len)} under the next sequential key,
     * decoding them strictly into the UTF-16 the buffer holds and hashing the chars in the
     * same pass, WITHOUT probing or updating the reverse index. This is the bulk-load path
     * of a checkpoint restore: {@link #buildReverseIndex} then indexes everything appended
     * this way in one sequential pass and reports a duplicate if there was one, and
     * interning is refused until that pass has run.
     * <p>
     * Strict means what a conforming decoder refuses: a truncated or invalid continuation
     * byte, an overlong encoding, a UTF-8-encoded surrogate, or a code point outside the
     * Unicode range. A malformed input leaves the map untouched.
     *
     * @return the key assigned, or -1 when the bytes are not valid UTF-8
     */
    public int appendUtf8ForRestore(long addr, int len) {
        ensureValueToKeyMap();
        // Every UTF-8 byte produces at most one UTF-16 code unit, so twice the input is
        // enough for the chars, plus the length prefix.
        ensureCapacity(bufSize + Integer.BYTES + ((long) len << 1));
        final long base = bufPtr + bufSize;
        final long charsBase = base + Integer.BYTES;
        int out = 0;
        int h = 0;
        int i = 0;
        while (i < len) {
            final int b0 = Unsafe.getUnsafe().getByte(addr + i) & 0xff;
            final char c;
            if (b0 < 0x80) {
                c = (char) b0;
                i++;
            } else if ((b0 & 0xe0) == 0xc0) {
                final int cp = decodeUtf8Continuation(addr, len, i, 1, b0 & 0x1f);
                if (cp < 0x80) {
                    return -1;
                }
                c = (char) cp;
                i += 2;
            } else if ((b0 & 0xf0) == 0xe0) {
                final int cp = decodeUtf8Continuation(addr, len, i, 2, b0 & 0x0f);
                if (cp < 0x800 || (cp >= 0xd800 && cp <= 0xdfff)) {
                    return -1;
                }
                c = (char) cp;
                i += 3;
            } else if ((b0 & 0xf8) == 0xf0) {
                final int cp = decodeUtf8Continuation(addr, len, i, 3, b0 & 0x07);
                if (cp < 0x10000 || cp > 0x10ffff) {
                    return -1;
                }
                final int adjusted = cp - 0x10000;
                final char high = (char) (0xd800 + (adjusted >> 10));
                Unsafe.getUnsafe().putChar(charsBase + ((long) out++ << 1), high);
                h = 31 * h + high;
                c = (char) (0xdc00 + (adjusted & 0x3ff));
                i += 4;
            } else {
                return -1;
            }
            Unsafe.getUnsafe().putChar(charsBase + ((long) out++ << 1), c);
            h = 31 * h + c;
        }
        Unsafe.getUnsafe().putInt(base, out);
        final int key = denseSize;
        ensureDenseCapacity(key + 1);
        Unsafe.getUnsafe().putInt(denseOffsetsPtr + 4L * key, toIntOffset(bufSize));
        denseSize = key + 1;
        bufSize += Integer.BYTES + ((long) out << 1);
        // The list is kept closed between loads, and a closed list cannot grow through add.
        pendingReverse.ensureCapacity(2);
        pendingReverse.add(h);
        pendingReverse.add(key);
        return key;
    }

    /**
     * Indexes every entry {@link #appendUtf8ForRestore} appended. Medium dictionaries insert
     * directly from their already decoded and hashed pending pairs. Larger dictionaries
     * compute each entry's home slot, radix-sort the pairs by it, and fill the table in slot
     * order so the writes walk the table sequentially. Both paths compare hashes and, on a
     * hash match, strings, which is the same duplicate check {@link #intern} makes.
     *
     * @return the key of the first duplicate string found, or -1 when every entry is distinct
     */
    public int buildReverseIndex() {
        final int n = (int) (pendingReverse.size() / 2);
        if (n == 0) {
            return -1;
        }
        try {
            ensureValueToKeyMap();
            valueToKey.ensureCapacity(valueToKey.size() + n);
            if (n <= BULK_REVERSE_INDEX_DIRECT_INSERT_THRESHOLD) {
                for (int i = 0; i < n; i++) {
                    final int h = (int) pendingReverse.get(2L * i);
                    final int key = (int) pendingReverse.get(2L * i + 1);
                    if (valueToKey.insertStored(h, key)) {
                        return key;
                    }
                }
                return -1;
            }

            bulkReverseIndexSortedEntries += n;
            // Rewrite each (hash, key) pair in place as (homeSlot, hash | key): the native sort
            // orders by the first long and carries the second through untouched. It also needs
            // a copy region as large as the pairs behind them.
            pendingReverse.setCapacity(4L * n);
            for (int i = 0; i < n; i++) {
                final int h = (int) pendingReverse.get(2L * i);
                final int key = (int) pendingReverse.get(2L * i + 1);
                pendingReverse.set(2L * i, valueToKey.homeSlot(h));
                pendingReverse.set(2L * i + 1, ((long) h << 32) | (key & 0xffffffffL));
            }
            final long address = pendingReverse.getAddress();
            Vect.radixSortLongIndexAscChecked(address, n, address + 2L * n * Long.BYTES, 0, valueToKey.capacity() - 1L);
            return valueToKey.fillSorted(pendingReverse, n);
        } finally {
            pendingReverse.close();
        }
    }

    /**
     * Returns the key previously assigned via {@link #intern} to a value equal
     * to {@code value}, or {@code -1} if no such key exists. Returns {@code -1}
     * for put-only maps (no reverse index is maintained).
     */
    public int keyOf(CharSequence value) {
        if (valueToKey == null) {
            return -1;
        }
        long idx = valueToKey.keyIndex(value, hashOf(value));
        return idx < 0 ? valueToKey.valueAt(idx) : -1;
    }

    /**
     * Returns an externally assigned key in {@code [loInclusive, hiExclusive)}
     * whose value equals {@code value}, or {@code -1} when no such key exists. The
     * reverse index is built lazily from the current explicit mappings and retains
     * duplicate values so bounds can select the correct caller-assigned key.
     */
    public int keyOf(CharSequence value, int loInclusive, int hiExclusive) {
        if (value == null || loInclusive >= hiExclusive) {
            return -1;
        }
        buildExplicitValueToKeyIfNeeded();
        return explicitValueToKey.keyOf(value, hashOf(value), loInclusive, hiExclusive);
    }

    /**
     * Associates {@code value} with the caller-supplied {@code key}. If the key
     * is already present its value is overwritten. The previous bytes stay in
     * the buffer until the next {@link #clear} or {@link #close}. Must not be
     * mixed with {@link #intern} on the same instance.
     */
    public void put(int key, CharSequence value) {
        long idx = keyToOffset.keyIndex(key);
        // keyIndex >= 0 marks an empty slot (a brand-new key); < 0 marks an existing key.
        final boolean isNewKey = idx >= 0;
        long offset = append(value);
        // The reverse index can be extended in O(1) only when it is already built, clean, and this
        // is a brand-new key. Overwriting a key strands its previous (oldOffset, key) entry, and
        // only a full rebuild can drop it; overwrites do not occur on the WAL symbol-diff hot path
        // (segment keys are immutable), so that fallback stays cold.
        final boolean canExtendIndex = isNewKey && explicitValueToKey != null && !explicitValueToKeyDirty;
        // Mark the index stale BEFORE the mutations that can throw. A put that fails part-way must
        // not leave the index both missing the entry and marked clean, or
        // buildExplicitValueToKeyIfNeeded() short-circuits and keyOf() reports VALUE_NOT_FOUND for
        // that symbol for the rest of the map's life.
        explicitValueToKeyDirty = true;
        keyToOffset.putAt(idx, key, toIntOffset(offset));
        if (canExtendIndex) {
            // A brand-new key adds exactly one (offset, key) pair. This keeps a WAL replay (put
            // then bounded keyOf per transaction) linear instead of rebuilding on every lookup.
            // Null symbols are deliberately absent, matching the rebuild's len >= 0 guard.
            if (value != null) {
                explicitValueToKey.insertExplicit(hashOfStored(toIntOffset(offset)), key);
            }
            explicitValueToKeyDirty = false;
        }
    }

    @Override
    public void reopen() {
        if (bufPtr == 0) {
            bufPtr = Unsafe.malloc(initialBufCapacity, memoryTag, memoryTracker);
            bufCapacity = initialBufCapacity;
            bufSize = 0;
        }
        keyToOffset.reopen();
        if (explicitValueToKey != null) {
            explicitValueToKey.reopen();
        }
        if (valueToKey != null) {
            valueToKey.reopen();
        }
    }

    /**
     * Binds the per-workload {@link MemoryTracker} every subsequent allocation charges. A
     * {@code null} tracker degrades the map to global-only accounting.
     * <p>
     * Rebinding releases the live blocks first: a block has to be freed under the tracker
     * that charged it, or the two counters drift apart and the per-workload limit stops
     * holding. Rebinding therefore also DISCARDS the map's contents, so callers bind at
     * workload start, while the map is still empty.
     */
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        if (tracker == memoryTracker) {
            return;
        }
        close();
        memoryTracker = tracker;
        // close() dropped the directory's own reference along with its block, so re-bind it
        // before reopen() allocates again - otherwise the directory charges the global
        // counter while this buffer charges the tracker, and the two never agree.
        keyToOffset.setMemoryTracker(tracker);
        pendingReverse.setMemoryTracker(tracker);
        reopen();
    }

    public int size() {
        return valueToKey != null ? denseSize : keyToOffset.size();
    }

    /**
     * Returns a reusable {@link DirectString} view over the bytes stored for
     * {@code key}, or {@code null} if the key is absent or its stored value is
     * null. The returned view is owned by this map and is invalidated by the
     * next mutating call; callers that must retain the value should copy it.
     */
    public CharSequence valueOf(int key) {
        return valueOf(key, reusableView);
    }

    /**
     * Like {@link #valueOf(int)} but binds the result into the caller-supplied
     * {@code view}. Lets callers that need multiple live views at once (e.g.
     * an A/B record pair) avoid aliasing on this map's shared view.
     */
    public CharSequence valueOf(int key, DirectString view) {
        final int offset;
        if (valueToKey != null) {
            if (key < 0 || key >= denseSize) {
                return null;
            }
            offset = Unsafe.getUnsafe().getInt(denseOffsetsPtr + 4L * key);
        } else {
            offset = keyToOffset.get(key);
            if (offset == NO_ENTRY_VALUE) {
                return null;
            }
        }
        int len = Unsafe.getUnsafe().getInt(bufPtr + offset);
        if (len < 0) {
            return null;
        }
        return view.of(bufPtr + offset + Integer.BYTES, len);
    }

    private long append(CharSequence value) {
        if (value == null) {
            ensureCapacity(bufSize + Integer.BYTES);
            long offset = bufSize;
            Unsafe.getUnsafe().putInt(bufPtr + offset, -1);
            bufSize += Integer.BYTES;
            return offset;
        }
        int len = value.length();
        long required = (long) Integer.BYTES + ((long) len << 1);
        ensureCapacity(bufSize + required);
        long offset = bufSize;
        long base = bufPtr + offset;
        Unsafe.getUnsafe().putInt(base, len);
        long charsBase = base + Integer.BYTES;
        if (value instanceof DirectString ds) {
            Vect.memcpy(charsBase, ds.ptr(), (long) len << 1);
        } else {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(charsBase + ((long) i << 1), value.charAt(i));
            }
        }
        bufSize += required;
        return offset;
    }

    /**
     * Folds {@code continuationCount} trailing bytes of a multi-byte sequence starting at
     * {@code i} into {@code leadBits}, the lead byte's payload bits. Every continuation byte
     * must carry the {@code 10} high bits; anything else, including running past
     * {@code len}, is malformed and returns -1, which every caller's range check rejects.
     */
    private static int decodeUtf8Continuation(long addr, int len, int i, int continuationCount, int leadBits) {
        if (i + continuationCount >= len) {
            return -1;
        }
        int codePoint = leadBits;
        for (int k = 1; k <= continuationCount; k++) {
            final int b = Unsafe.getUnsafe().getByte(addr + i + k) & 0xff;
            if ((b & 0xc0) != 0x80) {
                return -1;
            }
            codePoint = (codePoint << 6) | (b & 0x3f);
        }
        return codePoint;
    }

    /**
     * Appends {@code value} under the next dense key and records its offset; the reverse
     * index is the caller's business.
     */
    private int appendDense(CharSequence value) {
        final int key = denseSize;
        ensureDenseCapacity(key + 1);
        final long offset = append(value);
        Unsafe.getUnsafe().putInt(denseOffsetsPtr + 4L * key, toIntOffset(offset));
        denseSize = key + 1;
        return key;
    }

    /**
     * The {@code 31 * h + c} hash of {@code value}, read straight off native memory for a
     * {@link DirectString} and off the cached field for a {@link String}. Every reverse
     * index slot stores this value, so it has to agree with {@link #hashOfStored}.
     */
    private static int hashOf(CharSequence value) {
        if (value instanceof String || value instanceof DirectString) {
            return value.hashCode();
        }
        return Chars.hashCode(value);
    }

    private int hashOfStored(int offset) {
        final long addr = bufPtr + offset;
        final int len = Unsafe.getUnsafe().getInt(addr);
        if (len <= 0) {
            return 0;
        }
        final long charsBase = addr + Integer.BYTES;
        int h = 0;
        for (int i = 0; i < len; i++) {
            h = 31 * h + Unsafe.getUnsafe().getChar(charsBase + ((long) i << 1));
        }
        return h;
    }

    private boolean matches(int offset, CharSequence value) {
        final long addr = bufPtr + offset;
        final int storedLen = Unsafe.getUnsafe().getInt(addr);
        final int valueLen = value.length();
        if (storedLen != valueLen) {
            return false;
        }
        final long charsBase = addr + Integer.BYTES;
        if (value instanceof DirectString ds) {
            return Vect.memeq(charsBase, ds.ptr(), (long) valueLen << 1);
        }
        for (int i = 0; i < valueLen; i++) {
            if (Unsafe.getUnsafe().getChar(charsBase + ((long) i << 1)) != value.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesStored(int leftOffset, int rightOffset) {
        final long left = bufPtr + leftOffset;
        final long right = bufPtr + rightOffset;
        final int len = Unsafe.getUnsafe().getInt(left);
        if (len != Unsafe.getUnsafe().getInt(right)) {
            return false;
        }
        return len <= 0 || Vect.memeq(left + Integer.BYTES, right + Integer.BYTES, (long) len << 1);
    }

    /**
     * The buffer offset of {@code key}'s entry: a direct read in intern mode, a hash lookup
     * in put mode. The reverse index stores keys rather than offsets and resolves through
     * here on a hash match only.
     */
    private int offsetOf(int key) {
        if (valueToKey != null) {
            return Unsafe.getUnsafe().getInt(denseOffsetsPtr + 4L * key);
        }
        return keyToOffset.get(key);
    }

    private void buildExplicitValueToKeyIfNeeded() {
        if (explicitValueToKey == null) {
            explicitValueToKey = new ValueToKeyMap(initialMapCapacity, memoryTag);
        }
        if (!explicitValueToKeyDirty) {
            return;
        }

        explicitValueToKey.clear();
        final int n = keyToOffset.capacity();
        // Count the slots this full rebuild scans. put() keeps the built index up to date
        // incrementally, so a steady WAL replay (put then bounded keyOf per transaction)
        // rebuilds at most once instead of on every lookup; the counter lets a test prove
        // the total scan work stays linear rather than quadratic in the transaction count.
        explicitRebuildScannedEntries += n;
        for (int i = 0; i < n; i++) {
            final int key = keyToOffset.keyAt(i);
            if (key != NO_ENTRY_KEY) {
                final int offset = keyToOffset.get(key);
                // Null symbols have no string key and are deliberately absent.
                if (Unsafe.getUnsafe().getInt(bufPtr + offset) >= 0) {
                    explicitValueToKey.insertExplicit(hashOfStored(offset), key);
                }
            }
        }
        explicitValueToKeyDirty = false;
    }

    private void ensureCapacity(long required) {
        if (required <= bufCapacity) {
            return;
        }
        // Grow by half rather than doubling: the buffer holds the view's whole key domain
        // for its life, so the overshoot is resident, and a third beats a half.
        growBuffer(Math.max(required, bufCapacity + (bufCapacity >> 1)));
    }

    private void growBuffer(long newCap) {
        if (newCap > Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put("direct symbol map exceeds 2GiB");
        }
        bufPtr = Unsafe.realloc(bufPtr, bufCapacity, newCap, memoryTag, memoryTracker);
        bufCapacity = newCap;
    }

    private void ensureDenseCapacity(int required) {
        if (required <= denseOffsetsCapacity) {
            return;
        }
        final long grown = Math.max(denseOffsetsCapacity + (denseOffsetsCapacity >> 1), initialMapCapacity);
        growDense((int) Math.min(Integer.MAX_VALUE, Math.max(required, grown)));
    }

    private void growDense(int newCap) {
        if (newCap <= 0) {
            throw CairoException.nonCritical().put("direct symbol map key capacity overflow");
        }
        if (denseOffsetsPtr == 0) {
            denseOffsetsPtr = Unsafe.malloc(4L * newCap, memoryTag, memoryTracker);
        } else {
            denseOffsetsPtr = Unsafe.realloc(denseOffsetsPtr, 4L * denseOffsetsCapacity, 4L * newCap, memoryTag, memoryTracker);
        }
        denseOffsetsCapacity = newCap;
    }

    private void ensureValueToKeyMap() {
        if (valueToKey == null) {
            valueToKey = new ValueToKeyMap(initialMapCapacity, memoryTag);
        }
    }

    private static int toIntOffset(long offset) {
        assert offset >= 0 && offset <= Integer.MAX_VALUE;
        return (int) offset;
    }

    /**
     * Nested off-heap open-addressed hash table from string content to symbol key. Each
     * 8-byte slot holds {@code (hash: int, symbolKey: int)} - the string's
     * {@code 31 * h + c} hash and the key it carries - and an empty slot reads
     * {@code symbolKey == -1}. A slot never points at its string: a probe compares hashes
     * and dereferences the primary buffer through {@link DirectSymbolMap#offsetOf} only on
     * a hash match, so passing an occupied slot costs one 8-byte read, a rehash re-places
     * every slot by the hash it already holds without walking a string, and no heap copy
     * of the key is kept.
     * <p>
     * The home slot is the top {@code log2(capacity)} bits of an avalanche of the string
     * hash. The polynomial hash of sequential strings - {@code acct-1000000},
     * {@code acct-1000001}, ... - differs by exactly the last character's difference, so
     * masking it straight into a linear-probing table lands whole runs of keys on adjacent
     * slots and turns every insert into a walk to the end of the run; the avalanche spreads
     * them, and taking the top bits makes home slots monotonic in the avalanched value,
     * which is what lets {@link #fillSorted} fill the table sequentially.
     */
    private class ValueToKeyMap implements QuietCloseable {
        static final int SLOT_BYTES = 8;
        private static final int EMPTY_KEY = -1;
        private static final double LOAD_FACTOR = 0.5;
        private final int initialCapacity;
        private final int memoryTagLocal;
        private int capacity;
        private int free;
        private long mask;
        private int shift;
        private int size;
        private long slotsPtr;

        ValueToKeyMap(int initialLogicalCapacity, int memoryTag) {
            // Round the underlying slot array up so {@code capacity * LOAD_FACTOR}
            // seats at least {@code initialLogicalCapacity} entries before the first
            // rehash.
            this.initialCapacity = Numbers.ceilPow2((int) Math.max(8, initialLogicalCapacity / LOAD_FACTOR));
            this.capacity = this.initialCapacity;
            this.mask = capacity - 1L;
            this.shift = Long.SIZE - Numbers.msb(capacity);
            this.free = (int) (capacity * LOAD_FACTOR);
            this.memoryTagLocal = memoryTag;
            this.slotsPtr = Unsafe.malloc((long) SLOT_BYTES * capacity, memoryTag, memoryTracker);
            zero(slotsPtr, capacity);
        }

        public int capacity() {
            return capacity;
        }

        public void clear() {
            zero(slotsPtr, capacity);
            free = (int) (capacity * LOAD_FACTOR);
            size = 0;
        }

        @Override
        public void close() {
            if (slotsPtr != 0) {
                Unsafe.free(slotsPtr, (long) SLOT_BYTES * capacity, memoryTagLocal, memoryTracker);
                slotsPtr = 0;
                capacity = 0;
                free = 0;
                size = 0;
            }
        }

        /**
         * Grows the slot table so it seats {@code entries} at {@link #LOAD_FACTOR} without
         * a rehash; a no-op when it already does.
         */
        public void ensureCapacity(int entries) {
            final long wanted = Numbers.ceilPow2(Math.max(8L, (long) Math.ceil(entries / LOAD_FACTOR)));
            if (wanted > Integer.MAX_VALUE) {
                throw CairoException.nonCritical().put("direct symbol map reverse index capacity overflow");
            }
            if (wanted > capacity) {
                rehash((int) wanted);
            }
        }

        /**
         * Fills the table from {@code n} pairs of {@code (homeSlot, hash << 32 | key)} sorted
         * by home slot. Each pair probes forward from its home slot exactly as
         * {@link #insertExplicit} would, so the writes advance through the table in order.
         *
         * @return the key of the first pair whose string another slot already holds, or -1
         */
        public int fillSorted(DirectLongList pairs, int n) {
            for (int i = 0; i < n; i++) {
                long index = pairs.get(2L * i);
                final long packed = pairs.get(2L * i + 1);
                final int h = (int) (packed >>> 32);
                final int key = (int) packed;
                while (true) {
                    final long p = slotsPtr + (index << 3);
                    final int k = Unsafe.getUnsafe().getInt(p + 4);
                    if (k == EMPTY_KEY) {
                        break;
                    }
                    if (Unsafe.getUnsafe().getInt(p) == h && matchesStored(offsetOf(k), offsetOf(key))) {
                        return key;
                    }
                    index = (index + 1) & mask;
                }
                insertAt(index, h, key);
            }
            return -1;
        }

        /**
         * The slot a hash probes from: the top bits of its avalanche.
         */
        public long homeSlot(int h) {
            return Hash.hashInt64(h) >>> shift;
        }

        /**
         * Writes a new (hash, symbolKey) entry at the slot previously returned by
         * {@link #keyIndex}. Caller must pass the non-negative empty-slot index -
         * collisions with a matching entry are not handled here; that case is the
         * caller's responsibility (see {@link DirectSymbolMap#intern}).
         */
        public void insertAt(long idx, int h, int symbolKey) {
            long p = slotsPtr + (idx << 3);
            Unsafe.getUnsafe().putInt(p, h);
            Unsafe.getUnsafe().putInt(p + 4, symbolKey);
            size++;
            if (--free == 0) {
                try {
                    rehash();
                } catch (CairoException e) {
                    // Restore the grow trigger. rehash() throws before assigning, so leaving free
                    // at 0 lets it drift negative on later inserts, the table never grows again,
                    // and every probe loop spins forever once the slots fill up.
                    free = 1;
                    throw e;
                }
            }
        }

        /**
         * Inserts an externally keyed mapping without deduplicating equal values.
         * Multiple explicit keys may carry the same content and bounded lookup must
         * retain all of them.
         */
        public void insertExplicit(int h, int symbolKey) {
            long index = homeSlot(h);
            while (Unsafe.getUnsafe().getInt(slotsPtr + (index << 3) + 4) != EMPTY_KEY) {
                index = (index + 1) & mask;
            }
            insertAt(index, h, symbolKey);
        }

        /**
         * Inserts a pending bulk-restore entry by probing from its home slot.
         *
         * @return true if the reverse index already holds the same string
         */
        public boolean insertStored(int h, int symbolKey) {
            long index = homeSlot(h);
            while (true) {
                final long p = slotsPtr + (index << 3);
                final int key = Unsafe.getUnsafe().getInt(p + 4);
                if (key == EMPTY_KEY) {
                    insertAt(index, h, symbolKey);
                    return false;
                }
                if (Unsafe.getUnsafe().getInt(p) == h && matchesStored(offsetOf(key), offsetOf(symbolKey))) {
                    return true;
                }
                index = (index + 1) & mask;
            }
        }

        /**
         * Returns a matching explicit key in {@code [loInclusive, hiExclusive)},
         * or {@code -1}. The probe continues across equal values outside the band.
         */
        public int keyOf(CharSequence value, int h, int loInclusive, int hiExclusive) {
            long index = homeSlot(h);
            while (true) {
                final long p = slotsPtr + (index << 3);
                final int symbolKey = Unsafe.getUnsafe().getInt(p + 4);
                if (symbolKey == EMPTY_KEY) {
                    return -1;
                }
                if (symbolKey >= loInclusive && symbolKey < hiExclusive
                        && Unsafe.getUnsafe().getInt(p) == h
                        && matches(offsetOf(symbolKey), value)) {
                    return symbolKey;
                }
                index = (index + 1) & mask;
            }
        }

        /**
         * Probes for {@code value}, whose hash is {@code h}. Returns {@code -idx - 1} if an
         * entry with matching content is found at slot {@code idx}; returns a non-negative
         * empty-slot {@code idx} otherwise.
         */
        public long keyIndex(CharSequence value, int h) {
            long index = homeSlot(h);
            while (true) {
                final long p = slotsPtr + (index << 3);
                final int symbolKey = Unsafe.getUnsafe().getInt(p + 4);
                if (symbolKey == EMPTY_KEY) {
                    return index;
                }
                if (Unsafe.getUnsafe().getInt(p) == h && matches(offsetOf(symbolKey), value)) {
                    return -index - 1;
                }
                index = (index + 1) & mask;
            }
        }

        public void reopen() {
            if (slotsPtr == 0) {
                capacity = initialCapacity;
                mask = capacity - 1L;
                shift = Long.SIZE - Numbers.msb(capacity);
                free = (int) (capacity * LOAD_FACTOR);
                size = 0;
                slotsPtr = Unsafe.malloc((long) SLOT_BYTES * capacity, memoryTagLocal, memoryTracker);
                zero(slotsPtr, capacity);
            }
        }

        public int size() {
            return size;
        }

        /**
         * Returns the symbolKey stored at the slot referenced by a negative
         * {@code idx} from {@link #keyIndex}.
         */
        public int valueAt(long idx) {
            return Unsafe.getUnsafe().getInt(slotsPtr + ((-idx - 1) << 3) + 4);
        }

        private void rehash() {
            final int newCapacity = capacity << 1;
            if (newCapacity < 0) {
                throw CairoException.nonCritical().put("direct symbol map reverse index capacity overflow");
            }
            rehash(newCapacity);
        }

        private void rehash(int newCapacity) {
            final int oldCapacity = capacity;
            final long oldSlotsPtr = slotsPtr;
            final long newSlotsPtr = Unsafe.malloc((long) SLOT_BYTES * newCapacity, memoryTagLocal, memoryTracker);
            zero(newSlotsPtr, newCapacity);
            final long newMask = newCapacity - 1L;
            final int newShift = Long.SIZE - Numbers.msb(newCapacity);

            for (int i = 0; i < oldCapacity; i++) {
                final long src = oldSlotsPtr + ((long) i << 3);
                final int symbolKey = Unsafe.getUnsafe().getInt(src + 4);
                if (symbolKey == EMPTY_KEY) {
                    continue;
                }
                final int h = Unsafe.getUnsafe().getInt(src);
                long index = Hash.hashInt64(h) >>> newShift;
                while (Unsafe.getUnsafe().getInt(newSlotsPtr + (index << 3) + 4) != EMPTY_KEY) {
                    index = (index + 1) & newMask;
                }
                final long dst = newSlotsPtr + (index << 3);
                Unsafe.getUnsafe().putInt(dst, h);
                Unsafe.getUnsafe().putInt(dst + 4, symbolKey);
            }

            Unsafe.free(oldSlotsPtr, (long) SLOT_BYTES * oldCapacity, memoryTagLocal, memoryTracker);
            slotsPtr = newSlotsPtr;
            capacity = newCapacity;
            mask = newMask;
            shift = newShift;
            free += (int) ((newCapacity - oldCapacity) * LOAD_FACTOR);
        }

        private void zero(long ptr, int cap) {
            // Set all bytes to 0xFF so each slot's key field reads as -1 (EMPTY_KEY). The
            // hash field also reads 0xFFFFFFFF, but it is only consulted when the key is
            // not empty, so the sentinel covers both.
            Vect.memset(ptr, (long) SLOT_BYTES * cap, (byte) 0xFF);
        }
    }
}
