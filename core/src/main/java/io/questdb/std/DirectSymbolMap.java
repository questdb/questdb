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

/**
 * Off-heap symbol dictionary backing int keys with UTF-16 byte payloads.
 * <p>
 * Storage layout: a flat native byte buffer of back-to-back entries. Each entry
 * is a 4-byte little-endian length prefix followed by {@code 2 * length} bytes
 * of UTF-16 chars (matching QuestDB's on-disk STRING format). A sentinel length
 * of {@code -1} represents a stored null value.
 * <p>
 * The byte buffer grows on demand via {@link Unsafe#realloc}: capacity doubles
 * until the required size fits. Realloc may move the base pointer, so any
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
 * {@code (offsetInBuf, symbolKey)} pairs and defer byte-wise equality checks
 * to the primary buffer — no heap-side copy of the symbol string is kept.
 */
public class DirectSymbolMap implements Mutable, QuietCloseable, Reopenable {
    private static final int NO_ENTRY_KEY = Integer.MIN_VALUE;
    private static final int NO_ENTRY_VALUE = -1;

    private final long initialBufCapacity;
    private final int initialMapCapacity;
    private final DirectIntIntHashMap keyToOffset;
    private ValueToKeyMap explicitValueToKey;
    private final int memoryTag;
    private final DirectString reusableView = new DirectString();
    private long bufCapacity;
    private long bufPtr;
    private long bufSize;
    private boolean explicitValueToKeyDirty;
    private ValueToKeyMap valueToKey;

    public DirectSymbolMap(long initialBufCapacity, int initialMapCapacity, int memoryTag) {
        this.initialBufCapacity = Math.max(64L, initialBufCapacity);
        this.initialMapCapacity = Math.max(8, initialMapCapacity);
        this.memoryTag = memoryTag;
        this.bufPtr = Unsafe.malloc(this.initialBufCapacity, memoryTag);
        this.bufCapacity = this.initialBufCapacity;
        this.bufSize = 0L;
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
            Unsafe.free(bufPtr, bufCapacity, memoryTag);
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
        bufSize = 0L;
    }

    @Override
    public void close() {
        keyToOffset.close();
        if (explicitValueToKey != null) {
            explicitValueToKey.close();
            explicitValueToKey = null;
        }
        if (valueToKey != null) {
            valueToKey.close();
            valueToKey = null;
        }
        if (bufPtr != 0) {
            Unsafe.free(bufPtr, bufCapacity, memoryTag);
            bufPtr = 0;
            bufCapacity = 0;
            bufSize = 0;
        }
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
     * Returns an existing key if {@code value} has already been interned on this
     * map, otherwise appends a new entry with a sequentially assigned key and
     * returns it. Must not be mixed with {@link #put(int, CharSequence)} on the
     * same instance. Does not accept {@code null} - callers that must store a
     * null value use {@link #put(int, CharSequence)}.
     */
    public int intern(CharSequence value) {
        // The reverse index cannot key a null (hashBytes/matches assume a
        // non-negative length); reject to fail with a clear message, not an NPE.
        if (value == null) {
            throw CairoException.nonCritical().put("DirectSymbolMap.intern does not accept null values; use put()");
        }
        ensureValueToKeyMap();
        long idx = valueToKey.keyIndex(value);
        if (idx < 0) {
            return valueToKey.valueAt(idx);
        }
        int key = keyToOffset.size();
        long offset = append(value);
        keyToOffset.put(key, toIntOffset(offset));
        valueToKey.insertAt(idx, toIntOffset(offset), key);
        return key;
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
        long idx = valueToKey.keyIndex(value);
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
        return explicitValueToKey.keyOf(value, loInclusive, hiExclusive);
    }

    /**
     * Associates {@code value} with the caller-supplied {@code key}. If the key
     * is already present its value is overwritten. The previous bytes stay in
     * the buffer until the next {@link #clear} or {@link #close}. Must not be
     * mixed with {@link #intern} on the same instance.
     */
    public void put(int key, CharSequence value) {
        long idx = keyToOffset.keyIndex(key);
        long offset = append(value);
        keyToOffset.putAt(idx, key, toIntOffset(offset));
        explicitValueToKeyDirty = true;
    }

    @Override
    public void reopen() {
        if (bufPtr == 0) {
            bufPtr = Unsafe.malloc(initialBufCapacity, memoryTag);
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

    public int size() {
        return keyToOffset.size();
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
        int offset = keyToOffset.get(key);
        if (offset == NO_ENTRY_VALUE) {
            return null;
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
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(charsBase + ((long) i << 1), value.charAt(i));
        }
        bufSize += required;
        return offset;
    }

    private void buildExplicitValueToKeyIfNeeded() {
        if (explicitValueToKey == null) {
            explicitValueToKey = new ValueToKeyMap(initialMapCapacity, memoryTag);
        }
        if (!explicitValueToKeyDirty) {
            return;
        }

        explicitValueToKey.clear();
        for (int i = 0, n = keyToOffset.capacity(); i < n; i++) {
            final int key = keyToOffset.keyAt(i);
            if (key != NO_ENTRY_KEY) {
                final int offset = keyToOffset.get(key);
                // Null symbols have no string key and are deliberately absent.
                if (Unsafe.getUnsafe().getInt(bufPtr + offset) >= 0) {
                    explicitValueToKey.insertExplicit(offset, key);
                }
            }
        }
        explicitValueToKeyDirty = false;
    }

    private void ensureCapacity(long required) {
        if (required <= bufCapacity) {
            return;
        }
        long newCap = bufCapacity;
        while (newCap < required) {
            newCap <<= 1;
            if (newCap <= 0) {
                throw CairoException.nonCritical().put("direct symbol map capacity overflow");
            }
        }
        if (newCap > Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put("direct symbol map exceeds 2GiB");
        }
        bufPtr = Unsafe.realloc(bufPtr, bufCapacity, newCap, memoryTag);
        bufCapacity = newCap;
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
     * Nested off-heap open-addressed hash table from CharSequence content to
     * symbol key. Each 8-byte slot holds {@code (offsetInBuf: int, symbolKey: int)};
     * the hashed content comparison reads bytes at {@code bufPtr + offsetInBuf}
     * directly (length prefix + UTF-16 chars), so no heap copy of the key string
     * is retained. An empty slot is marked by a negative offset sentinel.
     * <p>
     * The hash function matches {@link Chars#hashCode(CharSequence)} so that
     * external probes with arbitrary CharSequence inputs land on the same slot
     * as entries inserted from the primary buffer.
     */
    private class ValueToKeyMap implements QuietCloseable {
        static final int SLOT_BYTES = 8;
        private static final int EMPTY_OFFSET = -1;
        private static final double LOAD_FACTOR = 0.5;
        private final int initialCapacity;
        private int capacity;
        private int free;
        private long mask;
        private int memoryTagLocal;
        private long slotsPtr;

        ValueToKeyMap(int initialLogicalCapacity, int memoryTag) {
            // Round the underlying slot array up so {@code capacity * LOAD_FACTOR}
            // seats at least {@code initialLogicalCapacity} entries before the first
            // rehash.
            this.initialCapacity = Numbers.ceilPow2((int) Math.max(8, initialLogicalCapacity / LOAD_FACTOR));
            this.capacity = this.initialCapacity;
            this.mask = capacity - 1L;
            this.free = (int) (capacity * LOAD_FACTOR);
            this.memoryTagLocal = memoryTag;
            this.slotsPtr = Unsafe.malloc((long) SLOT_BYTES * capacity, memoryTag);
            zero(slotsPtr, capacity);
        }

        public void clear() {
            zero(slotsPtr, capacity);
            free = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public void close() {
            if (slotsPtr != 0) {
                Unsafe.free(slotsPtr, (long) SLOT_BYTES * capacity, memoryTagLocal);
                slotsPtr = 0;
                capacity = 0;
                free = 0;
            }
        }

        /**
         * Writes a new (offset, symbolKey) entry at the slot previously returned by
         * {@link #keyIndex}. Caller must pass the non-negative empty-slot index —
         * collisions with a matching entry are not handled here; that case is the
         * caller's responsibility (see {@link DirectSymbolMap#intern}).
         */
        public void insertAt(long idx, int offsetInBuf, int symbolKey) {
            long p = slotsPtr + (idx << 3);
            Unsafe.getUnsafe().putInt(p, offsetInBuf);
            Unsafe.getUnsafe().putInt(p + 4, symbolKey);
            if (--free == 0) {
                rehash();
            }
        }

        /**
         * Inserts an externally keyed mapping without deduplicating equal values.
         * Multiple explicit keys may carry the same content and bounded lookup must
         * retain all of them.
         */
        public void insertExplicit(int offsetInBuf, int symbolKey) {
            long index = hashBytes(offsetInBuf) & mask;
            while (Unsafe.getUnsafe().getInt(slotsPtr + (index << 3)) != EMPTY_OFFSET) {
                index = (index + 1) & mask;
            }
            insertAt(index, offsetInBuf, symbolKey);
        }

        /**
         * Returns a matching explicit key in {@code [loInclusive, hiExclusive)},
         * or {@code -1}. The probe continues across equal values outside the band.
         */
        public int keyOf(CharSequence value, int loInclusive, int hiExclusive) {
            long index = Chars.hashCode(value) & mask;
            while (true) {
                final long p = slotsPtr + (index << 3);
                final int slotOffset = Unsafe.getUnsafe().getInt(p);
                if (slotOffset == EMPTY_OFFSET) {
                    return -1;
                }
                final int symbolKey = Unsafe.getUnsafe().getInt(p + 4);
                if (symbolKey >= loInclusive && symbolKey < hiExclusive && matches(slotOffset, value)) {
                    return symbolKey;
                }
                index = (index + 1) & mask;
            }
        }

        /**
         * Probes for {@code value}. Returns {@code -idx - 1} if an entry with
         * matching content is found at slot {@code idx}; returns a non-negative
         * empty-slot {@code idx} otherwise.
         */
        public long keyIndex(CharSequence value) {
            long index = Chars.hashCode(value) & mask;
            while (true) {
                long p = slotsPtr + (index << 3);
                int slotOffset = Unsafe.getUnsafe().getInt(p);
                if (slotOffset == EMPTY_OFFSET) {
                    return index;
                }
                if (matches(slotOffset, value)) {
                    return -index - 1;
                }
                index = (index + 1) & mask;
            }
        }

        public void reopen() {
            if (slotsPtr == 0) {
                capacity = initialCapacity;
                mask = capacity - 1L;
                free = (int) (capacity * LOAD_FACTOR);
                slotsPtr = Unsafe.malloc((long) SLOT_BYTES * capacity, memoryTagLocal);
                zero(slotsPtr, capacity);
            }
        }

        /**
         * Returns the symbolKey stored at the slot referenced by a negative
         * {@code idx} from {@link #keyIndex}.
         */
        public int valueAt(long idx) {
            return Unsafe.getUnsafe().getInt(slotsPtr + ((-idx - 1) << 3) + 4);
        }

        private int hashBytes(int offsetInBuf) {
            long addr = bufPtr + offsetInBuf;
            int len = Unsafe.getUnsafe().getInt(addr);
            // Null entries (len == -1) should never be reverse-indexed; intern()
            // rejects null and the WAL put() paths do not touch valueToKey. Guard
            // anyway to keep the fallback behavior explicit.
            if (len <= 0) {
                return 0;
            }
            long charsBase = addr + Integer.BYTES;
            int h = 0;
            for (int i = 0; i < len; i++) {
                h = 31 * h + Unsafe.getUnsafe().getChar(charsBase + ((long) i << 1));
            }
            return h;
        }

        private boolean matches(int slotOffset, CharSequence value) {
            long addr = bufPtr + slotOffset;
            int storedLen = Unsafe.getUnsafe().getInt(addr);
            int valueLen = value.length();
            if (storedLen != valueLen) {
                return false;
            }
            long charsBase = addr + Integer.BYTES;
            for (int i = 0; i < valueLen; i++) {
                if (Unsafe.getUnsafe().getChar(charsBase + ((long) i << 1)) != value.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        private void rehash() {
            int oldCapacity = capacity;
            long oldSlotsPtr = slotsPtr;
            int newCapacity = oldCapacity << 1;
            if (newCapacity < 0) {
                throw CairoException.nonCritical().put("direct symbol map reverse index capacity overflow");
            }
            long newSlotsPtr = Unsafe.malloc((long) SLOT_BYTES * newCapacity, memoryTagLocal);
            zero(newSlotsPtr, newCapacity);
            long newMask = newCapacity - 1L;

            for (int i = 0; i < oldCapacity; i++) {
                long src = oldSlotsPtr + ((long) i << 3);
                int slotOffset = Unsafe.getUnsafe().getInt(src);
                if (slotOffset == EMPTY_OFFSET) {
                    continue;
                }
                int symbolKey = Unsafe.getUnsafe().getInt(src + 4);
                long index = hashBytes(slotOffset) & newMask;
                while (Unsafe.getUnsafe().getInt(newSlotsPtr + (index << 3)) != EMPTY_OFFSET) {
                    index = (index + 1) & newMask;
                }
                long dst = newSlotsPtr + (index << 3);
                Unsafe.getUnsafe().putInt(dst, slotOffset);
                Unsafe.getUnsafe().putInt(dst + 4, symbolKey);
            }

            Unsafe.free(oldSlotsPtr, (long) SLOT_BYTES * oldCapacity, memoryTagLocal);
            slotsPtr = newSlotsPtr;
            capacity = newCapacity;
            mask = newMask;
            free += (int) ((newCapacity - oldCapacity) * LOAD_FACTOR);
        }

        private void zero(long ptr, int cap) {
            // Set all bytes to 0xFF so each slot's offset field reads as -1 (EMPTY_OFFSET).
            // The symbolKey field also reads 0xFFFFFFFF, but it's only consulted when
            // the offset is non-negative, so the sentinel covers both.
            Vect.memset(ptr, (long) SLOT_BYTES * cap, (byte) 0xFF);
        }
    }
}
