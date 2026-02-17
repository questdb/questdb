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

package io.questdb.cairo.wal;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.Chars;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * HashMap mapping char sequences to integers using unmanaged memory to store keys, values and offsets.
 */
public class DirectCharSequenceIntHashMap implements Closeable, Mutable {
    public static final int NO_ENTRY_VALUE = -1;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final int NO_ENTRY_OFFSET = 0;
    private final double loadFactor;
    private final int noEntryValue;
    private final DirectString sview = new DirectString();
    // address of the offset and hashcode buffer, the content follows this layout:
    // | offset0 | hashcode0 | ...
    // | 4 bytes |  4 bytes  | ...
    private long address;
    // capacity of the offset and hashcode buffer in bytes
    private long capacity;
    private int currentOffset = 0;
    // number of free slots in the map
    private int free;
    // address of the key-value buffer, the content follows this layout:
    // |  value0 | key0 len |    key0 chars...   | ...
    // | 4 bytes |  4 bytes | key0 len * 2 bytes |
    // Elements are aligned to 4 bytes.
    private long kvAddress;
    // capacity of the contiguous key-value buffer in bytes
    private long kvCapacity;
    // number of elements that can be stored into the hashmap before it needs to be rehashed
    private int mapCapacity;
    // mask over the current capacity of the map
    private int mask;

    /**
     * Creates the map with a default initial capacity.
     */
    public DirectCharSequenceIntHashMap() {
        this(8);
    }

    /**
     * Creates the map with the provided initial capacity.
     *
     * @param initialCapacity expected number of distinct keys
     */
    public DirectCharSequenceIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.4, NO_ENTRY_VALUE);
    }

    /**
     * Creates the map with a custom load factor and no-entry value.
     *
     * @param initialCapacity expected number of keys
     * @param loadFactor      load factor triggering rehash
     * @param noEntryValue    value returned when the key is missing
     */
    public DirectCharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        this(initialCapacity, loadFactor, noEntryValue, 32);
    }

    /**
     * Creates the map giving full control over sizing knobs.
     *
     * @param initialCapacity expected number of keys
     * @param loadFactor      load factor triggering rehash
     * @param noEntryValue    value returned when key is missing
     * @param avgKeySize      hint for key buffer sizing, in chars
     */
    public DirectCharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue, int avgKeySize) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.mapCapacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        final int len = Numbers.ceilPow2((int) (this.mapCapacity / loadFactor));
        mask = len - 1;
        this.capacity = (long) len << 3;
        this.address = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        this.kvCapacity = Numbers.ceilPow2((long) initialCapacity * (((long) avgKeySize << 1) + 8L));
        this.kvAddress = Unsafe.malloc(this.kvCapacity, MemoryTag.NATIVE_DEFAULT);
        this.noEntryValue = noEntryValue;
        clear();
    }

    /**
     * Clears the map of all entries.
     */
    public final void clear() {
        // The key size estimation may be off, we can give some memory back if we don't even use half of it
        if ((long) currentOffset << 3 < kvCapacity) {
            final long oldCapacity = kvCapacity;
            // We only shrink the capacity by 2 to avoid unnecessary grow-back later
            kvCapacity >>= 1;
            kvAddress = Unsafe.realloc(kvAddress, oldCapacity, kvCapacity, MemoryTag.NATIVE_DEFAULT);
        }
        free = mapCapacity;
        Vect.memset(address, capacity, 0);
        this.currentOffset = 0;
    }

    /**
     * Releases the native buffers; the map must not be used afterwards.
     */
    public void close() {
        if (this.address != 0) {
            Unsafe.free(address, this.capacity, MemoryTag.NATIVE_DEFAULT);
            this.address = 0;
            this.capacity = 0;
        }
        if (this.kvAddress != 0) {
            Unsafe.free(kvAddress, kvCapacity, MemoryTag.NATIVE_DEFAULT);
            this.kvAddress = 0;
            this.kvCapacity = 0;
        }
    }

    /**
     * Copies values and the keys that are greater or equal to the provided {@param value}
     * to {@param mem}.
     *
     * @return the number of values copied.
     */
    public int copyTo(MemoryARW mem, int minimumValue) {
        int copied = 0;

        long ptr = kvAddress;
        final long hi = kvAddress + ((long) currentOffset << 2);
        while (ptr < hi) {
            final int value = Unsafe.getUnsafe().getInt(ptr);
            final int len = Unsafe.getUnsafe().getInt(ptr + 4);
            if (value >= minimumValue) {
                final long storageLen = Vm.getStorageLength(len) + Integer.BYTES;
                final long addr = mem.appendAddressFor(storageLen);
                Unsafe.getUnsafe().putInt(addr, value);
                Unsafe.getUnsafe().putInt(addr + 4L, len);
                Vect.memcpy(addr + 8L, ptr + 8L, (long) len << 1);
                copied++;
            }
            ptr += ((((long) len << 1) + 11) & ~3);
        }

        return copied;
    }

    /**
     * Returns the value stored at the provided offset (as produced by {@link #nextOffset()}).
     */
    public int get(int offset) {
        final long ptr = kvAddress + ((long) offset << 2);
        return Unsafe.getUnsafe().getInt(ptr);
    }

    /**
     * Looks up the value for {@code key}.
     *
     * @param key char sequence to lookup
     * @return stored value or {@link #noEntryValue} if missing
     */
    public int get(@NotNull CharSequence key) {
        return valueAt(keyIndex(key));
    }

    /**
     * Returns the key stored at the provided offset (as produced by {@link #nextOffset()}).
     */
    public CharSequence getKey(int offset) {
        final long ptr = kvAddress + ((long) offset << 2);
        final int len = Unsafe.getUnsafe().getInt(ptr + 4);
        sview.of(ptr + 8, len);
        return sview;
    }

    /**
     * Returns the index of a free slot where this key can be placed.
     * Returns the negative offset of the key if it's already present.
     *
     * @param key      the key whose slot to look for
     * @param hashCode the hashCode of the key
     * @return the index of a free slot where this key can be placed,
     * or the negative offset of the key if it's already present.
     */
    public int keyIndex(@NotNull CharSequence key, int hashCode) {
        int index = Hash.spread(hashCode) & mask;
        int offset = getOffset(index);
        if (offset == NO_ENTRY_OFFSET) {
            return index;
        }
        offset -= 1;
        final int hashCode1 = getHashCode(index);
        if (hashCode == hashCode1 && areKeysEquals(offset, key)) {
            return -offset - 1;
        }
        return probe(key, hashCode, index);
    }

    /**
     * Returns the index of a free slot where this key can be placed.
     * Returns the negative index of the key if it's already present.
     *
     * @param key the key whose slot to look for
     * @return the index of a free slot where this key can be placed,
     * or the negative offset of the key if it's already present.
     */
    public int keyIndex(@NotNull CharSequence key) {
        int hashCode = Chars.hashCode(key);
        return keyIndex(key, hashCode);
    }

    /**
     * Returns the offset of the next char sequence inserted the map.
     * Returns -1 if there are no more values.
     *
     * @param offset the last offset that was returned
     * @return the offset of the next char sequence inserted the map
     * or -1 if there are no more values.
     */
    public int nextOffset(int offset) {
        final long lo = kvAddress + ((long) offset << 2);
        final int len = Unsafe.getUnsafe().getInt(lo + 4);
        final int next = offset + ((((len << 1) + 11) & ~3) >> 2);
        return next == this.currentOffset ? -1 : next;
    }

    /**
     * Returns the first offset of the map or -1 if the map is empty.
     *
     * @return the first offset of the map or -1 if the map is empty
     */
    public int nextOffset() {
        return this.currentOffset == 0 ? -1 : 0;
    }

    /**
     * Inserts or overwrites {@code key} with {@code value}.
     */
    public void put(@NotNull CharSequence key, int value) {
        final int hashCode = Chars.hashCode(key);
        final int index = keyIndex(key, hashCode);
        if (index < 0) {
            Unsafe.getUnsafe().putInt(kvAddress + ((long) (-index - 1) << 2), value);
        } else {
            putAt(index, key, value, hashCode);
        }
    }

    /**
     * Inserts a key using a previously calculated slot.
     */
    public void putAt(int index, @NotNull CharSequence key, int value, int hashCode) {
        final int offset = this.writeKey(key, value);
        putAt0(index, offset, hashCode);
    }

    /**
     * Doubles the backing hash-table capacity and reassigns existing keys.
     */
    public void rehash() {
        int size = mapCapacity - free;
        mapCapacity = mapCapacity << 1;
        free = mapCapacity - size;

        final int len = Numbers.ceilPow2((int) (this.mapCapacity / loadFactor));
        mask = len - 1;

        long oldCapacity = capacity;
        long oldAddress = address;
        capacity = (long) len << 3;
        address = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        Vect.memset(address, capacity, 0);

        for (int i = (int) (oldCapacity >> 3) - 1; i > -1; i--) {
            final long oldPtr = oldAddress + ((long) i << 3);
            final int offset = Unsafe.getUnsafe().getInt(oldPtr);
            if (offset != NO_ENTRY_OFFSET) {
                final int hashCode = Unsafe.getUnsafe().getInt(oldPtr + 4L);
                final int index = keyIndexNoDup(hashCode);
                final long ptr = address + ((long) index << 3);
                Unsafe.getUnsafe().putInt(ptr, offset);
                Unsafe.getUnsafe().putInt(ptr + 4L, hashCode);
            }
        }

        Unsafe.free(oldAddress, oldCapacity, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * @return number of stored keys.
     */
    public int size() {
        return mapCapacity - free;
    }

    /**
     * Returns the value stored at the supplied key index.
     *
     * @param offset negative index returned by {@link #keyIndex(CharSequence)}
     * @return stored value or {@link #noEntryValue}
     */
    public int valueAt(int offset) {
        int offset1 = -offset - 1;
        return offset < 0 ? Unsafe.getUnsafe().getInt(kvAddress + ((long) offset1 << 2)) : noEntryValue;
    }

    private boolean areKeysEquals(int offset, @NotNull CharSequence key) {
        long lo = kvAddress + ((long) offset << 2);
        final int len = Unsafe.getUnsafe().getInt(lo + 4);
        if (len != key.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (Unsafe.getUnsafe().getChar(lo + 8 + ((long) i << 1)) != key.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private int getHashCode(int index) {
        return Unsafe.getUnsafe().getInt(address + ((long) index << 3) + 4L);
    }

    private int getOffset(int index) {
        return Unsafe.getUnsafe().getInt(address + ((long) index << 3));
    }

    /**
     * Returns the index of a free slot where this key can be placed.
     * Assumes that the provided key is not already present in the map.
     *
     * @param hashCode the hashCode of the key
     * @return the index of a free slot where this key can be placed.
     */
    private int keyIndexNoDup(int hashCode) {
        int index = Hash.spread(hashCode) & mask;
        int offset = getOffset(index);
        if (offset == NO_ENTRY_OFFSET) {
            return index;
        }
        do {
            index = (index + 1) & mask;
            offset = getOffset(index);
            if (offset == NO_ENTRY_OFFSET) {
                return index;
            }
        } while (true);
    }

    private int probe(@NotNull CharSequence key, int hashCode, int index) {
        do {
            index = (index + 1) & mask;
            int offset = getOffset(index);
            if (offset == NO_ENTRY_OFFSET) {
                return index;
            }
            offset -= 1;
            final int hashCode1 = getHashCode(index);
            if (hashCode == hashCode1 && areKeysEquals(offset, key)) {
                return -offset - 1;
            }
        } while (true);
    }

    private void putAt0(int index, int offset, int hashCode) {
        final long ptr = address + ((long) index << 3);
        Unsafe.getUnsafe().putInt(ptr, offset + 1);
        Unsafe.getUnsafe().putInt(ptr + 4L, hashCode);
        if (--free == 0) {
            rehash();
        }
    }

    private int writeKey(@NotNull CharSequence key, int value) {
        // We need to store the key (2 bytes per char), its length (4 bytes) and the value (4 bytes) aligned to 4 bytes.
        int requiredCapacity = ((key.length() << 1) + 11) & ~3;
        if (kvCapacity < ((long) currentOffset << 2) + requiredCapacity) {
            final long oldSize = kvCapacity;
            kvCapacity = Numbers.ceilPow2(kvCapacity + (long) requiredCapacity);
            kvAddress = Unsafe.realloc(kvAddress, oldSize, kvCapacity, MemoryTag.NATIVE_DEFAULT);
        }
        final long lo = kvAddress + ((long) currentOffset << 2);
        Unsafe.getUnsafe().putInt(lo, value);
        Unsafe.getUnsafe().putInt(lo + 4, key.length());
        for (int i = 0; i < key.length(); i++) {
            Unsafe.getUnsafe().putChar(lo + 8 + ((long) i << 1), key.charAt(i));
        }

        final int oldOffset = currentOffset;
        currentOffset += requiredCapacity >> 2;

        return oldOffset;
    }
}
