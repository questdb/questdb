/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

import io.questdb.std.AbstractOffsetCharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Arrays;

public class SymbolMap extends AbstractOffsetCharSequenceHashSet implements Closeable {
    public static final int NO_ENTRY_VALUE = -1;
    private final int noEntryValue;
    private final DirectString sview = new DirectString();
    private long address;
    private long charsCapacity;
    private int currentOffset = 0;
    private int[] values;

    public SymbolMap() {
        this(8);
    }

    public SymbolMap(int initialCapacity) {
        this(initialCapacity, 0.4, NO_ENTRY_VALUE);
    }

    public SymbolMap(int initialCapacity, double loadFactor, int noEntryValue) {
        this(initialCapacity, loadFactor, noEntryValue, 32);
    }

    public SymbolMap(int initialCapacity, double loadFactor, int noEntryValue, int avgKeySize) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        this.charsCapacity = Numbers.ceilPow2(initialCapacity * (avgKeySize + 8));
        this.address = Unsafe.malloc(this.charsCapacity, MemoryTag.NATIVE_DEFAULT);
        values = new int[offsets.length];
    }

    @Override
    public final void clear() {
        super.clear();
        Arrays.fill(values, noEntryValue);
        this.currentOffset = 0;
    }

    @Override
    public void close() {
        if (this.address != 0) {
            Unsafe.free(address, this.charsCapacity, MemoryTag.NATIVE_DEFAULT);
            this.address = 0;
            this.charsCapacity = 0;
        }
    }

    public int get(@NotNull CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public CharSequence get(int offset) {
        final long lo = address + ((long) offset << 2);
        final int len = Unsafe.getUnsafe().getInt(lo + 4);
        return sview.of(lo + 8, len);
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
        final long lo = address + ((long) offset << 2);
        final int len = Unsafe.getUnsafe().getInt(lo + 4);
        final int off = offset + ((((len << 1) + 11) & ~3) >> 2);
        if (off == this.currentOffset) {
            return -1;
        }
        return off;
    }

    /**
     * Returns the first offset of the map or -1 if the map is empty.
     *
     * @return the first offset of the map or -1 if the map is empty
     */
    public int nextOffset() {
        if (this.currentOffset == 0) {
            return -1;
        }
        return 0;
    }

    public void put(@NotNull CharSequence key, int value) {
        final int hashCode = Chars.hashCode(key);
        final int index = keyIndex(key, hashCode);
        if (index < 0) {
            values[-index - 1] = value;
        } else {
            putAt(index, key, value, hashCode);
        }
    }

    public void putAt(int index, @NotNull CharSequence key, int value, int hashCode) {
        final int offset = this.writeKey(key, hashCode);
        putAt0(index, offset, value);
    }

    public void rehash() {
        int[] oldValues = values;
        int[] oldOffsets = offsets;
        int size = capacity - free;
        capacity = capacity << 1;
        free = capacity - size;
        mask = Numbers.ceilPow2((int) (capacity / loadFactor)) - 1;
        this.offsets = new int[mask + 1];
        Arrays.fill(offsets, noEntryOffset);
        this.values = new int[mask + 1];
        for (int i = oldOffsets.length - 1; i > -1; i--) {
            int offset = oldOffsets[i];
            if (offset != noEntryOffset) {
                final long lo = address + ((long) offset << 2);
                int hashCode = Unsafe.getUnsafe().getInt(lo);
                int len = Unsafe.getUnsafe().getInt(lo + 4);
                sview.of(lo + 8, len);
                final int index = keyIndex(sview, hashCode);
                offsets[index] = offset;
                values[index] = oldValues[i];
            }
        }
    }

    public int valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : noEntryValue;
    }

    private void putAt0(int index, int offset, int value) {
        offsets[index] = offset;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private int writeKey(@NotNull CharSequence key, int keyHashCode) {
        int requiredCapacity = ((key.length() << 1) + 11) & ~3;
        if (charsCapacity < ((long) currentOffset << 2) + requiredCapacity) {
            final long oldSize = charsCapacity;
            charsCapacity = Numbers.ceilPow2(charsCapacity + (long) requiredCapacity);
            address = Unsafe.realloc(address, oldSize, charsCapacity, MemoryTag.NATIVE_DEFAULT);
        }
        final long lo = address + ((long) currentOffset << 2);
        Unsafe.getUnsafe().putInt(lo, keyHashCode);
        Unsafe.getUnsafe().putInt(lo + 4, key.length());
        for (int i = 0; i < key.length(); i++) {
            Unsafe.getUnsafe().putChar(lo + 8 + ((long) i << 1), key.charAt(i));
        }

        final int oldOffset = currentOffset;
        currentOffset += requiredCapacity >> 2;

        return oldOffset;
    }

    @Override
    protected boolean areKeysEquals(int offset, @NotNull CharSequence key, int keyHashCode) {
        long lo = address + ((long) offset << 2);
        final int hashCode = Unsafe.getUnsafe().getInt(lo);
        if (hashCode != keyHashCode) {
            return false;
        }
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
}