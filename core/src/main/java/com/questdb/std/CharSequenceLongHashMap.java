/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.NullCharSequence;

import java.util.Arrays;


public class CharSequenceLongHashMap implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final long NO_ENTRY_VALUE = -1L;
    private static final CharSequence noEntryKey = NullCharSequence.INSTANCE;
    private final long noEntryValue;
    private final double loadFactor;
    private CharSequence[] keys;
    private long[] values;
    private int free;
    private int capacity;
    private int mask;

    public CharSequenceLongHashMap() {
        this(8);
    }

    public CharSequenceLongHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    @SuppressWarnings("unchecked")
    public CharSequenceLongHashMap(int initialCapacity, double loadFactor, long noEntryValue) {
        this.noEntryValue = noEntryValue;
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        keys = new CharSequence[capacity];
        values = new long[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(keys, noEntryKey);
        Arrays.fill(values, noEntryValue);
        free = this.capacity;
    }

    public long get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public void increment(CharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            Unsafe.arrayPut(values, -index - 1, Unsafe.arrayGet(values, -index - 1) + 1);
        } else {
            putAt0(index, key, 0);
        }
    }

    public CharSequence keyAt(int index) {
        return index < 0 ? Unsafe.arrayGet(keys, -index - 1) : null;
    }

    public int keyIndex(CharSequence key) {
        int index = Chars.hashCode(key) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            return index;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public boolean put(CharSequence key, long value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, CharSequence key, long value) {
        if (index < 0) {
            Unsafe.arrayPut(values, -index - 1, value);
            return false;
        }
        putAt0(index, key, value);
        return true;
    }

    public void putIfAbsent(CharSequence key, long value) {
        int index = keyIndex(key);
        if (index > -1) {
            putAt0(index, key, value);
        }
    }

    public boolean remove(CharSequence key) {
        return removeAt(keyIndex(key));
    }

    public boolean removeAt(int index) {
        if (index < 0) {
            Unsafe.arrayPut(values, -index - 1, noEntryValue);
            free++;
            return true;
        }
        return false;
    }

    public int size() {
        return capacity - free;
    }

    public long valueAt(int index) {
        return index < 0 ? Unsafe.arrayGet(values, -index - 1) : noEntryValue;
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                return index;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return -index - 1;
            }
        } while (true);
    }

    private void putAt0(int index, CharSequence key, long value) {
        Unsafe.arrayPut(keys, index, key);
        Unsafe.arrayPut(values, index, value);
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        int newCapacity = values.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);
        long[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[newCapacity];
        this.values = new long[newCapacity];
        Arrays.fill(keys, noEntryKey);
        Arrays.fill(values, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryKey) {
                put(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }
}