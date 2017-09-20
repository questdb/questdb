/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.std.str.NullCharSequence;

import java.util.Arrays;


public class CharSequenceIntHashMap implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final int NO_ENTRY_VALUE = -1;
    private static final CharSequence noEntryKey = NullCharSequence.INSTANCE;
    private final int noEntryValue;
    private final double loadFactor;
    private CharSequence[] keys;
    private int[] values;
    private int free;
    private int capacity;
    private int mask;

    public CharSequenceIntHashMap() {
        this(8);
    }

    public CharSequenceIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    @SuppressWarnings("unchecked")
    public CharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        this.noEntryValue = noEntryValue;
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        keys = new CharSequence[capacity];
        values = new int[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(keys, noEntryKey);
        Arrays.fill(values, noEntryValue);
        free = this.capacity;
    }

    public CharSequence entryKey(int index) {
        return Unsafe.arrayGet(keys, index);
    }

    public int get(CharSequence key) {
        int index = Chars.hashCode(key) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            return noEntryValue;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            return Unsafe.arrayGet(values, index);
        }

        return probe(key, index);
    }

    public int getEntry(CharSequence key) {
        int index = Chars.hashCode(key) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            return noEntryValue;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            return index;
        }

        return probeEntry(key, index);
    }

    public void increment(CharSequence key) {
        int index = Chars.hashCode(key) & mask;
        if (cantIncrementAt(index, key)) {
            probeIncrement(key, index);
        }
    }

    public boolean put(CharSequence key, int value) {
        int index = Chars.hashCode(key) & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            free--;
            if (free == 0) {
                rehash();
            }
            return true;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            Unsafe.arrayPut(values, index, value);
            return false;
        }

        return probeInsert(key, index, value);
    }

    public void putIfAbsent(CharSequence key, int value) {
        int index = key.hashCode() & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            keys[index] = key;
            values[index] = value;
            free--;
            if (free == 0) {
                rehash();
            }
        }

        if (Chars.equals(Unsafe.arrayGet(keys, index), key)) {
            return;
        }

        probeInsertIfAbsent(key, index, value);
    }

    public int size() {
        return capacity - free;
    }

    private boolean cantIncrementAt(int index, CharSequence key) {
        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, Unsafe.arrayGet(values, index) + 1);
            free--;
            if (free == 0) {
                rehash();
            }
            return false;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            Unsafe.arrayPut(values, index, Unsafe.arrayGet(values, index) + 1);
            return false;
        }
        return true;
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                return noEntryValue;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return Unsafe.arrayGet(values, index);
            }
        } while (true);
    }

    private int probeEntry(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                return noEntryValue;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return index;
            }
        } while (true);
    }

    private void probeIncrement(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
        } while (cantIncrementAt(index, key));
    }

    private boolean probeInsert(CharSequence key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                if (free == 0) {
                    rehash();
                }
                return true;
            }

            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(values, index, value);
                return false;
            }
        } while (true);
    }

    private void probeInsertIfAbsent(CharSequence key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                if (free == 0) {
                    rehash();
                }
                return;
            }

            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return;
            }
        } while (true);
    }

    private void rehash() {
        int newCapacity = values.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);
        int[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[newCapacity];
        this.values = new int[newCapacity];
        Arrays.fill(keys, noEntryKey);
        Arrays.fill(values, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryKey) {
                put(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }
}