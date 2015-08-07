/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections;

import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.util.Arrays;


public class CharSequenceIntHashMap implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final int NO_ENTRY_VALUE = -1;
    private static final CharSequence noEntryKey = new NullCharSequence();
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

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryKey) {
                put(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }

    private static class NullCharSequence implements CharSequence {
        @Override
        public int length() {
            return 0;
        }

        @Override
        public char charAt(int index) {
            return 0;
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return null;
        }
    }
}