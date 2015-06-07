/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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


public class CharSequenceObjHashMap<V> {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final CharSequence noEntryValue = new NullCharSequence();
    private final double loadFactor;
    private CharSequence[] keys;
    private V[] values;
    private int free;
    private int capacity;
    private int mask;

    public CharSequenceObjHashMap() {
        this(8);
    }

    public CharSequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    public CharSequenceObjHashMap(int initialCapacity, double loadFactor) {
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        keys = new CharSequence[capacity];
        values = (V[]) new Object[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(keys, noEntryValue);
    }

    public V get(CharSequence key) {
        int index = key.hashCode() & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return null;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            return Unsafe.arrayGet(values, index);
        }

        return probe(key, index);
    }

    public V put(CharSequence key, V value) {
        return insertKey(key, value);
    }

    public int size() {
        return capacity - free;
    }

    private V insertKey(CharSequence key, V value) {
        int index = key.hashCode() & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            free--;
            if (free == 0) {
                rehash();
            }
            return null;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            V old = Unsafe.arrayGet(values, index);
            Unsafe.arrayPut(values, index, value);
            return old;
        }

        return probeInsert(key, index, value);
    }

    private V probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return null;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return Unsafe.arrayGet(values, index);
            }
        } while (true);
    }

    private V probeInsert(CharSequence key, int index, V value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                if (free == 0) {
                    rehash();
                }
                return null;
            }

            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                V old = Unsafe.arrayGet(values, index);
                Unsafe.arrayPut(values, index, value);
                return old;
            }
        } while (true);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {

        int newCapacity = values.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);
        V[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[newCapacity];
        this.values = (V[]) new Object[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryValue) {
                insertKey(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
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