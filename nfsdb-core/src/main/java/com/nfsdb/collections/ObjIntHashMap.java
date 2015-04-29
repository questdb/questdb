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

import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Iterator;


public class ObjIntHashMap<K> implements Iterable<ObjIntHashMap.Entry<K>> {
    public static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryValue = new Object();
    private final int noKeyValue;
    private final double loadFactor;
    private final EntryIterator iterator = new EntryIterator();
    private K[] keys;
    private int[] values;
    private int free;
    private int capacity;
    private int mask;

    public ObjIntHashMap() {
        this(8);
    }

    public ObjIntHashMap(int initialCapacity, int noKeyValue) {
        this(initialCapacity, 0.5, noKeyValue);
    }

    public ObjIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, -1);
    }

    @SuppressWarnings("unchecked")
    public ObjIntHashMap(int initialCapacity, double loadFactor, int noKeyValue) {
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        this.noKeyValue = noKeyValue;
        keys = (K[]) new Object[capacity];
        values = new int[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(keys, noEntryValue);
    }

    public int get(K key) {
        int index = key.hashCode() & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return noKeyValue;
        }

        if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
            return values[index];
        }

        return probe(key, index);
    }

    public Iterable<Entry<K>> immutableIterator() {
        return new EntryIterator();
    }

    @Override
    public Iterator<Entry<K>> iterator() {
        iterator.index = 0;
        return iterator;
    }

    public int put(K key, int value) {
        return insertKey(key, value);
    }

    public boolean putIfAbsent(K key, int value) {
        int index = key.hashCode() & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            keys[index] = key;
            values[index] = value;
            free--;
            if (free == 0) {
                rehash();
            }
            return true;
        }

        return Unsafe.arrayGet(keys, index) != key && !key.equals(Unsafe.arrayGet(keys, index)) && probeInsertIfAbsent(key, index, value);
    }

    public int size() {
        return capacity - free;
    }

    private int insertKey(K key, int value) {
        int index = key.hashCode() & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            free--;
            if (free == 0) {
                rehash();
            }
            return noKeyValue;
        }

        if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
            int old = Unsafe.arrayGet(values, index);
            Unsafe.arrayPut(values, index, value);
            return old;
        }

        return probeInsert(key, index, value);
    }

    private int probe(K key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return noKeyValue;
            }
            if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
                return Unsafe.arrayGet(values, index);
            }
        } while (true);
    }

    private int probeInsert(K key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                if (free == 0) {
                    rehash();
                }
                return noKeyValue;
            }

            if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
                int old = Unsafe.arrayGet(values, index);
                Unsafe.arrayPut(values, index, value);
                return old;
            }
        } while (true);
    }

    private boolean probeInsertIfAbsent(K key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                if (free == 0) {
                    rehash();
                }
                return true;
            }

            if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
                return false;
            }
        } while (true);
    }

    @SuppressWarnings({"unchecked"})
    protected void rehash() {

        int newCapacity = values.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);
        int[] oldValues = values;
        K[] oldKeys = keys;
        this.keys = (K[]) new Object[newCapacity];
        this.values = new int[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryValue) {
                insertKey(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }

    public static class Entry<V> {
        public V key;
        public int value;
    }

    public class EntryIterator extends AbstractImmutableIterator<Entry<K>> {

        private final Entry<K> entry = new Entry<>();
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < values.length && (Unsafe.arrayGet(keys, index) != noEntryValue || scan());
        }

        @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
        @Override
        public Entry<K> next() {
            entry.key = Unsafe.arrayGet(keys, index);
            entry.value = Unsafe.arrayGet(values, index++);
            return entry;
        }

        private boolean scan() {
            do {
                index++;
            } while (index < values.length && Unsafe.arrayGet(keys, index) == noEntryValue);

            return index < values.length;
        }
    }
}
