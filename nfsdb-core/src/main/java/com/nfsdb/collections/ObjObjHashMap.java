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

import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Iterator;


public class ObjObjHashMap<K, V> implements Iterable<ObjObjHashMap.Entry<K, V>>, Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryValue = new Object();
    private final double loadFactor;
    private final EntryIterator iterator = new EntryIterator();
    private K[] keys;
    private V[] values;
    private int free;
    private int capacity;
    private int mask;

    public ObjObjHashMap() {
        this(8);
    }

    public ObjObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    public ObjObjHashMap(int initialCapacity, double loadFactor) {
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        keys = (K[]) new Object[capacity];
        values = (V[]) new Object[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(keys, noEntryValue);
    }

    public V get(K key) {
        int index = key.hashCode() & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return null;
        }

        if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
            return Unsafe.arrayGet(values, index);
        }

        return probe(key, index);
    }

    public Iterable<Entry<K, V>> immutableIterator() {
        return new EntryIterator();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        iterator.index = 0;
        return iterator;
    }

    public void put(K key, V value) {
        int index = key.hashCode() & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            free--;
            if (free == 0) {
                rehash();
            }
            return;
        }

        if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
            Unsafe.arrayPut(values, index, value);
            return;
        }

        probeInsert(key, index, value);
    }

    public int size() {
        return capacity - free;
    }

    private V probe(K key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return null;
            }
            if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
                return Unsafe.arrayGet(values, index);
            }
        } while (true);
    }

    private void probeInsert(K key, int index, V value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                if (free == 0) {
                    rehash();
                }
                return;
            }

            if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(values, index, value);
                return;
            }
        } while (true);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {

        int newCapacity = values.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);
        V[] oldValues = values;
        K[] oldKeys = keys;
        this.keys = (K[]) new Object[newCapacity];
        this.values = (V[]) new Object[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryValue) {
                put(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }

    public static class Entry<K, V> {
        public K key;
        public V value;
    }

    public class EntryIterator extends AbstractImmutableIterator<Entry<K, V>> {

        private final Entry<K, V> entry = new Entry<>();
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < values.length && (Unsafe.arrayGet(keys, index) != noEntryValue || scan());
        }

        @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
        @Override
        public Entry<K, V> next() {
            entry.key = keys[index];
            entry.value = values[index++];
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
