/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import org.jetbrains.annotations.NotNull;

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
    private ObjObjHashMap(int initialCapacity, double loadFactor) {
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
        return getAt(keyIndex(key));
    }

    public V getAt(int index) {
        return index < 0 ? Unsafe.arrayGet(values, -index - 1) : null;
    }

    public Iterable<Entry<K, V>> immutableIterator() {
        return new EntryIterator();
    }

    @Override
    @NotNull
    public Iterator<Entry<K, V>> iterator() {
        iterator.index = 0;
        return iterator;
    }

    public int keyIndex(K key) {
        final int index = key.hashCode() & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return index;
        }

        if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public void put(K key, V value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAt(int index, K key, V value) {
        if (index < 0) {
            Unsafe.arrayPut(values, -index - 1, value);
        } else {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            if (--free == 0) {
                rehash();
            }
        }
    }

    public V remove(K key) {
        int index = keyIndex(key);
        if (index < 0) {
            V value = Unsafe.arrayGet(values, -index - 1);
            Unsafe.arrayPut(values, -index - 1, null);
            free++;
            return value;
        }
        return null;
    }

    public int size() {
        return capacity - free;
    }

    private int probe(K key, int index) {
        for (; ; ) {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return index;
            }

            if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
                return -index - 1;
            }
        }
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

    public class EntryIterator implements ImmutableIterator<Entry<K, V>> {

        private final Entry<K, V> entry = new Entry<>();
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < values.length && (Unsafe.arrayGet(keys, index) != noEntryValue || scan());
        }

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
