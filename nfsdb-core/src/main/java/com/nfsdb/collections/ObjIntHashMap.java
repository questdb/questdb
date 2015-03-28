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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Iterator;


public class ObjIntHashMap<V> implements Iterable<ObjIntHashMap.Entry<V>> {
    public static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryValue = new Object();
    private final int noKeyValue;
    private final double loadFactor;
    private final EntryIterator iterator = new EntryIterator();
    private V[] keys;
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
        keys = (V[]) new Object[capacity];
        values = new int[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(keys, noEntryValue);
    }

    public int get(V key) {
        int index = key.hashCode() & mask;

        if (keys[index] == noEntryValue) {
            return noKeyValue;
        }

        if (keys[index] == key || key.equals(keys[index])) {
            return values[index];
        }

        return probe(key, index);
    }

    public Iterable<Entry<V>> immutableIterator() {
        return new EntryIterator();
    }

    @Override
    public Iterator<Entry<V>> iterator() {
        iterator.index = 0;
        return iterator;
    }

    public int put(V key, int value) {
        return insertKey(key, value);
    }

    public boolean putIfAbsent(V key, int value) {
        int index = key.hashCode() & mask;
        if (keys[index] == noEntryValue) {
            keys[index] = key;
            values[index] = value;
            free--;
            if (free == 0) {
                rehash();
            }
            return true;
        }

        return !(keys[index] == key || key.equals(keys[index])) && probeInsertIfAbsent(key, index, value);
    }

    public int size() {
        return capacity - free;
    }

    private int insertKey(V key, int value) {
        int index = key.hashCode() & mask;
        if (keys[index] == noEntryValue) {
            keys[index] = key;
            values[index] = value;
            free--;
            if (free == 0) {
                rehash();
            }
            return noKeyValue;
        }

        if (keys[index] == key || key.equals(keys[index])) {
            int old = values[index];
            values[index] = value;
            return old;
        }

        return probeInsert(key, index, value);
    }

    private int probe(V key, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryValue) {
                return noKeyValue;
            }
            if (keys[index] == key || key.equals(keys[index])) {
                return values[index];
            }
        } while (true);
    }

    private int probeInsert(V key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryValue) {
                keys[index] = key;
                values[index] = value;
                free--;
                if (free == 0) {
                    rehash();
                }
                return noKeyValue;
            }

            if (keys[index] == key || key.equals(keys[index])) {
                int old = values[index];
                values[index] = value;
                return old;
            }
        } while (true);
    }

    private boolean probeInsertIfAbsent(V key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryValue) {
                keys[index] = key;
                values[index] = value;
                free--;
                if (free == 0) {
                    rehash();
                }
                return true;
            }

            if (keys[index] == key || key.equals(keys[index])) {
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
        V[] oldKeys = keys;
        this.keys = (V[]) new Object[newCapacity];
        this.values = new int[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (oldKeys[i] != noEntryValue) {
                insertKey(oldKeys[i], oldValues[i]);
            }
        }
    }

    public static class Entry<V> {
        public V key;
        public int value;
    }

    public class EntryIterator extends AbstractImmutableIterator<Entry<V>> {

        private final Entry<V> entry = new Entry<>();
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < values.length && (keys[index] != noEntryValue || scan());
        }

        @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
        @Override
        public Entry<V> next() {
            entry.key = keys[index];
            entry.value = values[index++];
            return entry;
        }

        private boolean scan() {
            while (index < values.length && keys[index] == noEntryValue) {
                index++;
            }
            return index < values.length;
        }
    }
}
