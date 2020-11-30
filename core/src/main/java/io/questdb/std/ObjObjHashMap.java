/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;


public class ObjObjHashMap<K,V> implements Iterable<ObjObjHashMap.Entry<K,V>>, Mutable {
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

    private ObjObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    private ObjObjHashMap(int initialCapacity, double loadFactor) {
        assert loadFactor > 0 && loadFactor < 1.0;
        this.capacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        this.loadFactor = loadFactor;
        keys = (K[]) new Object[Numbers.ceilPow2((int) (this.capacity / loadFactor))];
        values = (V[]) new Object[keys.length];
        mask = keys.length - 1;
        clear();
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryValue);
    }

    public V get(K key) {
        return valueAt(keyIndex(key));
    }

    @Override
    @NotNull
    public Iterator<Entry<K,V>> iterator() {
        iterator.index = 0;
        return iterator;
    }

    public int keyIndex(K key) {
        int index = Hash.spread(key.hashCode()) & mask;

        final K kv = keys[index];
        if (kv == noEntryValue) {
            return index;
        }

        if (kv == key || key.equals(kv)) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public void put(K key, V value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAt(int index, K key, V value) {
        if (index < 0) {
            values[-index - 1] = value;
            return;
        }
        putAt0(index, key, value);
    }

    public boolean putIfAbsent(K key, V value) {
        final int index = keyIndex(key);
        if (index > -1) {
            putAt(index, key, value);
            return true;
        }
        return false;
    }

    public int size() {
        return capacity - free;
    }

    public V valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : null;
    }

    private int probe(K key, int index) {
        do {
            index = (index + 1) & mask;
            final K kv = keys[index];
            if (kv == noEntryValue) {
                return index;
            }
            if (kv == key || key.equals(kv)) {
                return -index - 1;
            }
        } while (true);
    }

    private void putAt0(int index, K key, V value) {
        keys[index] = key;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {

        free = capacity = this.capacity * 2;
        V[] oldValues = values;
        K[] oldKeys = keys;
        this.keys = (K[]) new Object[Numbers.ceilPow2(Numbers.ceilPow2((int) (this.capacity / loadFactor)))];
        this.values = (V[]) new Object[keys.length];
        Arrays.fill(keys, noEntryValue);
        mask = keys.length - 1;

        for (int i = oldKeys.length; i-- > 0; ) {
            if (oldKeys[i] != noEntryValue) {
                put(oldKeys[i], oldValues[i]);
            }
        }
    }

    public static class Entry<K, V> {
        public K key;
        public V value;
    }

    public class EntryIterator implements ImmutableIterator<Entry<K,V>> {

        private final Entry<K,V> entry = new Entry<>();
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < values.length && (keys[index] != noEntryValue || scan());
        }

        @Override
        public Entry<K,V> next() {
            entry.key = keys[index];
            int index1 = index++;
            entry.value = values[index1];
            return entry;
        }

        private boolean scan() {
            do {
                index++;
            } while (index < values.length && keys[index] == noEntryValue);

            return index < values.length;
        }
    }
}
