/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.journal.collections;

import java.util.Arrays;


public class IntObjHashMap<V> {

    private static final Object FREE = new Object();
    private final ValuesIterator valuesIterator = new ValuesIterator();
    private final double loadFactor;
    private V[] values;
    private int[] keys;
    private int free;

    public IntObjHashMap() {
        this(11);
    }

    public IntObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    @SuppressWarnings("unchecked")
    public IntObjHashMap(int initialCapacity, double loadFactor) {
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        this.loadFactor = loadFactor;
        values = (V[]) new Object[capacity];
        keys = new int[capacity];
        free = initialCapacity;
        clear();
    }

    @SuppressWarnings({"unchecked"})
    protected void rehash() {

        int newCapacity = (int) Primes.next(values.length << 1);

        free = (int) (newCapacity * loadFactor);

        V[] oldValues = values;
        int[] oldKeys = keys;
        this.keys = new int[newCapacity];
        this.values = (V[]) new Object[newCapacity];
        Arrays.fill(values, 0, values.length, FREE);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (oldValues[i] != FREE) {
                insertKey(oldKeys[i], oldValues[i]);
            }
        }
    }

    public V get(int key) {
        int index = (key & 0x7fffffff) % keys.length;
        if (values[index] == FREE) {
            return null;
        }

        if (keys[index] == key) {
            return values[index];
        }

        return probe(key, index);
    }

    private V probe(int key, int index) {
        do {
            index = (index + 1) % keys.length;
            if (values[index] == FREE) {
                return null;
            }
            if (key == keys[index]) {
                return values[index];
            }
        } while (true);
    }

    public V put(int key, V value) {
        V old = insertKey(key, value);
        if (free == 0) {
            rehash();
        }
        return old;
    }

    private V insertKey(int key, V value) {
        int index = (key & 0x7fffffff) % keys.length;
        if (values[index] == FREE) {
            keys[index] = key;
            values[index] = value;
            free--;
            return null;
        }

        if (keys[index] == key) {
            V r = values[index];
            values[index] = value;
            return r;
        }

        return probeInsert(key, index, value);
    }

    private V probeInsert(int key, int index, V value) {
        do {
            index = (index + 1) % keys.length;
            if (values[index] == FREE) {
                keys[index] = key;
                values[index] = value;
                free--;
                return null;
            }

            if (key == keys[index]) {
                V r = values[index];
                values[index] = value;
                return r;
            }
        } while (true);
    }

    public ValuesIterator values() {
        valuesIterator.index = 0;
        return valuesIterator;
    }

    public void clear() {
        Arrays.fill(values, FREE);
    }

    public class ValuesIterator extends AbstractImmutableIterator<V> {
        private int index;

        @Override
        public boolean hasNext() {
            return index < values.length && (values[index] != FREE || scan());
        }

        private boolean scan() {
            while (index < values.length && values[index] == FREE) {
                index++;
            }
            return index < values.length;
        }

        @Override
        public V next() {
            return values[index++];
        }
    }
}
