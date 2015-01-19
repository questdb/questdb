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

package com.nfsdb.collections;

import java.util.Arrays;


public class IntIntHashMap {

    private static final int FREE = -1;
    private final double loadFactor;
    private int[] values;
    private int[] keys;
    private int free;

    public IntIntHashMap() {
        this(11);
    }

    public IntIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    @SuppressWarnings("unchecked")
    public IntIntHashMap(int initialCapacity, double loadFactor) {
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        this.loadFactor = loadFactor;
        values = new int[capacity];
        keys = new int[capacity];
        free = initialCapacity;
        clear();
    }

    @SuppressWarnings({"unchecked"})
    protected void rehash() {

        int newCapacity = Primes.next(values.length << 1);

        free = (int) (newCapacity * loadFactor);

        int[] oldValues = values;
        int[] oldKeys = keys;
        this.keys = new int[newCapacity];
        this.values = new int[newCapacity];
        Arrays.fill(values, 0, values.length, FREE);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (oldValues[i] != FREE) {
                insertKey(oldKeys[i], oldValues[i]);
            }
        }
    }

    public int get(int key) {
        int index = (key & 0x7fffffff) % keys.length;
        if (values[index] == FREE || keys[index] == key) {
            return values[index];
        }
        return probe(key, index);
    }

    private int probe(int key, int index) {
        do {
            index = (index + 1) % keys.length;
            if (values[index] == FREE || keys[index] == key) {
                return values[index];
            }
        } while (true);
    }

    public int put(int key, int value) {
        int old = insertKey(key, value);
        if (free == 0) {
            rehash();
        }
        return old;
    }

    private int insertKey(int key, int value) {
        int index = (key & 0x7fffffff) % keys.length;
        if (values[index] == FREE) {
            keys[index] = key;
            values[index] = value;
            free--;
            return FREE;
        }

        if (keys[index] == key) {
            int r = values[index];
            values[index] = value;
            return r;
        }

        return probeInsert(key, index, value);
    }

    private int probeInsert(int key, int index, int value) {
        do {
            index = (index + 1) % keys.length;
            if (values[index] == FREE) {
                keys[index] = key;
                values[index] = value;
                free--;
                return FREE;
            }

            if (key == keys[index]) {
                int r = values[index];
                values[index] = value;
                return r;
            }
        } while (true);
    }

    public void clear() {
        Arrays.fill(values, FREE);
    }
}
