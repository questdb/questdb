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


public class IntHashSet {

    private static final int noEntryValue = -1;
    private final double loadFactor;
    private int[] keys;
    private int free;
    private int capacity;

    public IntHashSet() {
        this(11);
    }

    public IntHashSet(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    @SuppressWarnings("unchecked")
    public IntHashSet(int initialCapacity, double loadFactor) {
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        this.loadFactor = loadFactor;
        keys = new int[capacity];
        free = this.capacity = initialCapacity;
        clear();
    }

    @SuppressWarnings({"unchecked"})
    protected void rehash() {
        int newCapacity = Primes.next(keys.length << 1);

        free = capacity = (int) (newCapacity * loadFactor);

        int[] oldKeys = keys;
        this.keys = new int[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (oldKeys[i] != noEntryValue) {
                insertKey(oldKeys[i]);
            }
        }
    }

    public boolean add(int key) {
        boolean r = insertKey(key);
        if (free == 0) {
            rehash();
        }
        return r;
    }

    private boolean insertKey(int key) {
        int index = (key & 0x7fffffff) % keys.length;
        if (keys[index] == noEntryValue) {
            keys[index] = key;
            free--;
            return true;
        }
        return keys[index] != key && probeInsert(key, index);
    }

    private boolean probeInsert(int key, int index) {
        do {
            index = (index + 1) % keys.length;
            if (keys[index] == noEntryValue) {
                keys[index] = key;
                free--;
                return true;
            }

            if (key == keys[index]) {
                return false;
            }
        } while (true);
    }

    public void clear() {
        Arrays.fill(keys, 0, keys.length, noEntryValue);
    }

    public int size() {
        return capacity - free;
    }
}
