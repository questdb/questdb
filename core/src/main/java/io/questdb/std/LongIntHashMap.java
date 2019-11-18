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

import java.util.Arrays;


public class LongIntHashMap implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final int noEntryValue = -1;
    private final double loadFactor;
    private int[] values;
    private long[] keys;
    private int free;
    private int mask;

    public LongIntHashMap() {
        this(8);
    }

    private LongIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    private LongIntHashMap(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        values = new int[capacity];
        keys = new long[capacity];
        free = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(values, noEntryValue);
    }

    public int get(long key) {
        int index = (int) key & mask;
        if (values[index] == noEntryValue || keys[index] == key) {
            return values[index];
        }
        return probe(key, index);
    }

    public void put(long key, int value) {
        insertKey(key, value);
        if (free == 0) {
            rehash();
        }
    }

    private void insertKey(long key, int value) {
        int index = (int) key & mask;
        if (values[index] == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            free--;
            return;
        }

        if (keys[index] == key) {
            Unsafe.arrayPut(values, index, value);
            return;
        }

        probeInsert(key, index, value);
    }

    private int probe(long key, int index) {
        do {
            index = (index + 1) & mask;
            if (values[index] == noEntryValue || keys[index] == key) {
                return values[index];
            }
        } while (true);
    }

    private void probeInsert(long key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (values[index] == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                return;
            }

            if (key == keys[index]) {
                Unsafe.arrayPut(values, index, value);
                return;
            }
        } while (true);
    }

    private void rehash() {
        int newCapacity = values.length << 1;
        mask = newCapacity - 1;
        free = (int) (newCapacity * loadFactor);
        int[] oldValues = values;
        long[] oldKeys = keys;
        this.keys = new long[newCapacity];
        this.values = new int[newCapacity];
        Arrays.fill(values, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            int val = oldValues[i];
            if (val != noEntryValue) {
                insertKey(oldKeys[i], val);
            }
        }
    }
}
