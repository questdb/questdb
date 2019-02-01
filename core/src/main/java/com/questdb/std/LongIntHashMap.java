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
        if (Unsafe.arrayGet(values, index) == noEntryValue || Unsafe.arrayGet(keys, index) == key) {
            return Unsafe.arrayGet(values, index);
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
        if (Unsafe.arrayGet(values, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            free--;
            return;
        }

        if (Unsafe.arrayGet(keys, index) == key) {
            Unsafe.arrayPut(values, index, value);
            return;
        }

        probeInsert(key, index, value);
    }

    private int probe(long key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(values, index) == noEntryValue || Unsafe.arrayGet(keys, index) == key) {
                return Unsafe.arrayGet(values, index);
            }
        } while (true);
    }

    private void probeInsert(long key, int index, int value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(values, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                return;
            }

            if (key == Unsafe.arrayGet(keys, index)) {
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
            if (Unsafe.arrayGet(oldValues, i) != noEntryValue) {
                insertKey(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }
}
