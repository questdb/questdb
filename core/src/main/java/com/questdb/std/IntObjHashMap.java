/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;

import java.util.Arrays;


public class IntObjHashMap<V> implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryValue = new Object();
    private final double loadFactor;
    private V[] values;
    private int[] keys;
    private int free;
    private int capacity;
    private int mask;

    public IntObjHashMap() {
        this(8);
    }

    private IntObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    @SuppressWarnings("unchecked")
    private IntObjHashMap(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        values = (V[]) new Object[capacity];
        keys = new int[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(values, noEntryValue);
    }

    public V get(int key) {
        int index = key & mask;
        if (Unsafe.arrayGet(values, index) == noEntryValue) {
            return null;
        }

        if (Unsafe.arrayGet(keys, index) == key) {
            return Unsafe.arrayGet(values, index);
        }

        return probe(key, index);
    }

    public void put(int key, V value) {
        insertKey(key, value);
        if (free == 0) {
            rehash();
        }
    }

    public int size() {
        return capacity - free;
    }

    private void insertKey(int key, V value) {
        int index = key & mask;
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

    private V probe(int key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(values, index) == noEntryValue) {
                return null;
            }
            if (key == Unsafe.arrayGet(keys, index)) {
                return Unsafe.arrayGet(values, index);
            }
        } while (true);
    }

    private void probeInsert(int key, int index, V value) {
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

    @SuppressWarnings({"unchecked"})
    private void rehash() {

        int newCapacity = values.length << 1;
        mask = newCapacity - 1;

        free = this.capacity = (int) (newCapacity * loadFactor);

        V[] oldValues = values;
        int[] oldKeys = keys;
        this.keys = new int[newCapacity];
        this.values = (V[]) new Object[newCapacity];
        Arrays.fill(values, 0, values.length, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldValues, i) != noEntryValue) {
                insertKey(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }
}
