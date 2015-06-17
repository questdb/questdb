/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.collections;

import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;


public class IntObjHashMap<V> {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryValue = new Object();
    private final ValuesIterator valuesIterator = new ValuesIterator();
    private final double loadFactor;
    private V[] values;
    private int[] keys;
    private int free;
    private int capacity;
    private int mask;

    public IntObjHashMap() {
        this(8);
    }

    public IntObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    @SuppressWarnings("unchecked")
    public IntObjHashMap(int initialCapacity, double loadFactor) {
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

    public ValuesIterator values() {
        valuesIterator.index = 0;
        return valuesIterator;
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

    public class ValuesIterator extends AbstractImmutableIterator<V> {
        private int index;

        @Override
        public boolean hasNext() {
            return index < values.length && (Unsafe.arrayGet(values, index) != noEntryValue || scan());
        }

        @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
        @Override
        public V next() {
            return values[index++];
        }

        private boolean scan() {
            do {
                index++;
            }
            while (index < values.length && Unsafe.arrayGet(values, index) == noEntryValue);

            return index < values.length;
        }
    }
}
