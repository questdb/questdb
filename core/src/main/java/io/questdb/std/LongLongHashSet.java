/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

/**
 * HashSet specialized in storing long tuples.
 * <p>
 * TODO: Make semantics of load_factor / capacity consistent with other QDB collections.
 * Currently it differs. I could not understand the semantic of the other collections until I implemented this one.
 * Then it clicked :)
 */
public final class LongLongHashSet implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private final double loadFactor;
    private final long noEntryKeyValue;
    private int capacity;
    private long[] keys;
    private int mask;
    private int size;
    private int threshold;

    public LongLongHashSet(int initialCapacity, double loadFactor, long noKeyValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < load factor < 1");
        }
        this.noEntryKeyValue = noKeyValue;
        this.loadFactor = loadFactor;
        this.capacity = Numbers.ceilPow2(Math.max(initialCapacity, MIN_INITIAL_CAPACITY));
        this.threshold = (int) (capacity * loadFactor);
        this.keys = new long[capacity * 2];
        this.mask = capacity - 1;
        Arrays.fill(keys, noEntryKeyValue);
    }

    public boolean add(long key1, long key2) {
        if (key1 == noEntryKeyValue && key2 == noEntryKeyValue) {
            throw new IllegalArgumentException("keys cannot be NO_ENTRY_KEY (" + noEntryKeyValue + ")");
        }
        int index = keyIndex(key1, key2);
        if (index < 0) {
            return false;
        }

        addAt(index, key1, key2);
        if (size == threshold) {
            rehash();
        }
        return true;
    }

    public void addAt(int index, long key1, long key2) {
        setAt(index, key1, key2);
        size++;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, noEntryKeyValue);
        size = 0;
    }

    public boolean contains(long key1, long key2) {
        return keyIndex(key1, key2) < 0;
    }

    public int keyIndex(long key1, long key2) {
        int hash = Hash.hash(key1, key2);
        int index = (hash & mask);
        return probe(key1, key2, index);
    }

    public int size() {
        return size;
    }

    private static long getFirstValueAt(long[] slots, int index) {
        return slots[index * 2];
    }

    private long getSecondValueAt(long[] slots, int index) {
        return slots[index * 2 + 1];
    }

    private int probe(long key1, long key2, int index) {
        do {
            if (getFirstValueAt(keys, index) == noEntryKeyValue && getSecondValueAt(keys, index) == noEntryKeyValue) {
                return index;
            }
            if (getFirstValueAt(keys, index) == key1 && getSecondValueAt(keys, index) == key2) {
                return -index - 1;
            }
            index = (index + 1) & mask;
        } while (true);
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        threshold = (int) (newCapacity * loadFactor);
        int slots = newCapacity * 2;
        long[] newKeys = new long[slots];
        Arrays.fill(newKeys, noEntryKeyValue);
        mask = newCapacity - 1;
        long[] oldKeys = keys;
        keys = newKeys;
        for (int i = 0; i < capacity; i++) {
            long key1 = getFirstValueAt(oldKeys, i);
            long key2 = getSecondValueAt(oldKeys, i);
            if (key1 != noEntryKeyValue || key2 != noEntryKeyValue) {
                int index = keyIndex(key1, key2);
                setAt(index, key1, key2);
            }
        }
        capacity = newCapacity;
    }

    private void setAt(int index, long key1, long key2) {
        keys[index * 2] = key1;
        keys[index * 2 + 1] = key2;
    }
}
