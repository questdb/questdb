/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Open addressing linear probing hash set specialized in storing long tuples.
 *
 * <p>
 * Currently, it implements only limited amount of methods. Feel free to add more as you need them.
 * <p>
 * <b>Note:</b>
 * Semantic of <code>capacity</code> differs from collections in the JDK. It says how many elements you
 * can store in the set without triggering rehashing. This is consistent with other collections
 * in the QuestDB project.
 * <p>
 * <b>Implementation notes:</b>
 * The whole set is backed by a single long array. It uses a concept of slots: Each slot owns a tuple of two longs
 * stored along each other for spatial locality.  Hence, when you want to get an index into a backing array you need to
 * multiply slot by two to get the index of the first long and add one to get the index of the second long.
 * <p>
 * This class is not thread safe.
 */
public final class LongLongHashSet implements Mutable, Sinkable {
    public static final SinkStrategy LONG_LONG_STRATEGY = (key1, key2, sink) -> {
        sink.putAscii('[');
        Numbers.append(sink, key1, false);
        sink.putAscii(',');
        Numbers.append(sink, key2, false);
        sink.putAscii(']');
    };
    public static final SinkStrategy UUID_STRATEGY = (key1, key2, sink) -> {
        sink.putAscii('\'');
        Numbers.appendUuid(key1, key2, sink);
        sink.putAscii('\'');
    };
    private static final int MIN_INITIAL_CAPACITY = 16;
    private final double loadFactor;
    private final long noEntryKeyValue;
    private final SinkStrategy sinkStrategy;
    private int capacity;
    private int mask;
    private int size;
    private long[] values;

    /**
     * Creates a new set with a given capacity, load factor and no entry sentinel value.
     * <p>
     * No entry sentinel value is used to indicate that a slot is empty. It means that you cannot store a tuple
     * where both longs are equal to no entry sentinel value.
     *
     * @param initialCapacity initial capacity of the set.
     * @param loadFactor      load factor of the set.
     * @param noEntryValue    no entry sentinel value.
     * @param sinkStrategy    sink strategy
     */
    public LongLongHashSet(int initialCapacity, double loadFactor, long noEntryValue, SinkStrategy sinkStrategy) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < load factor < 1");
        }
        this.noEntryKeyValue = noEntryValue;
        this.loadFactor = loadFactor;
        this.capacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        int slots = Numbers.ceilPow2((int) (this.capacity / loadFactor));
        this.values = new long[2 * slots];
        this.mask = slots - 1;
        this.sinkStrategy = sinkStrategy;
        Arrays.fill(values, noEntryKeyValue);
    }

    /**
     * Adds a tuple to the set.
     *
     * @param key1 first long of the tuple
     * @param key2 second long of the tuple
     * @return true if the tuple was added, false if it was already in the set
     */
    public boolean add(long key1, long key2) {
        if (key1 == noEntryKeyValue && key2 == noEntryKeyValue) {
            throw new IllegalArgumentException("keys cannot be NO_ENTRY_KEY (" + noEntryKeyValue + ")");
        }
        int slot = keyIndex(key1, key2);
        if (slot < 0) {
            return false;
        }
        addAt(slot, key1, key2);
        return true;
    }

    /**
     * Store key1 and key2 at slot. This method does not check if slot is occupied.
     *
     * @param slot slot to store key1 and key2 at
     * @param key1 first key
     * @param key2 second key
     */
    public void addAt(int slot, long key1, long key2) {
        set(slot, key1, key2);
        if (++size == capacity) {
            rehash();
        }
    }

    /**
     * Clears the set.
     */
    @Override
    public void clear() {
        Arrays.fill(values, noEntryKeyValue);
        size = 0;
    }

    /**
     * Check if the set contains a tuple with the given keys.
     *
     * @return true if the set contains the tuple, false otherwise
     */
    public boolean contains(long key1, long key2) {
        return keyIndex(key1, key2) < 0;
    }

    /**
     * Get a slot where a give tuple would be stored.
     * <p>
     * If the tuple is already in the set, the slot is negative and the value is the index of the tuple.
     * If the tuple is not in the set, the slot is positive and the value is the index of the first empty slot.
     *
     * @param key1 first key
     * @param key2 second key
     * @return slot index
     */
    public int keyIndex(long key1, long key2) {
        int hash = Hash.hashLong128_32(key1, key2);
        int index = hash & mask;
        return probe(key1, key2, index);
    }

    /**
     * Returns the number of elements in the set.
     *
     * @return number of elements in the set
     */
    public int size() {
        return size;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[');
        boolean pastFirst = false;
        for (int i = 0, n = values.length; i < n; i += 2) {
            long key1 = values[i];
            long key2 = values[i + 1];
            if (key1 != noEntryKeyValue || key2 != noEntryKeyValue) {
                if (pastFirst) {
                    sink.putAscii(',');
                }
                sinkStrategy.put(key1, key2, sink);
                pastFirst = true;
            }
        }
        sink.putAscii(']');
    }

    private static long firstValue(long[] val, int slot) {
        return val[slot * 2];
    }

    private static long secondValue(long[] val, int slot) {
        return val[slot * 2 + 1];
    }

    private int probe(long key1, long key2, int slot) {
        do {
            if (firstValue(values, slot) == noEntryKeyValue && secondValue(values, slot) == noEntryKeyValue) {
                return slot;
            }
            if (firstValue(values, slot) == key1 && secondValue(values, slot) == key2) {
                return -slot - 1;
            }
            slot = (slot + 1) & mask;
        } while (true);
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        int slots = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        if (slots < 0 || slots * 2 < 0) {
            throw new IllegalStateException("cannot rehash, required capacity is too large. [current-capacity=" + capacity + ", load-factor=" + loadFactor + "]");
        }
        long[] newValues = new long[2 * slots];
        Arrays.fill(newValues, noEntryKeyValue);
        mask = slots - 1;
        long[] oldKeys = this.values;
        this.values = newValues;
        int oldSlots = oldKeys.length / 2;
        for (int i = 0; i < oldSlots; i++) {
            long key1 = firstValue(oldKeys, i);
            long key2 = secondValue(oldKeys, i);
            if (key1 != noEntryKeyValue || key2 != noEntryKeyValue) {
                int slot = keyIndex(key1, key2);
                set(slot, key1, key2);
            }
        }
        capacity = newCapacity;
    }

    private void set(int slot, long key1, long key2) {
        values[slot * 2] = key1;
        values[slot * 2 + 1] = key2;
    }

    public interface SinkStrategy {
        void put(long key1, long key2, CharSink<?> sink);
    }
}
