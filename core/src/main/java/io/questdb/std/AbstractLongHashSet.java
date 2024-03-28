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

import java.util.Arrays;

public abstract class AbstractLongHashSet implements Mutable {
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected static final long noEntryKey = -1;
    protected final int initialCapacity;
    protected final double loadFactor;
    protected final long noEntryKeyValue;
    protected int capacity;
    protected int free;
    protected long[] keys;
    protected int mask;

    public AbstractLongHashSet(int initialCapacity, double loadFactor) {
        this(initialCapacity, loadFactor, noEntryKey);
    }

    public AbstractLongHashSet(int initialCapacity, double loadFactor, long noKeyValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.noEntryKeyValue = noKeyValue;
        free = capacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        this.initialCapacity = capacity;
        this.loadFactor = loadFactor;
        keys = new long[Numbers.ceilPow2((int) (this.capacity / loadFactor))];
        mask = keys.length - 1;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, noEntryKeyValue);
        free = capacity;
    }

    public boolean excludes(long key) {
        return keyIndex(key) > -1;
    }

    public int keyIndex(long key) {
        int hashCode = Hash.hashLong32(key);
        int index = hashCode & mask;
        if (keys[index] == noEntryKeyValue) {
            return index;
        }
        if (key == keys[index]) {
            return -index - 1;
        }
        return probe(key, index);
    }

    public int remove(long key) {
        int index = keyIndex(key);
        if (index < 0) {
            removeAt(index);
            return -index - 1;
        }
        return -1;
    }

    public void removeAt(int index) {
        if (index < 0) {
            int from = -index - 1;
            erase(from);
            free++;

            // after we have freed up a slot
            // consider non-empty keys directly below
            // they may have been a direct hit but because
            // directly hit slot wasn't empty these keys would
            // have moved.
            //
            // After slot if freed these keys require re-hash
            from = (from + 1) & mask;
            for (
                    long key = keys[from];
                    key != noEntryKeyValue;
                    from = (from + 1) & mask, key = keys[from]
            ) {
                int hashCode = Hash.hashLong32(key);
                int idealHit = hashCode & mask;
                if (idealHit != from) {
                    int to;
                    if (keys[idealHit] != noEntryKeyValue) {
                        to = probe(key, idealHit);
                    } else {
                        to = idealHit;
                    }

                    if (to > -1) {
                        move(from, to);
                    }
                }
            }
        }
    }

    public void restoreInitialCapacity() {
        capacity = initialCapacity;
        keys = new long[Numbers.ceilPow2((int) (capacity / loadFactor))];
        mask = keys.length - 1;
        clear();
    }

    public int size() {
        return capacity - free;
    }

    private int probe(long key, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryKeyValue) {
                return index;
            }
            if (key == keys[index]) {
                return -index - 1;
            }
        } while (true);
    }

    abstract protected void erase(int index);

    abstract protected void move(int from, int to);
}
