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

public abstract class AbstractIntHashSet implements Mutable {
    protected static final int noEntryKey = -1;
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected final double loadFactor;
    protected final int noEntryKeyValue;
    protected int[] keys;
    protected int mask;
    protected int free;
    protected int capacity;

    public AbstractIntHashSet(int initialCapacity, double loadFactor) {
        this(initialCapacity, loadFactor, noEntryKey);
    }

    public AbstractIntHashSet(int initialCapacity, double loadFactor, int noKeyValue) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.noEntryKeyValue = noKeyValue;
        free = this.capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        keys = new int[(int) (this.capacity / loadFactor)];
        mask = capacity - 1;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, noEntryKeyValue);
        free = this.capacity;
    }

    public boolean excludes(int key) {
        return keyIndex(key) > -1;
    }

    public int keyIndex(int key) {
        int index = key & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryKeyValue) {
            return index;
        }

        if (key == Unsafe.arrayGet(keys, index)) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public int remove(int key) {
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
                    int key = Unsafe.arrayGet(keys, from);
                    key != noEntryKeyValue;
                    from = (from + 1) & mask, key = Unsafe.arrayGet(keys, from)
            ) {
                int idealHit = key & mask;
                if (idealHit != from) {
                    int to;
                    if (Unsafe.arrayGet(keys, idealHit) != noEntryKeyValue) {
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

    public int size() {
        return capacity - free;
    }

    abstract protected void erase(int index);

    abstract protected void move(int from, int to);

    private int probe(int key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKeyValue) {
                return index;
            }
            if (key == Unsafe.arrayGet(keys, index)) {
                return -index - 1;
            }
        } while (true);
    }
}
