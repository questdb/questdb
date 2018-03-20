/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.std.str.NullCharSequence;

import java.util.Arrays;

public abstract class AbstractCharSequenceHashSet implements Mutable {
    protected static final CharSequence noEntryKey = NullCharSequence.INSTANCE;
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected final double loadFactor;
    protected CharSequence[] keys;
    protected int mask;
    protected int free;
    protected int capacity;

    public AbstractCharSequenceHashSet(int initialCapacity, double loadFactor, double hashFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        if (hashFactor <= 0d || hashFactor >= 1d) {
            throw new IllegalArgumentException("0 < hashFactor < 1");
        }

        initialCapacity = (int) (initialCapacity * (1 + hashFactor));
        this.loadFactor = loadFactor;
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        keys = new CharSequence[capacity];
        free = this.capacity = capacity;
        mask = capacity - 1;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, noEntryKey);
        free = this.capacity;
    }

    public boolean excludes(CharSequence key) {
        return keyIndex(key) > -1;
    }

    public boolean excludes(CharSequence key, int lo, int hi) {
        return keyIndex(key, lo, hi) > -1;
    }

    public int keyIndex(CharSequence key) {
        int index = Chars.hashCode(key) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            return index;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public int keyIndex(CharSequence key, int lo, int hi) {
        int index = Chars.hashCode(key, lo, hi) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            return index;
        }

        CharSequence cs = Unsafe.arrayGet(keys, index);
        if (Chars.equals(key, lo, hi, cs, 0, cs.length())) {
            return -index - 1;
        }
        return probe(key, lo, hi, index);
    }

    public final boolean remove(CharSequence key) {
        return removeAt(keyIndex(key));
    }

    abstract public boolean removeAt(int index);

    public int size() {
        return capacity - free;
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                return index;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return -index - 1;
            }
        } while (true);
    }

    private int probe(CharSequence key, int lo, int hi, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                return index;
            }
            CharSequence cs = Unsafe.arrayGet(keys, index);
            if (Chars.equals(key, lo, hi, cs, 0, cs.length())) {
                return -index - 1;
            }
        } while (true);
    }
}
