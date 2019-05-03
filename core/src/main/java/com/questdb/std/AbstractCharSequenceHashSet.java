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

public abstract class AbstractCharSequenceHashSet implements Mutable {
    protected static final CharSequence noEntryKey = null;
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected final double loadFactor;
    protected CharSequence[] keys;
    protected int mask;
    protected int free;
    protected int capacity;

    public AbstractCharSequenceHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        free = this.capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        keys = new CharSequence[(int) (this.capacity / loadFactor)];
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

    public int remove(CharSequence key) {
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
                    CharSequence key = Unsafe.arrayGet(keys, from);
                    key != noEntryKey;
                    from = (from + 1) & mask, key = Unsafe.arrayGet(keys, from)
            ) {
                int idealHit = Chars.hashCode(key) & mask;
                if (idealHit != from) {
                    int to;
                    if (Unsafe.arrayGet(keys, idealHit) != noEntryKey) {
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

    /**
     * Erases entry in array.
     *
     * @param index always posititive, no arithmetic required.
     */
    abstract protected void erase(int index);

    abstract protected void move(int from, int to);

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
