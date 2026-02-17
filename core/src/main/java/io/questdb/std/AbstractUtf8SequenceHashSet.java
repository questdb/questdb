/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public abstract class AbstractUtf8SequenceHashSet implements Mutable {
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected static final Utf8String noEntryKey = null;
    protected final int initialCapacity;
    protected final double loadFactor;
    protected int capacity;
    protected int free;
    protected int[] hashCodes;
    protected Utf8Sequence[] keys;
    protected int mask;

    public AbstractUtf8SequenceHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        this.loadFactor = loadFactor;
        this.initialCapacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        free = capacity = this.initialCapacity;

        final int len = Numbers.ceilPow2((int) (capacity / loadFactor));
        keys = new Utf8String[len];
        hashCodes = new int[len];
        mask = len - 1;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, noEntryKey);
        free = capacity;
    }

    public boolean contains(@NotNull Utf8Sequence key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(@NotNull Utf8Sequence key) {
        return keyIndex(key) > -1;
    }

    public Utf8Sequence keyAt(int index) {
        return keys[-index - 1];
    }

    public int keyIndex(@NotNull Utf8Sequence key) {
        int hashCode = Utf8s.hashCode(key);
        int index = Hash.spread(hashCode) & mask;
        if (keys[index] == noEntryKey) {
            return index;
        }
        if (hashCode == hashCodes[index] && Utf8s.equals(key, keys[index])) {
            return -index - 1;
        }
        return probe(key, hashCode, index);
    }

    public int keyIndex(@NotNull Utf8Sequence key, int lo, int hi) {
        int hashCode = Utf8s.hashCode(key, lo, hi);
        int index = Hash.spread(hashCode) & mask;
        if (keys[index] == noEntryKey) {
            return index;
        }
        Utf8Sequence us = keys[index];
        if (hashCode == hashCodes[index] && Utf8s.equals(key, lo, hi, us, 0, us.size())) {
            return -index - 1;
        }
        return probe(key, hashCode, lo, hi, index);
    }

    public int remove(@NotNull Utf8Sequence key) {
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
                    Utf8Sequence key = keys[from];
                    key != noEntryKey;
                    from = (from + 1) & mask, key = keys[from]
            ) {
                int hashCode = Utf8s.hashCode(key);
                int idealHit = Hash.spread(hashCode) & mask;
                if (idealHit != from) {
                    int to;
                    if (keys[idealHit] != noEntryKey) {
                        to = probe(key, hashCode, idealHit);
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

    private int probe(Utf8Sequence key, int hashCode, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryKey) {
                return index;
            }
            if (hashCode == hashCodes[index] && Utf8s.equals(key, keys[index])) {
                return -index - 1;
            }
        } while (true);
    }

    private int probe(Utf8Sequence key, int hashCode, int lo, int hi, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryKey) {
                return index;
            }
            Utf8Sequence cs = keys[index];
            if (hashCode == hashCodes[index] && Utf8s.equals(key, lo, hi, cs, 0, cs.size())) {
                return -index - 1;
            }
        } while (true);
    }

    /**
     * Erases entry in array.
     *
     * @param index always positive, no arithmetic required.
     */
    abstract protected void erase(int index);

    abstract protected void move(int from, int to);
}
