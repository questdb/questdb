/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public abstract class AbstractOffsetCharSequenceHashSet implements Mutable {
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected static final int noEntryOffset = -1;
    protected final double loadFactor;
    protected int capacity;
    protected int free;
    protected int mask;
    protected int[] offsets;

    public AbstractOffsetCharSequenceHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        free = this.capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        int len = Numbers.ceilPow2((int) (this.capacity / loadFactor));
        offsets = new int[len];
        Arrays.fill(offsets, noEntryOffset);
        mask = len - 1;
    }

    @Override
    public void clear() {
        Arrays.fill(offsets, noEntryOffset);
        free = capacity;
    }

    public boolean contains(@NotNull CharSequence key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(@NotNull CharSequence key) {
        return keyIndex(key) > -1;
    }

    /**
     * Returns the index of a free slot where this key can be placed.
     * Returns the negative index of the key if it's already present.
     *
     * @param key the key whose slot to look for
     * @return the index of a free slot where this key can be placed,
     * or the negative index of the key if it's already present.
     */
    public int keyIndex(@NotNull CharSequence key) {
        int hashCode = Chars.hashCode(key);
        return keyIndex(key, hashCode);
    }

    /**
     * Returns the index of a free slot where this key can be placed.
     * Returns the negative index of the key if it's already present.
     *
     * @param key      the key whose slot to look for
     * @param hashCode the hashCode of the key
     * @return the index of a free slot where this key can be placed,
     * or the negative index of the key if it's already present.
     */
    public int keyIndex(@NotNull CharSequence key, int hashCode) {
        int index = Hash.spread(hashCode) & mask;
        int offset = offsets[index];
        if (offset == noEntryOffset) {
            return index;
        }
        if (areKeysEquals(offset, key, hashCode)) {
            return -index - 1;
        }
        return probe(key, hashCode, index);
    }

    public int size() {
        return capacity - free;
    }

    private int probe(@NotNull CharSequence key, int hashCode, int index) {
        do {
            index = (index + 1) & mask;
            if (offsets[index] == noEntryOffset) {
                return index;
            }
            if (areKeysEquals(offsets[index], key, hashCode)) {
                return -index - 1;
            }
        } while (true);
    }

    abstract protected boolean areKeysEquals(int offset, @NotNull CharSequence key, int keyHashCode);

    protected long keyAt(int index) {
        return offsets[-index - 1];
    }
}
