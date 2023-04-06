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


public class Long256HashSet implements Mutable {

    protected static final long noEntryKey = -1;
    private static final int MIN_INITIAL_CAPACITY = 16;
    protected final double loadFactor;
    protected int capacity;
    protected int free;
    protected int mask;
    private long[] keys;

    public Long256HashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public Long256HashSet(Long256HashSet that) {
        this(that.capacity, that.loadFactor);
        for (int i = 0, n = that.keys.length; i < n; i++) {
            this.keys[i] = that.keys[i];
        }
    }

    private Long256HashSet(int initialCapacity) {
        this(initialCapacity, 0.4);
    }

    private Long256HashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }
        this.capacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        int len = Numbers.ceilPow2((int) (this.capacity / loadFactor));
        this.loadFactor = loadFactor;
        this.keys = alloc(len);
        this.mask = len - 1;
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness. 256 bit long encoded in 4 64-bit values
     *
     * @param k0 0-63 bit
     * @param k1 64-127 bit
     * @param k2 128-191 bit
     * @param k3 192-256 bit
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(long k0, long k1, long k2, long k3) {
        int index = keyIndex(k0, k1, k2, k3);
        if (index < 0) {
            return false;
        }

        addAt(index, k0, k1, k2, k3);
        return true;
    }

    public void addAt(int index, long k0, long k1, long k2, long k3) {
        setAt(index, k0, k1, k2, k3);
        if (--free < 1) {
            rehash();
        }
    }

    @Override
    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryKey);
    }

    public long k0At(int index) {
        return keys[(-index - 1) * 4];
    }

    public long k1At(int index) {
        return keys[(-index - 1) * 4 + 1];
    }

    public long k2At(int index) {
        return keys[(-index - 1) * 4 + 2];
    }

    public long k3At(int index) {
        return keys[(-index - 1) * 4 + 3];
    }

    public int keyIndex(long k0, long k1, long k2, long k3) {
        int index = hashCode0(k0, k1, k2, k3) & mask;

        if (isSlotFree(index)) {
            return index;
        }

        if (isSlotMatches(index, k0, k1, k2, k3)) {
            return -index - 1;
        }

        return probe(k0, k1, k2, k3, index);
    }

    public int size() {
        return capacity - free;
    }

    private static int hashCode0(long k0, long k1, long k2, long k3) {
        int h;
        h = (int) (k0);
        h = (int) (31 * h + k1);
        h = (int) (31 * h + k2);
        h = (int) (31 * h + k3);
        return Hash.spread(h);
    }

    private long[] alloc(int capacity) {
        return new long[capacity * 4];
    }

    private boolean isSlotFree(int index) {
        return keys[index * 4] == noEntryKey && keys[index * 4 + 1] == noEntryKey && keys[index * 4 + 2] == noEntryKey && keys[index * 4 + 3] == noEntryKey;
    }

    private boolean isSlotMatches(int index, long k0, long k1, long k2, long k3) {
        return keys[index * 4] == k0 && keys[index * 4 + 1] == k1 && keys[index * 4 + 2] == k2 && keys[index * 4 + 3] == k3;
    }

    private int probe(long k0, long k1, long k2, long k3, int index) {
        do {
            index = (index + 1) & mask;
            if (isSlotFree(index)) {
                return index;
            }
            if (isSlotMatches(index, k0, k1, k2, k3)) {
                return -index - 1;
            }
        } while (true);
    }

    private void rehash() {
        long[] old = this.keys;
        int oldSize = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        this.keys = alloc(len);
        this.mask = len - 1;
        Arrays.fill(keys, noEntryKey);
        free -= oldSize;

        for (int i = 0, n = old.length / 4; i < n; i++) {
            if (old[i * 4] == noEntryKey && old[i * 4 + 1] == noEntryKey && old[i * 4 + 2] == noEntryKey && old[i * 4 + 3] == noEntryKey) {
                continue;
            }
            int index = keyIndex(old[i * 4], old[i * 4 + 1], old[i * 4 + 2], old[i * 4 + 3]);
            setAt(index, old[i * 4], old[i * 4 + 1], old[i * 4 + 2], old[i * 4 + 3]);
        }
    }

    private void setAt(int index, long k0, long k1, long k2, long k3) {
        keys[index * 4] = k0;
        keys[index * 4 + 1] = k1;
        keys[index * 4 + 2] = k2;
        keys[index * 4 + 3] = k3;
    }
}
