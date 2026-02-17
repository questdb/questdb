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

import java.util.Arrays;

/**
 * Unlike {@link CharSequenceHashSet} doesn't keep an additional list for faster iteration and index-based access
 * and also has a slightly higher load factor. One more difference is that this set doesn't support {@code null} keys.
 */
public class CompactCharSequenceHashSet implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private final int initialCapacity;
    private final double loadFactor;
    private int capacity;
    private int free;
    private String[] keys;
    private int mask;

    public CompactCharSequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    public CompactCharSequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.6);
    }

    public CompactCharSequenceHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        this.loadFactor = loadFactor;
        this.initialCapacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        resetCapacity();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(CharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }

        addAt(index, key);
        return true;
    }

    public void addAt(int index, CharSequence key) {
        final String s = Chars.toString(key);
        keys[index] = s;
        if (--free < 1) {
            rehash();
        }
    }

    @Override
    public void clear() {
        Arrays.fill(keys, null);
        free = capacity;
    }

    public boolean contains(CharSequence key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(CharSequence key) {
        return keyIndex(key) > -1;
    }

    public int keyIndex(CharSequence key) {
        int hashCode = Chars.hashCode(key);
        int index = hashCode & mask;
        if (keys[index] == null) {
            return index;
        }
        if (hashCode == keys[index].hashCode() && Chars.equals(key, keys[index])) {
            return -index - 1;
        }
        return probe(key, index, hashCode);
    }

    public void resetCapacity() {
        free = capacity = this.initialCapacity;
        final int len = Numbers.ceilPow2((int) (capacity / loadFactor));
        keys = new String[len];
        mask = len - 1;
    }

    public int size() {
        return capacity - free;
    }

    @Override
    public String toString() {
        return Arrays.toString(keys);
    }

    private int probe(CharSequence key, int index, int hashCode) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == null) {
                return index;
            }
            if (hashCode == keys[index].hashCode() && Chars.equals(key, keys[index])) {
                return -index - 1;
            }
        } while (true);
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        String[] oldKeys = keys;
        keys = new String[len];
        mask = len - 1;
        for (int i = 0, n = oldKeys.length; i < n; i++) {
            String key = oldKeys[i];
            if (key != null) {
                keys[keyIndex(key)] = key;
                free--;
            }
        }
    }
}
