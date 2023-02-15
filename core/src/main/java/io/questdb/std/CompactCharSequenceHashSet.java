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

/**
 * Unlike {@link CharSequenceHashSet} doesn't keep an additional list for faster iteration and index-based access
 * and also has a slightly higher load factor. One more difference is that this set doesn't support {@code null} keys.
 */
public class CompactCharSequenceHashSet extends AbstractCharSequenceHashSet {

    private static final int MIN_INITIAL_CAPACITY = 16;

    public CompactCharSequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    private CompactCharSequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.6);
    }

    private CompactCharSequenceHashSet(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        clear();
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

    public boolean contains(CharSequence key) {
        return keyIndex(key) < 0;
    }

    public boolean excludes(CharSequence key) {
        return keyIndex(key) > -1;
    }

    public CharSequence keyAt(int index) {
        int index1 = -index - 1;
        return keys[index1];
    }

    @Override
    public int remove(CharSequence key) {
        int keyIndex = keyIndex(key);
        if (keyIndex < 0) {
            removeAt(keyIndex);
            return -keyIndex - 1;
        }
        return -1;
    }

    @Override
    public String toString() {
        return Arrays.toString(keys);
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        CharSequence[] oldKeys = keys;
        keys = new CharSequence[len];
        mask = len - 1;
        for (int i = 0, n = oldKeys.length; i < n; i++) {
            CharSequence key = oldKeys[i];
            if (key != noEntryKey) {
                keys[keyIndex(key)] = key;
                free--;
            }
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        erase(from);
    }
}
