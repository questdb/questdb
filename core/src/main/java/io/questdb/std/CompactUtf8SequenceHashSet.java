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

/**
 * Unlike {@link Utf8SequenceHashSet} doesn't keep an additional list for faster iteration and index-based access
 * and also has a slightly higher load factor. One more difference is that this set doesn't support {@code null} keys.
 */
public class CompactUtf8SequenceHashSet extends AbstractUtf8SequenceHashSet {
    private static final int MIN_INITIAL_CAPACITY = 16;

    public CompactUtf8SequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    public CompactUtf8SequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.6);
    }

    public CompactUtf8SequenceHashSet(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(@NotNull Utf8Sequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }

        addAt(index, key);
        return true;
    }

    public void addAt(int index, @NotNull Utf8Sequence key) {
        final Utf8String s = Utf8s.toUtf8String(key);
        keys[index] = s;
        hashCodes[index] = Utf8s.hashCode(key);
        if (--free < 1) {
            rehash();
        }
    }

    @Override
    public final void clear() {
        Arrays.fill(keys, null);
        free = capacity;
    }

    @Override
    public Utf8Sequence keyAt(int index) {
        int index1 = -index - 1;
        return keys[index1];
    }

    public void resetCapacity() {
        free = capacity = this.initialCapacity;
        final int len = Numbers.ceilPow2((int) (capacity / loadFactor));
        keys = new Utf8String[len];
        hashCodes = new int[len];
        mask = len - 1;
    }

    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        Utf8Sequence[] oldKeys = keys;
        int[] oldHashCodes = hashCodes;
        this.keys = new Utf8Sequence[len];
        this.hashCodes = new int[len];
        Arrays.fill(keys, null);
        mask = len - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            Utf8Sequence key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                hashCodes[index] = oldHashCodes[i];
            }
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
        hashCodes[index] = 0;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        hashCodes[to] = hashCodes[from];
        erase(from);
    }
}
