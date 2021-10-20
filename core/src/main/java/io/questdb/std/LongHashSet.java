/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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


public class LongHashSet extends AbstractLongHashSet {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final LongList list;

    public LongHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public LongHashSet(LongHashSet that) {
        this(that.capacity, that.loadFactor, noEntryKey);
        addAll(that);
    }

    public LongHashSet(int initialCapacity) {
        this(initialCapacity, 0.4, noEntryKey);
    }

    public LongHashSet(int initialCapacity, double loadFactor, long noKeyValue) {
        super(initialCapacity, loadFactor, noKeyValue);
        this.list = new LongList(free);
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
     */
    public void add(long key) {
        int index = keyIndex(key);
        if (index < 0) {
            return;
        }

        addAt(index, key);
    }

    public final void addAll(LongHashSet that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public void addAt(int index, long key) {
        keys[index] = key;
        list.add(key);
        if (--free < 1) {
            rehash();
        }
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryKeyValue
        );
        list.clear();
    }

    public boolean excludes(long key) {
        return keyIndex(key) > -1;
    }

    public void removeAt(int index) {
        if (index < 0) {
            long key = keys[-index - 1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKeyValue;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        erase(from);
    }

    public boolean contains(long key) {
        return keyIndex(key) < 0;
    }

    public long get(int index) {
        return list.getQuick(index);
    }

    public long getLast() {
        return list.getLast();
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        this.keys = new long[len];
        Arrays.fill(keys, noEntryKeyValue);
        mask = len - 1;
        int n = list.size();
        free -= n;
        for (int i = 0; i < n; i++) {
            long key = list.getQuick(i);
            int keyIndex = keyIndex(key);
            keys[keyIndex] = key;
        }
    }
}