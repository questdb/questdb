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

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;


public class IntHashSet extends AbstractIntHashSet implements Sinkable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final IntList list;

    public IntHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    public IntHashSet(IntHashSet that) {
        this(that.capacity, that.loadFactor, noEntryKey);
        addAll(that);
    }

    public IntHashSet(int initialCapacity) {
        this(initialCapacity, 0.4, noEntryKey);
    }

    public IntHashSet(int initialCapacity, double loadFactor, int noKeyValue) {
        super(initialCapacity, loadFactor, noKeyValue);
        list = new IntList(free);
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key key to be added.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(int key) {
        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }

        addAt(index, key);
        return true;
    }

    public final void addAll(IntHashSet that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public final void addAll(IntList that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public void addAt(int index, int key) {
        keys[index] = key;
        list.add(key);
        if (--free < 1) {
            rehash();
        }
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryKeyValue);
        list.clear();
    }

    public boolean contains(int key) {
        return keyIndex(key) < 0;
    }

    public int get(int index) {
        return list.getQuick(index);
    }

    public int getLast() {
        return list.getLast();
    }

    @Override
    public int remove(int key) {
        int keyIndex = keyIndex(key);
        if (keyIndex < 0) {
            removeAt(keyIndex);
            return -keyIndex - 1;
        }
        return -1;
    }

    @Override
    public void removeAt(int index) {
        if (index < 0) {
            int index1 = -index - 1;
            int key = keys[index1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        list.toSink(sink, noEntryKeyValue);
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        keys = new int[len];
        Arrays.fill(keys, noEntryKeyValue);
        mask = len - 1;
        int n = list.size();
        free -= n;
        for (int i = 0; i < n; i++) {
            final int key = list.getQuick(i);
            keys[keyIndex(key)] = key;
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
}
