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


public class IntHashSet extends AbstractIntHashSet {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final IntList list;

    public IntHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public IntHashSet(IntHashSet that) {
        this(that.capacity, that.loadFactor, noEntryKey);
        addAll(that);
    }

    public IntHashSet(int initialCapacity) {
        this(initialCapacity, 0.4, noEntryKey);
    }

    public IntHashSet(int initialCapacity, double loadFactor, int noKeyValue) {
        super(initialCapacity, loadFactor, noKeyValue);
        this.list = new IntList(free);
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
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

    public void addAt(int index, int key) {
        Unsafe.arrayPut(keys, index, key);
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

    public boolean excludes(int key) {
        return keyIndex(key) > -1;
    }

    public int remove(int key) {
        int keyIndex = keyIndex(key);
        if (keyIndex < 0) {
            removeAt(keyIndex);
            return -keyIndex - 1;
        }
        return -1;
    }

    public void removeAt(int index) {
        if (index < 0) {
            int key = Unsafe.arrayGet(keys, -index - 1);
            super.removeAt(index);
            list.remove(key);
        }
    }

    @Override
    protected void erase(int index) {
        Unsafe.arrayPut(keys, index, noEntryKeyValue);
    }

    @Override
    protected void move(int from, int to) {
        Unsafe.arrayPut(keys, to, Unsafe.arrayGet(keys, from));
        erase(from);
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
    public String toString() {
        return list.toString();
    }

    private void rehash() {
        int newCapacity = capacity * 2;
        mask = newCapacity - 1;
        free = capacity = newCapacity;
        int arrayCapacity = (int) (newCapacity / loadFactor);
        this.keys = new int[arrayCapacity];
        Arrays.fill(keys, noEntryKeyValue);
        int n = list.size();
        free -= n;
        for (int i = 0; i < n; i++) {
            int key = list.getQuick(i);
            int keyIndex = keyIndex(key);
            Unsafe.arrayPut(keys, keyIndex, key);
        }
    }
}