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


public class CharSequenceHashSet extends AbstractCharSequenceHashSet {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final ObjList<CharSequence> list;
    private boolean hasNull = false;

    public CharSequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public CharSequenceHashSet(CharSequenceHashSet that) {
        this(that.capacity, that.loadFactor);
        addAll(that);
    }

    private CharSequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.4);
    }

    private CharSequenceHashSet(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        this.list = new ObjList<>(free);
        clear();
    }

    /**
     * Adds key to hash set preserving key uniqueness.
     *
     * @param key immutable sequence of characters.
     * @return false if key is already in the set and true otherwise.
     */
    public boolean add(CharSequence key) {
        if (key == null) {
            return addNull();
        }

        int index = keyIndex(key);
        if (index < 0) {
            return false;
        }

        addAt(index, key);
        return true;
    }

    public final void addAll(CharSequenceHashSet that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public void addAt(int index, CharSequence key) {
        String s = key.toString();
        Unsafe.arrayPut(keys, index, s);
        list.add(s);
        if (--free < 1) {
            rehash();
        }
    }

    public boolean addNull() {
        if (hasNull) {
            return false;
        }
        --free;
        hasNull = true;
        list.add(null);
        return true;
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, null);
        list.clear();
        hasNull = false;
    }

    public boolean excludes(CharSequence key) {
        return key == null ? !hasNull : keyIndex(key) > -1;
    }

    public int remove(CharSequence key) {
        if (key == null) {
            return removeNull();
        }

        int keyIndex = keyIndex(key);
        if (keyIndex < 0) {
            removeAt(keyIndex);
            return -keyIndex - 1;
        }
        return -1;
    }

    public void removeAt(int index) {
        if (index < 0) {
            CharSequence key = Unsafe.arrayGet(keys, -index - 1);
            super.removeAt(index);
            list.remove(key);
        }
    }

    @Override
    protected void erase(int index) {
        Unsafe.arrayPut(keys, index, noEntryKey);
    }

    @Override
    protected void move(int from, int to) {
        Unsafe.arrayPut(keys, to, Unsafe.arrayGet(keys, from));
        erase(from);
    }

    public boolean contains(CharSequence key) {
        return key == null ? hasNull : keyIndex(key) < 0;
    }

    public CharSequence get(int index) {
        return list.getQuick(index);
    }

    public CharSequence getLast() {
        return list.getLast();
    }

    public int getListIndexAt(int keyIndex) {
        return list.indexOf(Unsafe.arrayGet(keys, -keyIndex - 1));
    }

    public CharSequence keyAt(int index) {
        return Unsafe.arrayGet(keys, -index - 1);
    }

    public int removeNull() {
        if (hasNull) {
            hasNull = false;
            int index = list.remove(null);
            free++;
            return index;
        }
        return -1;
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
        this.keys = new CharSequence[arrayCapacity];
        Arrays.fill(keys, null);
        int n = list.size();
        free -= n;
        for (int i = 0; i < n; i++) {
            CharSequence key = list.getQuick(i);
            int keyIndex = keyIndex(key);
            Unsafe.arrayPut(keys, keyIndex, key);
        }
    }
}