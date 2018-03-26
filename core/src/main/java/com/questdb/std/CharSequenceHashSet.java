/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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


public class CharSequenceHashSet implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final double loadFactor;
    private final ObjList<CharSequence> list;
    private CharSequence[] keys;
    private int free;
    private int capacity;
    private int mask;
    private boolean hasNull = false;

    public CharSequenceHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    public CharSequenceHashSet(CharSequenceHashSet that) {
        this(that.capacity, that.loadFactor, 0.3);
        addAll(that);
    }

    private CharSequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.4, 0.3);
    }

    private CharSequenceHashSet(int initialCapacity, double loadFactor, double hashFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        if (hashFactor <= 0d || hashFactor >= 1d) {
            throw new IllegalArgumentException("0 < hashFactor < 1");
        }

        initialCapacity = (int) (initialCapacity * (1 + hashFactor));
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        this.loadFactor = loadFactor;
        keys = new CharSequence[capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity)];
        mask = keys.length - 1;
        free = this.capacity = initialCapacity;
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

        Unsafe.arrayPut(keys, index, key);
        list.add(key);
        if (--free < 1) {
            resize();
        }
        return true;
    }

    public final void addAll(CharSequenceHashSet that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, null);
        list.clear();
        hasNull = false;
    }

    public boolean contains(CharSequence key) {
        return key == null ? hasNull : keyIndex(key) < 0;
    }

    public boolean excludes(CharSequence key) {
        return key == null ? !hasNull : keyIndex(key) > -1;
    }

    public CharSequence get(int index) {
        return list.getQuick(index);
    }

    public CharSequence getLast() {
        return list.getLast();
    }

    public int keyIndex(CharSequence key) {
        int index = Chars.hashCode(key) & mask;
        if (Unsafe.arrayGet(keys, index) == null) {
            return index;
        }
        if (eq(index, key)) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public int keyIndex(CharSequence key, int lo, int hi) {
        int index = Chars.hashCode(key, lo, hi) & mask;

        if (Unsafe.arrayGet(keys, index) == null) {
            return index;
        }

        CharSequence cs = Unsafe.arrayGet(keys, index);
        if (Chars.equals(key, lo, hi, cs, 0, cs.length())) {
            return -index - 1;
        }
        return probe(key, lo, hi, index);
    }

    public int remove(CharSequence key) {
        if (key == null) {
            return removeNull();
        }
        return removeAt(keyIndex(key));
    }

    public int removeAt(int index) {
        if (index < 0) {
            int result = list.remove(Unsafe.arrayGet(keys, -index - 1));
            Unsafe.arrayPut(keys, -index - 1, null);
            free++;
            rehash();
            return result;
        }

        return -1;
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

    public int size() {
        return capacity - free;
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private boolean addNull() {
        if (hasNull) {
            return false;
        }
        --free;
        hasNull = true;
        list.add(null);
        return true;
    }

    private boolean eq(int index, CharSequence key) {
        return key == Unsafe.arrayGet(keys, index) || Chars.equals(key, Unsafe.arrayGet(keys, index));
    }

    private int probe(CharSequence key, int lo, int hi, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == null) {
                return index;
            }
            CharSequence cs = Unsafe.arrayGet(keys, index);
            if (Chars.equals(key, lo, hi, cs, 0, cs.length())) {
                return -index - 1;
            }
        } while (true);
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;

            if (Unsafe.arrayGet(keys, index) == null) {
                return index;
            }

            if (eq(index, key)) {
                return -index - 1;
            }

        } while (true);
    }

    private void rehash() {
        Arrays.fill(keys, null);
        for (int i = 0, n = list.size(); i < n; i++) {
            CharSequence key = list.getQuick(i);
            Unsafe.arrayPut(keys, keyIndex(key), key);
        }
    }

    @SuppressWarnings({"unchecked"})
    private void resize() {
        int newCapacity = keys.length << 1;
        mask = newCapacity - 1;
        int oldCapacity = this.capacity;
        capacity = (int) (newCapacity * loadFactor);
        free = capacity - oldCapacity;
        this.keys = new CharSequence[newCapacity];
        rehash();
    }
}