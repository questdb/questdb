/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.misc.Chars;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;

import java.util.Arrays;


public class CharSequenceHashSet implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private final double loadFactor;
    private final ObjList<CharSequence> list;
    private CharSequence[] keys;
    private int free;
    private int capacity;
    private int mask;

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

    public boolean add(CharSequence key) {
        if (insertKey(key)) {
            list.add(key);
            if (free == 0) {
                rehash();
            }
            return true;
        }
        return false;
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
    }

    public boolean contains(CharSequence value) {
        int index = idx(value);
        return Unsafe.arrayGet(keys, index) != null && (value == Unsafe.arrayGet(keys, index) || Chars.equals(value, Unsafe.arrayGet(keys, index)) || probeContains(value, index));
    }

    public CharSequence get(int index) {
        return list.getQuick(index);
    }

    public CharSequence getLast() {
        return list.getLast();
    }

    public int size() {
        return capacity - free;
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private int idx(CharSequence key) {
        return key == null ? 0 : (Chars.hashCode(key) & mask);
    }

    private boolean insertKey(CharSequence key) {
        int index = idx(key);
        if (Unsafe.arrayGet(keys, index) == null) {
            Unsafe.arrayPut(keys, index, key);
            free--;
            return true;
        } else {
            return !(key == Unsafe.arrayGet(keys, index) || Chars.equals(key, Unsafe.arrayGet(keys, index))) && probeInsert(key, index);
        }
    }

    private boolean probeContains(CharSequence key, int index) {
        int i = index;
        do {
            index = (index + 1) & mask;

            if (Unsafe.arrayGet(keys, index) == null) {
                return false;
            }

            if (key == Unsafe.arrayGet(keys, index) || Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return true;
            }

        } while (i != index);

        return false;
    }

    private boolean probeInsert(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == null) {
                Unsafe.arrayPut(keys, index, key);
                free--;
                return true;
            }

            if (key == Unsafe.arrayGet(keys, index) || Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return false;
            }
        } while (true);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        int newCapacity = keys.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);

        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[newCapacity];
        Arrays.fill(keys, null);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != null) {
                insertKey(Unsafe.arrayGet(oldKeys, i));
            }
        }
    }
}