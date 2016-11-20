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
    private final ObjList<String> list;
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

    public boolean add(CharSequence key) {
        if (key == null) {
            if (hasNull) {
                return false;
            }
            hasNull = true;
            list.add(null);
            free--;
            return true;
        }

        if (insertKey(key)) {
            if (free == 0) {
                resize();
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
        hasNull = false;
    }

    public boolean contains(CharSequence key) {
        if (key == null) {
            return hasNull;
        }

        int index = idx(key);
        return Unsafe.arrayGet(keys, index) != null && (eq(index, key) || probe(key, index) > -1);
    }

    public CharSequence get(int index) {
        return list.getQuick(index);
    }

    public CharSequence getLast() {
        return list.getLast();
    }

    public int remove(CharSequence key) {
        if (key == null) {
            if (hasNull) {
                hasNull = false;
                int index = list.remove(null);
                free++;
                return index;
            }
            return -1;
        }

        int index = idx(key);

        if (Unsafe.arrayGet(keys, index) == null) {
            return -1;
        }

        if (eq(index, key)) {
            return removeAt(index);
        }

        index = probe(key, index);
        if (index < 0) {
            return -1;
        }

        return removeAt(index);
    }

    public int size() {
        return capacity - free;
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private boolean eq(int index, CharSequence key) {
        return key == Unsafe.arrayGet(keys, index) || Chars.equals(key, Unsafe.arrayGet(keys, index));
    }

    private int idx(CharSequence key) {
        return key == null ? 0 : (Chars.hashCode(key) & mask);
    }

    private boolean insertKey(CharSequence key) {
        int index = idx(key);
        if (Unsafe.arrayGet(keys, index) == null) {
            String sk = key.toString();
            Unsafe.arrayPut(keys, index, sk);
            list.add(sk);
            free--;
            return true;
        } else {
            if (eq(index, key)) {
                return false;
            }

            int next = probe(key, index);

            if (next < 0) {
                String sk = key.toString();
                Unsafe.arrayPut(keys, -next - 1, sk);
                list.add(sk);
                free--;
                return true;
            }

            return false;
        }
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;

            if (Unsafe.arrayGet(keys, index) == null) {
                return -(index + 1);
            }

            if (eq(index, key)) {
                return index;
            }

        } while (true);
    }

    private void rehash() {
        Arrays.fill(keys, null);
        for (int i = 0, n = list.size(); i < n; i++) {
            String key = list.getQuick(i);
            int idx = idx(key);
            if (Unsafe.arrayGet(keys, idx) == null) {
                Unsafe.arrayPut(keys, idx, key);
            } else {
                int next = probe(key, idx);
                assert next < 0;
                Unsafe.arrayPut(keys, -next - 1, key);
            }
        }
    }

    private int removeAt(int index) {
        int result = list.remove(Unsafe.arrayGet(keys, index));
        free++;
        rehash();
        return result;
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