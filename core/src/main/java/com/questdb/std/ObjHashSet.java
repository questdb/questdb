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

import org.jetbrains.annotations.NotNull;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;


public class ObjHashSet<T> extends AbstractSet<T> implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryKey = new Object();
    private final double loadFactor;
    private final ObjList<T> list;
    private T[] keys;
    private int free;
    private int capacity;
    private int mask;

    public ObjHashSet() {
        this(MIN_INITIAL_CAPACITY);
    }

    public ObjHashSet(int initialCapacity) {
        this(initialCapacity, 0.4f, 0.3f);
    }

    @SuppressWarnings("unchecked")
    private ObjHashSet(int initialCapacity, double loadFactor, double hashFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        if (hashFactor <= 0d || hashFactor >= 1d) {
            throw new IllegalArgumentException("0 < hashFactor < 1");
        }

        initialCapacity = (int) (initialCapacity * (1 + hashFactor));
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        this.loadFactor = loadFactor;
        keys = (T[]) new Object[capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity)];
        mask = keys.length - 1;
        free = this.capacity = initialCapacity;
        this.list = new ObjList<>(free);
        clear();
    }

    public void addAll(ObjHashSet<? extends T> that) {
        for (int i = 0, n = that.size(); i < n; i++) {
            this.add(that.get(i));
        }
    }

    public boolean addAt(int index, T key) {
        if (addAt0(index, key)) {
            list.add(key);
            return true;
        }
        return false;
    }

    public T get(int index) {
        return list.getQuick(index);
    }

    @Override
    @NotNull
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    public int size() {
        return capacity - free;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        return keyIndex((T) o) < 0;
    }

    public boolean add(T key) {
        return addAt(keyIndex(key), key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object key) {
        int keyIndex = keyIndex((T) key);
        if (keyIndex < 0) {
            list.remove(Unsafe.arrayGet(keys, -keyIndex - 1));
            removeAt(keyIndex);
            return true;
        }
        return false;
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryKey);
        list.clear();
    }

    @Override
    public String toString() {
        return list.toString();
    }

    public int keyIndex(T key) {
        int index = idx(key);

        if (Unsafe.arrayGet(keys, index) == noEntryKey) {
            return index;
        }

        if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
            return -index - 1;
        }

        return probe(key, index);
    }

    private boolean addAt0(int index, T key) {
        if (index > -1) {
            Unsafe.arrayPut(keys, index, key);
            if (--free == 0) {
                rehash();
            }
            return true;
        }
        return false;
    }

    private void erase(int index) {
        Unsafe.arrayPut(keys, index, noEntryKey);
    }

    private int idx(T key) {
        return key == null ? 0 : (key.hashCode() & mask);
    }

    private void move(int from, int to) {
        Unsafe.arrayPut(keys, to, Unsafe.arrayGet(keys, from));
        erase(from);
    }

    private int probe(T key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryKey) {
                return index;
            }
            if (Unsafe.arrayGet(keys, index) == key || key.equals(Unsafe.arrayGet(keys, index))) {
                return -index - 1;
            }
        } while (true);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        int newCapacity = keys.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);

        T[] oldKeys = keys;
        this.keys = (T[]) new Object[newCapacity];
        Arrays.fill(keys, noEntryKey);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryKey) {
                T key = Unsafe.arrayGet(oldKeys, i);
                addAt0(keyIndex(key), key);
            }
        }
    }

    private void removeAt(int index) {
        if (index < 0) {
            int from = -index - 1;
            erase(from);
            free++;

            // after we have freed up a slot
            // consider non-empty keys directly below
            // they may have been a direct hit but because
            // directly hit slot wasn't empty these keys would
            // have moved.
            //
            // After slot if freed these keys require re-hash
            from = (from + 1) & mask;
            for (
                    T key = Unsafe.arrayGet(keys, from);
                    key != noEntryKey;
                    from = (from + 1) & mask, key = Unsafe.arrayGet(keys, from)
            ) {
                int idealHit = key.hashCode() & mask;
                if (idealHit != from) {
                    int to;
                    if (Unsafe.arrayGet(keys, idealHit) != noEntryKey) {
                        to = probe(key, idealHit);
                    } else {
                        to = idealHit;
                    }

                    if (to > -1) {
                        move(from, to);
                    }
                }
            }
        }
    }
}