/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections;

import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.util.Arrays;


public class ObjHashSet<T> {

    public static final int MIN_INITIAL_CAPACITY = 16;
    private static final Object noEntryValue = new Object();
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
        this(initialCapacity, 0.5, 0.3);
    }

    @SuppressWarnings("unchecked")
    public ObjHashSet(int initialCapacity, double loadFactor, double hashFactor) {
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
        list = new ObjList<>(free);
        clear();
    }

    public boolean add(T key) {
        boolean r = insertKey(key);
        if (r) {
            list.add(key);
            if (free == 0) {
                rehash();
            }
        }
        return r;
    }

    public void addAll(ObjHashSet<T> that) {
        for (int i = 0, k = that.size(); i < k; i++) {
            add(that.get(i));
        }
    }

    public final void clear() {
        Arrays.fill(keys, noEntryValue);
        list.clear();
    }

    public boolean contains(T key) {
        int index = key.hashCode() & mask;
        return Unsafe.arrayGet(keys, index) != noEntryValue && (key == Unsafe.arrayGet(keys, index) || key.equals(Unsafe.arrayGet(keys, index)) || probeContains(key, index));
    }

    public T get(int index) {
        return list.getQuick(index);
    }

    public boolean remove(T key) {
        if (list.remove(key)) {
            int index = key.hashCode() & mask;

            if (key == Unsafe.arrayGet(keys, index) || key.equals(Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(keys, index, noEntryValue);
                free++;
                return true;
            }

            probeRemove(key, index);
            return true;
        }
        return false;
    }

    public int size() {
        return capacity - free;
    }

    @Override
    public String toString() {
        return list.toString();
    }

    @SuppressWarnings({"unchecked"})
    protected void rehash() {
        int newCapacity = keys.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);

        T[] oldKeys = keys;
        this.keys = (T[]) new Object[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryValue) {
                insertKey(Unsafe.arrayGet(oldKeys, i));
            }
        }
    }

    private boolean insertKey(T key) {
        int index = key.hashCode() & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            free--;
            return true;
        }
        return !(key == Unsafe.arrayGet(keys, index) || key.equals(Unsafe.arrayGet(keys, index))) && probeInsert(key, index);
    }

    private boolean probeInsert(T key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                free--;
                return true;
            }

            if (key.equals(Unsafe.arrayGet(keys, index))) {
                return false;
            }
        } while (true);
    }

    private boolean probeContains(T key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return false;
            }

            if (key.equals(Unsafe.arrayGet(keys, index))) {
                return true;
            }
        } while (true);
    }

    private void probeRemove(T key, int index) {
        do {
            index = (index + 1) & mask;
            if (key == Unsafe.arrayGet(keys, index) || key.equals(Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(keys, index, noEntryValue);
                free++;
                break;
            }
        } while (true);
    }
}
