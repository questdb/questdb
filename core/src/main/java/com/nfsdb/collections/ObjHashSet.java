/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.collections;

import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;


public class ObjHashSet<T> extends AbstractSet<T> implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 16;
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
        this(initialCapacity, 0.4f, 0.3f);
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
        this.list = new ObjList<>(free);
        clear();
    }

    public T get(int index) {
        return list.getQuick(index);
    }

    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"})
    @Override
    @NotNull
    public Iterator<T> iterator() {
        return list.iterator();
    }

    public int size() {
        return capacity - free;
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

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object key) {
        if (list.remove(key)) {
            int index = idx((T) key);
            if (key.equals(Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(keys, index, noEntryValue);
                free++;
                return true;
            }
            return probeRemove(key, index);
        }
        return false;
    }

    public final void clear() {
        free = capacity;
        Arrays.fill(keys, noEntryValue);
        list.clear();
    }

    @Override
    public String toString() {
        return list.toString();
    }

    private int idx(T key) {
        return key == null ? 0 : (key.hashCode() & mask);
    }

    private boolean insertKey(T key) {
        int index = idx(key);
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            free--;
            return true;
        } else {
            return !(key == Unsafe.arrayGet(keys, index) || key.equals(Unsafe.arrayGet(keys, index))) && probeInsert(key, index);
        }
    }

    private boolean probeInsert(T key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                free--;
                return true;
            }

            if (key == Unsafe.arrayGet(keys, index) || key.equals(Unsafe.arrayGet(keys, index))) {
                return false;
            }
        } while (true);
    }

    private boolean probeRemove(Object key, int index) {
        int i = index;
        do {
            index = (index + 1) & mask;
            if (key.equals(Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(keys, index, noEntryValue);
                free++;
                return true;
            }
        } while (i != index);
        return false;
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
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
}