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

import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

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

    public CharSequenceHashSet(int initialCapacity) {
        this(initialCapacity, 0.4f, 0.3f);
    }

    public CharSequenceHashSet(int initialCapacity, double loadFactor, double hashFactor) {
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
        boolean r = insertKey(key);
        if (r) {
            list.add(key);
            if (free == 0) {
                rehash();
            }
        }
        return r;
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

    public boolean remove(CharSequence key) {
        if (list.remove(key)) {
            int index = idx(key);
            if (key.equals(Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(keys, index, null);
                free++;
                return true;
            }
            return probeRemove(key, index);
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

    private boolean probeRemove(CharSequence key, int index) {
        int i = index;
        do {
            index = (index + 1) & mask;
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(keys, index, null);
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