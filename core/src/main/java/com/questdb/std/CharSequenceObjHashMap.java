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
import java.util.Comparator;


public class CharSequenceObjHashMap<V> implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final CharSequence noEntryValue = new NullCharSequence();
    private final double loadFactor;
    private final ObjList<CharSequence> list;
    private CharSequence[] keys;
    private V[] values;
    private int free;
    private int capacity;
    private int mask;

    public CharSequenceObjHashMap() {
        this(8);
    }

    private CharSequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    private CharSequenceObjHashMap(int initialCapacity, double loadFactor) {
        this.list = new ObjList<>(initialCapacity);
        int capacity = Math.max(initialCapacity, (int) (initialCapacity / loadFactor));
        capacity = capacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(capacity);
        this.loadFactor = loadFactor;
        keys = new CharSequence[capacity];
        values = (V[]) new Object[capacity];
        free = this.capacity = initialCapacity;
        mask = capacity - 1;
        clear();
    }

    public final void clear() {
        Arrays.fill(keys, noEntryValue);
        free = this.capacity;
        list.clear();
    }

    public V get(CharSequence key) {
        int index = Chars.hashCode(key) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return null;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            return Unsafe.arrayGet(values, index);
        }

        return probe(key, index);
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(CharSequence key, V value) {
        if (put0(key, value)) {
            list.add(key);
            if (free == 0) {
                rehash();
            }
            return true;
        }
        return false;
    }

    public void remove(CharSequence key) {
        int index = Chars.hashCode(key) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            Unsafe.arrayPut(keys, index, noEntryValue);
            Unsafe.arrayPut(values, index, null);
            list.remove(key);
            free++;
            return;
        }

        probeRemove(key, index);
    }

    public int size() {
        return list.size();
    }

    public void sortKeys(Comparator<CharSequence> comparator) {
        list.sort(comparator);
    }

    public V valueQuick(int index) {
        return get(list.getQuick(index));
    }

    private V probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return null;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return Unsafe.arrayGet(values, index);
            }
        } while (true);
    }

    private boolean probeInsert(CharSequence key, int index, V value) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, value);
                free--;
                return true;
            }

            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(values, index, value);
                return false;
            }
        } while (true);
    }

    private void probeRemove(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                Unsafe.arrayPut(keys, index, noEntryValue);
                Unsafe.arrayPut(values, index, null);
                list.remove(key);
                free++;
                return;
            }
        } while (true);
    }

    private boolean put0(CharSequence key, V value) {
        int index = Chars.hashCode(key) & mask;
        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            free--;
            return true;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            Unsafe.arrayPut(values, index, value);
            return false;
        }

        return probeInsert(key, index, value);
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {

        int newCapacity = values.length << 1;
        mask = newCapacity - 1;
        free = capacity = (int) (newCapacity * loadFactor);
        V[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[newCapacity];
        this.values = (V[]) new Object[newCapacity];
        Arrays.fill(keys, noEntryValue);

        for (int i = oldKeys.length; i-- > 0; ) {
            if (Unsafe.arrayGet(oldKeys, i) != noEntryValue) {
                put0(Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }
}