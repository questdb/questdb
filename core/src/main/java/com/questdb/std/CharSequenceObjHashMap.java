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

import com.questdb.std.str.NullCharSequence;

import java.util.Arrays;
import java.util.Comparator;


public class CharSequenceObjHashMap<V> implements Mutable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private static final CharSequence noEntryValue = NullCharSequence.INSTANCE;
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
        return valueAt(keyIndex(key));
    }

    public int keyIndex(CharSequence key) {
        int index = Chars.hashCode(key) & mask;

        if (Unsafe.arrayGet(keys, index) == noEntryValue) {
            return index;
        }

        if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
            return -index - 1;
        }

        return probe(key, index);
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(CharSequence key, V value) {
        return putAt(keyIndex(key), key, value);
    }

    public void remove(CharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            Unsafe.arrayPut(keys, -index - 1, noEntryValue);
            Unsafe.arrayPut(values, -index - 1, null);
            list.remove(key);
            free++;
        }
    }

    public int size() {
        return list.size();
    }

    public void sortKeys(Comparator<CharSequence> comparator) {
        list.sort(comparator);
    }

    public boolean putAt(int index, CharSequence key, V value) {
        if (putAt0(index, key, value)) {
            list.add(key);
            return true;
        }
        return false;
    }

    public V valueQuick(int index) {
        return get(list.getQuick(index));
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (Unsafe.arrayGet(keys, index) == noEntryValue) {
                return index;
            }
            if (Chars.equals(key, Unsafe.arrayGet(keys, index))) {
                return -index - 1;
            }
        } while (true);
    }

    public V valueAt(int index) {
        return index < 0 ? Unsafe.arrayGet(values, -index - 1) : null;
    }

    private boolean putAt0(int index, CharSequence key, V value) {
        if (index < 0) {
            Unsafe.arrayPut(values, -index - 1, value);
            return false;
        } else {
            Unsafe.arrayPut(keys, index, key);
            Unsafe.arrayPut(values, index, value);
            if (--free == 0) {
                rehash();
            }
            return true;
        }
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
                putAt0(keyIndex(Unsafe.arrayGet(oldKeys, i)), Unsafe.arrayGet(oldKeys, i), Unsafe.arrayGet(oldValues, i));
            }
        }
    }
}