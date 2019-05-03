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
import java.util.Comparator;


public class CharSequenceObjHashMap<V> extends AbstractCharSequenceHashSet {
    private final ObjList<CharSequence> list;
    private V[] values;

    public CharSequenceObjHashMap() {
        this(8);
    }

    private CharSequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    private CharSequenceObjHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        this.list = new ObjList<>(capacity);
        keys = new CharSequence[capacity];
        values = (V[]) new Object[capacity];
        clear();
    }

    public final void clear() {
        super.clear();
        list.clear();
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
        Unsafe.arrayPut(values, index, null);
    }

    @Override
    protected void move(int from, int to) {
        Unsafe.arrayPut(keys, to, Unsafe.arrayGet(keys, from));
        Unsafe.arrayPut(values, to, Unsafe.arrayGet(values, from));
        erase(from);
    }

    public V get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(CharSequence key, V value) {
        return putAt(keyIndex(key), key, value);
    }

    public void putAll(CharSequenceObjHashMap<V> other) {
        CharSequence[] otherKeys = other.keys;
        V[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (Unsafe.arrayGet(otherKeys, i) != noEntryKey) {
                put(Unsafe.arrayGet(otherKeys, i), Unsafe.arrayGet(otherValues, i));
            }
        }
    }

    public boolean putAt(int index, CharSequence key, V value) {
        if (putAt0(index, key, value)) {
            list.add(key);
            return true;
        }
        return false;
    }

    public void sortKeys(Comparator<CharSequence> comparator) {
        list.sort(comparator);
    }

    public V valueAt(int index) {
        return index < 0 ? Unsafe.arrayGet(values, -index - 1) : null;
    }

    public V valueQuick(int index) {
        return get(list.getQuick(index));
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
        int size = size();
        int newCapacity = capacity * 2;
        mask = newCapacity - 1;
        free = capacity = newCapacity;
        int arrayCapacity = (int) (newCapacity / loadFactor);

        V[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[arrayCapacity];
        this.values = (V[]) new Object[arrayCapacity];
        Arrays.fill(keys, null);

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            CharSequence key = Unsafe.arrayGet(oldKeys, i);
            if (key != null) {
                final int index = keyIndex(key);
                Unsafe.arrayPut(keys, index, key);
                Unsafe.arrayPut(values, index, Unsafe.arrayGet(oldValues, i));
            }
        }
    }
}