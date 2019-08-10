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

import org.omg.CORBA.Object;

import java.util.Arrays;


public class LowerCaseAsciiCharSequenceObjHashMap<T> extends AbstractLowerCaseAsciiCharSequenceHashSet {
    private final java.lang.Object noEntryValue = null;
    private T[] values;

    public LowerCaseAsciiCharSequenceObjHashMap() {
        this(8);
    }

    public LowerCaseAsciiCharSequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    public LowerCaseAsciiCharSequenceObjHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        values = (T[]) new Object[capacity];
        clear();
    }

    public final void clear() {
        super.clear();
        Arrays.fill(values, noEntryValue);
    }

    @Override
    protected void erase(int index) {
        Unsafe.arrayPut(keys, index, noEntryKey);
        Unsafe.arrayPut(values, index, noEntryValue);
    }

    @Override
    protected void move(int from, int to) {
        Unsafe.arrayPut(keys, to, Unsafe.arrayGet(keys, from));
        Unsafe.arrayPut(values, to, Unsafe.arrayGet(values, from));
        erase(from);
    }

    public boolean contains(CharSequence key) {
        return keyIndex(key) < 0;
    }

    public T get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public boolean put(CharSequence key, T value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, CharSequence key, T value) {
        if (index < 0) {
            Unsafe.arrayPut(values, -index - 1, value);
            return false;
        }
        putAt0(index, key.toString().toLowerCase(), value);
        return true;
    }

    public void putIfAbsent(CharSequence key, T value) {
        int index = keyIndex(key);
        if (index > -1) {
            putAt0(index, key.toString(), value);
        }
    }

    public T valueAt(int index) {
        return index < 0 ? Unsafe.arrayGet(values, -index - 1) : null;
    }

    private void putAt0(int index, CharSequence key, T value) {
        Unsafe.arrayPut(keys, index, key);
        Unsafe.arrayPut(values, index, value);
        if (--free == 0) {
            rehash();
        }
    }

    @SuppressWarnings("unchecked")
    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        mask = newCapacity - 1;
        free = capacity = newCapacity;
        int arrayCapacity = (int) (newCapacity / loadFactor);

        T[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[arrayCapacity];
        this.values = (T[]) new Object[arrayCapacity];
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