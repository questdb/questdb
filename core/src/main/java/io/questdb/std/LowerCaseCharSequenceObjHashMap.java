/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

import java.util.Arrays;
import java.util.Objects;

public class LowerCaseCharSequenceObjHashMap<T> extends AbstractLowerCaseCharSequenceHashMap {
    private T[] values;

    public LowerCaseCharSequenceObjHashMap() {
        this(8);
    }

    public LowerCaseCharSequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    public LowerCaseCharSequenceObjHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        values = (T[]) new Object[keys.length];
        clear();
    }

    public void clear() {
        super.clear();
        Arrays.fill(values, null);
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
        values[index] = null;
    }

    public T get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }

    public boolean put(CharSequence key, T value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, CharSequence key, T value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }

        putAt0(index, key, value);
        return true;
    }

    public void putIfAbsent(CharSequence key, T value) {
        int index = keyIndex(key);
        if (index > -1) {
            putAt0(index, key, value);
        }
    }

    public T valueAt(int index) {
        return index < 0 ? valueAtQuick(index) : null;
    }

    public T valueAtQuick(int index) {
        return values[-index - 1];
    }

    private void putAt0(int index, CharSequence key, T value) {
        values[index] = value;
        putAt0(index, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LowerCaseCharSequenceObjHashMap<?> that = (LowerCaseCharSequenceObjHashMap<?>) o;
        if (size() != that.size()) {
            return false;
        }
        for (CharSequence key : keys) {
            if (key == null) {
                continue;
            }
            if (that.excludes(key)) {
                return false;
            }
            Object value = get(key);
            if (value != null) {
                Object thatValue = that.get(key);
                if (!Objects.equals(value, thatValue)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int i = 0, n = keys.length; i < n; i++) {
            if (keys[i] != noEntryKey) {
                hashCode += Chars.hashCode(keys[i]) ^ Objects.hashCode(values[i]);
            }
        }
        return hashCode;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int arrayCapacity = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        T[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[arrayCapacity];
        this.values = (T[]) new Object[arrayCapacity];
        Arrays.fill(keys, null);
        mask = arrayCapacity - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            CharSequence key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                values[index] = oldValues[i];
            }
        }
    }

    public void putAll(LowerCaseCharSequenceObjHashMap<T> other) {
        CharSequence[] otherKeys = other.keys;
        T[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != noEntryKey) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }
}