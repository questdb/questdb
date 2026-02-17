/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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


public class LowerCaseAsciiCharSequenceObjHashMap<T> extends AbstractLowerCaseAsciiCharSequenceHashSet {
    private final ObjList<CharSequence> list;
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
        values = (T[]) new Object[keys.length];
        this.list = new ObjList<>(capacity);
        clear();
    }

    public final void clear() {
        super.clear();
        list.clear();
        Arrays.fill(values, null);
    }

    public T get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(CharSequence key, T value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, CharSequence key, T value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }

        final String lcKey = Chars.toLowerCaseAscii(key);
        putAt0(index, lcKey, value);
        list.add(lcKey);
        return true;
    }

    public void putIfAbsent(CharSequence key, T value) {
        int index = keyIndex(key);
        if (index > -1) {
            putAt0(index, Chars.toLowerCaseAscii(key), value);
        }
    }

    @Override
    public void removeAt(int index) {
        if (index < 0) {
            CharSequence key = keys[-index - 1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    public T valueAt(int index) {
        return index < 0 ? values[-index - 1] : null;
    }

    private void putAt0(int index, CharSequence key, T value) {
        keys[index] = key;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    @SuppressWarnings("unchecked")
    private void rehash() {
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

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
        values[index] = null;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }
}