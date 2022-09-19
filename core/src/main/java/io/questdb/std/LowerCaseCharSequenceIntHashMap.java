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

public class LowerCaseCharSequenceIntHashMap extends AbstractLowerCaseCharSequenceHashMap {
    private static final int NO_ENTRY_VALUE = -1;
    private final int noEntryValue;
    private int[] values;

    public LowerCaseCharSequenceIntHashMap() {
        this(8);
    }

    public LowerCaseCharSequenceIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    public LowerCaseCharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        values = new int[keys.length];
        clear();
    }

    @Override
    public void clear() {
        super.clear();
        Arrays.fill(values, noEntryValue);
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
        values[index] = noEntryValue;
    }

    public int valueAt(int index) {
        return index < 0 ? values[-index - 1] : noEntryValue;
    }

    public int get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public boolean put(CharSequence key, int value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, CharSequence key, int value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        final String keyString = Chars.toString(key);
        putAt0(index, keyString, value);
        return true;
    }

    public boolean putIfAbsent(CharSequence key, int value) {
        int index = keyIndex(key);
        if (index > -1) {
            String keyStr = Chars.toString(key);
            putAt0(index, keyStr, value);
            return true;
        }
        return false;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }

    private void putAt0(int index, CharSequence key, int value) {
        values[index] = value;
        putAt0(index, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LowerCaseCharSequenceIntHashMap that = (LowerCaseCharSequenceIntHashMap) o;
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
            int value = get(key);
            if (value != noEntryValue) {
                int thatValue = that.get(key);
                if (value != thatValue) {
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
                hashCode += Chars.hashCode(keys[i]) ^ values[i];
            }
        }
        return hashCode;
    }

    @Override
    protected void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        int[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[len];
        this.values = new int[len];
        Arrays.fill(keys, null);
        mask = len - 1;

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

    public int removeEntry(CharSequence key) {
        int index = keyIndex(key);
        int value = valueAt(index);
        removeAt(index);
        return value;
    }
}