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


// TODO [amunra]: Port test cases.

public class CharSequenceLongHashMap extends AbstractCharSequenceHashSet {
    public static final int NO_ENTRY_VALUE = -1;
    private final long noEntryValue;
    private final ObjList<CharSequence> list;
    private long[] values;

    public CharSequenceLongHashMap() {
        this(8);
    }

    public CharSequenceLongHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    public CharSequenceLongHashMap(int initialCapacity, double loadFactor, long noEntryValue) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        this.list = new ObjList<>(capacity);
        values = new long[keys.length];
        clear();
    }

    @Override
    public final void clear() {
        super.clear();
        list.clear();
        Arrays.fill(values, noEntryValue);
    }

    public boolean contains(CharSequence key) {
        return keyIndex(key) < 0;
    }

    public long get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    @Override
    public void removeAt(int index) {
        if (index < 0) {
            int index1 = -index - 1;
            CharSequence key = keys[index1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
        values[index] = noEntryValue;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }

    public void increment(CharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            values[-index - 1] = values[-index - 1] + 1;
        } else {
            putAt0(index, Chars.toString(key), 0);
        }
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(CharSequence key, long value) {
        return putAt(keyIndex(key), key, value);
    }

    public void putAll(CharSequenceLongHashMap other) {
        CharSequence[] otherKeys = other.keys;
        long[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != noEntryKey) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }

    public boolean putAt(int index, CharSequence key, long value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        final String keyString = Chars.toString(key);
        putAt0(index, keyString, value);
        list.add(keyString);
        return true;
    }

    public void putIfAbsent(CharSequence key, long value) {
        int index = keyIndex(key);
        if (index > -1) {
            String keyString = Chars.toString(key);
            putAt0(index, keyString, value);
            list.add(keyString);
        }
    }

    public long valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : noEntryValue;
    }

    public long valueQuick(int index) {
        return get(list.getQuick(index));
    }

    private void putAt0(int index, CharSequence key, long value) {
        keys[index] = key;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        long[] oldValues = values;
        CharSequence[] oldKeys = keys;
        int size = capacity - free;
        capacity = capacity * 2;
        free = capacity - size;
        mask = Numbers.ceilPow2((int) (capacity / loadFactor)) - 1;
        this.keys = new CharSequence[mask + 1];
        this.values = new long[mask + 1];
        for (int i = oldKeys.length - 1; i > -1; i--) {
            CharSequence key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                values[index] = oldValues[i];
            }
        }
    }
}
