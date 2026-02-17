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

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;


public class CharSequenceBoolHashMap extends AbstractCharSequenceHashSet {
    public static final boolean NO_ENTRY_VALUE = false;
    private final ObjList<CharSequence> list;
    private final boolean noEntryValue;
    private boolean[] values;

    public CharSequenceBoolHashMap() {
        this(8);
    }

    public CharSequenceBoolHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    public CharSequenceBoolHashMap(int initialCapacity, double loadFactor, boolean noEntryValue) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        this.list = new ObjList<>(capacity);
        values = new boolean[keys.length];
        clear();
    }

    @Override
    public final void clear() {
        super.clear();
        list.clear();
        Arrays.fill(values, noEntryValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof CharSequenceBoolHashMap)) {
            return false;
        }

        CharSequenceBoolHashMap other = (CharSequenceBoolHashMap) obj;
        if (size() != other.size()) {
            return false;
        }

        ObjList<CharSequence> keys = keys();
        for (int i = 0, k = keys.size(); i < k; i++) {
            CharSequence key = keys.get(i);

            if (get(key) != other.get(key)) {
                return false;
            }
        }
        return true;
    }

    public boolean get(@NotNull CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(@NotNull CharSequence key, boolean value) {
        return putAt(keyIndex(key), key, value);
    }

    public void putAll(@NotNull CharSequenceBoolHashMap other) {
        CharSequence[] otherKeys = other.keys;
        boolean[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != noEntryKey) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }

    public boolean putAt(int index, CharSequence key, boolean value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        final String keyString = Chars.toString(key);
        putAt0(index, keyString, value);
        list.add(keyString);
        return true;
    }

    public void putIfAbsent(@NotNull CharSequence key, boolean value) {
        int index = keyIndex(key);
        if (index > -1) {
            String keyString = Chars.toString(key);
            putAt0(index, keyString, value);
            list.add(keyString);
        }
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

    public boolean valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : noEntryValue;
    }

    public boolean valueQuick(int index) {
        return get(list.getQuick(index));
    }

    private void putAt0(int index, CharSequence key, boolean value) {
        keys[index] = key;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        boolean[] oldValues = values;
        CharSequence[] oldKeys = keys;
        int size = capacity - free;
        capacity = capacity * 2;
        free = capacity - size;
        mask = Numbers.ceilPow2((int) (capacity / loadFactor)) - 1;
        this.keys = new CharSequence[mask + 1];
        this.values = new boolean[mask + 1];
        for (int i = oldKeys.length - 1; i > -1; i--) {
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
        values[index] = noEntryValue;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }
}
