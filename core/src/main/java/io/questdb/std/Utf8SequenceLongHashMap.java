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


import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;


public class Utf8SequenceLongHashMap extends AbstractUtf8SequenceHashSet {
    public static final long NO_ENTRY_VALUE = -1;
    private final ObjList<Utf8String> list;
    private final long noEntryValue;
    private long[] values;

    public Utf8SequenceLongHashMap() {
        this(8);
    }

    public Utf8SequenceLongHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    public Utf8SequenceLongHashMap(int initialCapacity, double loadFactor, long noEntryValue) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        this.list = new ObjList<>(capacity);
        values = new long[keys.length];
        clear();
    }

    public final void clear() {
        super.clear();
        list.clear();
        Arrays.fill(values, noEntryValue);
    }

    public long get(@NotNull Utf8Sequence key) {
        return valueAt(keyIndex(key));
    }

    public void inc(@NotNull Utf8Sequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            values[-index - 1]++;
        } else {
            putAt(index, key, 1);
        }
    }

    public ObjList<Utf8String> keys() {
        return list;
    }

    public boolean put(@NotNull Utf8String key, long value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean put(@NotNull Utf8Sequence key, long value) {
        return putAt(keyIndex(key), key, value);
    }

    public void putAll(@NotNull Utf8SequenceLongHashMap other) {
        Utf8Sequence[] otherKeys = other.keys;
        long[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != noEntryKey) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }

    public boolean putAt(int index, @NotNull Utf8Sequence key, long value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        Utf8String onHeapKey = Utf8String.newInstance(key);
        keys[index] = onHeapKey;
        hashCodes[index] = Utf8s.hashCode(key);
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
        list.add(onHeapKey);
        return true;
    }

    public boolean putAt(int index, @NotNull Utf8String key, long value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        keys[index] = key;
        hashCodes[index] = Utf8s.hashCode(key);
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
        list.add(key);
        return true;
    }

    public void removeAt(int index) {
        if (index < 0) {
            Utf8Sequence key = keys[-index - 1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    public long valueAt(int index) {
        return index < 0 ? valueAtQuick(index) : noEntryValue;
    }

    public long valueAtQuick(int index) {
        return values[-index - 1];
    }

    public long valueQuick(int index) {
        return get(list.getQuick(index));
    }

    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        Utf8Sequence[] oldKeys = keys;
        int[] oldHashCodes = hashCodes;
        long[] oldValues = values;
        this.keys = new Utf8Sequence[len];
        this.hashCodes = new int[len];
        this.values = new long[len];
        Arrays.fill(keys, null);
        mask = len - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            Utf8Sequence key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                hashCodes[index] = oldHashCodes[i];
                values[index] = oldValues[i];
            }
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKey;
        hashCodes[index] = 0;
        values[index] = noEntryValue;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        hashCodes[to] = hashCodes[from];
        values[to] = values[from];
        erase(from);
    }
}
