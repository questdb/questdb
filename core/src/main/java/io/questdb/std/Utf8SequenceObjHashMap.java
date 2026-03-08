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


public class Utf8SequenceObjHashMap<V> extends AbstractUtf8SequenceHashSet {
    private final ObjList<Utf8String> list;
    private V[] values;

    public Utf8SequenceObjHashMap() {
        this(8);
    }

    public Utf8SequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    private Utf8SequenceObjHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        this.list = new ObjList<>(capacity);
        values = (V[]) new Object[keys.length];
        clear();
    }

    public final void clear() {
        super.clear();
        list.clear();
    }

    public V get(@NotNull Utf8Sequence key) {
        return valueAt(keyIndex(key));
    }

    public ObjList<Utf8String> keys() {
        return list;
    }

    public boolean put(@NotNull Utf8String key, V value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean put(@NotNull Utf8Sequence key, V value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, @NotNull Utf8Sequence key, V value) {
        assert value != null;
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

    public boolean putAt(int index, @NotNull Utf8String key, V value) {
        assert value != null;
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

    public V valueAt(int index) {
        return index < 0 ? valueAtQuick(index) : null;
    }

    public V valueAtQuick(int index) {
        return values[-index - 1];
    }

    public V valueQuick(int index) {
        return get(list.getQuick(index));
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        Utf8Sequence[] oldKeys = keys;
        int[] oldHashCodes = hashCodes;
        V[] oldValues = values;
        this.keys = new Utf8Sequence[len];
        this.hashCodes = new int[len];
        this.values = (V[]) new Object[len];
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
        values[index] = null;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        hashCodes[to] = hashCodes[from];
        values[to] = values[from];
        erase(from);
    }
}
