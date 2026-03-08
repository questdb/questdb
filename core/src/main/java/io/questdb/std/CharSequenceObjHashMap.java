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
import java.util.Comparator;


public class CharSequenceObjHashMap<V> extends AbstractCharSequenceHashSet {
    private final ObjList<CharSequence> list;
    private V[] values;

    public CharSequenceObjHashMap() {
        this(8);
    }

    public CharSequenceObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5);
    }

    @SuppressWarnings("unchecked")
    private CharSequenceObjHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        this.list = new ObjList<>(capacity);
        values = (V[]) new Object[keys.length];
        clear();
    }

    public final void clear() {
        super.clear();
        list.clear();
    }

    public V get(@NotNull CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public V getAt(int index) {
        return get(list.getQuick(index));
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(@NotNull CharSequence key, V value) {
        return putAt(keyIndex(key), key, value);
    }

    public void putAll(@NotNull CharSequenceObjHashMap<V> other) {
        CharSequence[] otherKeys = other.keys;
        V[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != noEntryKey) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }

    public boolean putAt(int index, @NotNull CharSequence key, V value) {
        assert value != null;
        if (putAt0(index, key, value)) {
            list.add(key);
            return true;
        }
        return false;
    }

    public void removeAt(int index) {
        if (index < 0) {
            CharSequence key = keys[-index - 1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    public void sortKeys(Comparator<CharSequence> comparator) {
        list.sort(comparator);
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

    private boolean putAt0(int index, CharSequence key, V value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        } else {
            keys[index] = key;
            values[index] = value;
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
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        V[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[len];
        this.values = (V[]) new Object[len];
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