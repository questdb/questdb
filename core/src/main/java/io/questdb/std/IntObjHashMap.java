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


public class IntObjHashMap<V> extends AbstractIntHashSet {
    private V[] values;

    public IntObjHashMap() {
        this(8);
    }

    public IntObjHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f, noEntryKey);
    }

    @SuppressWarnings("unchecked")
    public IntObjHashMap(int initialCapacity, double loadFactor, int noKeyValue) {
        super(initialCapacity, loadFactor, noKeyValue);
        values = (V[]) new Object[keys.length];
        clear();
    }

    public final void clear() {
        super.clear();
        Arrays.fill(values, null);
    }

    public V get(int key) {
        return valueAt(keyIndex(key));
    }

    public int[] getKeys() {
        return keys;
    }

    public int getNoEntryKey() {
        return noEntryKey;
    }

    public V[] getValues() {
        return values;
    }

    public void put(int key, V value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAt(int index, int key, V value) {
        if (index < 0) {
            values[-index - 1] = value;
        } else {
            keys[index] = key;
            values[index] = value;
            if (--free == 0) {
                rehash();
            }
        }
    }

    public V valueAt(int index) {
        return index < 0 ? valueAtQuick(index) : null;
    }

    public V valueAtQuick(int index) {
        return values[-index - 1];
    }

    @SuppressWarnings({"unchecked"})
    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        V[] oldValues = values;
        int[] oldKeys = keys;
        this.keys = new int[len];
        this.values = (V[]) new Object[len];
        Arrays.fill(keys, noEntryKeyValue);
        mask = len - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            int key = oldKeys[i];
            if (key != noEntryKeyValue) {
                final int index = keyIndex(key);
                keys[index] = key;
                values[index] = oldValues[i];
            }
        }
    }

    @Override
    protected void erase(int index) {
        keys[index] = noEntryKeyValue;
        ((Object[]) values)[index] = null;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }
}
