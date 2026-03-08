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

public class IntShortHashMap extends AbstractIntHashSet {
    private static final short noEntryValue = -1;
    private short[] values;

    public IntShortHashMap() {
        this(8);
    }

    public IntShortHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    private IntShortHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        values = new short[keys.length];
        clear();
    }

    public short get(int key) {
        return valueAt(keyIndex(key));
    }

    public void put(int key, short value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAll(IntShortHashMap other) {
        int[] otherKeys = other.keys;
        short[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != other.noEntryKeyValue) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }

    public void putAt(int index, int key, short value) {
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

    public short valueAt(int index) {
        return index < 0 ? values[-index - 1] : noEntryValue;
    }

    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        short[] oldValues = values;
        int[] oldKeys = keys;
        this.keys = new int[len];
        this.values = new short[len];
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
        keys[index] = this.noEntryKeyValue;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }
}
