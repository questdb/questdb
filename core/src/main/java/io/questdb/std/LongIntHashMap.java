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

public class LongIntHashMap extends AbstractLongHashSet implements Mutable {

    public static final int NO_ENTRY_VALUE = -1;
    private int[] values;

    public LongIntHashMap() {
        this(8);
    }

    public LongIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.5f);
    }

    private LongIntHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        values = new int[keys.length];
        clear();
    }

    public int get(long key) {
        return valueAt(keyIndex(key));
    }

    public void put(long key, int value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAll(LongIntHashMap other) {
        long[] otherKeys = other.keys;
        int[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != other.noEntryKeyValue) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }

    public void putAt(int index, long key, int value) {
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

    public int valueAt(int index) {
        return index < 0 ? values[-index - 1] : NO_ENTRY_VALUE;
    }

    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        int[] oldValues = values;
        long[] oldKeys = keys;
        this.keys = new long[len];
        this.values = new int[len];
        Arrays.fill(keys, noEntryKeyValue);
        mask = len - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            long key = oldKeys[i];
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
