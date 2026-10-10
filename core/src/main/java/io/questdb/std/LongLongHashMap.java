/*+*****************************************************************************
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

/**
 * Open-addressing hash map from {@code long} keys to {@code long} values, held on the Java heap.
 * {@code noEntryKey} marks an empty slot, so it cannot be used as a key; {@link #get(long)} and
 * {@link #valueAt(int)} return {@code noEntryValue} for a key the map does not hold.
 */
public class LongLongHashMap extends AbstractLongHashSet implements Mutable {
    private final long noEntryValue;
    private long[] values;

    public LongLongHashMap(int initialCapacity, double loadFactor, long noEntryKey, long noEntryValue) {
        super(initialCapacity, loadFactor, noEntryKey);
        this.noEntryValue = noEntryValue;
        values = new long[keys.length];
        clear();
    }

    public long get(long key) {
        return valueAt(keyIndex(key));
    }

    public void put(long key, long value) {
        putAt(keyIndex(key), key, value);
    }

    public void putAt(int index, long key, long value) {
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

    @Override
    public void restoreInitialCapacity() {
        super.restoreInitialCapacity();
        values = new long[keys.length];
    }

    public long valueAt(int index) {
        return index < 0 ? values[-index - 1] : noEntryValue;
    }

    private void rehash() {
        final int size = size();
        final int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        final int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        final long[] oldValues = values;
        final long[] oldKeys = keys;
        keys = new long[len];
        values = new long[len];
        Arrays.fill(keys, noEntryKeyValue);
        mask = len - 1;

        free -= size;
        for (int i = oldKeys.length; i-- > 0; ) {
            final long key = oldKeys[i];
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
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }
}
