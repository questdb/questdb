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


public class LowerCaseAsciiCharSequenceLongHashMap extends AbstractLowerCaseAsciiCharSequenceHashSet {
    private static final long NO_ENTRY_VALUE = -1L;
    private final long noEntryValue;
    private long[] values;

    public LowerCaseAsciiCharSequenceLongHashMap() {
        this(8);
    }

    public LowerCaseAsciiCharSequenceLongHashMap(int initialCapacity) {
        this(initialCapacity, 0.5, NO_ENTRY_VALUE);
    }

    public LowerCaseAsciiCharSequenceLongHashMap(int initialCapacity, double loadFactor, long noEntryValue) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        values = new long[keys.length];
        clear();
    }

    public final void clear() {
        super.clear();
        Arrays.fill(values, noEntryValue);
    }

    public long get(CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public boolean put(CharSequence key, long value) {
        return putAt(keyIndex(key), key, value);
    }

    public boolean putAt(int index, CharSequence key, long value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        putAt0(index, Chars.toLowerCaseAscii(key), value);
        return true;
    }

    public void putIfAbsent(CharSequence key, long value) {
        int index = keyIndex(key);
        if (index > -1) {
            putAt0(index, Chars.toLowerCaseAscii(key), value);
        }
    }

    public long valueAt(int index) {
        return index < 0 ? values[-index - 1] : noEntryValue;
    }

    private void putAt0(int index, CharSequence key, long value) {
        keys[index] = key;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        int size = size();
        int newCapacity = capacity * 2;
        free = capacity = newCapacity;
        int len = Numbers.ceilPow2((int) (newCapacity / loadFactor));

        long[] oldValues = values;
        CharSequence[] oldKeys = keys;
        this.keys = new CharSequence[len];
        this.values = new long[len];
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
        values[index] = noEntryValue;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }
}