/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.str.DirectUtf16Sink;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class DirectCharSequenceIntHashHashMap extends AbstractCharSequenceIntHashMap {

    public DirectCharSequenceIntHashHashMap() {
        this(8);
    }

    public DirectCharSequenceIntHashHashMap(int initialCapacity) {
        this(initialCapacity, 0.4, NO_ENTRY_VALUE);
    }

    public DirectCharSequenceIntHashHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        super(initialCapacity, loadFactor, noEntryValue);
    }

    @Override
    public void clear() {
        for (int index = 0; index < keys.length; index++) {
            closeDirectMemory(keys[index]);
        }
        super.clear();
        list.clear();
        Arrays.fill(values, noEntryValue);
    }

    @Override
    public void increment(@NotNull CharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            values[-index - 1] = values[-index - 1] + 1;
        } else {
            putAt0(index, Chars.toDirectUtf16Sink(key), 0);
        }
    }

    @Override
    public boolean putAt(int index, @NotNull CharSequence key, int value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        DirectUtf16Sink directUtf16Sink = Chars.toDirectUtf16Sink(key);
        putAt0(index, directUtf16Sink, value);
        list.add(directUtf16Sink);
        return true;
    }

    @Override
    public void putIfAbsent(@NotNull CharSequence key, int value) {
        int index = keyIndex(key);
        if (index > -1) {
            DirectUtf16Sink directUtf16Sink = Chars.toDirectUtf16Sink(key);
            putAt0(index, directUtf16Sink, value);
            list.add(directUtf16Sink);
        }
    }

    @Override
    public void removeAt(int index) {
        if (index < 0) {
            int index1 = -index - 1;
            CharSequence key = keys[index1];
            closeDirectMemory(key);
            super.removeAt(index);
            list.remove(key);
        }
    }

    @Override
    protected void erase(int index) {
        closeDirectMemory(keys[index]);
        keys[index] = noEntryKey;
        values[index] = noEntryValue;
    }

    @Override
    protected void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        keys[from] = noEntryKey;
        values[from] = noEntryValue;
    }

    private void closeDirectMemory(CharSequence key) {
        if (key != noEntryKey) {
            ((DirectUtf16Sink) key).close();
        }
    }
}
