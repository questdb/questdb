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

import io.questdb.std.str.Utf16Sink;

import java.util.Arrays;

/**
 * Thread-safe grow-only integer list. Assumes multiple reader threads
 * and a single writer thread.
 */
public class AtomicIntList {

    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final int NO_ENTRY_VALUE = -1;

    private volatile int[] data;
    private volatile int pos;

    public AtomicIntList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public AtomicIntList(int capacity) {
        int[] data = new int[capacity];
        Arrays.fill(data, 0, data.length, NO_ENTRY_VALUE);
        this.data = data;
    }

    public void add(int value) {
        int[] data = this.data;
        int pos = this.pos;
        if (pos >= data.length) {
            int[] dataCopy = new int[2 * data.length];
            System.arraycopy(data, 0, dataCopy, 0, data.length);
            dataCopy[pos] = value;
            this.data = dataCopy;
        } else {
            Unsafe.arrayPutOrdered(data, pos, value);
        }
        this.pos = pos + 1;
    }

    public int capacity() {
        int[] data = this.data;
        return data.length;
    }

    public void extendAndSet(int index, int value) {
        int[] data = this.data;
        int pos = this.pos;
        if (index >= data.length) {
            int[] dataCopy = new int[2 * (index + 1)];
            System.arraycopy(data, 0, dataCopy, 0, data.length);
            Arrays.fill(dataCopy, data.length, dataCopy.length, NO_ENTRY_VALUE);
            dataCopy[index] = value;
            this.data = dataCopy;
        } else {
            Unsafe.arrayPutOrdered(data, index, value);
        }
        if (pos <= index) {
            this.pos = index + 1;
        }
    }

    public int get(int index) {
        // First we read position, then data as they're written in the reverse order.
        // That's because it's a grow-only list, so the data array can only grow
        // once we've read the position.
        int pos = this.pos;
        int[] data = this.data;
        if (index >= pos) {
            throw new IndexOutOfBoundsException("Index out of range: " + index);
        }
        return Unsafe.arrayGetVolatile(data, index);
    }

    public void set(int index, int value) {
        int[] data = this.data;
        int pos = this.pos;
        if (index < pos) {
            Unsafe.arrayPutOrdered(data, index, value);
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public int size() {
        return this.pos;
    }

    @Override
    public String toString() {
        Utf16Sink b = Misc.getThreadLocalSink();
        int[] data = this.data;
        int pos = this.pos;
        b.put('[');
        for (int i = 0; i < pos; i++) {
            if (i > 0) {
                b.put(',');
            }
            b.put(data[i]);
        }
        b.put(']');
        return b.toString();
    }
}