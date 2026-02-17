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

public class BoolList implements Mutable {
    private static final int DEFAULT_ARRAY_SIZE = 16;
    private static final boolean NO_ENTRY_VALUE = false;
    private boolean[] buffer;
    private int pos = 0;

    public BoolList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public BoolList(int capacity) {
        this.buffer = new boolean[Math.max(capacity, DEFAULT_ARRAY_SIZE)];
    }

    public void add(boolean value) {
        checkCapacity(pos + 1);
        buffer[pos++] = value;
    }

    public void addAll(BoolList that) {
        int p = pos;
        int s = that.size();
        setPos(p + s);
        System.arraycopy(that.buffer, 0, this.buffer, p, s);
    }

    public void arrayCopy(int srcPos, int dstPos, int length) {
        System.arraycopy(buffer, srcPos, buffer, dstPos, length);
    }

    public void clear() {
        pos = 0;
    }

    public void clear(int capacity) {
        checkCapacity(capacity);
        pos = 0;
        Arrays.fill(buffer, NO_ENTRY_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof BoolList && equals((BoolList) that);
    }

    public boolean extendAndReplace(int index, boolean value) {
        extendCapacity(index + 1);
        return replace0(index, value);
    }

    public void extendAndSet(int index, boolean value) {
        extendCapacity(index + 1);
        buffer[index] = value;
    }

    public boolean get(int index) {
        assert index < pos;
        return buffer[index];
    }

    public boolean getLast() {
        if (pos > 0) {
            return buffer[pos - 1];
        }
        return NO_ENTRY_VALUE;
    }

    /**
     * Returns element at the specified position or null, if element index is
     * out of bounds. This is an alternative to throwing runtime exception or
     * doing preemptive check.
     *
     * @param index position of element
     * @return element at the specified position.
     */
    public boolean getQuiet(int index) {
        if (index < pos) {
            return buffer[index];
        }
        return NO_ENTRY_VALUE;
    }

    public int getTrueCount() {
        int cnt = 0;
        for (int i = 0, n = pos; i < n; i++) {
            if (buffer[i]) {
                cnt++;
            }
        }
        return cnt;
    }

    public void insert(int index, boolean element) {
        checkCapacity(++pos);
        System.arraycopy(buffer, index, buffer, index + 1, pos - index - 1);
        buffer[index] = element;
    }

    public void removeIndex(int index) {
        if (pos < 1 || index >= pos) {
            return;
        }
        int move = pos - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        int index1 = --pos;
        buffer[index1] = NO_ENTRY_VALUE;
    }

    public boolean replace(int index, boolean value) {
        assert index < pos;
        return replace0(index, value);
    }

    public void set(int index, boolean element) {
        if (index < pos) {
            buffer[index] = element;
            return;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    public void setAll(int capacity, boolean value) {
        setPos(capacity);
        Arrays.fill(buffer, 0, pos, value);
    }

    public void setPos(int capacity) {
        checkCapacity(capacity);
        pos = capacity;
    }

    public void setQuick(int index, boolean value) {
        assert index < pos;
        buffer[index] = value;
    }

    public int size() {
        return pos;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        Utf16Sink b = Misc.getThreadLocalSink();

        b.put('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                b.put(',');
            }
            b.put(get(i));
        }
        b.put(']');
        return b.toString();
    }

    public void zero(boolean value) {
        Arrays.fill(buffer, 0, pos, value);
    }

    private void checkCapacity(int capacity) {
        int l = buffer.length;
        if (capacity > l) {
            int newCap = Math.max(l << 1, capacity);
            boolean[] buf = new boolean[newCap];
            System.arraycopy(buffer, 0, buf, 0, l);
            this.buffer = buf;
        }
    }

    private boolean equals(BoolList that) {
        if (this.pos != that.pos) {
            return false;
        }

        for (int i = 0, n = pos; i < n; i++) {
            if (buffer[i] != that.buffer[i]) {
                return false;
            }
        }
        return true;
    }

    private void extendCapacity(int newPos) {
        if (newPos > pos) {
            checkCapacity(newPos);
            Arrays.fill(buffer, pos, newPos, NO_ENTRY_VALUE);
            pos = newPos;
        }
    }

    private boolean replace0(int index, boolean value) {
        boolean val = buffer[index];
        buffer[index] = value;
        return val;
    }
}
