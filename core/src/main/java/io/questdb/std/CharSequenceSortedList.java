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

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;

public class CharSequenceSortedList implements Mutable, Sinkable, ReadOnlyObjList<CharSequence> {
    public static final int DEFAULT_ARRAY_SIZE = 16;
    private CharSequence[] buffer;
    private int size;

    public CharSequenceSortedList() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public CharSequenceSortedList(CharSequenceSortedList other) {
        this.buffer = new CharSequence[Math.max(other.size(), DEFAULT_ARRAY_SIZE)];
        size = other.size();
        System.arraycopy(other.buffer, 0, this.buffer, 0, size);
    }

    public CharSequenceSortedList(int size) {
        this.buffer = new CharSequence[size];
        this.size = 0;
    }

    public void add(CharSequence value) {
        int p = binSearch(value);
        if (size > 0 && p > 0 && CharSequence.compare(buffer[p - 1], value) == 0) {
            return;
        }

        if (size == buffer.length) {
            resize();
        }
        if (p < size) {
            System.arraycopy(buffer, p, buffer, p + 1, size - p);
        }
        buffer[p] = value;
        size++;
    }

    @Override
    public void clear() {
        for (int i = 0; i < size; i++) {
            buffer[i] = null;
        }
        size = 0;
    }

    @Override
    public ReadOnlyObjList<CharSequence> copy() {
        return new CharSequenceSortedList(this);
    }

    @Override
    public boolean equals(Object that) {
        return this == that || that instanceof CharSequenceSortedList && equals((CharSequenceSortedList) that);
    }

    private boolean equals(CharSequenceSortedList that) {
        if (this.size == that.size) {
            for (int i = 0, n = size; i < n; i++) {
                Object lhs = this.get(i);
                if (lhs == null) {
                    if (that.get(i) != null) {
                        return false;
                    }
                } else if (!lhs.equals(that.get(i))) {
                    return false;
                }
            }

            return true;
        }
        return false;
    }

    @Override
    public CharSequence get(int index) {
        if (index < size) {
            return buffer[index];
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    @Override
    public CharSequence getLast() {
        if (size > 0) {
            return buffer[size - 1];
        }
        return null;
    }

    @Override
    public CharSequence getQuick(int index) {
        assert index < size : "index out of bounds, " + index + " >= " + size;
        return buffer[index];
    }

    @Override
    public CharSequence getQuiet(int index) {
        if (index < size) {
            return buffer[index];
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = 0, n = size; i < n; i++) {
            CharSequence o = get(i);
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        }
        return hashCode;
    }

    @Override
    public int indexOf(Object o) {
        if (o instanceof CharSequence && size > 0) {
            int pos = binSearch((CharSequence) o);

            if (pos > 0) {
                pos--;
            }

            if (buffer[pos] != null && buffer[pos].equals(o)) {
                return pos;
            }
        }

        return -1;
    }

    public void remove(CharSequence val) {
        int index = binSearch(val) - 1;

        if (size > 0 && index > -1 && CharSequence.compare(buffer[index], val) == 0) {
            removeIndex(index);
        }
    }

    public void removeIndex(int index) {
        if (size < 1 || index >= size || index < 0) {
            return;
        }
        int move = size - index - 1;
        if (move > 0) {
            System.arraycopy(buffer, index + 1, buffer, index, move);
        }
        buffer[--size] = null;
    }

    public int size() {
        return size;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[');
        for (int i = 0, k = size(); i < k; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.put(get(i));
        }
        sink.putAscii(']');
    }

    @Override
    public String toString() {
        Utf16Sink b = Misc.getThreadLocalSink();
        toSink(b);
        return b.toString();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int binSearch(CharSequence v) {
        int low = 0;
        int high = size;

        while (high - low > 65) {
            int mid = (low + high - 1) >>> 1;
            CharSequence midVal = buffer[mid];

            int compareResult = CharSequence.compare(midVal, v);
            if (compareResult < 0)
                low = mid + 1;
            else if (compareResult > 0)
                high = mid;
            else {
                while (++mid < high && CharSequence.compare(buffer[mid], v) == 0) {
                }
                return mid;
            }
        }
        return scanSearch(v, low);
    }

    private void resize() {
        CharSequence[] tmp = new CharSequence[buffer.length * 2];
        System.arraycopy(buffer, 0, tmp, 0, buffer.length);
        buffer = tmp;
    }

    private int scanSearch(CharSequence v, int low) {
        for (int i = low; i < size; i++) {
            if (CharSequence.compare(buffer[i], v) > 0) {
                return i;
            }
        }
        return size;
    }
}
