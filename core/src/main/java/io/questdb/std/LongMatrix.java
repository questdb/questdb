/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class LongMatrix<T> {
    private final int bits;
    private int pos;
    private long[] data;
    private T[] payload;
    private int rows;

    @SuppressWarnings("unchecked")
    public LongMatrix(int columnCount) {
        int cc = Numbers.ceilPow2(columnCount);
        this.pos = 0;
        this.rows = 512;
        this.data = new long[rows * cc];
        this.payload = (T[]) new Object[rows];
        this.bits = Numbers.msb(cc);
    }

    public int addRow() {
        if (pos < rows) {
            return pos++;
        } else {
            return resize();
        }
    }

    public int binarySearch(long v, int index) {
        int low = 0;
        int high = pos;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v, index);
            }

            int mid = (low + high - 1) >>> 1;
            long midVal = get(mid, index);

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    public void deleteRow(int r) {
        if (r < pos - 1) {
            int l = pos - r - 1;
            int next = r + 1;
            System.arraycopy(data, next << bits, data, r << bits, l << bits);
            System.arraycopy(payload, next, payload, r, l);
        }

        if (r < pos) {
            pos--;
        }
    }

    public long get(int r, int c) {
        return data[offset(r, c)];
    }

    public T get(int r) {
        return payload[r];
    }

    public void set(int r, T obj) {
        payload[r] = obj;
    }

    public void set(int r, int c, long value) {
        data[offset(r, c)] = value;
    }

    public int size() {
        return pos;
    }

    public void zapTop(int count) {
        if (count < pos) {
            System.arraycopy(data, count << bits, data, 0, (pos - count) << bits);
            System.arraycopy(payload, count, payload, 0, pos - count);
            pos -= count;
        } else {
            pos = 0;
        }
    }

    private int offset(int r, int c) {
        return (r << bits) + c;
    }

    @SuppressWarnings("unchecked")
    private int resize() {
        long[] _data = new long[rows << (bits + 1)];
        T[] _payload = (T[]) new Object[rows << 1];
        System.arraycopy(data, 0, _data, 0, rows << bits);
        System.arraycopy(payload, 0, _payload, 0, rows);
        this.data = _data;
        this.payload = _payload;
        this.rows <<= 1;
        return pos++;
    }

    private int scanSearch(long v, int index) {
        int sz = size();
        for (int i = 0; i < sz; i++) {
            long f = get(i, index);
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(sz + 1);
    }
}
