/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

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
        return Unsafe.arrayGet(data, offset(r, c));
    }

    public T get(int r) {
        return Unsafe.arrayGet(payload, r);
    }

    public void set(int r, T obj) {
        Unsafe.arrayPut(payload, r, obj);
    }

    public void set(int r, int c, long value) {
        Unsafe.arrayPut(data, offset(r, c), value);
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
