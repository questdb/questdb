/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.std;

import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;

public class LongMatrix<T> {
    private final int bits;
    private int pos;
    private long data[];
    private T payload[];
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

    public int binarySearch(long v) {
        int low = 0;
        int high = pos;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v);
            }

            int mid = (low + high - 1) >>> 1;
            long midVal = get(mid, 0);

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
        long _data[] = new long[rows << (bits + 1)];
        T _payload[] = (T[]) new Object[rows << 1];
        System.arraycopy(data, 0, _data, 0, rows << bits);
        System.arraycopy(payload, 0, _payload, 0, rows);
        this.data = _data;
        this.payload = _payload;
        this.rows <<= 1;
        return pos++;
    }

    private int scanSearch(long v) {
        int sz = size();
        for (int i = 0; i < sz; i++) {
            long f = get(i, 0);
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
