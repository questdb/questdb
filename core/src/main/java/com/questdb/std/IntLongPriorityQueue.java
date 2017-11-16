/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

public class IntLongPriorityQueue {
    private final long[] buf;
    private final int[] src;
    private int size;

    public IntLongPriorityQueue(int nSources) {
        this.buf = new long[nSources];
        this.src = new int[nSources];
        this.size = 0;
    }

    public void add(int index, long value) {
        int p = binSearch(value);
        if (p < size) {
            System.arraycopy(buf, p, buf, p + 1, size - p);
            System.arraycopy(src, p, src, p + 1, size - p);
        }
        Unsafe.arrayPut(src, p, index);
        Unsafe.arrayPut(buf, p, value);
        size++;
    }

    public void clear() {
        size = 0;
    }

    public boolean hasNext() {
        return size > 0;
    }

    public int peekBottom() {
        return Unsafe.arrayGet(src, size - 1);
    }

    public long popAndReplace(int index, long value) {
        long v = Unsafe.arrayGet(buf, 0);
        int p = binSearch(value);
        if (p > 1) {
            p--;
            System.arraycopy(buf, 1, buf, 0, p);
            System.arraycopy(src, 1, src, 0, p);

            Unsafe.arrayPut(buf, p, value);
            Unsafe.arrayPut(src, p, index);
        } else {
            Unsafe.arrayPut(buf, 0, value);
            Unsafe.arrayPut(src, 0, index);
        }
        return v;
    }

    public int popIndex() {
        return Unsafe.arrayGet(src, 0);
    }

    public long popValue() {
        long v = Unsafe.arrayGet(buf, 0);
        if (--size > 0) {
            System.arraycopy(buf, 1, buf, 0, size);
            System.arraycopy(src, 1, src, 0, size);
        }
        return v;
    }

    private int binSearch(long v) {
        if (size < 65) {
            return scanSearch(v);
        } else {
            return binSearch0(v);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int binSearch0(long v) {
        int low = 0;
        int high = size;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v);
            }

            int mid = (low + high - 1) >>> 1;
            long midVal = Unsafe.arrayGet(buf, mid);

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else {
                for (; ++mid < high && Unsafe.arrayGet(buf, mid) == v; ) ;
                return mid;
            }
        }
        return low;
    }

    private int scanSearch(long v) {
        for (int i = 0; i < size; i++) {
            if (Unsafe.arrayGet(buf, i) > v) {
                return i;
            }
        }
        return size;
    }
}
