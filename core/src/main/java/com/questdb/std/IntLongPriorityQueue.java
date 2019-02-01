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

public class IntLongPriorityQueue {
    private final LongList buf;
    private final IntList src;
    private int size;

    public IntLongPriorityQueue() {
        this.buf = new LongList(8);
        this.src = new IntList(8);
        this.size = 0;
    }

    public void add(int index, long value) {
        int p = binSearch(value);
        if (p < size) {
            buf.ensureCapacity(size + 1);
            src.ensureCapacity(size + 1);
            buf.arrayCopy(p, p + 1, size - p);
            src.arrayCopy(p, p + 1, size - p);
        }
        buf.extendAndSet(p, value);
        src.extendAndSet(p, index);
        size++;
    }

    public void clear() {
        size = 0;
    }

    public boolean hasNext() {
        return size > 0;
    }

    public int peekBottom() {
        return src.getQuick(size - 1);
    }

    public long popAndReplace(int index, long value) {
        long v = buf.getQuick(0);
        int p = binSearch(value);
        if (p > 1) {
            p--;
            buf.arrayCopy(1, 0, p);
            src.arrayCopy(1, 0, p);

            buf.setQuick(p, value);
            src.setQuick(p, index);
        } else {
            buf.setQuick(0, value);
            src.setQuick(0, index);
        }
        return v;
    }

    public int popIndex() {
        return src.getQuick(0);
    }

    public long popValue() {
        long v = buf.getQuick(0);
        if (--size > 0) {
            buf.arrayCopy(1, 0, size);
            src.arrayCopy(1, 0, size);
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
            long midVal = buf.getQuick(mid);

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else {
                for (; ++mid < high && buf.getQuick(mid) == v; ) ;
                return mid;
            }
        }
        return low;
    }

    private int scanSearch(long v) {
        for (int i = 0; i < size; i++) {
            if (buf.getQuick(i) > v) {
                return i;
            }
        }
        return size;
    }
}
