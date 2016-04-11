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

import com.nfsdb.misc.Unsafe;

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
