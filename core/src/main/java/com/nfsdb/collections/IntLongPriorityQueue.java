/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.collections;

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

    @SuppressWarnings("StatementWithEmptyBody")
    private int _binSearch(long v) {
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

    private int binSearch(long v) {
        if (size < 65) {
            return scanSearch(v);
        } else {
            return _binSearch(v);
        }
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
