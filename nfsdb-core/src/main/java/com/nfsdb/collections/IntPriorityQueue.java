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

import com.nfsdb.utils.Unsafe;

public class IntPriorityQueue {
    private int[] buffer;
    private int limit;

    public IntPriorityQueue() {
        this(8);
    }

    public IntPriorityQueue(int size) {
        this.buffer = new int[size];
        this.limit = 0;
    }

    public void clear() {
        limit = 0;
    }

    public boolean notEmpty() {
        return limit > 0;
    }

    public int pop() {
        int v = Unsafe.arrayGet(buffer, 0);
        if (--limit > 0) {
            System.arraycopy(buffer, 1, buffer, 0, limit);
        }
        return v;
    }

    public void push(int value) {
        int p = binSearch(value);
        if (p < limit) {
            System.arraycopy(buffer, p, buffer, p + 1, limit - p);
        }
        if (p >= buffer.length) {
            resize();
        }
        Unsafe.arrayPut(buffer, p, value);
        limit++;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int _binSearch(int v) {
        int low = 0;
        int high = limit;

        while (low < high) {

            if (high - low < 65) {
                return scanSearch(v);
            }

            int mid = (low + high - 1) >>> 1;
            int midVal = Unsafe.arrayGet(buffer, mid);

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else {
                for (; ++mid < high && Unsafe.arrayGet(buffer, mid) == v; ) ;
                return mid;
            }
        }
        return low;
    }

    private int binSearch(int v) {
        if (limit < 65) {
            return scanSearch(v);
        } else {
            return _binSearch(v);
        }
    }

    private void resize() {
        int tmp[] = new int[buffer.length * 2];
        System.arraycopy(buffer, 0, tmp, 0, buffer.length);
        buffer = tmp;
    }

    private int scanSearch(int v) {
        for (int i = 0; i < limit; i++) {
            if (Unsafe.arrayGet(buffer, i) > v) {
                return i;
            }
        }
        return limit;
    }
}
