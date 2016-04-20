/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.std;

import com.questdb.misc.Unsafe;

public class IntPriorityQueue {
    private int[] buffer;
    private int limit;

    public IntPriorityQueue() {
        this(8);
    }

    private IntPriorityQueue(int size) {
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

    private int binSearch(int v) {
        if (limit < 65) {
            return scanSearch(v);
        } else {
            return binSearch0(v);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int binSearch0(int v) {
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
