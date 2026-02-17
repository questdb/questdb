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

public class LongMatrix {
    private final int bits;
    private long[] data;
    private int pos;
    private int rows;

    public LongMatrix(int columnCount) {
        int cc = Numbers.ceilPow2(columnCount);
        this.pos = 0;
        this.rows = 512;
        this.data = new long[rows * cc];
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

        while (high - low > 65) {
            int mid = (low + high - 1) >>> 1;
            long midVal = get(mid, index);

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid - 1;
            else
                return mid;
        }
        return scanSearch(v, index, low);
    }

    public void deleteRow(int r) {
        if (r < pos - 1) {
            int l = pos - r - 1;
            int next = r + 1;
            System.arraycopy(data, next << bits, data, r << bits, l << bits);
        }

        if (r < pos) {
            pos--;
        }
    }

    public long get(int r, int c) {
        return data[offset(r, c)];
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
            pos -= count;
        } else {
            pos = 0;
        }
    }

    private int offset(int r, int c) {
        return (r << bits) + c;
    }

    private int resize() {
        long[] _data = new long[rows << (bits + 1)];
        System.arraycopy(data, 0, _data, 0, rows << bits);
        this.data = _data;
        this.rows <<= 1;
        return pos++;
    }

    private int scanSearch(long v, int index, int low) {
        int sz = size();
        for (int i = low; i < sz; i++) {
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
