/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.BinarySearch;

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

    public int binarySearchBlock(int shl, long value, int scanDir, int offset) {
        // Binary searches using 2^shl blocks
        // e.g. when shl == 2
        // this method treats 4 longs as 1 entry
        // taking first long for the comparisons
        // and ignoring the other 3 values.

        // This is useful when list is a dictionary where first long is a key
        // and subsequent X (1, 3, 7 etc.) values are the value of the dictionary.

        // this is the same algorithm as implemented in C (util.h)
        // template<class T, class V>
        // inline int64_t binary_search(T *data, V value, int64_t low, int64_t high, int32_t scan_dir)
        // please ensure these implementations are in sync

        int low = 0;
        int high = (pos - 1) >> shl;
        while (high - low > 65) {
            final int mid = (low + high) / 2;
            final long midVal = data[(mid << shl) + offset];

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the first
                return scanDir == BinarySearch.SCAN_UP ?
                        scrollUpBlock(shl, mid, midVal) :
                        scrollDownBlock(shl, mid, high, midVal);
            }
        }
        return scanDir == BinarySearch.SCAN_UP ?
                scanUpBlock(shl, value, low, high + 1) :
                scanDownBlock(shl, value, low, high + 1);
    }

    private int scanDownBlock(int shl, long v, int low, int high) {
        for (int i = high - 1; i >= low; i--) {
            long that = data[i << shl];
            if (that == v) {
                return i << shl;
            }
            if (that < v) {
                return -(((i + 1) << shl) + 1);
            }
        }
        return -((low << shl) + 1);
    }

    private int scanUpBlock(int shl, long value, int low, int high) {
        for (int i = low; i < high; i++) {
            long that = data[i << shl];
            if (that == value) {
                return i << shl;
            }
            if (that > value) {
                return -((i << shl) + 1);
            }
        }
        return -((high << shl) + 1);
    }

    private int scrollDownBlock(int shl, int low, int high, long value) {
        do {
            if (low < high) {
                low++;
            } else {
                return low << shl;
            }
        } while (data[low << shl] == value);
        return (low - 1) << shl;
    }

    private int scrollUpBlock(int shl, int high, long value) {
        do {
            if (high > 0) {
                high--;
            } else {
                return 0;
            }
        } while (data[high << shl] == value);
        return (high + 1) << shl;
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
