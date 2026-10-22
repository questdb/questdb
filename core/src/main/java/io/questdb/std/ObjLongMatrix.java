/*+*****************************************************************************
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

public class ObjLongMatrix<T> {
    private final int bits;
    private long[] data;
    private int firstMarkedRow;
    private int markedCount;
    // one bit per row, set by markDeleted()
    private long[] markedRows;
    private T[] payload;
    private int pos;
    private int rows;

    @SuppressWarnings("unchecked")
    public ObjLongMatrix(int columnCount) {
        int cc = Numbers.ceilPow2(columnCount);
        this.pos = 0;
        this.rows = 512;
        this.data = new long[rows * cc];
        this.markedRows = new long[rows >>> 6];
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

        while (high - low > 65) {

            int mid = (low + high - 1) >>> 1;
            long midVal = get(mid, index);

            if (midVal < v) {
                low = mid + 1;
            } else if (midVal > v) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return scanSearch(v, index, low);
    }

    /**
     * Removes the rows marked by {@link #markDeleted(int)}, keeping the order of the remaining
     * rows. It visits the marked rows in index order and moves each remaining row at most once,
     * so removing many rows costs about as much as removing the first of them with
     * {@link #deleteRow(int)}.
     *
     * @return the number of rows removed
     */
    public int deleteMarked() {
        final int count = markedCount;
        if (count == 0) {
            return 0;
        }
        // rows before the first marked row stay where they are
        int dst = firstMarkedRow;
        int src = firstMarkedRow;
        for (int w = firstMarkedRow >>> 6, remaining = count; remaining > 0; w++) {
            long word = markedRows[w];
            if (word != 0) {
                markedRows[w] = 0;
                do {
                    final int r = (w << 6) + Long.numberOfTrailingZeros(word);
                    dst = moveRows(src, r, dst);
                    src = r + 1;
                    word &= word - 1;
                    remaining--;
                } while (word != 0);
            }
        }
        moveRows(src, pos, dst);
        pos -= count;
        markedCount = 0;
        return count;
    }

    public void deleteRow(int r) {
        assert markedCount == 0 : "rows are marked for deleteMarked()";
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

    /**
     * Tells whether row {@code r} is marked for removal by {@link #deleteMarked()}.
     */
    public boolean isMarked(int r) {
        return (markedRows[r >>> 6] & 1L << r) != 0;
    }

    /**
     * Marks row {@code r} for removal by {@link #deleteMarked()}. The row keeps its position and
     * values until then, so lookups and reads still see it, and marking a row again has no
     * further effect. Rows must not move between the first mark and {@link #deleteMarked()}:
     * {@link #addRow()} is safe, while {@link #deleteRow(int)} and {@link #zapTop(int)} assert
     * that no row is marked.
     */
    public void markDeleted(int r) {
        assert r >= 0 && r < pos : "row out of range [r=" + r + ", size=" + pos + ']';
        final int w = r >>> 6;
        final long bit = 1L << r;
        if ((markedRows[w] & bit) == 0) {
            markedRows[w] |= bit;
            if (markedCount++ == 0 || r < firstMarkedRow) {
                firstMarkedRow = r;
            }
        }
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
        assert markedCount == 0 : "rows are marked for deleteMarked()";
        if (count < pos) {
            System.arraycopy(data, count << bits, data, 0, (pos - count) << bits);
            System.arraycopy(payload, count, payload, 0, pos - count);
            pos -= count;
        } else {
            pos = 0;
        }
    }

    // moves rows [lo, hi) to start at row dst, and returns the row after the last one moved
    private int moveRows(int lo, int hi, int dst) {
        final int count = hi - lo;
        if (count > 0) {
            System.arraycopy(data, lo << bits, data, dst << bits, count << bits);
            System.arraycopy(payload, lo, payload, dst, count);
        }
        return dst + count;
    }

    private int offset(int r, int c) {
        return (r << bits) + c;
    }

    @SuppressWarnings("unchecked")
    private int resize() {
        long[] _data = new long[rows << (bits + 1)];
        long[] _markedRows = new long[rows >>> 5];
        T[] _payload = (T[]) new Object[rows << 1];
        System.arraycopy(data, 0, _data, 0, rows << bits);
        System.arraycopy(markedRows, 0, _markedRows, 0, rows >>> 6);
        System.arraycopy(payload, 0, _payload, 0, rows);
        this.data = _data;
        this.markedRows = _markedRows;
        this.payload = _payload;
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
