/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
            buf.setPos(size + 1);
            src.setPos(size + 1);
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
                while (++mid < high && buf.getQuick(mid) == v) {
                }
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
