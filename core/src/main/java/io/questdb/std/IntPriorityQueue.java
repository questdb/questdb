/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
        int v = buffer[0];
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
        buffer[p] = value;
        limit++;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int binSearch(int v) {
        int low = 0;
        int high = limit;

        while (high - low > 65) {
            int mid = (low + high - 1) >>> 1;
            int midVal = buffer[mid];

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid;
            else {
                while (++mid < high && buffer[mid] == v) {
                }
                return mid;
            }
        }
        return scanSearch(v, low);
    }

    private void resize() {
        int[] tmp = new int[buffer.length * 2];
        System.arraycopy(buffer, 0, tmp, 0, buffer.length);
        buffer = tmp;
    }

    private int scanSearch(int v, int low) {
        for (int i = low; i < limit; i++) {
            if (buffer[i] > v) {
                return i;
            }
        }
        return limit;
    }
}
