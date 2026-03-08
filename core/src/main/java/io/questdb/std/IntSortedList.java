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

public class IntSortedList implements Mutable {
    private int[] buffer;
    private int size;

    public IntSortedList() {
        this(8);
    }

    private IntSortedList(int size) {
        this.buffer = new int[size];
        this.size = 0;
    }

    public void add(int value) {
        int p = binSearch(value);
        if (size == buffer.length) {
            resize();
        }
        if (p < size) {
            System.arraycopy(buffer, p, buffer, p + 1, size - p);
        }
        buffer[p] = value;
        size++;
    }

    @Override
    public void clear() {
        for (int i = 0; i < size; i++) {
            buffer[i] = 0;
        }
        size = 0;
    }

    public boolean notEmpty() {
        return size > 0;
    }

    public int poll() {
        int v = buffer[0];
        if (size > 0) {
            System.arraycopy(buffer, 1, buffer, 0, --size);
        }
        return v;
    }

    public int size() {
        return size;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int binSearch(int v) {
        int low = 0;
        int high = size;

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
        for (int i = low; i < size; i++) {
            if (buffer[i] > v) {
                return i;
            }
        }
        return size;
    }
}
