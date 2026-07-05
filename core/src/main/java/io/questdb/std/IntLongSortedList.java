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

/**
 * A min-poll priority queue of (index, value) pairs, ordered by {@code value}
 * ascending, backed by an array binary min-heap: O(log N) {@link #add},
 * {@link #pollValue}, {@link #pollAndReplace}. The name is historical (it was a
 * sorted array); callers use it only as a min-heap ({@link #peekIndex},
 * {@link #pollAndReplace}, {@link #pollValue}). Values are assumed distinct by
 * every current caller (k-way row-id merges), so tie-order is unspecified.
 */
public class IntLongSortedList implements Mutable {
    private int[] indices = new int[8];
    private int size;
    private long[] values = new long[8];

    public void add(int index, long value) {
        if (size == values.length) {
            values = java.util.Arrays.copyOf(values, size << 1);
            indices = java.util.Arrays.copyOf(indices, size << 1);
        }
        int i = size++;
        // sift up
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            if (values[parent] <= value) {
                break;
            }
            values[i] = values[parent];
            indices[i] = indices[parent];
            i = parent;
        }
        values[i] = value;
        indices[i] = index;
    }

    @Override
    public void clear() {
        size = 0;
    }

    public boolean hasNext() {
        return size > 0;
    }

    public int peekIndex() {
        return indices[0];
    }

    public long pollAndReplace(int index, long value) {
        final long old = values[0];
        siftDownFromRoot(value, index);
        return old;
    }

    public long pollValue() {
        final long old = values[0];
        final int last = --size;
        if (last > 0) {
            siftDownFromRoot(values[last], indices[last]);
        }
        return old;
    }

    public int size() {
        return size;
    }

    // Place (value, index) at the root and sift it down to restore the heap.
    private void siftDownFromRoot(long value, int index) {
        int i = 0;
        final int half = size >>> 1;
        while (i < half) {
            int child = (i << 1) + 1;
            final int right = child + 1;
            if (right < size && values[right] < values[child]) {
                child = right;
            }
            if (values[child] >= value) {
                break;
            }
            values[i] = values[child];
            indices[i] = indices[child];
            i = child;
        }
        values[i] = value;
        indices[i] = index;
    }
}
