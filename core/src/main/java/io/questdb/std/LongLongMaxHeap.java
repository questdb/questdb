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
 * Allocation-free max-heap after its backing list reaches the required capacity. Each entry occupies two adjacent
 * longs in one array. Higher keys sort first; equal keys sort by lower values first.
 */
public final class LongLongMaxHeap implements Mutable {
    private static final int ENTRY_LONGS = 2;
    private static final int VALUE_OFFSET = 1;
    private final LongList heap = new LongList();

    @Override
    public void clear() {
        heap.clear();
    }

    public boolean isEmpty() {
        return heap.size() == 0;
    }

    public long peekKey() {
        assert !isEmpty();
        return heap.getQuick(0);
    }

    public long peekValue() {
        assert !isEmpty();
        return heap.getQuick(VALUE_OFFSET);
    }

    public void pop() {
        assert !isEmpty();
        final int lastEntry = size() - 1;
        if (lastEntry == 0) {
            heap.clear();
            return;
        }

        final int lastOffset = offset(lastEntry);
        final long key = heap.getQuick(lastOffset);
        final long value = heap.getQuick(lastOffset + VALUE_OFFSET);
        heap.setPos(lastOffset);

        int parent = 0;
        while (true) {
            final int left = parent * 2 + 1;
            if (left >= lastEntry) {
                break;
            }
            final int right = left + 1;
            int greater = left;
            if (right < lastEntry && isGreater(right, left)) {
                greater = right;
            }
            final int greaterOffset = offset(greater);
            if (!isGreater(heap.getQuick(greaterOffset), heap.getQuick(greaterOffset + VALUE_OFFSET), key, value)) {
                break;
            }
            final int parentOffset = offset(parent);
            heap.setQuick(parentOffset, heap.getQuick(greaterOffset));
            heap.setQuick(parentOffset + VALUE_OFFSET, heap.getQuick(greaterOffset + VALUE_OFFSET));
            parent = greater;
        }

        final int parentOffset = offset(parent);
        heap.setQuick(parentOffset, key);
        heap.setQuick(parentOffset + VALUE_OFFSET, value);
    }

    public void push(long key, long value) {
        int child = size();
        heap.add(key, value);
        while (child > 0) {
            final int parent = (child - 1) >>> 1;
            final int parentOffset = offset(parent);
            if (!isGreater(key, value, heap.getQuick(parentOffset), heap.getQuick(parentOffset + VALUE_OFFSET))) {
                break;
            }
            final int childOffset = offset(child);
            heap.setQuick(childOffset, heap.getQuick(parentOffset));
            heap.setQuick(childOffset + VALUE_OFFSET, heap.getQuick(parentOffset + VALUE_OFFSET));
            child = parent;
        }
        final int childOffset = offset(child);
        heap.setQuick(childOffset, key);
        heap.setQuick(childOffset + VALUE_OFFSET, value);
    }

    public int size() {
        return heap.size() / ENTRY_LONGS;
    }

    private static boolean isGreater(long keyA, long valueA, long keyB, long valueB) {
        return keyA > keyB || keyA == keyB && valueA < valueB;
    }

    private static int offset(int entry) {
        return entry * ENTRY_LONGS;
    }

    private boolean isGreater(int entryA, int entryB) {
        final int offsetA = offset(entryA);
        final int offsetB = offset(entryB);
        return isGreater(
                heap.getQuick(offsetA),
                heap.getQuick(offsetA + VALUE_OFFSET),
                heap.getQuick(offsetB),
                heap.getQuick(offsetB + VALUE_OFFSET)
        );
    }
}
