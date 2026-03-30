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

package io.questdb.cairo.ev;

import io.questdb.std.IntList;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.LongList;

/**
 * Min-heap tracking the earliest expiry timestamp per partition for TIMESTAMP_COMPARE expiring views.
 * Entries: (minExpiryTimestamp, partitionIndex) pairs ordered by timestamp.
 */
public class RowExpiryHeap {
    // Parallel lists: timestamps[i] and partitions[i] form heap entry i
    private final IntList partitions = new IntList();
    // partitionIndex -> heap position (for O(log P) upsert/remove)
    private final IntIntHashMap positionMap = new IntIntHashMap();
    private final LongList timestamps = new LongList();

    public void clear() {
        timestamps.clear();
        partitions.clear();
        positionMap.clear();
    }

    public boolean isEmpty() {
        return timestamps.size() == 0;
    }

    public long peekNextExpiry() {
        if (timestamps.size() == 0) {
            return Long.MAX_VALUE;
        }
        return timestamps.getQuick(0);
    }

    public int peekNextPartitionIndex() {
        if (partitions.size() == 0) {
            return -1;
        }
        return partitions.getQuick(0);
    }

    public int pollPartitionIndex() {
        if (partitions.size() == 0) {
            return -1;
        }
        final int partitionIndex = partitions.getQuick(0);
        removeAt(0);
        return partitionIndex;
    }

    public void remove(int partitionIndex) {
        final int pos = positionMap.get(partitionIndex);
        if (pos != -1) {
            removeAt(pos);
        }
    }

    public int size() {
        return timestamps.size();
    }

    public void upsert(int partitionIndex, long minExpiryTimestamp) {
        final int existingPos = positionMap.get(partitionIndex);
        if (existingPos != -1) {
            final long oldTimestamp = timestamps.getQuick(existingPos);
            timestamps.setQuick(existingPos, minExpiryTimestamp);
            if (minExpiryTimestamp < oldTimestamp) {
                siftUp(existingPos);
            } else if (minExpiryTimestamp > oldTimestamp) {
                siftDown(existingPos);
            }
        } else {
            final int pos = timestamps.size();
            timestamps.add(minExpiryTimestamp);
            partitions.add(partitionIndex);
            positionMap.put(partitionIndex, pos);
            siftUp(pos);
        }
    }

    private void removeAt(int pos) {
        final int lastPos = timestamps.size() - 1;
        final int removedPartition = partitions.getQuick(pos);
        positionMap.remove(removedPartition);

        if (pos == lastPos) {
            timestamps.setPos(lastPos);
            partitions.setPos(lastPos);
            return;
        }

        final long lastTs = timestamps.getQuick(lastPos);
        final int lastPartition = partitions.getQuick(lastPos);
        timestamps.setQuick(pos, lastTs);
        partitions.setQuick(pos, lastPartition);
        positionMap.put(lastPartition, pos);
        timestamps.setPos(lastPos);
        partitions.setPos(lastPos);

        siftUp(pos);
        siftDown(pos);
    }

    private void siftDown(int pos) {
        final int size = timestamps.size();
        while (true) {
            int smallest = pos;
            final int left = 2 * pos + 1;
            final int right = 2 * pos + 2;

            if (left < size && timestamps.getQuick(left) < timestamps.getQuick(smallest)) {
                smallest = left;
            }
            if (right < size && timestamps.getQuick(right) < timestamps.getQuick(smallest)) {
                smallest = right;
            }

            if (smallest == pos) {
                break;
            }
            swap(pos, smallest);
            pos = smallest;
        }
    }

    private void siftUp(int pos) {
        while (pos > 0) {
            final int parent = (pos - 1) / 2;
            if (timestamps.getQuick(pos) < timestamps.getQuick(parent)) {
                swap(pos, parent);
                pos = parent;
            } else {
                break;
            }
        }
    }

    private void swap(int a, int b) {
        final long tsA = timestamps.getQuick(a);
        final int partA = partitions.getQuick(a);
        final long tsB = timestamps.getQuick(b);
        final int partB = partitions.getQuick(b);

        timestamps.setQuick(a, tsB);
        partitions.setQuick(a, partB);
        timestamps.setQuick(b, tsA);
        partitions.setQuick(b, partA);

        positionMap.put(partA, b);
        positionMap.put(partB, a);
    }
}
