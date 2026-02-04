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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Multi-offset horizon timestamp iterator using K-way merge.
 * <p>
 * Each offset defines a sorted stream of horizon timestamps over the master rows.
 * A min-heap of size K (number of offsets) merges these streams into globally
 * sorted order without materializing or sorting all tuples.
 */
public class MultiOffsetHorizonTimestampIterator implements HorizonTimestampIterator {
    private final int[] heapOffsetIdx;
    private final long[] heapPos;
    private final long[] heapTs;
    private final long masterTsScale;
    private final LongList offsets;
    private long currentHorizonTs;
    private long currentIndex;
    private long currentMasterRowIdx;
    private int currentOffsetIdx;
    private DirectLongList filteredRows;
    private long frameRowLo;
    private int heapSize;
    private boolean isFiltered;
    private long masterRowCount;
    private PageFrameMemoryRecord record;
    private int timestampColumnIndex;
    private long tupleCount;

    public MultiOffsetHorizonTimestampIterator(LongList offsets, long masterTsScale) {
        this.offsets = offsets;
        this.masterTsScale = masterTsScale;
        int k = offsets.size();
        this.heapTs = new long[k];
        this.heapPos = new long[k];
        this.heapOffsetIdx = new int[k];
    }

    @Override
    public void close() {
    }

    @Override
    public long getHorizonTimestamp() {
        return currentHorizonTs;
    }

    @Override
    public long getMasterRowIndex() {
        return currentMasterRowIdx;
    }

    @Override
    public int getOffsetIndex() {
        return currentOffsetIdx;
    }

    @Override
    public boolean next() {
        if (currentIndex >= tupleCount) {
            return false;
        }
        // Read min from heap root
        currentHorizonTs = heapTs[0];
        long pos = heapPos[0];
        int offsetIdx = heapOffsetIdx[0];
        currentOffsetIdx = offsetIdx;
        currentMasterRowIdx = isFiltered ? filteredRows.get(pos) : pos;

        // Advance this stream to the next position
        long nextPos = pos + 1;
        if (nextPos < masterRowCount) {
            long nextRowIdx = isFiltered ? filteredRows.get(nextPos) : (frameRowLo + nextPos);
            record.setRowIndex(nextRowIdx);
            long nextMasterTs = record.getTimestamp(timestampColumnIndex);
            long nextHorizonTs = scaleTimestamp(nextMasterTs + offsets.getQuick(offsetIdx), masterTsScale);
            // Replace root and restore heap property
            heapTs[0] = nextHorizonTs;
            heapPos[0] = nextPos;
            siftDown();
        } else {
            // Stream exhausted: remove root by replacing with last element
            heapSize--;
            if (heapSize > 0) {
                heapTs[0] = heapTs[heapSize];
                heapPos[0] = heapPos[heapSize];
                heapOffsetIdx[0] = heapOffsetIdx[heapSize];
                siftDown();
            }
        }
        currentIndex++;
        return true;
    }

    @Override
    public void of(PageFrameMemoryRecord record, long frameRowLo, long frameRowCount, int timestampColumnIndex) {
        this.record = record;
        this.timestampColumnIndex = timestampColumnIndex;
        this.frameRowLo = frameRowLo;
        this.masterRowCount = frameRowCount;
        this.isFiltered = false;
        this.filteredRows = null;
        this.tupleCount = frameRowCount * offsets.size();
        this.currentIndex = 0;
        initHeap(frameRowCount > 0 ? frameRowLo : -1, frameRowCount > 0);
    }

    @Override
    public void ofFiltered(PageFrameMemoryRecord record, DirectLongList filteredRows, int timestampColumnIndex) {
        this.record = record;
        this.timestampColumnIndex = timestampColumnIndex;
        this.masterRowCount = filteredRows.size();
        this.filteredRows = filteredRows;
        this.isFiltered = true;
        this.tupleCount = filteredRows.size() * offsets.size();
        this.currentIndex = 0;
        initHeap(filteredRows.size() > 0 ? filteredRows.get(0) : -1, filteredRows.size() > 0);
    }

    private void heapInsert(long ts, int offsetIdx) {
        int i = heapSize++;
        heapTs[i] = ts;
        heapPos[i] = 0;
        heapOffsetIdx[i] = offsetIdx;
        siftUp(i);
    }

    private void initHeap(long firstRowIdx, boolean hasRows) {
        heapSize = 0;
        if (hasRows) {
            record.setRowIndex(firstRowIdx);
            long firstMasterTs = record.getTimestamp(timestampColumnIndex);
            for (int k = 0, n = offsets.size(); k < n; k++) {
                long horizonTs = scaleTimestamp(firstMasterTs + offsets.getQuick(k), masterTsScale);
                heapInsert(horizonTs, k);
            }
        }
    }

    private void siftDown() {
        // "Hole" sift-down: save root, move smaller children up, place saved element at final position.
        // This halves array writes compared to swapping at each level.
        long savedTs = heapTs[0];
        long savedPos = heapPos[0];
        int savedOffsetIdx = heapOffsetIdx[0];
        int i = 0;
        while (true) {
            int left = 2 * i + 1;
            if (left >= heapSize) {
                break;
            }
            int smallest = left;
            int right = left + 1;
            if (right < heapSize && heapTs[right] < heapTs[left]) {
                smallest = right;
            }
            if (savedTs <= heapTs[smallest]) {
                break;
            }
            heapTs[i] = heapTs[smallest];
            heapPos[i] = heapPos[smallest];
            heapOffsetIdx[i] = heapOffsetIdx[smallest];
            i = smallest;
        }
        heapTs[i] = savedTs;
        heapPos[i] = savedPos;
        heapOffsetIdx[i] = savedOffsetIdx;
    }

    private void siftUp(int i) {
        while (i > 0) {
            int parent = (i - 1) / 2;
            if (heapTs[i] >= heapTs[parent]) {
                break;
            }
            swap(i, parent);
            i = parent;
        }
    }

    private void swap(int a, int b) {
        long t;
        t = heapTs[a];
        heapTs[a] = heapTs[b];
        heapTs[b] = t;
        t = heapPos[a];
        heapPos[a] = heapPos[b];
        heapPos[b] = t;
        int ti = heapOffsetIdx[a];
        heapOffsetIdx[a] = heapOffsetIdx[b];
        heapOffsetIdx[b] = ti;
    }
}
