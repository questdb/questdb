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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Vect;

/**
 * Horizon timestamp iterator for {@link RecordCursor} with random access.
 * <p>
 * Mirrors {@link AsyncHorizonTimestampIterator}'s K-way merge design but uses
 * {@link RecordCursor#recordAt(Record, long)} instead of
 * {@link io.questdb.cairo.sql.PageFrameMemoryRecord#setRowIndex(long)}.
 * <p>
 * Master rows are discovered lazily via {@link RecordCursor#hasNext()} and their rowIds
 * are cached in a sliding window. Timestamps are read on-the-fly via {@code recordAt()}.
 * The sliding window evicts rowIds below the minimum stream position, bounding memory
 * to the spread between the fastest and slowest offset streams. A ring buffer holds the
 * window, so an eviction only advances the window's head and moves no rowIds.
 * <p>
 * When there is only one offset, the heap is bypassed and master rows are iterated in order.
 */
public class HorizonTimestampIterator implements QuietCloseable {
    private static final int MAX_WINDOW_SIZE = Integer.MAX_VALUE / 2;
    private final int[] heapOffsetIdx;
    private final long[] heapPos;
    private final long[] heapTs;
    private final long[] offsets;
    // Ring buffer of the rowIds of master positions [windowBase, discoveredCount) (off-heap).
    // Its capacity stays a power of two: 64 longs, doubled when the window fills it.
    private final DirectLongList rowIds;
    private final boolean singleOffset;
    private long currentHorizonTs;
    private long currentMasterRowId;
    private int currentOffsetIdx;
    // Total master rows discovered so far
    private long discoveredCount;
    private boolean exhausted;
    private int heapSize;
    private RecordCursor masterCursor;
    private Record recordB;
    // Single offset optimization state
    private long singleOffsetValue;
    private int timestampColumnIndex;
    // The first master position in the window
    private long windowBase;
    // Index in rowIds of the rowId at windowBase
    private long windowHead;

    public HorizonTimestampIterator(long[] offsets) {
        this.offsets = offsets;
        this.rowIds = new DirectLongList(64, MemoryTag.NATIVE_LONG_LIST, true);
        int k = offsets.length;
        if (k == 1) {
            this.singleOffset = true;
            this.singleOffsetValue = offsets[0];
            this.heapTs = null;
            this.heapPos = null;
            this.heapOffsetIdx = null;
        } else {
            this.singleOffset = false;
            this.heapTs = new long[k];
            this.heapPos = new long[k];
            this.heapOffsetIdx = new int[k];
        }
    }

    @Override
    public void close() {
        Misc.free(rowIds);
    }

    public long getHorizonTimestamp() {
        return currentHorizonTs;
    }

    public long getMasterRowId() {
        return currentMasterRowId;
    }

    public int getOffsetIndex() {
        return currentOffsetIdx;
    }

    public boolean next() {
        if (singleOffset) {
            return nextSingleOffset();
        }
        return nextMultiOffset();
    }

    /**
     * Initializes the iterator for a new master cursor.
     *
     * @param masterCursor         the master cursor (must support random access)
     * @param recordB              a record for random access positioning
     * @param timestampColumnIndex the timestamp column index in the master record
     */
    public void of(RecordCursor masterCursor, Record recordB, int timestampColumnIndex) {
        this.masterCursor = masterCursor;
        this.recordB = recordB;
        this.timestampColumnIndex = timestampColumnIndex;
        this.windowBase = 0;
        this.windowHead = 0;
        this.discoveredCount = 0;
        this.exhausted = false;
        rowIds.reopen();
        assert Numbers.isPow2(rowIds.getCapacity());

        if (singleOffset) {
            // Nothing to seed; nextSingleOffset() will discover rows on demand
            return;
        }

        // Discover the first master row for heap seeding
        if (discoverNextRow()) {
            initHeap();
        } else {
            heapSize = 0;
        }
    }

    private static long addTimestampAndOffset(long timestamp, long offset) {
        try {
            return Math.addExact(timestamp, offset);
        } catch (ArithmeticException e) {
            throw CairoException.nonCritical().put("horizon timestamp overflow [timestamp=").put(timestamp).put(", offset=").put(offset).put(']');
        }
    }

    private boolean discoverNextRow() {
        if (exhausted) {
            return false;
        }
        if (masterCursor.hasNext()) {
            Record record = masterCursor.getRecord();
            final long windowSize = discoveredCount - windowBase;
            if (windowSize >= MAX_WINDOW_SIZE) {
                throw CairoException.nonCritical().put("horizon join sliding window is too large; consider reducing the offset range");
            }
            final long capacity = rowIds.getCapacity();
            if (windowSize == capacity) {
                growWindow(capacity);
            }
            rowIds.set((windowHead + windowSize) & (rowIds.getCapacity() - 1), record.getRowId());
            discoveredCount++;
            return true;
        }
        exhausted = true;
        return false;
    }

    /**
     * Evict rowIds that are no longer needed (below the minimum stream position).
     */
    private void evict() {
        if (heapSize == 0) {
            return;
        }
        long minPos = heapPos[0];
        for (int i = 1; i < heapSize; i++) {
            if (heapPos[i] < minPos) {
                minPos = heapPos[i];
            }
        }
        if (minPos > windowBase) {
            windowHead = (windowHead + minPos - windowBase) & (rowIds.getCapacity() - 1);
            windowBase = minPos;
        }
    }

    private long getRowId(long pos) {
        return rowIds.get((windowHead + pos - windowBase) & (rowIds.getCapacity() - 1));
    }

    /**
     * Doubles the ring buffer when the window fills it. The window then runs from windowHead
     * to the end of the buffer and wraps around to index 0. The wrapped part moves past the old
     * end, so the window runs contiguously from windowHead in the doubled buffer.
     */
    private void growWindow(long capacity) {
        rowIds.setCapacity(capacity << 1);
        if (windowHead > 0) {
            Vect.memcpy(rowIds.getAddress() + (capacity << 3), rowIds.getAddress(), windowHead << 3);
        }
    }

    private void heapInsert(long ts, int offsetIdx) {
        int i = heapSize++;
        heapTs[i] = ts;
        heapPos[i] = 0;
        heapOffsetIdx[i] = offsetIdx;
        siftUp(i);
    }

    private void initHeap() {
        heapSize = 0;
        // Read timestamp of first row
        long firstRowId = getRowId(0);
        masterCursor.recordAt(recordB, firstRowId);
        long firstMasterTs = recordB.getTimestamp(timestampColumnIndex);
        for (int k = 0, n = offsets.length; k < n; k++) {
            long horizonTs = addTimestampAndOffset(firstMasterTs, offsets[k]);
            heapInsert(horizonTs, k);
        }
    }

    private boolean nextMultiOffset() {
        if (heapSize == 0) {
            return false;
        }

        // Read min from heap root
        currentHorizonTs = heapTs[0];
        long pos = heapPos[0];
        int offsetIdx = heapOffsetIdx[0];
        currentOffsetIdx = offsetIdx;
        currentMasterRowId = getRowId(pos);

        // Advance this stream to the next position
        long nextPos = pos + 1;
        if (nextPos < discoveredCount || discoverNextRow()) {
            long nextRowId = getRowId(nextPos);
            masterCursor.recordAt(recordB, nextRowId);
            long nextHorizonTs = addTimestampAndOffset(recordB.getTimestamp(timestampColumnIndex), offsets[offsetIdx]);
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

        // Evict rowIds below the minimum stream position
        evict();
        return true;
    }

    private boolean nextSingleOffset() {
        // With a single offset, master rows are already in sorted horizon-timestamp order
        if (!discoverNextRow()) {
            return false;
        }
        long rowId = getRowId(discoveredCount - 1);
        masterCursor.recordAt(recordB, rowId);
        currentHorizonTs = addTimestampAndOffset(recordB.getTimestamp(timestampColumnIndex), singleOffsetValue);
        currentMasterRowId = rowId;
        currentOffsetIdx = 0;
        // No need to cache rowIds; evict immediately. The window is empty, and the next
        // rowId lands at windowHead again.
        windowBase = discoveredCount;
        return true;
    }

    private void siftDown() {
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
