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
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Iterator that produces horizon timestamps (master_timestamp + offset) in sorted order.
 * <p>
 * For each master row in a page frame and each offset in the offset list, this generates
 * a (horizonTimestamp, masterRowIndex, offsetIndex) tuple. The tuples are sorted by
 * horizonTimestamp, enabling efficient sequential ASOF lookups.
 * <p>
 * Memory layout uses native memory for efficient sorting. Each tuple is stored as:
 * - 8 bytes: horizon timestamp (used as sort key)
 * - 8 bytes: packed value (masterRowIndex in upper 32 bits, offsetIndex in lower 32 bits)
 */
public class HorizonTimestampIterator implements QuietCloseable {
    private static final int TUPLE_SIZE = 16; // 8 bytes timestamp + 8 bytes packed value
    private static final long RADIX_SORT_THRESHOLD = 1000;

    private final LongList offsets;
    private final long masterTsScale;

    // Native memory for tuples
    private long tupleBuffer;
    private long tupleBufferCapacity; // in tuples
    private long tupleCount;

    // Temporary buffer for radix sort
    private long sortTempBuffer;
    private long sortTempBufferCapacity;

    // Current iteration position
    private long currentIndex;

    // Current tuple values (populated by next())
    private long currentHorizonTs;
    private long currentMasterRowIdx;
    private int currentOffsetIdx;

    /**
     * Creates a new horizon timestamp iterator.
     *
     * @param offsets       list of time offsets to apply to each master timestamp
     * @param masterTsScale scale factor for master timestamps (1 if no scaling needed)
     */
    public HorizonTimestampIterator(LongList offsets, long masterTsScale) {
        this.offsets = offsets;
        this.masterTsScale = masterTsScale;
        this.tupleBuffer = 0;
        this.tupleBufferCapacity = 0;
    }

    @Override
    public void close() {
        if (tupleBuffer != 0) {
            Unsafe.free(tupleBuffer, tupleBufferCapacity * TUPLE_SIZE, MemoryTag.NATIVE_DEFAULT);
            tupleBuffer = 0;
            tupleBufferCapacity = 0;
        }
        if (sortTempBuffer != 0) {
            Unsafe.free(sortTempBuffer, sortTempBufferCapacity, MemoryTag.NATIVE_DEFAULT);
            sortTempBuffer = 0;
            sortTempBufferCapacity = 0;
        }
    }

    /**
     * Initializes the iterator for a new page frame.
     * Generates and sorts all horizon timestamps for the frame.
     *
     * @param record               record positioned at the frame (used to read timestamps)
     * @param frameRowLo           first row index in the frame
     * @param frameRowCount        number of rows in the frame
     * @param timestampColumnIndex index of the timestamp column in the record
     */
    public void of(
            PageFrameMemoryRecord record,
            long frameRowLo,
            long frameRowCount,
            int timestampColumnIndex
    ) {
        final int offsetCount = offsets.size();
        final long totalTuples = frameRowCount * offsetCount;

        // Ensure buffer capacity
        ensureCapacity(totalTuples);

        // Generate all tuples
        long tupleIdx = 0;
        for (long rowIdx = 0; rowIdx < frameRowCount; rowIdx++) {
            record.setRowIndex(frameRowLo + rowIdx);
            long masterTs = record.getTimestamp(timestampColumnIndex);

            for (int offsetIdx = 0; offsetIdx < offsetCount; offsetIdx++) {
                // Scale AFTER adding offset (offset is in master's original timestamp unit)
                long horizonTs = scaleTimestamp(masterTs + offsets.getQuick(offsetIdx), masterTsScale);
                long packedValue = packValue(rowIdx, offsetIdx);

                long tupleAddr = tupleBuffer + tupleIdx * TUPLE_SIZE;
                Unsafe.getUnsafe().putLong(tupleAddr, horizonTs);
                Unsafe.getUnsafe().putLong(tupleAddr + 8, packedValue);
                tupleIdx++;
            }
        }

        this.tupleCount = totalTuples;

        // Sort tuples by horizon timestamp
        if (totalTuples > 1) {
            if (totalTuples > RADIX_SORT_THRESHOLD) {
                // Radix sort for large arrays
                ensureSortTempCapacity(totalTuples * TUPLE_SIZE);
                Vect.radixSortLongIndexAscInPlace(tupleBuffer, totalTuples, sortTempBuffer);
            } else {
                // Quick sort for small arrays
                Vect.quickSortLongIndexAscInPlace(tupleBuffer, totalTuples);
            }
        }

        // Reset iteration position
        this.currentIndex = 0;
    }

    /**
     * Initializes the iterator for filtered rows in a page frame.
     * Generates and sorts horizon timestamps only for the filtered rows.
     *
     * @param record               record positioned at the frame (used to read timestamps)
     * @param filteredRows         list of filtered row indices (relative to frame start)
     * @param timestampColumnIndex index of the timestamp column in the record
     */
    public void ofFiltered(
            PageFrameMemoryRecord record,
            DirectLongList filteredRows,
            int timestampColumnIndex
    ) {
        final int offsetCount = offsets.size();
        final long filteredRowCount = filteredRows.size();
        final long totalTuples = filteredRowCount * offsetCount;

        // Ensure buffer capacity
        ensureCapacity(totalTuples);

        // Generate tuples only for filtered rows
        long tupleIdx = 0;
        for (long i = 0; i < filteredRowCount; i++) {
            long rowIdx = filteredRows.get(i);
            record.setRowIndex(rowIdx);
            long masterTs = record.getTimestamp(timestampColumnIndex);

            for (int offsetIdx = 0; offsetIdx < offsetCount; offsetIdx++) {
                // Scale AFTER adding offset (offset is in master's original timestamp unit)
                long horizonTs = scaleTimestamp(masterTs + offsets.getQuick(offsetIdx), masterTsScale);
                long packedValue = packValue(rowIdx, offsetIdx);

                long tupleAddr = tupleBuffer + tupleIdx * TUPLE_SIZE;
                Unsafe.getUnsafe().putLong(tupleAddr, horizonTs);
                Unsafe.getUnsafe().putLong(tupleAddr + 8, packedValue);
                tupleIdx++;
            }
        }

        this.tupleCount = totalTuples;

        // Sort tuples by horizon timestamp
        if (totalTuples > 1) {
            if (totalTuples > RADIX_SORT_THRESHOLD) {
                // Radix sort for large arrays
                ensureSortTempCapacity(totalTuples * TUPLE_SIZE);
                Vect.radixSortLongIndexAscInPlace(tupleBuffer, totalTuples, sortTempBuffer);
            } else {
                // Quick sort for small arrays
                Vect.quickSortLongIndexAscInPlace(tupleBuffer, totalTuples);
            }
        }

        // Reset iteration position
        this.currentIndex = 0;
    }

    /**
     * Returns true if there are more tuples to iterate.
     */
    public boolean hasNext() {
        return currentIndex < tupleCount;
    }

    /**
     * Advances to the next tuple. Call hasNext() first.
     * After calling, use getHorizonTimestamp(), getMasterRowIndex(), and getOffsetIndex()
     * to access the current tuple's values.
     */
    public void next() {
        long tupleAddr = tupleBuffer + currentIndex * TUPLE_SIZE;
        currentHorizonTs = Unsafe.getUnsafe().getLong(tupleAddr);
        long packedValue = Unsafe.getUnsafe().getLong(tupleAddr + 8);
        currentMasterRowIdx = unpackMasterRowIdx(packedValue);
        currentOffsetIdx = unpackOffsetIdx(packedValue);
        currentIndex++;
    }

    /**
     * Returns the horizon timestamp of the current tuple.
     */
    public long getHorizonTimestamp() {
        return currentHorizonTs;
    }

    /**
     * Returns the master row index of the current tuple (relative to frame start).
     */
    public long getMasterRowIndex() {
        return currentMasterRowIdx;
    }

    /**
     * Returns the offset index of the current tuple.
     */
    public int getOffsetIndex() {
        return currentOffsetIdx;
    }

    /**
     * Resets the iterator to the beginning.
     */
    public void toTop() {
        currentIndex = 0;
    }

    private void ensureCapacity(long requiredTuples) {
        if (requiredTuples > tupleBufferCapacity) {
            if (tupleBuffer != 0) {
                Unsafe.free(tupleBuffer, tupleBufferCapacity * TUPLE_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
            // Allocate with some extra capacity to avoid frequent reallocation
            tupleBufferCapacity = Math.max(requiredTuples, tupleBufferCapacity * 2);
            tupleBuffer = Unsafe.malloc(tupleBufferCapacity * TUPLE_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void ensureSortTempCapacity(long requiredBytes) {
        if (requiredBytes > sortTempBufferCapacity) {
            if (sortTempBuffer != 0) {
                Unsafe.free(sortTempBuffer, sortTempBufferCapacity, MemoryTag.NATIVE_DEFAULT);
            }
            sortTempBufferCapacity = Math.max(requiredBytes, sortTempBufferCapacity * 2);
            sortTempBuffer = Unsafe.malloc(sortTempBufferCapacity, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long packValue(long masterRowIdx, int offsetIdx) {
        return (masterRowIdx << 32) | (offsetIdx & 0xFFFFFFFFL);
    }

    private static long unpackMasterRowIdx(long packed) {
        return packed >>> 32;
    }

    private static int unpackOffsetIdx(long packed) {
        return (int) packed;
    }
}
