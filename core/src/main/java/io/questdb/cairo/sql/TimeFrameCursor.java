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

package io.questdb.cairo.sql;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.std.LongList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;

/**
 * Cursor for time-based navigation. Supports lazy navigation in both directions
 * and random row access.
 */
public interface TimeFrameCursor extends SymbolTableSource, QuietCloseable {
    // Page frame count threshold: use linear scan below this, binary search above.
    int PAGE_FRAME_SCAN_THRESHOLD = 64;

    // Marker bit set on all TimeFrameCursor row IDs (bit 43). This bit falls
    // within the local row ID range (bits 0-43) of the Rows encoding, so it
    // does not affect the partition index (bits 44-62). It reduces the max
    // rows per partition from 2^44 to 2^43 (~8.8T), which is more than enough.
    // Bit 43 ensures toRowID(0, 0) != Long.MIN_VALUE, avoiding collisions
    // with Long.MIN_VALUE sentinels in join helpers. Row IDs stay positive,
    // preserving the sign-based found/not-found convention in memoized cursors.
    long TIME_FRAME_ROW_ID_MARKER = 1L << 43;

    // Finds the partition-local page frame index containing the given row.
    // The cumulativeRows list stores cumulative row counts: cumulativeRows[i]
    // is the total number of rows in page frames 0..i. So page frame i covers
    // rows [cumulativeRows[i-1], cumulativeRows[i]). Uses linear scan for
    // small partitions and binary search for larger ones.
    static int findPageFrame(int pageFrameStart, int pageFrameCount, LongList cumulativeRows, long rowInPartition) {
        int lo;
        if (pageFrameCount <= PAGE_FRAME_SCAN_THRESHOLD) {
            lo = 0;
            while (lo < pageFrameCount - 1 && cumulativeRows.getQuick(pageFrameStart + lo) <= rowInPartition) {
                lo++;
            }
        } else {
            lo = 0;
            int hi = pageFrameCount - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cumulativeRows.getQuick(pageFrameStart + mid) <= rowInPartition) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
        }
        return lo;
    }

    static boolean isTimeFrameRowID(long rowId) {
        return (rowId & TIME_FRAME_ROW_ID_MARKER) != 0;
    }

    static long toLocalRowID(long timeFrameRowId) {
        return Rows.toLocalRowID(timeFrameRowId & ~TIME_FRAME_ROW_ID_MARKER);
    }

    static int toPartitionIndex(long timeFrameRowId) {
        return Rows.toPartitionIndex(timeFrameRowId & ~TIME_FRAME_ROW_ID_MARKER);
    }

    /**
     * Encodes a time frame index and row index into a row ID with the
     * {@link #TIME_FRAME_ROW_ID_MARKER} bit set.
     *
     * @param frameIndex time frame index (partition index)
     * @param rowIndex   row index within the time frame
     * @return encoded row ID with marker bit set
     */
    static long toRowID(int frameIndex, long rowIndex) {
        return Rows.toRowID(frameIndex, rowIndex) | TIME_FRAME_ROW_ID_MARKER;
    }

    /**
     * Gets the bitmap index reader for the specified column in the current frame (partition).
     * This method enables efficient symbol-based lookups in ASOF JOIN operations.
     * <p>
     * Not available on concurrent implementation.
     *
     * @param columnIndex the column index to get the bitmap index for
     * @param direction   the direction for index traversal (BitmapIndexReader.DIR_FORWARD or DIR_BACKWARD)
     * @return BitmapIndexReader for the specified column, or null if the column is not indexed
     * or if this cursor doesn't support indexed access
     */
    default BitmapIndexReader getIndexReaderForCurrentFrame(int columnIndex, int direction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return record to be used for random access on open time frames
     */
    Record getRecord();

    /**
     * May be used to compare references with getRecord. Not available on concurrent implementation.
     *
     * @return record to be used for random access on open time frames
     */
    default Record getRecordB() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns symbol table for the given symbol column.
     */
    @Override
    StaticSymbolTable getSymbolTable(int columnIndex);

    /**
     * Time frame should be used only if a previously called {@link #next()} or {@link #prev()}
     * method returned true.
     */
    TimeFrame getTimeFrame();

    int getTimestampIndex();

    /**
     * Rewinds cursor to the beginning of the given frame. The frame must have been previously iterated.
     * An {@link #open()} call is expected after this one.
     *
     * @param frameIndex index of the frame to rewind to
     */
    void jumpTo(int frameIndex);

    /**
     * Advances the cursor to the next non-empty time frame. Empty partitions
     * (0 rows after interval filtering) are skipped automatically.
     *
     * @return true if a non-empty frame was found, false if no more frames
     */
    boolean next();

    /**
     * Opens frame rows for record navigation and updates frame's row lo/hi fields.
     * <p>
     * Note: this method does NOT initialize the record for access. Use
     * {@link #recordAt(Record, int, long)} or {@link #recordAt(Record, long)}
     * to position and initialize the record after opening a frame.
     *
     * @return frame size in rows
     */
    long open();

    /**
     * Moves the cursor to the previous non-empty time frame. Empty partitions
     * (0 rows after interval filtering) are skipped automatically.
     *
     * @return true if a non-empty frame was found, false if no more frames
     */
    boolean prev();

    /**
     * Positions record at given row id. The row id must be in the [rowIdLo, rowIdHi] range
     * for any of the previously open time frames.
     *
     * @param record to position
     * @param rowId  row id of the desired record
     */
    void recordAt(Record record, long rowId);

    /**
     * Positions record at given frame index and row index. The frame must have been previously opened.
     * This method avoids row id encoding/decoding overhead compared to {@link #recordAt(Record, long)}.
     *
     * @param record     to position
     * @param frameIndex index of the frame
     * @param rowIndex   row index within the frame
     */
    void recordAt(Record record, int frameIndex, long rowIndex);

    /**
     * This sets the record to the given row index, without changing frame ID of the record. Given rowIndex
     * *must* be in the range of the current frame of the record.
     *
     * @param record   to position
     * @param rowIndex row id of the desired record
     */
    void recordAtRowIndex(Record record, long rowIndex);

    /**
     * Binary search for the last frame whose partition ceiling is at or before
     * the given timestamp. Positions the cursor at that frame without opening it.
     * If no such frame exists, positions the cursor before the first frame
     * (so that {@link #next()} returns frame 0).
     * <p>
     * Note: within a split partition, multiple frames share the same ceiling,
     * so this method positions at the last frame in the matching partition.
     * <p>
     * The timestamp must be in the cursor's native timestamp space.
     * <p>
     * This method operates on partition-level metadata (timestamps and ceilings)
     * without opening partitions. It does not skip empty partitions because
     * emptiness is only known after {@link #open()}. Callers must handle the
     * case where the seeked frame turns out to be empty.
     * <p>
     * This is useful for ASOF-style lookups where we need to skip to the area
     * near a target timestamp without scanning all preceding frames.
     *
     * @param timestamp the target timestamp to search for, in native timestamp space
     */
    void seekEstimate(long timestamp);

    /**
     * Return the cursor to the beginning of the page frame.
     * Sets page address to first column.
     */
    void toTop();
}
