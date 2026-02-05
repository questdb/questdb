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
import io.questdb.std.QuietCloseable;

/**
 * Cursor for time-based navigation. Supports lazy navigation in both directions
 * and random row access.
 */
public interface TimeFrameCursor extends SymbolTableSource, QuietCloseable {

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

    boolean next();

    /**
     * Opens frame rows for record navigation and updates frame's row lo/hi fields.
     *
     * @return frame size in rows
     */
    long open();

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
     * Return the cursor to the beginning of the page frame.
     * Sets page address to first column.
     */
    void toTop();
}
