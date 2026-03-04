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

import io.questdb.std.Rows;

/**
 * A {@link PageFrameMemoryRecord} whose {@link #getRowId()} encodes
 * the partition index and row-in-partition (not page frame index and
 * row-in-page-frame) with the {@link TimeFrameCursor#TIME_FRAME_ROW_ID_MARKER}
 * bits set. Use this record type in time frame cursor implementations so
 * that callers that store and later replay row IDs get correctly marked
 * partition-level values.
 */
public class TimeFrameMemoryRecord extends PageFrameMemoryRecord {
    private int partitionIndex;
    private long partitionLocalRow;

    public TimeFrameMemoryRecord(byte letter) {
        super(letter);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(partitionIndex, partitionLocalRow) | TimeFrameCursor.TIME_FRAME_ROW_ID_MARKER;
    }

    /**
     * Sets the row index within the page frame and updates partition-local row.
     * Does not change the partition index.
     */
    public void setRowIndex(long partitionRowIndex, long pageFrameRowLo) {
        super.setRowIndex(partitionRowIndex - pageFrameRowLo);
        this.partitionLocalRow = partitionRowIndex;
    }

    /**
     * Sets the row index within the page frame and updates both partition index
     * and partition-local row.
     */
    public void setRowIndex(int partitionIndex, long partitionRowIndex, long pageFrameRowLo) {
        super.setRowIndex(partitionRowIndex - pageFrameRowLo);
        this.partitionIndex = partitionIndex;
        this.partitionLocalRow = partitionRowIndex;
    }
}
