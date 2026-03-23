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
 * <p>
 * The page frame context ({@code partitionIndex} and {@code pageFrameRowLo})
 * is set once per page frame switch via {@link #setPageFrameContext}. The
 * partition-local row for {@link #getRowId()} is reconstructed on demand
 * from {@code rowIndex + pageFrameRowLo} to avoid an extra field store on
 * the hot per-row path.
 */
public class TimeFrameMemoryRecord extends PageFrameMemoryRecord {
    private long pageFrameRowLo;
    private int partitionIndex;

    public TimeFrameMemoryRecord(byte letter) {
        super(letter);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(partitionIndex, rowIndex + pageFrameRowLo) | TimeFrameCursor.TIME_FRAME_ROW_ID_MARKER;
    }

    /**
     * Sets the page frame context for this record. Called once per page frame
     * switch from the cursor's navigateToRow method.
     *
     * @param partitionIndex partition index
     * @param pageFrameRowLo first partition-local row of the current page frame
     */
    public void setPageFrameContext(int partitionIndex, long pageFrameRowLo) {
        this.partitionIndex = partitionIndex;
        this.pageFrameRowLo = pageFrameRowLo;
    }
}
