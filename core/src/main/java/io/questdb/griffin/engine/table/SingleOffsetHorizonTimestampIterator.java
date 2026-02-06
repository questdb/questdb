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

/**
 * Single-offset horizon timestamp iterator. When there is only one offset,
 * master rows are already in sorted horizon timestamp order, so no sorting
 * or merging is needed.
 * <p>
 * Returns horizon timestamps in master's resolution (master_ts + offset).
 * Callers must scale externally for ASOF lookup when master/slave timestamp types differ.
 */
public class SingleOffsetHorizonTimestampIterator implements HorizonTimestampIterator {
    private final long offset;
    private long currentHorizonTs;
    private long currentIndex;
    private long currentMasterRowIdx;
    private DirectLongList filteredRows;
    private long frameRowLo;
    private boolean isFiltered;
    private PageFrameMemoryRecord record;
    private int timestampColumnIndex;
    private long tupleCount;

    public SingleOffsetHorizonTimestampIterator(long offset) {
        this.offset = offset;
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
        return 0;
    }

    @Override
    public boolean next() {
        if (currentIndex >= tupleCount) {
            return false;
        }
        long rowIdx;
        if (isFiltered) {
            rowIdx = filteredRows.get(currentIndex);
            currentMasterRowIdx = rowIdx;
        } else {
            rowIdx = frameRowLo + currentIndex;
            currentMasterRowIdx = currentIndex;
        }
        record.setRowIndex(rowIdx);
        currentHorizonTs = record.getTimestamp(timestampColumnIndex) + offset;
        currentIndex++;
        return true;
    }

    @Override
    public void of(PageFrameMemoryRecord record, long frameRowLo, long frameRowCount, int timestampColumnIndex) {
        this.record = record;
        this.timestampColumnIndex = timestampColumnIndex;
        this.frameRowLo = frameRowLo;
        this.isFiltered = false;
        this.filteredRows = null;
        this.tupleCount = frameRowCount;
        this.currentIndex = 0;
    }

    @Override
    public void ofFiltered(PageFrameMemoryRecord record, DirectLongList filteredRows, int timestampColumnIndex) {
        this.record = record;
        this.timestampColumnIndex = timestampColumnIndex;
        this.filteredRows = filteredRows;
        this.isFiltered = true;
        this.tupleCount = filteredRows.size();
        this.currentIndex = 0;
    }
}
