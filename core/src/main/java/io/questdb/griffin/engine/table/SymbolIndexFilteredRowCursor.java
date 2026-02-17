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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;

class SymbolIndexFilteredRowCursor implements RowCursor {
    private final boolean cachedIndexReaderCursor;
    private final int columnIndex;
    private final Function filter;
    private final int indexDirection;
    private final PageFrameMemoryRecord record;
    private RowCursor rowCursor;
    private long rowIndex;
    private int symbolKey;

    public SymbolIndexFilteredRowCursor(
            int columnIndex,
            int symbolKey,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection
    ) {
        this(columnIndex, filter, cachedIndexReaderCursor, indexDirection);
        of(symbolKey);
    }

    public SymbolIndexFilteredRowCursor(
            int columnIndex,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection
    ) {
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
        this.indexDirection = indexDirection;
        this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    }

    @Override
    public boolean hasNext() {
        while (rowCursor.hasNext()) {
            final long rowIndex = rowCursor.next();
            record.setRowIndex(rowIndex);
            if (filter.getBool(record)) {
                this.rowIndex = rowIndex;
                return true;
            }
        }
        return false;
    }

    @Override
    public long next() {
        return rowIndex;
    }

    public void of(int symbolKey) {
        this.symbolKey = TableUtils.toIndexKey(symbolKey);
    }

    public SymbolIndexFilteredRowCursor of(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        this.rowCursor = pageFrame
                .getBitmapIndexReader(columnIndex, indexDirection)
                .getCursor(cachedIndexReaderCursor, symbolKey, pageFrame.getPartitionLo(), pageFrame.getPartitionHi() - 1);
        record.init(pageFrameMemory);
        record.setRowIndex(0);
        return this;
    }

    Function getFilter() {
        return filter;
    }

    int getIndexDirection() {
        return indexDirection;
    }

    int getSymbolKey() {
        return symbolKey;
    }

    void prepare(PageFrameCursor pageFrameCursor) {
        record.of(pageFrameCursor);
    }
}
