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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Misc;

class SymbolIndexFilteredRowCursor implements RowCursor {
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
            int indexDirection
    ) {
        this(columnIndex, filter, indexDirection);
        of(symbolKey);
    }

    public SymbolIndexFilteredRowCursor(
            int columnIndex,
            Function filter,
            int indexDirection
    ) {
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.indexDirection = indexDirection;
        this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    }

    @Override
    public void close() {
        rowCursor = Misc.free(rowCursor);
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
        if (rowCursor != null) {
            rowCursor.close();
        }
        this.rowCursor = pageFrame
                .getIndexReader(columnIndex, indexDirection)
                .getCursor(symbolKey, pageFrame.getPartitionLo(), pageFrame.getPartitionHi() - 1);
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
