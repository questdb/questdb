/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderSelectedColumnRecord;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.IntList;

class SymbolIndexFilteredRowCursor implements RowCursor {
    private final Function filter;
    private final TableReaderSelectedColumnRecord record;
    private final int columnIndex;
    private final boolean cachedIndexReaderCursor;
    private int symbolKey;
    private RowCursor rowCursor;
    private long rowid;
    private final int indexDirection;

    public SymbolIndexFilteredRowCursor(
            int columnIndex,
            int symbolKey,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection,
            IntList columnIndexes
    ) {
        this(columnIndex, filter, cachedIndexReaderCursor, indexDirection, columnIndexes);
        of(symbolKey);
    }

    public SymbolIndexFilteredRowCursor(
            int columnIndex,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection,
            IntList columnIndexes
    ) {
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
        this.indexDirection = indexDirection;
        this.record = new TableReaderSelectedColumnRecord(columnIndexes);
    }

    @Override
    public boolean hasNext() {
        while (rowCursor.hasNext()) {
            final long rowid = rowCursor.next();
            record.setRecordIndex(rowid);
            if (filter.getBool(record)) {
                this.rowid = rowid;
                return true;
            }
        }
        return false;
    }

    @Override
    public long next() {
        return rowid;
    }

    public final void of(int symbolKey) {
        this.symbolKey = TableUtils.toIndexKey(symbolKey);
    }

    public SymbolIndexFilteredRowCursor of(DataFrame dataFrame) {
        this.rowCursor = dataFrame
                .getBitmapIndexReader(columnIndex, indexDirection)
                .getCursor(cachedIndexReaderCursor, symbolKey, dataFrame.getRowLo(), dataFrame.getRowHi() - 1);
        record.jumpTo(dataFrame.getPartitionIndex(), 0);
        return this;
    }

    void prepare(TableReader tableReader) {
        this.record.of(tableReader);
    }
}
