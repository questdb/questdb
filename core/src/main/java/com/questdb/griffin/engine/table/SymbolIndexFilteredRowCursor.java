/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import com.questdb.cairo.BitmapIndexReader;
import com.questdb.cairo.TableReader;
import com.questdb.cairo.TableReaderRecord;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RowCursor;

class SymbolIndexFilteredRowCursor implements RowCursor {
    private final Function filter;
    private final TableReaderRecord record = new TableReaderRecord();
    private final int columnIndex;
    private final boolean cachedIndexReaderCursor;
    private int symbolKey;
    private RowCursor rowCursor;
    private long rowid;

    public SymbolIndexFilteredRowCursor(int columnIndex, int symbolKey, Function filter, boolean cachedIndexReaderCursor) {
        this(columnIndex, filter, cachedIndexReaderCursor);
        of(symbolKey);
    }

    public SymbolIndexFilteredRowCursor(int columnIndex, Function filter, boolean cachedIndexReaderCursor) {
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
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
                .getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_FORWARD)
                .getCursor(cachedIndexReaderCursor, symbolKey, dataFrame.getRowLo(), dataFrame.getRowHi() - 1);
        record.jumpTo(dataFrame.getPartitionIndex(), 0);
        return this;
    }

    void setTableReader(TableReader tableReader) {
        this.record.of(tableReader);
    }
}
