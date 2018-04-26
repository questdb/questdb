/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.TableReaderRecord;
import com.questdb.cairo.sql.*;
import com.questdb.common.RowCursor;
import com.questdb.std.Rows;

class FilteredTableRecordCursor implements RecordCursor {
    private final TableReaderRecord record = new TableReaderRecord();
    private final RowCursorFactory rowCursorFactory;
    private DataFrameCursor dataFrameCursor;
    private RowCursor rowCursor;

    public FilteredTableRecordCursor(RowCursorFactory rowCursorFactory) {
        this.rowCursorFactory = rowCursorFactory;
    }

    @Override
    public void close() {
        if (dataFrameCursor != null) {
            dataFrameCursor.close();
            dataFrameCursor = null;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return dataFrameCursor.getTableReader().getMetadata();
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        TableReaderRecord record = new TableReaderRecord();
        record.of(dataFrameCursor.getTableReader());
        return record;
    }

    @Override
    public Record recordAt(long rowId) {
        recordAt(record, rowId);
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TableReaderRecord) record).jumpTo(Rows.toPartitionIndex(atRowId), Rows.toLocalRowID(atRowId));
    }

    @Override
    public void toTop() {
        dataFrameCursor.toTop();
    }

    @Override
    public boolean hasNext() {
        if (rowCursor != null && rowCursor.hasNext()) {
            record.setRecordIndex(rowCursor.next());
            return true;
        }
        return nextFrame();
    }

    @Override
    public Record next() {
        return record;
    }

    public void of(DataFrameCursor dataFrameCursor) {
        close();
        this.dataFrameCursor = dataFrameCursor;
        this.record.of(dataFrameCursor.getTableReader());
        toTop();
    }

    private boolean nextFrame() {
        while (dataFrameCursor.hasNext()) {
            DataFrame dataFrame = dataFrameCursor.next();
            rowCursor = rowCursorFactory.getCursor(dataFrame);
            if (rowCursor.hasNext()) {
                record.jumpTo(dataFrame.getPartitionIndex(), rowCursor.next());
                return true;
            }
        }
        return false;
    }
}
