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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.TableReaderRecord;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Rows;

public abstract class AbstractDataFrameRecordCursor implements RecordCursor {
    protected final TableReaderRecord record = new TableReaderRecord();
    protected DataFrameCursor dataFrameCursor;

    @Override
    public void close() {
        if (dataFrameCursor != null) {
            dataFrameCursor.close();
            dataFrameCursor = null;
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return dataFrameCursor.getSymbolTable(columnIndex);
    }

    @Override
    public Record newRecord() {
        TableReaderRecord record = new TableReaderRecord();
        record.of(dataFrameCursor.getTableReader());
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TableReaderRecord) record).jumpTo(Rows.toPartitionIndex(atRowId), Rows.toLocalRowID(atRowId));
    }

    @Override
    public void recordAt(long rowId) {
        recordAt(record, rowId);
    }

    abstract void of(DataFrameCursor cursor, SqlExecutionContext executionContext);
}
