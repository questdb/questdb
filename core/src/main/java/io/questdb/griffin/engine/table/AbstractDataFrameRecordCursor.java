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
