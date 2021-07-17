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

import io.questdb.cairo.TableReaderSelectedColumnRecord;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractDataFrameRecordCursor implements RecordCursor {
    protected final TableReaderSelectedColumnRecord recordA;
    protected final TableReaderSelectedColumnRecord recordB;
    protected DataFrameCursor dataFrameCursor;
    protected final IntList columnIndexes;

    public AbstractDataFrameRecordCursor(@NotNull IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
        this.recordA = new TableReaderSelectedColumnRecord(columnIndexes);
        this.recordB = new TableReaderSelectedColumnRecord(columnIndexes);
    }

    @Override
    public void close() {
        if (dataFrameCursor != null) {
            dataFrameCursor.close();
            dataFrameCursor = null;
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return dataFrameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TableReaderSelectedColumnRecord) record).jumpTo(Rows.toPartitionIndex(atRowId), Rows.toLocalRowID(atRowId));
    }

    abstract void of(DataFrameCursor cursor, SqlExecutionContext executionContext) throws SqlException;
}
