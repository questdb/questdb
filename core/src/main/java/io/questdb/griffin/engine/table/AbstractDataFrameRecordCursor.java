/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractDataFrameRecordCursor implements DataFrameRecordCursor {
    protected final IntList columnIndexes;
    protected final TableReaderSelectedColumnRecord recordA;
    protected final TableReaderSelectedColumnRecord recordB;
    protected DataFrameCursor dataFrameCursor;

    public AbstractDataFrameRecordCursor(@NotNull IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
        this.recordA = new TableReaderSelectedColumnRecord(columnIndexes);
        this.recordB = new TableReaderSelectedColumnRecord(columnIndexes);
    }

    @Override
    public void close() {
        dataFrameCursor = Misc.free(dataFrameCursor);
    }

    @Override
    public IntList getColumnIndexes() {
        return columnIndexes;
    }

    @Override
    public DataFrameCursor getDataFrameCursor() {
        return dataFrameCursor;
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return dataFrameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return dataFrameCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TableReaderSelectedColumnRecord) record).jumpTo(Rows.toPartitionIndex(atRowId), Rows.toLocalRowID(atRowId));
    }
}
