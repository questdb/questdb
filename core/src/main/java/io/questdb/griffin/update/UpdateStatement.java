/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.update;

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

import java.io.Closeable;

public class UpdateStatement implements Closeable {
    public final static UpdateStatement EMPTY = new UpdateStatement();
    private final CharSequence updateTableName;
    private RecordCursorFactory rowIdFactory;
    private Function rowIdFilter;
    private Function postJoinFilter;
    private RecordMetadata valuesMetadata;
    private UpdateStatementMasterCursorFactory joinRecordCursorFactory;
    private RecordColumnMapper columnMapper;
    private int tableId;
    private long tableVersion;

    public UpdateStatement(
            CharSequence tableName,
            RecordCursorFactory rowIdFactory,
            Function rowIdFilter,
            Function joinFilter,
            RecordMetadata valuesMetadata,
            UpdateStatementMasterCursorFactory joinRecordCursorFactory,
            RecordColumnMapper columnMapper,
            int tableId,
            long tableVersion
    ) {
        this.updateTableName = tableName;
        this.rowIdFactory = rowIdFactory;
        this.rowIdFilter = rowIdFilter;
        this.postJoinFilter = joinFilter;
        this.valuesMetadata = valuesMetadata;
        this.joinRecordCursorFactory = joinRecordCursorFactory;
        this.columnMapper = columnMapper;
        this.tableId = tableId;
        this.tableVersion = tableVersion;
    }

    private UpdateStatement() {
        updateTableName = "";
    }

    @Override
    public void close() {
        columnMapper = Misc.free(columnMapper);
        rowIdFactory = Misc.free(rowIdFactory);
        rowIdFilter = Misc.free(rowIdFilter);
        valuesMetadata = Misc.free(valuesMetadata);
        postJoinFilter = Misc.free(postJoinFilter);
        joinRecordCursorFactory = Misc.free(joinRecordCursorFactory);
    }

    public int getTableId() {
        return tableId;
    }

    public long getTableVersion() {
        return tableVersion;
    }

    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        columnMapper.init(symbolTableSource, executionContext);
        if (rowIdFilter != null) {
            rowIdFilter.init(symbolTableSource, executionContext);
        }
    }

    public RecordColumnMapper getColumnMapper() {
        return columnMapper;
    }

    public UpdateStatementMasterCursorFactory getJoinRecordCursorFactory() {
        return joinRecordCursorFactory;
    }

    public Function getPostJoinFilter() {
        return postJoinFilter;
    }

    public RecordCursorFactory getRowIdFactory() {
        return rowIdFactory;
    }

    public Function getRowIdFilter() {
        return rowIdFilter;
    }

    public CharSequence getUpdateTableName() {
        return updateTableName;
    }

    public RecordMetadata getValuesMetadata() {
        return valuesMetadata;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public void setTableVersion(long tableVersion) {
        this.tableVersion = tableVersion;
    }
}
