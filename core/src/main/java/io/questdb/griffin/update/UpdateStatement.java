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

import io.questdb.cairo.TableStructureChangesException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AsyncWriterCommand;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

import java.io.Closeable;

public class UpdateStatement implements AsyncWriterCommand, Closeable {
    private String tableName;
    private int tableId;
    private long tableVersion;
    private RecordCursorFactory updateToDataCursorFactory;
    private UpdateExecution updateExecution;
    private SqlExecutionContext executionContext;

    public UpdateStatement of(
            String tableName,
            int tableId,
            long tableVersion,
            RecordCursorFactory updateToCursorFactory,
            UpdateExecution updateExecution,
            SqlExecutionContext executionContext
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.tableVersion = tableVersion;
        this.updateToDataCursorFactory = updateToCursorFactory;
        this.updateExecution = updateExecution;
        this.executionContext = executionContext;
        return this;
    }

    @Override
    public void close() {
        updateToDataCursorFactory = Misc.free(updateToDataCursorFactory);
    }

    public int getTableId() {
        return tableId;
    }

    @Override
    public int getTableNamePosition() {
        return 7;
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public void apply(TableWriter tableWriter, boolean acceptStructureChange) throws SqlException, TableStructureChangesException {
        updateExecution.executeUpdate(tableWriter, this, executionContext);
    }

    public long getTableVersion() {
        return tableVersion;
    }

    public RecordCursorFactory getUpdateToDataCursorFactory() {
        return updateToDataCursorFactory;
    }
}
