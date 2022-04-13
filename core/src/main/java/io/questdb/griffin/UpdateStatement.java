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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.tasks.TableWriterTask;

import java.io.Closeable;

public class UpdateStatement extends AsyncWriterCommandBase implements Closeable {
    private RecordCursorFactory updateToDataCursorFactory;
    private final SqlExecutionContext executionContext;

    public UpdateStatement(
            String tableName,
            int tableId,
            long tableVersion,
            int tableNamePosition,
            SqlExecutionContext executionContext,
            RecordCursorFactory updateToDataCursorFactory
    ) {
        init(TableWriterTask.CMD_UPDATE_TABLE, "UPDATE", tableName, tableId, tableVersion, tableNamePosition);
        this.executionContext = executionContext;
        this.updateToDataCursorFactory = updateToDataCursorFactory;
    }

    @Override
    public void close() {
        updateToDataCursorFactory = Misc.free(updateToDataCursorFactory);
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        return task.getAsyncWriterCommand();
    }

    @Override
    public long apply(TableWriter tableWriter, boolean acceptStructureChange) throws SqlException {
        if (updateToDataCursorFactory != null) {
            return tableWriter.getUpdateExecution().executeUpdate(tableWriter, this, executionContext);
        }
        return 0L;
    }

    public RecordCursorFactory getUpdateToDataCursorFactory() {
        return updateToDataCursorFactory;
    }

    @Override
    public void serialize(TableWriterTask task) {
        super.serialize(task);
        task.setAsyncWriterCommand(this);
    }
}
