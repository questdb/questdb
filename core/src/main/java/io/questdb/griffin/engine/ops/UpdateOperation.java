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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.QuietClosable;
import io.questdb.tasks.TableWriterTask;

public class UpdateOperation extends AbstractOperation implements QuietClosable {
    private RecordCursorFactory factory;
    private SqlExecutionContext sqlExecutionContext;

    public UpdateOperation(
            String tableName,
            int tableId,
            long tableVersion,
            int tableNamePosition,
            RecordCursorFactory factory
    ) {
        init(TableWriterTask.CMD_UPDATE_TABLE, "UPDATE", tableName, tableId, tableVersion, tableNamePosition);
        this.factory = factory;
    }

    @Override
    public long apply(TableWriter tableWriter, boolean contextAllowsAnyStructureChanges) throws SqlException {
        return tableWriter.getUpdateOperator().executeUpdate(sqlExecutionContext, this);
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        return task.getAsyncWriterCommand();
    }

    @Override
    public void close() {
        factory = Misc.free(factory);
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }

    @Override
    public void serialize(TableWriterTask task) {
        super.serialize(task);
        task.setAsyncWriterCommand(this);
    }

    public void withContext(SqlExecutionContext sqlExecutionContext) {
        this.sqlExecutionContext = sqlExecutionContext;
    }
}
