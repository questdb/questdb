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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.QuietClosable;
import io.questdb.tasks.TableWriterTask;

import java.util.concurrent.atomic.AtomicInteger;

public class UpdateOperation extends AbstractOperation implements QuietClosable {
    public static final int WRITER_CLOSED_INCREMENT = 10;
    public static final int SENDER_CLOSED_INCREMENT = 7;
    public static final int FULLY_CLOSED_STATE = WRITER_CLOSED_INCREMENT + SENDER_CLOSED_INCREMENT;
    private final AtomicInteger closeState = new AtomicInteger();
    private RecordCursorFactory factory;
    private SqlExecutionContext sqlExecutionContext;
    private volatile boolean requesterTimeout = false;
    private boolean executingAsync;
    private SqlExecutionCircuitBreaker circuitBreaker;

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
        requesterTimeout = true;
        if (!executingAsync || closeState.addAndGet(SENDER_CLOSED_INCREMENT) == FULLY_CLOSED_STATE) {
            factory = Misc.free(factory);
        }
    }

    public void closeWriter() {
        if (executingAsync && closeState.addAndGet(WRITER_CLOSED_INCREMENT) == FULLY_CLOSED_STATE) {
            factory = Misc.free(factory);
        }
    }

    public boolean isWriterClosePending() {
        return executingAsync && closeState.get() != WRITER_CLOSED_INCREMENT;
    }

    public void start() {
        executingAsync = false;
        closeState.set(0);
        requesterTimeout = false;
    }

    @Override
    public void startAsync() {
        this.executingAsync = true;
    }

    public void testTimeout() {
        if (requesterTimeout) {
            throw CairoException.instance(0).put("requester timed out, update aborted").setInterruption(true);
        }

        circuitBreaker.statefulThrowExceptionIfTripped();
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
        this.circuitBreaker = sqlExecutionContext.getCircuitBreaker();
    }
}
