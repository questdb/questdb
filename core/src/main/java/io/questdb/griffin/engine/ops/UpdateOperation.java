/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.tasks.TableWriterTask;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.tasks.TableWriterTask.CMD_UPDATE_TABLE;

public class UpdateOperation extends AbstractOperation {
    public static final String MAT_VIEW_INVALIDATION_REASON = "update operation";
    public static final int SENDER_CLOSED_INCREMENT = 7;
    public static final int WRITER_CLOSED_INCREMENT = 10;
    public static final int FULLY_CLOSED_STATE = WRITER_CLOSED_INCREMENT + SENDER_CLOSED_INCREMENT;
    private final AtomicInteger closeState = new AtomicInteger();
    private SqlExecutionCircuitBreaker circuitBreaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private boolean executingAsync;
    private RecordCursorFactory factory;
    private volatile boolean requesterTimeout;

    public UpdateOperation(
            @NotNull TableToken tableToken,
            int tableId,
            long tableVersion,
            int tableNamePosition
    ) {
        this(tableToken, tableId, tableVersion, tableNamePosition, null);
    }

    public UpdateOperation(
            @NotNull TableToken tableToken,
            int tableId,
            long tableVersion,
            int tableNamePosition,
            RecordCursorFactory factory
    ) {
        init(CMD_UPDATE_TABLE, TableWriterTask.getCommandName(CMD_UPDATE_TABLE), tableToken, tableId, tableVersion, tableNamePosition);
        this.factory = factory;
    }

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        return svc.getUpdateOperator().executeUpdate(sqlExecutionContext, this);
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

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        return task.getAsyncWriterCommand();
    }

    public void forceTestTimeout() {
        int state = SqlExecutionCircuitBreaker.STATE_OK;
        if (requesterTimeout || (state = circuitBreaker.getState()) != SqlExecutionCircuitBreaker.STATE_OK) {
            if (state == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
                throw CairoException.queryCancelled(circuitBreaker.getFd());
            } else {
                throw CairoException.queryTimedOut(circuitBreaker.getFd(), 0, 0);
            }
        }
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }

    @Override
    public boolean isStructural() {
        return false;
    }

    public boolean isWriterClosePending() {
        return executingAsync && closeState.get() != WRITER_CLOSED_INCREMENT;
    }

    @Override
    public String matViewInvalidationReason() {
        return MAT_VIEW_INVALIDATION_REASON;
    }

    @Override
    public void serialize(TableWriterTask task) {
        super.serialize(task);
        task.setAsyncWriterCommand(this);
    }

    public void start() {
        executingAsync = false;
        closeState.set(0);
        requesterTimeout = false;
    }

    @Override
    public void startAsync() {
        assert closeState.get() == 0;
        executingAsync = true;
    }

    public void testTimeout() {
        if (requesterTimeout) {
            throw CairoException.queryTimedOut(circuitBreaker.getFd(), 0, 0);
        }

        circuitBreaker.statefulThrowExceptionIfTripped();
    }

    @Override
    public void withContext(@NotNull SqlExecutionContext sqlExecutionContext) {
        super.withContext(sqlExecutionContext);
        circuitBreaker = sqlExecutionContext.getSimpleCircuitBreaker();
    }
}
