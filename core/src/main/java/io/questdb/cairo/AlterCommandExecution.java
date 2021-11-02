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

package io.questdb.cairo;

import io.questdb.cairo.sql.AlterStatement;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.FanOut;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.tasks.TableWriterTask;

import java.util.concurrent.locks.LockSupport;

public class AlterCommandExecution {
    /***
     * Executes alter command
     * If writer is busy, posts alter command asynchronous to writer queue and waits for the response in blocking manner
     * @param engine Cairo Engine
     * @param alterStatement statement to execute
     * @param sqlExecutionContext context to execute
     * @param requestContext request context use logging from
     * @throws SqlException if alter table execution fails in sync or async manner
     */
    public static void executeAlterCommand(
            CairoEngine engine,
            AlterStatement alterStatement,
            SqlExecutionContext sqlExecutionContext,
            AlterTableExecutionContext requestContext
    ) throws SqlException {
        try {
            executeAlterStatementSyncOrFail(engine, alterStatement, sqlExecutionContext);
        } catch (EntryUnavailableException ex) {
            executeWriterCommandAsync(engine, alterStatement, requestContext);
        }
    }

    /***
     * Executes alter command
     * If writer is busy, posts alter command asynchronous to writer queue
     * and DOES NOT wait for the response
     * @param engine Cairo Engine
     * @param alterStatement statement to execute
     * @param sqlExecutionContext context to execute
     * @param requestContext request context use logging from
     * @return commandId if async or -1L if executed synchronous
     * @throws SqlException if alter table execution fails in sync
     */
    public static long executeAlterCommandNoWait(
            CairoEngine engine,
            AlterStatement alterStatement,
            SqlExecutionContext sqlExecutionContext,
            AlterTableExecutionContext requestContext
    ) throws SqlException {
        try {
            executeAlterStatementSyncOrFail(engine, alterStatement, sqlExecutionContext);
            return -1L;
        } catch (EntryUnavailableException ex) {
            return executeWriterCommandAsyncNoWait(engine, alterStatement, requestContext);
        }
    }

    // Executes alter command
    // If writer is busy exception is thrown
    public static void executeAlterStatementSyncOrFail(
            CairoEngine engine,
            AlterStatement alterStatement,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (alterStatement != null) {
            try (TableWriter writer = engine.getWriter(
                    sqlExecutionContext.getCairoSecurityContext(),
                    alterStatement.getTableName(), "Alter table statement")) {
                alterStatement.apply(writer, true);
            } catch (TableStructureChangesException e) {
                assert false : "TableStructureChangesException not happen when acceptStructureChange passed as true";
            }
        }
    }

    /***
     * Sets up execution wait sequence to listen to the Engine async writer events
     * @param engine Cairo Engine to consume events from
     * @param consumerSequence sequence to subscribe ot the events
     */
    public static void setUpEngineAsyncWriterEventWait(CairoEngine engine, Sequence consumerSequence) {
        final FanOut writerEventFanOut = engine.getMessageBus().getTableWriterEventFanOut();
        writerEventFanOut.and(consumerSequence);
    }

    /***
     * Cleans up execution wait sequence to listen to the Engine async writer events
     * @param engine Cairo Engine subscribed to
     * @param sequence to unsubscribe from Writer Events
     */
    public static void stopEngineAsyncWriterEventWait(CairoEngine engine, SCSequence sequence) {
        engine.getMessageBus().getTableWriterEventFanOut().remove(sequence);
        sequence.clear();
    }

    /***
     * Blocking wait for the Writer Event
     * @param engine Cairo Engine
     * @param commandId command to wait the reply to
     * @param requestContext request context used for logging and reply subscription
     * @param writerAsyncCommandBusyWaitTimeout maximum wait timeout, microseconds
     * @param queryTableNamePosition table name position in alter SQL query
     * @return null if success, or instance of SqlException of wait is timed out or resulted in an error
     */
    public static SqlException waitWriterEvent(
            CairoEngine engine,
            long commandId,
            AlterTableExecutionContext requestContext,
            long writerAsyncCommandBusyWaitTimeout,
            int queryTableNamePosition
    ) {
        long start = System.currentTimeMillis();
        long maxWaitTimeoutMilli = Math.max(writerAsyncCommandBusyWaitTimeout / 1000L, 1L);
        SCSequence tableWriterEventSeq = requestContext.getWriterEventConsumeSequence();
        RingQueue<TableWriterTask> tableWriterEventQueue = engine.getMessageBus().getTableWriterEventQueue();
        while (true) {
            long seq = tableWriterEventSeq.next();
            if (seq < 0) {
                // Queue is empty, check if the execution blocked for too long
                if (System.currentTimeMillis() - start > maxWaitTimeoutMilli) {
                    return SqlException.$(queryTableNamePosition, "Timeout expired on waiting for the ALTER TABLE execution result");
                }
                LockSupport.parkNanos(100);
                continue;
            }

            TableWriterTask event = tableWriterEventQueue.get(seq);
            if (event.getInstance() != commandId || event.getType() != TableWriterTask.TSK_ALTER_TABLE) {
                requestContext.info()
                        .$("writer command response received and ignored [instance=").$(event.getInstance())
                        .$(",type=").$(event.getType())
                        .$(",expectedInstance=").$(commandId)
                        .I$();
                tableWriterEventSeq.done(seq);
                LockSupport.parkNanos(100);
                continue;
            }

            // If writer failed to execute the ALTER command it will send back string error
            // in the event data
            SqlException result = null;
            int strLen = Unsafe.getUnsafe().getInt(event.getData());
            if (strLen != 0) {
                DirectCharSequence tempDirectCharSequence = requestContext.getDirectCharSequence();
                result = SqlException.$(
                        queryTableNamePosition,
                        tempDirectCharSequence.of(event.getData() + 4L, event.getData() + 4L + 2L * strLen)
                );
            }
            tableWriterEventSeq.done(seq);
            requestContext.info().$("writer command response received [instance=").$(commandId).I$();
            return result;
        }
    }

    private static void executeWriterCommandAsync(
            CairoEngine engine,
            AlterStatement alterStatement,
            AlterTableExecutionContext requestContext
    ) throws SqlException {
        requestContext.info().$("writer busy, will pass ALTER TABLE execution to writer owner async [table=").$(alterStatement.getTableName()).I$();
        setUpEngineAsyncWriterEventWait(engine, requestContext.getWriterEventConsumeSequence());
        try {
            long instance = engine.publishTableWriterCommand(alterStatement);
            requestContext.debug().$("published writer event [table=")
                    .$(alterStatement.getTableName())
                    .$(",instance=").$(instance).I$();

            SqlException status = waitWriterEvent(
                    engine,
                    instance,
                    requestContext,
                    engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout(),
                    alterStatement.getTableNamePosition()
            );
            if (status == null) {
                requestContext.debug().$("received DONE response writer event [table=")
                        .$(alterStatement.getTableName())
                        .$(",instance=").$(instance).I$();
            } else {
                requestContext.info().$("received error response for ALTER TABLE from writer [table=")
                        .$(alterStatement.getTableName())
                        .$(",instance=").$(instance)
                        .$(",error=").$(status.getFlyweightMessage())
                        .I$();
                throw status;
            }
        } finally {
            stopEngineAsyncWriterEventWait(engine, requestContext.getWriterEventConsumeSequence());
        }
    }

    private static long executeWriterCommandAsyncNoWait(
            CairoEngine engine,
            AlterStatement alterStatement,
            AlterTableExecutionContext requestContext
    ) {
        requestContext.info().$("writer busy, will pass ALTER TABLE execution to writer owner async [table=").$(alterStatement.getTableName()).I$();
        long instance = engine.publishTableWriterCommand(alterStatement);
        requestContext.debug().$("published writer event [table=")
                .$(alterStatement.getTableName())
                .$(",instance=").$(instance).I$();
        return instance;
    }
}
