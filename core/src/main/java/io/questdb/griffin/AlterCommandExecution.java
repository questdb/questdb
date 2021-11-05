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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableStructureChangesException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.sql.AlterStatement;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TableWriterTask;

import java.util.concurrent.locks.LockSupport;

public class AlterCommandExecution {
    private static final Log LOG = LogFactory.getLog(WriterPool.class);

    /***
     * Executes alter command
     * If writer is busy, posts alter command asynchronous to writer queue and waits for the response in blocking manner
     * @param engine Cairo Engine
     * @param alterStatement statement to execute
     * @param sqlExecutionContext context to execute
     * @param responseConsumeSequence consume sequence to await async response from Table Writer
     * @throws SqlException if alter table execution fails in sync or async manner
     */
    static void executeAlterCommand(
            CairoEngine engine,
            AlterStatement alterStatement,
            SqlExecutionContext sqlExecutionContext,
            SCSequence responseConsumeSequence
    ) throws SqlException {
        try {
            executeAlterStatementSyncOrFail(engine, alterStatement, sqlExecutionContext);
        } catch (EntryUnavailableException ex) {
            executeWriterCommandAsync(engine, alterStatement, responseConsumeSequence);
        }
    }

    /***
     * Executes alter command
     * If writer is busy, posts alter command asynchronous to writer queue
     * and DOES NOT wait for the response
     * @param engine Cairo Engine
     * @param alterStatement statement to execute
     * @param sqlExecutionContext context to execute
     * @return commandId if async or -1L if executed synchronous
     * @throws SqlException if alter table execution fails in sync
     */
    static long executeAlterCommandNoWait(
            CairoEngine engine,
            AlterStatement alterStatement,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        try {
            executeAlterStatementSyncOrFail(engine, alterStatement, sqlExecutionContext);
            return -1L;
        } catch (EntryUnavailableException ex) {
            return executeWriterCommandAsyncNoWait(engine, alterStatement);
        }
    }

    // Executes alter command
    // If writer is busy exception is thrown
    static void executeAlterStatementSyncOrFail(
            CairoEngine engine,
            AlterStatement alterStatement,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (alterStatement != null) {
            assert alterStatement.getTableName() != null;
            try (
                    TableWriter writer = engine.getWriter(
                            sqlExecutionContext.getCairoSecurityContext(),
                            alterStatement.getTableName(), "alter statement execution"
                    )
            ) {
                alterStatement.apply(writer, true);
            } catch (TableStructureChangesException e) {
                assert false : "TableStructureChangesException not happen when acceptStructureChange passed as true";
            }
        }
    }

    /***
     * Sets up execution wait sequence to listen to the Engine async writer events
     * @param engine Cairo Engine to consume events from
     * @param sequence sequence to subscribe ot the events
     */
    public static void setUpEngineAsyncWriterEventWait(CairoEngine engine, SCSequence sequence) {
        final FanOut writerEventFanOut = engine.getMessageBus().getTableWriterEventFanOut();
        writerEventFanOut.and(sequence);
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
     * @param responseConsumeSequence consume sequence to await async response from Table Writer
     * @param writerAsyncCommandBusyWaitTimeout maximum wait timeout, microseconds
     * @param queryTableNamePosition table name position in alter SQL query
     * @return null if success, or instance of SqlException of wait is timed out or resulted in an error
     */
    public static SqlException waitWriterEvent(
            CairoEngine engine,
            long commandId,
            Sequence responseConsumeSequence,
            long writerAsyncCommandBusyWaitTimeout,
            int queryTableNamePosition
    ) {
        final MicrosecondClock clock = engine.getConfiguration().getMicrosecondClock();
        final long start = clock.getTicks();
        final RingQueue<TableWriterTask> tableWriterEventQueue = engine.getMessageBus().getTableWriterEventQueue();

        while (true) {
            long seq = responseConsumeSequence.next();
            if (seq < 0) {
                // Queue is empty, check if the execution blocked for too long
                if (clock.getTicks() - start > writerAsyncCommandBusyWaitTimeout) {
                    return SqlException.$(queryTableNamePosition, "Timeout expired on waiting for the ALTER TABLE execution result");
                }
                LockSupport.parkNanos(1);
                continue;
            }

            TableWriterTask event = tableWriterEventQueue.get(seq);
            if (event.getInstance() != commandId || event.getType() != TableWriterTask.TSK_ALTER_TABLE) {
                LOG.info()
                        .$("writer command response received and ignored [instance=").$(event.getInstance())
                        .$(", type=").$(event.getType())
                        .$(", expectedInstance=").$(commandId)
                        .I$();
                responseConsumeSequence.done(seq);
                LockSupport.parkNanos(1);
                continue;
            }

            // If writer failed to execute the ALTER command it will send back string error
            // in the event data
            SqlException result = null;
            int strLen = Unsafe.getUnsafe().getInt(event.getData());
            if (strLen != 0) {
                result = SqlException.$(queryTableNamePosition, event.getData() + 4L, event.getData() + 4L + 2L * strLen);
            }
            responseConsumeSequence.done(seq);
            LOG.info().$("writer command response received [instance=").$(commandId).I$();
            return result;
        }
    }

    private static void executeWriterCommandAsync(
            CairoEngine engine,
            AlterStatement alterStatement,
            SCSequence responseConsumeSequence
    ) throws SqlException {
        LOG.info().$("writer busy, will pass ALTER TABLE execution to writer owner async [table=").$(alterStatement.getTableName()).I$();
        setUpEngineAsyncWriterEventWait(engine, responseConsumeSequence);
        try {
            long instance = engine.publishTableWriterCommand(alterStatement);
            SqlException status = waitWriterEvent(
                    engine,
                    instance,
                    responseConsumeSequence,
                    engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout(),
                    alterStatement.getTableNamePosition()
            );
            if (status == null) {
                LOG.debug().$("received DONE response writer event [table=")
                        .$(alterStatement.getTableName())
                        .$(",instance=").$(instance).I$();
            } else {
                LOG.info().$("received error response for ALTER TABLE from writer [table=")
                        .$(alterStatement.getTableName())
                        .$(", instance=").$(instance)
                        .$(", error=").$(status.getFlyweightMessage())
                        .I$();
                throw status;
            }
        } finally {
            stopEngineAsyncWriterEventWait(engine, responseConsumeSequence);
        }
    }

    private static long executeWriterCommandAsyncNoWait(
            CairoEngine engine,
            AlterStatement alterStatement
    ) {
        LOG.info().$("writer busy, will pass ALTER TABLE execution to writer owner async [table=").$(alterStatement.getTableName()).I$();
        long instance = engine.publishTableWriterCommand(alterStatement);
        LOG.debug().$("published writer event [table=")
                .$(alterStatement.getTableName())
                .$(",instance=").$(instance).I$();
        return instance;
    }
}
