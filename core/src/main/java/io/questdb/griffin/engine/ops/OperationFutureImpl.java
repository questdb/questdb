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

import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.griffin.QueryFutureUpdateListener;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TimeoutSqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.WeakAutoClosableObjectPool;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TableWriterTask;

import static io.questdb.tasks.TableWriterTask.*;
import static io.questdb.cairo.sql.AsyncWriterCommand.Error.*;

class OperationFutureImpl implements OperationFuture {
    private static final Log LOG = LogFactory.getLog(OperationFutureImpl.class);
    private final CairoEngine engine;
    private final WeakAutoClosableObjectPool<OperationFutureImpl> objectPool;
    private SCSequence eventSubSeq;
    private int status;
    private long affectedRowsCount;
    private long correlationId;
    private String tableName;
    private QueryFutureUpdateListener queryFutureUpdateListener;
    private int tableNamePositionInSql;
    private boolean closing;

    OperationFutureImpl(CairoEngine engine, WeakAutoClosableObjectPool<OperationFutureImpl> pool) {
        this.engine = engine;
        objectPool = pool;
    }

    @Override
    public void await() throws SqlException {
        await(engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout());
        if (status == QUERY_STARTED) {
            await(engine.getConfiguration().getWriterAsyncCommandMaxTimeout() - engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout());
        }
        if (status != QUERY_COMPLETE) {
            throw TimeoutSqlException.timeout("Timeout expired on waiting for the async command execution result [instance=").put(correlationId).put(']');
        }
    }

    @Override
    public int await(long timeout) throws SqlException {
        if (status == QUERY_COMPLETE) {
            return status;
        }
        status = Math.max(status, awaitWriterEvent(timeout));
        return status;
    }

    @Override
    public long getInstanceId() {
        return correlationId;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public long getAffectedRowsCount() {
        return affectedRowsCount;
    }

    @Override
    public void close() {
        if (eventSubSeq != null) {
            engine.getMessageBus().getTableWriterEventFanOut().remove(eventSubSeq);
            eventSubSeq.clear();
            eventSubSeq = null;
            correlationId = -1;
            tableName = null;
        }

        if (!closing) {
            closing = true;
            objectPool.push(this);
            closing = false;
        }
    }

    /***
     * Initializes instance of OperationFuture with the parameters to wait for the new command
     * @param eventSubSeq - event sequence used to wait for the command execution to be signaled as complete
     */
    public void of(
            AsyncWriterCommand asyncWriterCommand,
            SqlExecutionContext executionContext,
            SCSequence eventSubSeq,
            int tableNamePositionInSql
    ) throws SqlException, AlterTableContextException {
        assert eventSubSeq != null : "event subscriber sequence must be provided";
        this.queryFutureUpdateListener = executionContext.getQueryFutureUpdateListener();
        this.tableNamePositionInSql = tableNamePositionInSql;
        // Set up execution wait sequence to listen to async writer events
        final FanOut writerEventFanOut = engine.getMessageBus().getTableWriterEventFanOut();
        writerEventFanOut.and(eventSubSeq);
        this.eventSubSeq = eventSubSeq;

        try {
            // Publish new command and get published command correlation id
            final CharSequence cmdName = asyncWriterCommand.getCommandName();
            tableName = asyncWriterCommand.getTableName();
            correlationId = engine.getCommandCorrelationId();
            asyncWriterCommand.setCommandCorrelationId(correlationId);

            try (TableWriter writer = engine.getWriterOrPublishCommand(
                    executionContext.getCairoSecurityContext(),
                    tableName,
                    asyncWriterCommand
            )) {
                if (writer != null) {
                    LOG.info()
                            .$("published SYNC writer command [name=").$(cmdName)
                            .$(",tableName=").$(tableName)
                            .$(",instance=").$(correlationId)
                            .I$();
                    affectedRowsCount = asyncWriterCommand.apply(writer, true);
                    status = QUERY_COMPLETE;
                } else {
                    LOG.info()
                            .$("published ASYNC writer command [name=").$(cmdName)
                            .$(",tableName=").$(tableName)
                            .$(",instance=").$(correlationId)
                            .I$();
                    asyncWriterCommand.startAsync();
                    affectedRowsCount = 0;
                    status = QUERY_NO_RESPONSE;
                }
            }

            queryFutureUpdateListener.reportStart(asyncWriterCommand.getTableName(), correlationId);
        } catch (Throwable ex) {
            close();
            throw ex;
        }
    }

    private int awaitWriterEvent(long writerAsyncCommandBusyWaitTimeout) throws SqlException {
        assert eventSubSeq != null : "No sequence to wait on";
        assert correlationId > -1 : "No command id to wait for";

        final MicrosecondClock clock = engine.getConfiguration().getMicrosecondClock();
        final long start = clock.getTicks();
        final RingQueue<TableWriterTask> tableWriterEventQueue = engine.getMessageBus().getTableWriterEventQueue();

        int status = this.status;
        while (true) {
            long seq = eventSubSeq.next();
            if (seq < 0) {
                // Queue is empty, check if the execution blocked for too long
                if (clock.getTicks() - start > writerAsyncCommandBusyWaitTimeout) {
                    queryFutureUpdateListener.reportBusyWaitExpired(tableName, correlationId);
                    return status;
                }
                Os.pause();
                continue;
            }

            try {
                TableWriterTask event = tableWriterEventQueue.get(seq);
                int type = event.getType();
                if (event.getInstance() != correlationId || (type != TSK_BEGIN && type != TSK_COMPLETE)) {
                    LOG.debug()
                            .$("writer command response received and ignored [instance=").$(event.getInstance())
                            .$(", type=").$(type)
                            .$(", expectedInstance=").$(correlationId)
                            .I$();
                    Os.pause();
                } else if (type == TSK_COMPLETE) {
                    LOG.info().$("writer command response received [instance=").$(correlationId).I$();
                    final int errorCode = Unsafe.getUnsafe().getInt(event.getData());
                    switch (errorCode) {
                        case OK:
                            affectedRowsCount = Unsafe.getUnsafe().getInt(event.getData() + Integer.BYTES);
                            queryFutureUpdateListener.reportProgress(correlationId, QUERY_COMPLETE);
                            return QUERY_COMPLETE;
                        case READER_OUT_OF_DATE:
                            throw ReaderOutOfDateException.of(tableName);
                        default:
                            final int strLen = Unsafe.getUnsafe().getInt(event.getData() + Integer.BYTES);
                            final long strLo = event.getData() + 2L * Integer.BYTES;
                            throw SqlException.$(tableNamePositionInSql, strLo, strLo + 2L * strLen);
                    }
                } else {
                    status = QUERY_STARTED;
                    queryFutureUpdateListener.reportProgress(correlationId, QUERY_STARTED);
                    LOG.info().$("writer command QUERY_STARTED response received [instance=").$(correlationId).I$();
                }
            } finally {
                eventSubSeq.done(seq);
            }
        }
    }
}
