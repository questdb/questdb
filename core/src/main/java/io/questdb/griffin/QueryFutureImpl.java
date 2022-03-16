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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.TableWriterTask;

import static io.questdb.tasks.TableWriterTask.*;

class QueryFutureImpl implements QueryFuture {
    private static final Log LOG = LogFactory.getLog(QueryFutureImpl.class);
    private final CairoEngine engine;
    private AsyncWriterCommand asyncWriterCommand;
    private SCSequence eventSubSeq;
    private int status;
    private long commandId;
    private QueryFutureUpdateListener queryFutureUpdateListener;

    QueryFutureImpl(CairoEngine engine) {
        this.engine = engine;
    }

    @Override
    public void await() throws SqlException {
        status = await(engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout());
        if (status == QUERY_STARTED) {
            status = await(engine.getConfiguration().getWriterAsyncCommandMaxTimeout() - engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout());
        }
        if (status != QUERY_COMPLETE) {
            throw SqlException.$(asyncWriterCommand.getTableNamePosition(), "Timeout expired on waiting for the async command execution result");
        }
    }

    @Override
    public int await(long timeout) throws SqlException {
        if (status == QUERY_COMPLETE) {
            return status;
        }
        return status = Math.max(status, awaitWriterEvent(timeout, asyncWriterCommand.getTableNamePosition()));
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void close() {
        if (eventSubSeq != null) {
            engine.getMessageBus().getTableWriterEventFanOut().remove(eventSubSeq);
            eventSubSeq.clear();
            eventSubSeq = null;
            commandId = -1;
        }
    }

    /***
     * Initializes instance of QueryFuture with the parameters to wait for the new command
     * @param eventSubSeq - event sequence used to wait for the command execution to be signaled as complete
     */
    public void of(AsyncWriterCommand asyncWriterCommand, SqlExecutionContext executionContext, SCSequence eventSubSeq) {
        assert eventSubSeq != null : "event subscriber sequence must be provided";

        this.asyncWriterCommand = asyncWriterCommand;
        this.queryFutureUpdateListener = executionContext.getQueryFutureUpdateListener();
        // Set up execution wait sequence to listen to the Engine async writer events
        final FanOut writerEventFanOut = engine.getMessageBus().getTableWriterEventFanOut();
        writerEventFanOut.and(eventSubSeq);
        this.eventSubSeq = eventSubSeq;

        try {
            // Publish new command and get published Command Id
            commandId = engine.publishTableWriterCommand(asyncWriterCommand);
            queryFutureUpdateListener.reportStart(asyncWriterCommand.getTableName(), commandId);
            status = QUERY_NO_RESPONSE;
        } catch (Throwable ex) {
            close();
            throw ex;
        }
    }

    private int awaitWriterEvent(
            long writerAsyncCommandBusyWaitTimeout,
            int queryTableNamePosition
    ) throws SqlException {
        assert eventSubSeq != null : "No sequence to wait on";
        assert commandId > -1 : "No command id to wait for";

        final MicrosecondClock clock = engine.getConfiguration().getMicrosecondClock();
        final long start = clock.getTicks();
        final RingQueue<TableWriterTask> tableWriterEventQueue = engine.getMessageBus().getTableWriterEventQueue();

        int status = this.status;
        while (true) {
            long seq = eventSubSeq.next();
            if (seq < 0) {
                // Queue is empty, check if the execution blocked for too long
                if (clock.getTicks() - start > writerAsyncCommandBusyWaitTimeout) {
                    return status;
                }
                Os.pause();
                continue;
            }

            try {
                TableWriterTask event = tableWriterEventQueue.get(seq);
                int type = event.getType();
                if (event.getInstance() != commandId || (type != TSK_BEGIN && type != TSK_COMPLETE)) {
                    LOG.debug()
                            .$("writer command response received and ignored [instance=").$(event.getInstance())
                            .$(", type=").$(type)
                            .$(", expectedInstance=").$(commandId)
                            .I$();
                    Os.pause();
                } else if (type == TSK_COMPLETE) {
                    // If writer failed to execute the async command it will send back string error in the event data
                    LOG.info().$("writer command response received [instance=").$(commandId).I$();
                    int strLen = Unsafe.getUnsafe().getInt(event.getData());
                    if (strLen > -1) {
                        throw SqlException.$(queryTableNamePosition, event.getData() + 4L, event.getData() + 4L + 2L * strLen);
                    }
                    queryFutureUpdateListener.reportProgress(commandId, QUERY_COMPLETE);
                    return QUERY_COMPLETE;
                } else {
                    status = QUERY_STARTED;
                    queryFutureUpdateListener.reportProgress(commandId, QUERY_STARTED);
                    LOG.info().$("writer command QUERY_STARTED response received [instance=").$(commandId).I$();
                }
            } finally {
                eventSubSeq.done(seq);
            }
        }
    }
}
