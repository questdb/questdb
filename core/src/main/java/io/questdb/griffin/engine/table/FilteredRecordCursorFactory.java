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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.*;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.tasks.PageFrameTask;

import java.util.ArrayDeque;
import java.util.Deque;

public class FilteredRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final FilteredRecordCursor cursor;
    private final Function filter;
    private final PageFrameRecord pageFrameRecord = new PageFrameRecord();

    public FilteredRecordCursorFactory(RecordCursorFactory base, Function filter) {
        assert !(base instanceof FilteredRecordCursorFactory);
        this.base = base;
        this.cursor = new FilteredRecordCursor(filter);
        this.filter = filter;
    }

    @Override
    public void close() {
        base.close();
        filter.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursor = base.getCursor(executionContext);
        try {
            this.cursor.of(cursor, executionContext);
            return this.cursor;
        } catch (Throwable e) {
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public void execute(SqlExecutionContext executionContext) throws SqlException {

        // todo: this has to be spun by a thread
        final Rnd rnd = executionContext.getRandom();
        final MessageBus bus = executionContext.getMessageBus();
        final int shard = rnd.nextInt(bus.getPageFrameQueueShardCount());
        // subscriber sequence to receive recycle events
        final SCSequence subSeq = new SCSequence();

        // The recycle subscriber sequence should pick up queue items that
        // has been released by the user code. The motivation is to place used
        // row lists back on stack to be reused
        final FanOut recycleFanOut = bus.getPageFrameRecycleFanOut(shard);
        recycleFanOut.and(subSeq);

        // before thread begins we will need to pick a shard
        // of queues that we will interact with

        final PageFrameCursor cursor = base.getPageFrameCursor(executionContext);
        final RingQueue<PageFrameTask> queue = bus.getPageFrameQueue(shard);

        // publisher sequence to pass work to worker jobs
        final MPSequence pubSeq = bus.getPageFramePubSeq(shard);

        // this is the identifier of events we are producing
        final long producerId = rnd.nextLong();

        final Deque<DirectLongList> listDeque = new ArrayDeque<>();

        DirectLongList reuseRows = null;
        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            long c = pubSeq.next();

            if (c > -1) {
                queue.get(c).of(
                        producerId,
                        frame,
                        filter,
                        reuseRows != null ? reuseRows : popRows(listDeque)
                );
                reuseRows = null;
                pubSeq.done(c);

                c = subSeq.next();

                if (c > -1) {
                    final PageFrameTask tsk = queue.get(c);
                    // each producer will see "returns" for all active producers on this
                    // queue, therefore we will only grab our own row lists to avoid potential
                    // imbalance between producers. By imbalance, I mean one producer could be
                    // allocating lists while another could be stashing it on its dequeue.
                    if (tsk.getProducerId() == producerId) {
                        listDeque.push(tsk.getRows());
                    }
                    subSeq.done(c);
                }
            } else {
                RecordFilterJob.filter(
                        frame,
                        filter,
                        pageFrameRecord,
                        reuseRows != null ? reuseRows : (reuseRows = popRows(listDeque))
                );
            }
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    private static DirectLongList popRows(Deque<DirectLongList> deque) {
        DirectLongList result = deque.getFirst();
        if (result == null) {
            result = new DirectLongList(1024);
        }
        return result;
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }
}
