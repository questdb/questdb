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
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.tasks.PageFrameTask;

import java.util.ArrayDeque;
import java.util.Deque;

public class FilteredRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final FilteredRecordCursor cursor;
    private final Function filter;

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

    private final PageFrameRecord pageFrameRecord = new PageFrameRecord();

    @Override
    public void execute(SqlExecutionContext executionContext) throws SqlException {
        final PageFrameCursor cursor = base.getPageFrameCursor(executionContext);
        final MessageBus bus = executionContext.getMessageBus();
        final RingQueue<PageFrameTask> queue = bus.getPageFrameQueue();
        final MPSequence pubSeq = bus.getPageFramePubSeq();

        Deque<DirectLongList> listDeque = new ArrayDeque<>();

        PageFrame frame;
        while ((frame = cursor.next()) != null) {

            long c = pubSeq.next();
            if (c > -1) {
                queue.get(c).of(
                        frame,
                        filter,
                        new DirectLongList(1024)
                );
                pubSeq.done(c);
            } else {
                RecordFilterJob.filter(
                        frame,
                        filter,
                        pageFrameRecord,

                );
            }
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }
}
