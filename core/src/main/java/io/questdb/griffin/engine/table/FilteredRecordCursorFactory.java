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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameDispatchTask;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.MPSequence;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;

public class FilteredRecordCursorFactory implements RecordCursorFactory {
    private static final PageFrameReducer REDUCER = FilteredRecordCursorFactory::filter;
    private final RecordCursorFactory base;
    private final FilteredRecordCursor cursor;
    private final Function filter;
    private final FrameSequence frameSequence;

    public FilteredRecordCursorFactory(CairoConfiguration configuration, RecordCursorFactory base, Function filter) {
        assert !(base instanceof FilteredRecordCursorFactory);
        this.base = base;
        this.cursor = new FilteredRecordCursor(filter);
        this.filter = filter;
        this.frameSequence = new FrameSequence(configuration, REDUCER);
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
    public FrameSequence execute(SqlExecutionContext executionContext, SCSequence consumerSubSeq) throws SqlException {
        final LongList frameSequenceFrameRowCounts = frameSequence.getFrameRowCounts();
        final PageAddressCache pageAddressCache = frameSequence.getPageAddressCache();
        final Rnd rnd = executionContext.getRandom();
        final MessageBus bus = executionContext.getMessageBus();
        // before thread begins we will need to pick a shard
        // of queues that we will interact with
        final int shard = rnd.nextInt(bus.getPageFrameReduceShardCount());
        final PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext);
        final MPSequence dispatchPubSeq = bus.getPageFrameDispatchPubSeq();
        long dispatchCursor = dispatchPubSeq.next();

        // pass one to cache page addresses
        // this has to be separate pass to ensure there no cache reads
        // while cache might be resizing
        pageAddressCache.of(base.getMetadata());

        PageFrame frame;
        int frameIndex = 0;
        while ((frame = pageFrameCursor.next()) != null) {
            pageAddressCache.add(frameIndex++, frame);
            frameSequenceFrameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
        }

        frameSequence.of(shard, rnd.nextLong(), frameIndex, consumerSubSeq, pageFrameCursor);

        if (dispatchCursor < 0) {
            dispatchCursor = dispatchPubSeq.nextBully();
        }

        // We need to subscribe publisher sequence before we return
        // control to the caller of this method. However, this sequence
        // will be unsubscribed asynchronously.
        bus.getPageFrameCollectFanOut(shard).and(consumerSubSeq);

        PageFrameDispatchTask dispatchTask = bus.getPageFrameDispatchQueue().get(dispatchCursor);
        dispatchTask.of(
                frameSequence.getId(),
                frameSequence.getFrameCount(),
                frameSequence.getValid(),
                frameSequence.getReducer(),
                frameSequence.getFrameRowCounts(),
                frameSequence.getReduceCounter(),
                frameSequence.getDoneLatch(),
                frameSequence.getConsumerSubSeq(),
                frameSequence.getSymbolTableSource(),
                frameSequence.getPageAddressCache(),
                filter,
                frameSequence.getShard()
        );

        dispatchPubSeq.done(dispatchCursor);

        return frameSequence;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private static void filter(PageAddressCacheRecord record, PageFrameReduceTask task) {
        final DirectLongList rows = task.getRows();
        final long frameRowCount = task.getFrameRowCount();
        final Function filter = task.getFilter();

        rows.clear();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            if (filter.getBool(record)) {
                rows.add(r);
            }
        }
    }
}
