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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.tasks.VectorAggregateTask;

public class GroupByNotKeyedVectorRecordCursorFactory implements RecordCursorFactory {

    private static final Log LOG = LogFactory.getLog(GroupByNotKeyedVectorRecordCursorFactory.class);
    private final RecordCursorFactory base;
    private final ObjList<VectorAggregateFunction> vafList;
    private final ObjectPool<VectorAggregateEntry> entryPool = new ObjectPool<>(VectorAggregateEntry::new, 64);
    private final ObjList<VectorAggregateEntry> activeEntries = new ObjList<>();
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final RecordMetadata metadata;
    private final GroupByNotKeyedVectorRecordCursor cursor;

    public GroupByNotKeyedVectorRecordCursorFactory(
            RecordCursorFactory base,
            RecordMetadata metadata,
            ObjList<VectorAggregateFunction> vafList
    ) {
        this.base = base;
        this.metadata = metadata;
        this.vafList = vafList;
        this.cursor = new GroupByNotKeyedVectorRecordCursor(vafList);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        final MessageBus bus = executionContext.getMessageBus();
        assert bus != null;

        final PageFrameCursor cursor = base.getPageFrameCursor(executionContext);
        final int vafCount = vafList.size();

        // clear state of aggregate functions
        for (int i = 0; i < vafCount; i++) {
            vafList.getQuick(i).clear();
        }

        final RingQueue<VectorAggregateTask> queue = bus.getVectorAggregateQueue();
        final Sequence pubSeq = bus.getVectorAggregatePubSequence();

        this.entryPool.clear();
        int count = 0;
        int ownCount = 0;

        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            for (int i = 0; i < vafCount; i++) {
                final VectorAggregateFunction vaf = vafList.getQuick(i);
                final long pageAddress = frame.getPageAddress(i);
                final long pageValueCount = frame.getPageValueCount(i);
                long seq = pubSeq.next();
                if (seq < 0) {
                    // diy the func
                    // vaf need to know which column it is hitting int he frame and will need to
                    // aggregate between frames until done
                    vaf.aggregate(pageAddress, pageValueCount);
                    ownCount++;
                } else {
                    final VectorAggregateEntry entry = entryPool.next();
                    entry.of(count++, vaf, pageAddress, pageValueCount, doneLatch);
                    activeEntries.add(entry);
                    queue.get(seq).entry = entry;
                    pubSeq.done(seq);
                }
            }
        }

        // all done? great start consuming the queue we just published
        // how do we get to the end? If we consume our own queue there is chance we will be consuming
        // aggregation tasks not related to this execution (we work in concurrent environment)
        // To deal with that we need to have our own checklist.

        // start at the back to reduce chance of clashing
        for (int i = activeEntries.size() - 1; i > -1; i--) {
            if (activeEntries.getQuick(i).run()) {
                ownCount++;
            }
        }

        LOG.info().$("waiting for parts [count=").$(count).$(']').$();
        doneLatch.await(count);
        LOG.info().$("done [count=").$(count).$(", ownCount=").$(ownCount).$(']').$();
        return this.cursor.of(cursor);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private static class GroupByNotKeyedVectorRecordCursor implements NoRandomAccessRecordCursor {
        private final Record recordA;
        private int countDown = 1;
        private PageFrameCursor pageFrameCursor;

        public GroupByNotKeyedVectorRecordCursor(ObjList<? extends Function> functions) {
            this.recordA = new VirtualRecordNoRowid(functions);
        }

        @Override
        public void close() {
            Misc.free(pageFrameCursor);
        }

        private GroupByNotKeyedVectorRecordCursor of(PageFrameCursor pageFrameCursor) {
            this.pageFrameCursor = pageFrameCursor;
            toTop();
            return this;
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public boolean hasNext() {
            return countDown-- > 0;
        }


        @Override
        public void toTop() {
            countDown = 1;
        }

        @Override
        public long size() {
            return 1;
        }
    }
}
