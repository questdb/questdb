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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.mp.Worker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Transient;
import io.questdb.tasks.VectorAggregateTask;

public class GroupByNotKeyedVectorRecordCursorFactory implements RecordCursorFactory {

    private static final Log LOG = LogFactory.getLog(GroupByNotKeyedVectorRecordCursorFactory.class);
    private final RecordCursorFactory base;
    private final ObjList<VectorAggregateFunction> vafList;
    private final ObjectPool<VectorAggregateEntry> entryPool;
    private final ObjList<VectorAggregateEntry> activeEntries;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final RecordMetadata metadata;
    private final GroupByNotKeyedVectorRecordCursor cursor;

    public GroupByNotKeyedVectorRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            @Transient ObjList<VectorAggregateFunction> vafList
    ) {
        this.entryPool = new ObjectPool<>(VectorAggregateEntry::new, configuration.getGroupByPoolCapacity());
        this.activeEntries = new ObjList<>(configuration.getGroupByPoolCapacity());
        this.base = base;
        this.metadata = metadata;
        this.vafList = new ObjList<>(vafList.size());
        this.vafList.addAll(vafList);
        this.cursor = new GroupByNotKeyedVectorRecordCursor(this.vafList);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final MessageBus bus = executionContext.getMessageBus();
        assert bus != null;

        final PageFrameCursor cursor = base.getPageFrameCursor(executionContext);
        final int vafCount = vafList.size();

        // clear state of aggregate functions
        for (int i = 0; i < vafCount; i++) {
            vafList.getQuick(i).clear();
        }

        final RingQueue<VectorAggregateTask> queue = bus.getVectorAggregateQueue();
        final Sequence pubSeq = bus.getVectorAggregatePubSeq();

        this.entryPool.clear();
        this.activeEntries.clear();
        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;

        doneLatch.reset();

        // check if this executed via worker pool
        final Thread thread = Thread.currentThread();
        final int workerId;
        if (thread instanceof Worker) {
            workerId = ((Worker) thread).getWorkerId();
        } else {
            workerId = 0;
        }

        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            for (int i = 0; i < vafCount; i++) {
                final VectorAggregateFunction vaf = vafList.getQuick(i);
                final int columnIndex = vaf.getColumnIndex();
                final long pageAddress = frame.getPageAddress(columnIndex);
                final long pageSize = frame.getPageSize(columnIndex);
                final int colSizeShr = frame.getColumnSize(columnIndex);
                long seq = pubSeq.next();
                if (seq < 0) {
                    // diy the func
                    // vaf need to know which column it is hitting in the frame and will need to
                    // aggregate between frames until done
                    vaf.aggregate(pageAddress, pageSize, colSizeShr, workerId);
                    ownCount++;
                } else {
                    final VectorAggregateEntry entry = entryPool.next();
                    // null pRosti means that we do not need keyed aggregation
                    entry.of(queuedCount++, vaf, null, 0, pageAddress, pageSize, colSizeShr, doneLatch);
                    activeEntries.add(entry);
                    queue.get(seq).entry = entry;
                    pubSeq.done(seq);
                }
                total++;
            }
        }

        // all done? great start consuming the queue we just published
        // how do we get to the end? If we consume our own queue there is chance we will be consuming
        // aggregation tasks not related to this execution (we work in concurrent environment)
        // To deal with that we need to have our own checklist.

        // start at the back to reduce chance of clashing
        reclaimed = getRunWhatsLeft(queuedCount, reclaimed, workerId, activeEntries, doneLatch, LOG);

        LOG.info().$("done [total=").$(total).$(", ownCount=").$(ownCount).$(", reclaimed=").$(reclaimed).$(", queuedCount=").$(queuedCount).$(']').$();
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

    static int getRunWhatsLeft(int queuedCount, int reclaimed, int workerId, ObjList<VectorAggregateEntry> activeEntries, SOUnboundedCountDownLatch doneLatch, Log log) {
        for (int i = activeEntries.size() - 1; i > -1 && doneLatch.getCount() > -queuedCount; i--) {
            if (activeEntries.getQuick(i).run(workerId)) {
                reclaimed++;
            }
        }

        log.info().$("waiting for parts [queuedCount=").$(queuedCount).$(']').$();
        doneLatch.await(queuedCount);
        return reclaimed;
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

        private GroupByNotKeyedVectorRecordCursor of(PageFrameCursor pageFrameCursor) {
            this.pageFrameCursor = pageFrameCursor;
            toTop();
            return this;
        }
    }
}
