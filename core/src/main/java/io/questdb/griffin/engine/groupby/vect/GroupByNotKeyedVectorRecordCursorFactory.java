/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.text.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.tasks.VectorAggregateTask;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;

public class GroupByNotKeyedVectorRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final Log LOG = LogFactory.getLog(GroupByNotKeyedVectorRecordCursorFactory.class);
    private final RecordCursorFactory base;
    private final GroupByNotKeyedVectorRecordCursor cursor;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final ObjectPool<VectorAggregateEntry> entryPool;
    private final PerWorkerLocks perWorkerLocks; // used to protect VAF's internal slots
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker;
    private final ObjList<VectorAggregateFunction> vafList;
    private final int workerCount;

    public GroupByNotKeyedVectorRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            int workerCount,
            @Transient ObjList<VectorAggregateFunction> vafList
    ) {
        super(metadata);
        this.entryPool = new ObjectPool<>(VectorAggregateEntry::new, configuration.getGroupByPoolCapacity());
        this.base = base;
        this.vafList = new ObjList<>(vafList.size());
        this.vafList.addAll(vafList);
        this.cursor = new GroupByNotKeyedVectorRecordCursor(this.vafList);
        this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        this.sharedCircuitBreaker = new AtomicBooleanCircuitBreaker();
        this.workerCount = workerCount;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // clear state of aggregate functions
        for (int i = 0, n = vafList.size(); i < n; i++) {
            vafList.getQuick(i).clear();
        }
        final PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext, ORDER_ASC);
        return cursor.of(pageFrameCursor, executionContext.getMessageBus(), executionContext.getCircuitBreaker());
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupBy");
        sink.meta("vectorized").val(true);
        sink.optAttr("values", vafList, true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    static int getRunWhatsLeft(
            MCSequence subSeq,
            RingQueue<VectorAggregateTask> queue,
            int queuedCount,
            int reclaimed,
            int workerId,
            SOUnboundedCountDownLatch doneLatch,
            SqlExecutionCircuitBreaker circuitBreaker,
            AtomicBooleanCircuitBreaker sharedCB
    ) {
        while (!doneLatch.done(queuedCount)) {
            if (circuitBreaker.checkIfTripped()) {
                sharedCB.cancel();
            }

            long cursor = subSeq.next();
            if (cursor > -1) {
                VectorAggregateTask task = queue.get(cursor);
                task.entry.run(workerId, subSeq, cursor);
                reclaimed++;
            } else {
                Os.pause();
            }
        }
        return reclaimed;
    }

    @Override
    protected void _close() {
        Misc.freeObjList(vafList);
        Misc.free(base);
    }

    private class GroupByNotKeyedVectorRecordCursor implements NoRandomAccessRecordCursor {
        private final Record recordA;
        private boolean areFunctionsBuilt;
        private MessageBus bus;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private int countDown = 1;
        private PageFrameCursor pageFrameCursor;

        public GroupByNotKeyedVectorRecordCursor(ObjList<? extends Function> functions) {
            this.recordA = new VirtualRecordNoRowid(functions);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (countDown > 0) {
                counter.add(countDown);
                countDown = 0;
            }
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
            if (!areFunctionsBuilt) {
                buildFunctions();
                areFunctionsBuilt = true;
            }
            return countDown-- > 0;
        }

        public GroupByNotKeyedVectorRecordCursor of(PageFrameCursor pageFrameCursor, MessageBus bus, SqlExecutionCircuitBreaker circuitBreaker) {
            this.pageFrameCursor = pageFrameCursor;
            this.bus = bus;
            this.circuitBreaker = circuitBreaker;
            areFunctionsBuilt = false;
            toTop();
            return this;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            countDown = 1;
        }

        private void buildFunctions() {
            final int vafCount = vafList.size();
            final RingQueue<VectorAggregateTask> queue = bus.getVectorAggregateQueue();
            final Sequence pubSeq = bus.getVectorAggregatePubSeq();

            sharedCircuitBreaker.reset();
            entryPool.clear();
            int queuedCount = 0;
            int ownCount = 0;
            int reclaimed = 0;
            int total = 0;

            doneLatch.reset();

            final Thread thread = Thread.currentThread();
            final int workerId;
            if (thread instanceof Worker) {
                // it's a worker thread, potentially from the shared pool
                workerId = ((Worker) thread).getWorkerId() % workerCount;
            } else {
                // it's an embedder's thread, so use a random slot
                workerId = -1;
            }

            try {
                PageFrame frame;
                while ((frame = pageFrameCursor.next()) != null) {
                    for (int i = 0; i < vafCount; i++) {
                        final VectorAggregateFunction vaf = vafList.getQuick(i);
                        final int columnIndex = vaf.getColumnIndex();
                        // for functions like `count()`, that do not have arguments we are required to provide
                        // count of rows in table in a form of "pageSize >> shr". Since `vaf` doesn't provide column
                        // this code used column 0. Assumption here that column 0 is fixed size.
                        // This assumption only holds because our aggressive algorithm for "top down columns", e.g.
                        // the algorithm that forces page frame to provide only columns required by the select. At the time
                        // of writing this code there is no way to return variable length column out of non-keyed aggregation
                        // query. This might change if we introduce something like `first(string)`. When this happens we will
                        // need to rethink our way of computing size for the count. This would be either type checking column
                        // 0 and working out size differently or finding any fixed-size column and using that.
                        final long pageAddress = columnIndex > -1 ? frame.getPageAddress(columnIndex) : 0;
                        final long pageSize = columnIndex > -1 ? frame.getPageSize(columnIndex) : frame.getPageSize(0);
                        final int colSizeShr = columnIndex > -1 ? frame.getColumnShiftBits(columnIndex) : frame.getColumnShiftBits(0);
                        long seq = pubSeq.next();
                        if (seq < 0) {
                            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                            // acquire the slot and DIY the func
                            // vaf need to know which column it is hitting in the frame and will need to
                            // aggregate between frames until done
                            final int slot = perWorkerLocks.acquireSlot(workerId, circuitBreaker);
                            try {
                                vaf.aggregate(pageAddress, pageSize, colSizeShr, slot);
                                ownCount++;
                            } finally {
                                perWorkerLocks.releaseSlot(slot);
                            }
                        } else {
                            final VectorAggregateEntry entry = entryPool.next();
                            // null pRosti means that we do not need keyed aggregation
                            queuedCount++;
                            entry.of(
                                    vaf,
                                    null,
                                    0,
                                    pageAddress,
                                    pageSize,
                                    colSizeShr,
                                    doneLatch,
                                    null,
                                    null,
                                    perWorkerLocks,
                                    sharedCircuitBreaker
                            );
                            queue.get(seq).entry = entry;
                            pubSeq.done(seq);
                        }
                        total++;
                    }
                }

                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            } catch (DataUnavailableException e) {
                // We're not yet done, so no need to cancel the circuit breaker. 
                throw e;
            } catch (Throwable e) {
                sharedCircuitBreaker.cancel();
                throw e;
            } finally {
                // all done? great start consuming the queue we just published
                // how do we get to the end? If we consume our own queue there is chance we will be consuming
                // aggregation tasks not related to this execution (we work in concurrent environment)
                // To deal with that we need to have our own checklist.
                reclaimed = getRunWhatsLeft(
                        bus.getVectorAggregateSubSeq(),
                        queue,
                        queuedCount,
                        reclaimed,
                        workerId,
                        doneLatch,
                        circuitBreaker,
                        sharedCircuitBreaker
                );
            }

            toTop();

            LOG.info().$("done [total=").$(total)
                    .$(", ownCount=").$(ownCount)
                    .$(", reclaimed=").$(reclaimed)
                    .$(", queuedCount=").$(queuedCount).I$();
        }
    }
}
