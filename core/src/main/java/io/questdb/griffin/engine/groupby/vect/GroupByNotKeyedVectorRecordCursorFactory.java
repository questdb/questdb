/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecordNoRowid;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategyFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.mp.Worker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.tasks.VectorAggregateTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class GroupByNotKeyedVectorRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final Log LOG = LogFactory.getLog(GroupByNotKeyedVectorRecordCursorFactory.class);
    private final RecordCursorFactory base;
    private final GroupByNotKeyedVectorRecordCursor cursor;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final ObjectPool<VectorAggregateEntry> entryPool;
    private final PageFrameAddressCache frameAddressCache;
    private final ObjList<PageFrameMemoryPool> frameMemoryPools; // per worker pools
    private final PerWorkerLocks perWorkerLocks; // used to protect VAF's internal slots
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker;
    private final AtomicInteger startedCounter = new AtomicInteger();
    private final ObjList<VectorAggregateFunction> vafList;
    private final WorkStealingStrategy workStealingStrategy;
    private final int workerCount;

    public GroupByNotKeyedVectorRecordCursorFactory(
            CairoEngine engine,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            int workerCount,
            @Transient ObjList<VectorAggregateFunction> vafList
    ) {
        super(metadata);
        try {
            this.base = base;
            this.frameAddressCache = new PageFrameAddressCache();
            this.entryPool = new ObjectPool<>(VectorAggregateEntry::new, configuration.getGroupByPoolCapacity());
            this.vafList = new ObjList<>(vafList.size());
            this.vafList.addAll(vafList);
            this.cursor = new GroupByNotKeyedVectorRecordCursor(this.vafList);
            this.workerCount = workerCount;
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
            this.sharedCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            this.workStealingStrategy = WorkStealingStrategyFactory.getInstance(configuration, workerCount);
            this.workStealingStrategy.of(startedCounter);
            this.frameMemoryPools = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                // We're using page frame memory only and do single scan, hence cache size of 1.
                frameMemoryPools.add(new PageFrameMemoryPool(1));
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
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
        final PageFrameCursor frameCursor = base.getPageFrameCursor(executionContext, ORDER_ASC);
        return cursor.of(
                base.getMetadata(),
                frameCursor,
                executionContext.getMessageBus(),
                executionContext.getCircuitBreaker()
        );
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupBy");
        sink.meta("vectorized").val(true);
        sink.meta("workers").val(workerCount);
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

    static int runWhatsLeft(
            MCSequence subSeq,
            RingQueue<VectorAggregateTask> queue,
            int queuedCount,
            int reclaimed,
            int mergedCount,
            int workerId,
            SOUnboundedCountDownLatch doneLatch,
            SqlExecutionCircuitBreaker circuitBreaker,
            AtomicBooleanCircuitBreaker sharedCB,
            WorkStealingStrategy workStealingStrategy
    ) {
        while (!doneLatch.done(queuedCount)) {
            if (circuitBreaker.checkIfTripped()) {
                sharedCB.cancel();
            }

            if (workStealingStrategy.shouldSteal(mergedCount)) {
                long cursor = subSeq.next();
                if (cursor > -1) {
                    VectorAggregateTask task = queue.get(cursor);
                    task.entry.run(workerId, subSeq, cursor);
                    reclaimed++;
                } else {
                    Os.pause();
                }
            }
            mergedCount = doneLatch.getCount();
        }
        return reclaimed;
    }

    @Override
    protected void _close() {
        Misc.freeObjListAndKeepObjects(frameMemoryPools);
        Misc.freeObjList(vafList);
        Misc.free(base);
    }

    private class GroupByNotKeyedVectorRecordCursor implements NoRandomAccessRecordCursor {
        private final Record recordA;
        private boolean areFunctionsBuilt;
        private MessageBus bus;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private int frameCount;
        private PageFrameCursor frameCursor;
        private boolean isExhausted;

        public GroupByNotKeyedVectorRecordCursor(ObjList<? extends Function> functions) {
            this.recordA = new VirtualRecordNoRowid(functions);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isExhausted) {
                counter.inc();
                isExhausted = true;
            }
        }

        @Override
        public void close() {
            Misc.free(frameAddressCache);
            frameCursor = Misc.free(frameCursor);
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public boolean hasNext() {
            if (isExhausted) {
                return false;
            }
            if (!areFunctionsBuilt) {
                buildFunctions();
                areFunctionsBuilt = true;
            }
            isExhausted = true;
            return true;
        }

        public GroupByNotKeyedVectorRecordCursor of(
                RecordMetadata metadata,
                PageFrameCursor frameCursor,
                MessageBus bus,
                SqlExecutionCircuitBreaker circuitBreaker
        ) {
            this.frameCursor = frameCursor;
            this.bus = bus;
            this.circuitBreaker = circuitBreaker;
            frameAddressCache.of(metadata, frameCursor.getColumnIndexes(), frameCursor.isExternal());
            for (int i = 0; i < workerCount; i++) {
                frameMemoryPools.getQuick(i).of(frameAddressCache);
            }
            frameCount = 0;
            areFunctionsBuilt = false;
            toTop();
            return this;
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(areFunctionsBuilt);
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            isExhausted = false;
        }

        private void buildFunctions() {
            final int vafCount = vafList.size();
            final RingQueue<VectorAggregateTask> queue = bus.getVectorAggregateQueue();
            final Sequence pubSeq = bus.getVectorAggregatePubSeq();

            sharedCircuitBreaker.reset();
            startedCounter.set(0);
            entryPool.clear();

            int queuedCount = 0;
            int ownCount = 0;
            int reclaimed = 0;
            int total = 0;
            int mergedCount = 0; // used for work stealing decisions

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
                while ((frame = frameCursor.next()) != null) {
                    frameAddressCache.add(frameCount++, frame);
                }

                for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                    final long frameRowCount = frameAddressCache.getFrameSize(frameIndex);
                    for (int vafIndex = 0; vafIndex < vafCount; vafIndex++) {
                        final VectorAggregateFunction vaf = vafList.getQuick(vafIndex);
                        final int columnIndex = vaf.getColumnIndex();

                        while (true) {
                            long cursor = pubSeq.next();
                            if (cursor < 0) {
                                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                                if (workStealingStrategy.shouldSteal(mergedCount)) {
                                    VectorAggregateEntry.aggregateUnsafe(
                                            workerId,
                                            null,
                                            frameIndex,
                                            frameRowCount,
                                            -1,
                                            columnIndex,
                                            null,
                                            frameMemoryPools,
                                            null,
                                            vaf,
                                            perWorkerLocks,
                                            circuitBreaker
                                    );
                                    ownCount++;
                                    total++;
                                    mergedCount = doneLatch.getCount();
                                    break;
                                }
                                mergedCount = doneLatch.getCount();
                            } else {
                                final VectorAggregateEntry entry = entryPool.next();
                                entry.of(
                                        frameIndex,
                                        frameRowCount,
                                        -1,
                                        columnIndex,
                                        vaf,
                                        null, // null pRosti means that we do not need keyed aggregation
                                        frameMemoryPools,
                                        startedCounter,
                                        doneLatch,
                                        null,
                                        null,
                                        perWorkerLocks,
                                        sharedCircuitBreaker
                                );
                                queue.get(cursor).entry = entry;
                                pubSeq.done(cursor);
                                queuedCount++;
                                total++;
                                break;
                            }
                        }
                    }
                }

                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            } catch (Throwable th) {
                sharedCircuitBreaker.cancel();
                throw th;
            } finally {
                // all done? great start consuming the queue we just published
                // how do we get to the end? If we consume our own queue there is chance we will be consuming
                // aggregation tasks not related to this execution (we work in concurrent environment)
                // To deal with that we need to have our own checklist.
                reclaimed = runWhatsLeft(
                        bus.getVectorAggregateSubSeq(),
                        queue,
                        queuedCount,
                        reclaimed,
                        mergedCount,
                        workerId,
                        doneLatch,
                        circuitBreaker,
                        sharedCircuitBreaker,
                        workStealingStrategy
                );
                // Release page frame memory now, when no worker is using it.
                Misc.freeObjListAndKeepObjects(frameMemoryPools);
            }

            toTop();

            LOG.info().$("done [total=").$(total)
                    .$(", ownCount=").$(ownCount)
                    .$(", reclaimed=").$(reclaimed)
                    .$(", queuedCount=").$(queuedCount)
                    .I$();
        }
    }
}
