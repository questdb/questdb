/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.ShardedMapCursor;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.tasks.GroupByMergeShardTask;

import java.util.concurrent.atomic.AtomicInteger;

class AsyncGroupByRecordCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncGroupByRecordCursor.class);
    private final ObjList<GroupByFunction> groupByFunctions;
    private final AtomicBooleanCircuitBreaker mergeCircuitBreaker; // used to signal cancellation to merge shard workers
    private final SOUnboundedCountDownLatch mergeDoneLatch = new SOUnboundedCountDownLatch(); // used for merge shard workers
    private final AtomicInteger mergeStartedCounter = new AtomicInteger();
    private final MessageBus messageBus;
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final ObjList<Function> recordFunctions;
    private final ShardedMapCursor shardedCursor = new ShardedMapCursor();
    private SqlExecutionCircuitBreaker circuitBreaker;
    private int frameLimit;
    private PageFrameSequence<AsyncGroupByAtom> frameSequence;
    private boolean isDataMapBuilt;
    private boolean isOpen;
    private MapRecordCursor mapCursor;

    public AsyncGroupByRecordCursor(
            CairoConfiguration configuration,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            MessageBus messageBus
    ) {
        this.groupByFunctions = groupByFunctions;
        this.recordFunctions = recordFunctions;
        this.messageBus = messageBus;
        recordA = new VirtualRecord(recordFunctions);
        recordB = new VirtualRecord(recordFunctions);
        mergeCircuitBreaker = new AtomicBooleanCircuitBreaker();
        isOpen = true;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isDataMapBuilt) {
            buildMap();
        }
        mapCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.clearObjList(groupByFunctions);
            mapCursor = Misc.free(mapCursor);

            if (frameSequence != null) {
                LOG.debug()
                        .$("closing [shard=").$(frameSequence.getShard())
                        .$(", frameCount=").$(frameLimit)
                        .I$();

                if (frameLimit > -1) {
                    frameSequence.await();
                }
                frameSequence.clear();
            }
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) recordFunctions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isDataMapBuilt) {
            buildMap();
        }
        return mapCursor.hasNext();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (mapCursor != null) {
            mapCursor.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
        }
    }

    @Override
    public long size() {
        if (!isDataMapBuilt) {
            return -1;
        }
        return mapCursor != null ? mapCursor.size() : -1;
    }

    @Override
    public void toTop() {
        if (mapCursor != null) {
            mapCursor.toTop();
            GroupByUtils.toTop(recordFunctions);
            frameSequence.getAtom().toTop();
        }
    }

    private void buildMap() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }

        int frameIndex = -1;
        boolean allFramesActive = true;
        try {
            do {
                final long cursor = frameSequence.next();
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();
                    if (task.hasError()) {
                        throw CairoException.nonCritical()
                                .position(task.getErrorMessagePosition())
                                .put(task.getErrorMsg());
                    }

                    allFramesActive &= frameSequence.isActive();
                    frameIndex = task.getFrameIndex();

                    frameSequence.collect(cursor, false);
                } else if (cursor == -2) {
                    break; // No frames to filter.
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (CairoException e) {
            if (e.isInterruption()) {
                throwTimeoutException();
            } else {
                throw e;
            }
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }

        final AsyncGroupByAtom atom = frameSequence.getAtom();

        if (!atom.isSharded()) {
            // No sharding was necessary, so the maps are small, and we merge them ourselves.
            final Map dataMap = atom.mergeOwnerMap();
            mapCursor = dataMap.getCursor();
        } else {
            // We had to shard the maps, so they must be big.
            final ObjList<Map> shards = mergeShards(atom);
            // The shards contain non-intersecting row groups, so we can return what's in the shards without merging them.
            shardedCursor.of(shards);
            mapCursor = shardedCursor;
        }

        recordA.of(mapCursor.getRecord());
        recordB.of(mapCursor.getRecordB());
        isDataMapBuilt = true;
    }

    private ObjList<Map> mergeShards(AsyncGroupByAtom atom) {
        mergeCircuitBreaker.reset();
        mergeStartedCounter.set(0);
        mergeDoneLatch.reset();

        // First, make sure to shard all non-sharded maps, if any.
        atom.shardAll();

        // Next, merge each set of partial shard maps into the final shard map. This is done in parallel.
        final int shardCount = atom.getShardCount();
        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
        final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
        final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
        final WorkStealingStrategy workStealingStrategy = frameSequence.getWorkStealingStrategy().of(mergeStartedCounter);

        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;
        int mergedCount = 0; // used for work stealing decisions

        try {
            for (int i = 0; i < shardCount; i++) {
                while (true) {
                    long cursor = pubSeq.next();
                    if (cursor < 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                        if (workStealingStrategy.shouldSteal(mergedCount)) {
                            atom.mergeShard(-1, i);
                            ownCount++;
                            total++;
                            mergedCount = mergeDoneLatch.getCount();
                            break;
                        }
                        mergedCount = mergeDoneLatch.getCount();
                    } else {
                        queue.get(cursor).of(mergeCircuitBreaker, mergeStartedCounter, mergeDoneLatch, atom, i);
                        pubSeq.done(cursor);
                        queuedCount++;
                        total++;
                        break;
                    }
                }
            }
        } catch (Throwable th) {
            mergeCircuitBreaker.cancel();
            throw th;
        } finally {
            // All done? Great, start consuming the queue we just published.
            // How do we get to the end? If we consume our own queue there is chance we will be consuming
            // aggregation tasks not related to this execution (we work in concurrent environment).
            // To deal with that we need to check our latch.

            while (!mergeDoneLatch.done(queuedCount)) {
                if (circuitBreaker.checkIfTripped()) {
                    mergeCircuitBreaker.cancel();
                }

                if (workStealingStrategy.shouldSteal(mergedCount)) {
                    long cursor = subSeq.next();
                    if (cursor > -1) {
                        GroupByMergeShardTask task = queue.get(cursor);
                        GroupByMergeShardJob.run(-1, task, subSeq, cursor, atom);
                        reclaimed++;
                    }
                }
                mergedCount = mergeDoneLatch.getCount();
            }
        }

        if (mergeCircuitBreaker.checkIfTripped()) {
            throwTimeoutException();
        }

        atom.finalizeShardStats();

        LOG.debug().$("merge shards done [total=").$(total)
                .$(", ownCount=").$(ownCount)
                .$(", reclaimed=").$(reclaimed)
                .$(", queuedCount=").$(queuedCount).I$();

        return atom.getDestShards();
    }

    private void throwTimeoutException() {
        if (frameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(PageFrameSequence<AsyncGroupByAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncGroupByAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.circuitBreaker = executionContext.getCircuitBreaker();
        Function.init(recordFunctions, frameSequence.getSymbolTableSource(), executionContext);
        isDataMapBuilt = false;
        frameLimit = -1;
    }
}
