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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
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
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByLongTopKJob;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.tasks.GroupByLongTopKTask;
import io.questdb.tasks.GroupByMergeShardTask;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

class AsyncGroupByRecordCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncGroupByRecordCursor.class);
    private final CairoConfiguration configuration;
    private final MessageBus messageBus;
    private final AtomicBooleanCircuitBreaker postAggregationCircuitBreaker; // used to signal cancellation to merge shard workers
    private final SOUnboundedCountDownLatch postAggregationDoneLatch = new SOUnboundedCountDownLatch(); // used for merge shard workers
    private final AtomicInteger postAggregationStartedCounter = new AtomicInteger();
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final ObjList<Function> recordFunctions;
    private final ShardedMapCursor shardedCursor = new ShardedMapCursor();
    private SqlExecutionCircuitBreaker circuitBreaker;
    private UnorderedPageFrameSequence<AsyncGroupByAtom> frameSequence;
    private boolean isDataMapBuilt;
    private boolean isOpen;
    private MapRecordCursor mapCursor;

    public AsyncGroupByRecordCursor(
            @NotNull CairoEngine engine,
            @NotNull ObjList<Function> recordFunctions,
            @NotNull MessageBus messageBus
    ) {
        this.configuration = engine.getConfiguration();
        this.recordFunctions = recordFunctions;
        this.messageBus = messageBus;
        recordA = new VirtualRecord(recordFunctions);
        recordB = new VirtualRecord(recordFunctions);
        postAggregationCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
        isOpen = true;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        buildMapConditionally();
        mapCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    frameSequence.await();
                    frameSequence.reset();
                }
            } finally {
                mapCursor = Misc.free(mapCursor);
                isOpen = false;
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
        buildMapConditionally();
        return mapCursor.hasNext();
    }

    @Override
    public void longTopK(DirectLongLongSortedList list, int columnIndex) {
        buildMapConditionally();
        final Function recordFunction = recordFunctions.getQuick(columnIndex);
        // Only run in parallel when the function is thread-safe. This is a simplified check that won't
        // pass for functions like count(varchar), but it's good enough for now since it'll pass for
        // count() and count() over a fixed-size column.
        //
        // Later on, we can introduce a special method for GroupByFunction that will stand for aggregation
        // thread-safety (the current value of the isThreadSafe() flag) while the isThreadSafe() flag
        // will stand for read thread-safety, just like for non-GROUP BY functions.
        if (recordFunction.isThreadSafe() && mapCursor == shardedCursor && mapCursor.size() > configuration.getGroupByParallelTopKThreshold()) {
            parallelLongTopK(list, recordFunction);
        } else {
            mapCursor.longTopK(list, recordFunction);
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return isDataMapBuilt ? 1 : 0;
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
        frameSequence.prepareForDispatch();
        frameSequence.getAtom().getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache());
        frameSequence.dispatchAndAwait();

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

    private void buildMapConditionally() {
        if (!isDataMapBuilt) {
            buildMap();
        }
    }

    private ObjList<Map> mergeShards(AsyncGroupByAtom atom) {
        postAggregationCircuitBreaker.reset();
        postAggregationStartedCounter.set(0);
        postAggregationDoneLatch.reset();

        // First, make sure to shard all non-sharded maps, if any.
        atom.shardAll();

        // Next, merge each set of partial shard maps into the final shard map. This is done in parallel.
        final int shardCount = atom.getShardCount();
        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
        final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
        final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
        final WorkStealingStrategy workStealingStrategy = frameSequence.getWorkStealingStrategy().of(postAggregationStartedCounter);

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
                            mergedCount = postAggregationDoneLatch.getCount();
                            break;
                        }
                        mergedCount = postAggregationDoneLatch.getCount();
                    } else {
                        queue.get(cursor).of(postAggregationCircuitBreaker, postAggregationStartedCounter, postAggregationDoneLatch, atom, i);
                        pubSeq.done(cursor);
                        queuedCount++;
                        total++;
                        break;
                    }
                }
            }
        } catch (Throwable th) {
            postAggregationCircuitBreaker.cancel();
            throw th;
        } finally {
            // All done? Great, start consuming the queue we just published.
            // How do we get to the end? If we consume our own queue there is chance we will be consuming
            // aggregation tasks not related to this execution (we work in concurrent environment).
            // To deal with that we need to check our latch.
            while (!postAggregationDoneLatch.done(queuedCount)) {
                if (circuitBreaker.checkIfTripped()) {
                    postAggregationCircuitBreaker.cancel();
                }

                if (workStealingStrategy.shouldSteal(mergedCount)) {
                    long cursor = subSeq.next();
                    if (cursor > -1) {
                        GroupByMergeShardTask task = queue.get(cursor);
                        GroupByMergeShardJob.run(-1, task, subSeq, cursor, atom);
                        reclaimed++;
                    } else {
                        Os.pause();
                    }
                } else {
                    Os.pause();
                }
                mergedCount = postAggregationDoneLatch.getCount();
            }
        }

        if (postAggregationCircuitBreaker.checkIfTripped()) {
            throwTimeoutException();
        }

        atom.finalizeShardStats();

        LOG.debug().$("merge shards done [total=").$(total)
                .$(", ownCount=").$(ownCount)
                .$(", reclaimed=").$(reclaimed)
                .$(", queuedCount=").$(queuedCount).I$();

        return atom.getDestShards();
    }

    private void parallelLongTopK(DirectLongLongSortedList destList, Function longFunc) {
        postAggregationCircuitBreaker.reset();
        postAggregationStartedCounter.set(0);
        postAggregationDoneLatch.reset();

        final AsyncGroupByAtom atom = frameSequence.getAtom();
        final int shardCount = atom.getShardCount();
        final RingQueue<GroupByLongTopKTask> queue = messageBus.getGroupByLongTopKQueue();
        final MPSequence pubSeq = messageBus.getGroupByLongTopKPubSeq();
        final MCSequence subSeq = messageBus.getGroupByLongTopKSubSeq();
        final WorkStealingStrategy workStealingStrategy = frameSequence.getWorkStealingStrategy().of(postAggregationStartedCounter);

        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;
        int processedCount = 0; // used for work stealing decisions

        try {
            for (int i = 0; i < shardCount; i++) {
                while (true) {
                    long cursor = pubSeq.next();
                    if (cursor < 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                        if (workStealingStrategy.shouldSteal(processedCount)) {
                            final Map shard = atom.getDestShards().getQuick(i);
                            final DirectLongLongSortedList ownerList = atom.getLongTopKList(-1, destList.getOrder(), destList.getCapacity());
                            shard.getCursor().longTopK(ownerList, longFunc);
                            ownCount++;
                            total++;
                            processedCount = postAggregationDoneLatch.getCount();
                            break;
                        }
                        processedCount = postAggregationDoneLatch.getCount();
                    } else {
                        queue.get(cursor).of(
                                postAggregationCircuitBreaker,
                                postAggregationStartedCounter,
                                postAggregationDoneLatch,
                                atom,
                                longFunc,
                                i,
                                destList.getOrder(),
                                destList.getCapacity()
                        );
                        pubSeq.done(cursor);
                        queuedCount++;
                        total++;
                        break;
                    }
                }
            }
        } catch (Throwable th) {
            postAggregationCircuitBreaker.cancel();
            throw th;
        } finally {
            // All done? Great, start consuming the queue we just published.
            // How do we get to the end? If we consume our own queue there is chance we will be consuming
            // aggregation tasks not related to this execution (we work in concurrent environment).
            // To deal with that we need to check our latch.
            while (!postAggregationDoneLatch.done(queuedCount)) {
                if (circuitBreaker.checkIfTripped()) {
                    postAggregationCircuitBreaker.cancel();
                }

                if (workStealingStrategy.shouldSteal(processedCount)) {
                    long cursor = subSeq.next();
                    if (cursor > -1) {
                        GroupByLongTopKTask task = queue.get(cursor);
                        GroupByLongTopKJob.run(-1, task, subSeq, cursor, atom);
                        reclaimed++;
                    } else {
                        Os.pause();
                    }
                } else {
                    Os.pause();
                }
                processedCount = postAggregationDoneLatch.getCount();
            }
        }

        if (postAggregationCircuitBreaker.checkIfTripped()) {
            throwTimeoutException();
        }

        // Now merge everything into the destination list.
        final DirectLongLongSortedList ownerList = atom.getOwnerLongTopKList();
        if (ownerList != null) {
            final DirectLongLongSortedList.Cursor cursor = ownerList.getCursor();
            while (cursor.hasNext()) {
                destList.add(cursor.index(), cursor.value());
            }
        }
        final ObjList<DirectLongLongSortedList> perWorkerLists = atom.getPerWorkerLongTopKLists();
        for (int i = 0, n = perWorkerLists.size(); i < n; i++) {
            final DirectLongLongSortedList workerList = perWorkerLists.getQuick(i);
            if (workerList != null) {
                final DirectLongLongSortedList.Cursor cursor = workerList.getCursor();
                while (cursor.hasNext()) {
                    destList.add(cursor.index(), cursor.value());
                }
            }
        }

        LOG.debug().$("parallel long top K done [total=").$(total)
                .$(", ownCount=").$(ownCount)
                .$(", reclaimed=").$(reclaimed)
                .$(", queuedCount=").$(queuedCount).I$();
    }

    private void throwTimeoutException() {
        if (frameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(UnorderedPageFrameSequence<AsyncGroupByAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncGroupByAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.circuitBreaker = executionContext.getCircuitBreaker();
        Function.init(recordFunctions, frameSequence.getSymbolTableSource(), executionContext, null);
        isDataMapBuilt = false;
    }
}
