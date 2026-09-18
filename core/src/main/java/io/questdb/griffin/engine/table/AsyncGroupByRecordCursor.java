/*+*****************************************************************************
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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.ShardedMapCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByLongTopKJob;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
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
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS;

class AsyncGroupByRecordCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncGroupByRecordCursor.class);
    private final CairoConfiguration configuration;
    private final MessageBus messageBus;
    // Borrowed non-group-by views into recordFunctions; the factory owns and closes the functions.
    private final ObjList<Function> nonGroupByFunctions;
    private final PostAggregationCircuitBreaker postAggregationCircuitBreaker; // used to signal cancellation to merge shard workers
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
    private Map ownerMap;
    private ObjList<Map> shards;

    public AsyncGroupByRecordCursor(
            @NotNull CairoEngine engine,
            @NotNull MessageBus messageBus,
            @NotNull ObjList<Function> recordFunctions
    ) {
        this.configuration = engine.getConfiguration();
        this.messageBus = messageBus;
        this.recordFunctions = recordFunctions;
        this.nonGroupByFunctions = GroupByUtils.extractNonGroupByFunctions(recordFunctions);
        recordA = new VirtualRecord(recordFunctions);
        recordB = new VirtualRecord(recordFunctions);
        postAggregationCircuitBreaker = new PostAggregationCircuitBreaker(engine);
        // Start closed so the first of() runs atom.reopen(), which opens the lazy
        // (openOnInit=false) allocators and binds the per-query tracker before any
        // allocation. Skipping reopen() on the first cursor would leave the allocator's
        // chunk index unallocated.
        isOpen = false;
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
        // Consult the breaker before dispatching frames, so an empty base scan still observes cancellation.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        frameSequence.prepareForDispatch();
        frameSequence.getAtom().getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache(), frameSequence.getMemoryTracker());
        frameSequence.dispatchAndAwait();

        final AsyncGroupByAtom atom = frameSequence.getAtom();

        final GroupByShardingContext shardingCtx = atom.getShardingContext();
        if (!atom.isSharded()) {
            // No sharding was necessary, so the maps are small, and we merge them ourselves.
            ownerMap = shardingCtx.mergeOwnerMap();
            mapCursor = ownerMap.getCursor();
            shards = null;
        } else {
            // We had to shard the maps, so they must be big.
            shards = shardingCtx.mergeShards(
                    messageBus,
                    frameSequence.getWorkStealingStrategy(),
                    circuitBreaker,
                    postAggregationCircuitBreaker,
                    postAggregationDoneLatch,
                    postAggregationStartedCounter
            );
            if (postAggregationCircuitBreaker.checkIfTripped()) {
                throwPostAggregationException();
            }
            // The shards contain non-intersecting row groups, so we can return what's in the shards without merging them.
            shardedCursor.of(shards);
            mapCursor = shardedCursor;
            ownerMap = null;
        }

        recordA.of(mapCursor.getRecord());
        recordB.of(mapCursor.getRecordB());
        isDataMapBuilt = true;
    }

    private void parallelLongTopK(DirectLongLongSortedList destList, Function longFunc) {
        postAggregationCircuitBreaker.reset();
        postAggregationStartedCounter.set(0);
        postAggregationDoneLatch.reset();

        final AsyncGroupByAtom atom = frameSequence.getAtom();
        final RingQueue<GroupByLongTopKTask> queue = messageBus.getGroupByLongTopKQueue();
        final MPSequence pubSeq = messageBus.getGroupByLongTopKPubSeq();
        final MCSequence subSeq = messageBus.getGroupByLongTopKSubSeq();
        final WorkStealingStrategy workStealingStrategy = frameSequence.getWorkStealingStrategy().of(postAggregationStartedCounter);
        final QueryParallelFiberDispatcher dispatcher = messageBus.getQueryParallelFiberDispatcher();
        final AsyncQueryProgressState progressState = atom.getShardingContext().getProgressState();
        final boolean publicationPermit = dispatcher != null && dispatcher.tryAcquirePublication();

        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;
        int processedCount = 0; // used for work stealing decisions
        final boolean isFiberOwner = dispatcher != null
                && !publicationPermit
                && QueryParallelFiberDispatcher.isFiberOwner();
        long lastOwnerYieldNanos = QueryParallelFiberDispatcher.OWNER_YIELD_UNSET;

        try {
            for (int shardIndex = 0; shardIndex < NUM_SHARDS; shardIndex++) {
                if (dispatcher != null && !publicationPermit) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    if (isFiberOwner) {
                        lastOwnerYieldNanos = dispatcher.cooperateFiberOwner(lastOwnerYieldNanos);
                    }
                    final Map shard = atom.getDestShards().getQuick(shardIndex);
                    final DirectLongLongSortedList ownerList = atom.getLongTopKList(
                            -1,
                            destList.getOrder(),
                            destList.getCapacity()
                    );
                    shard.getCursor().longTopK(ownerList, longFunc);
                    ownCount++;
                    total++;
                    continue;
                }
                while (true) {
                    final long observedProgress = progressState.getVersion();
                    final long observedGlobalProgress = dispatcher != null ? dispatcher.getProgressVersion() : 0;
                    final boolean isOwnerParkable = dispatcher != null && dispatcher.isOwnerParkable();
                    long cursor = pubSeq.next();
                    if (cursor < 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();

                        if (workStealingStrategy.shouldSteal(processedCount)) {
                            if (isOwnerParkable) {
                                lastOwnerYieldNanos = dispatcher.cooperateFiberOwner(lastOwnerYieldNanos);
                            }
                            final Map shard = atom.getDestShards().getQuick(shardIndex);
                            final DirectLongLongSortedList ownerList = atom.getLongTopKList(-1, destList.getOrder(), destList.getCapacity());
                            shard.getCursor().longTopK(ownerList, longFunc);
                            ownCount++;
                            total++;
                            processedCount = postAggregationDoneLatch.getCount();
                            break;
                        }
                        if (isOwnerParkable) {
                            if (!dispatcher.awaitProgress(progressState, observedProgress, observedGlobalProgress, circuitBreaker)) {
                                Os.pause();
                            }
                        } else {
                            Os.pause();
                        }
                        processedCount = postAggregationDoneLatch.getCount();
                    } else {
                        queue.get(cursor).of(
                                postAggregationCircuitBreaker,
                                postAggregationStartedCounter,
                                postAggregationDoneLatch,
                                atom,
                                longFunc,
                                shardIndex,
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
            try {
                if (dispatcher != null && publicationPermit) {
                    dispatcher.releasePublication();
                }
            } finally {
                while (true) {
                    final long observedProgress = progressState.getVersion();
                    final long observedGlobalProgress = dispatcher != null ? dispatcher.getProgressVersion() : 0;
                    final boolean isOwnerParkable = dispatcher != null && dispatcher.isOwnerParkable();
                    if (postAggregationDoneLatch.done(queuedCount)) {
                        break;
                    }
                    final boolean isOwnerTripped = circuitBreaker.checkIfTripped();
                    if (isOwnerTripped) {
                        postAggregationCircuitBreaker.cancel();
                    }

                    if (!isOwnerParkable && workStealingStrategy.shouldSteal(processedCount)) {
                        long cursor = subSeq.next();
                        if (cursor > -1) {
                            GroupByLongTopKTask task = queue.get(cursor);
                            // run() releases the slot
                            if (dispatcher != null) {
                                GroupByLongTopKJob.run(-1, task, subSeq, cursor, atom, dispatcher);
                            } else {
                                GroupByLongTopKJob.run(-1, task, subSeq, cursor, atom);
                            }
                            reclaimed++;
                        } else {
                            Os.pause();
                        }
                    } else if (isOwnerParkable) {
                        final boolean isProgressObserved = isOwnerTripped
                                ? dispatcher.awaitProgressWhileDraining(
                                progressState,
                                observedProgress,
                                observedGlobalProgress
                        )
                                : dispatcher.awaitProgressWhileDraining(
                                progressState,
                                observedProgress,
                                observedGlobalProgress,
                                circuitBreaker
                        );
                        if (!isProgressObserved) {
                            Os.pause();
                        }
                    } else {
                        Os.pause();
                    }
                    processedCount = postAggregationDoneLatch.getCount();
                }
            }
        }

        if (postAggregationCircuitBreaker.checkIfTripped()) {
            throwPostAggregationException();
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
                .$(", queuedCount=").$(queuedCount)
                .I$();
    }

    private void throwPostAggregationException() {
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        if (postAggregationCircuitBreaker.hasError()) {
            throw postAggregationCircuitBreaker.buildError();
        }
        throw frameSequence.buildInterruptionException();
    }

    void buildMapConditionally() {
        if (!isDataMapBuilt) {
            buildMap();
        }
    }

    MapRecordCursor initSharedMapCursor(ShardedMapCursor reusableSharded, MapRecordCursor cachedNonSharded) {
        final AsyncGroupByAtom atom = frameSequence.getAtom();
        if (atom.isSharded()) {
            reusableSharded.ofShared(shards);
            return reusableSharded;
        } else if (cachedNonSharded != null) {
            ownerMap.initCursor(cachedNonSharded);
            return cachedNonSharded;
        } else {
            return ownerMap.newCursor();
        }
    }

    void longTopK(DirectLongLongSortedList list, Function recordFunction, MapRecordCursor sharedMapCursor) {
        final AsyncGroupByAtom atom = frameSequence.getAtom();
        if (recordFunction.isThreadSafe() && atom.isSharded() && sharedMapCursor.size() > configuration.getGroupByParallelTopKThreshold()) {
            parallelLongTopK(list, recordFunction);
        } else {
            sharedMapCursor.longTopK(list, recordFunction);
        }
    }

    void of(UnorderedPageFrameSequence<AsyncGroupByAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncGroupByAtom atom = frameSequence.getAtom();
        // Assign before reopen() so close() can drain a partially reopened atom on a breach.
        this.frameSequence = frameSequence;
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.circuitBreaker = executionContext.getCircuitBreaker();
        // Skip the group by functions: the atom initializes them in init(), before any frame is
        // dispatched, and donates the owner state to the per-worker clones. Re-initializing them
        // here would re-run stateful initialization, such as a cursor comparison re-executing its
        // scalar sub-query, and could diverge from the state the workers observe. The constructor
        // pre-filters the non-group-by functions once, so cached re-executions skip the
        // per-function classification scan.
        Function.init(nonGroupByFunctions, frameSequence.getSymbolTableSource(), executionContext, null);
        isDataMapBuilt = false;
    }
}
