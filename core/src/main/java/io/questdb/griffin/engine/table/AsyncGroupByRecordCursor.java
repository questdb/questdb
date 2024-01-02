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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.ShardedMapCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cutlass.text.AtomicBooleanCircuitBreaker;
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

class AsyncGroupByRecordCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncGroupByRecordCursor.class);

    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch(); // used for merge shard workers
    private final ObjList<GroupByFunction> groupByFunctions;
    private final MessageBus messageBus;
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final ObjList<Function> recordFunctions;
    private final ShardedMapCursor shardedCursor = new ShardedMapCursor();
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker; // used to signal cancellation to merge shard workers
    private SqlExecutionCircuitBreaker circuitBreaker;
    private int frameLimit;
    private PageFrameSequence<AsyncGroupByAtom> frameSequence;
    private boolean isDataMapBuilt;
    private boolean isOpen;
    private MapRecordCursor mapCursor;

    public AsyncGroupByRecordCursor(
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            MessageBus messageBus
    ) {
        this.groupByFunctions = groupByFunctions;
        this.recordFunctions = recordFunctions;
        this.messageBus = messageBus;
        recordA = new VirtualRecord(recordFunctions);
        recordB = new VirtualRecord(recordFunctions);
        sharedCircuitBreaker = new AtomicBooleanCircuitBreaker();
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
                        throw CairoException.nonCritical().put(task.getErrorMsg());
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
        } catch (Throwable e) {
            LOG.error().$("group by error [ex=").$(e).I$();
            if (e instanceof CairoException) {
                CairoException ce = (CairoException) e;
                if (ce.isInterruption()) {
                    throwTimeoutException();
                } else {
                    throw ce;
                }
            }
            throw CairoException.nonCritical().put(e.getMessage());
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }

        final AsyncGroupByAtom atom = frameSequence.getAtom();

        if (!atom.isSharded()) {
            // No sharding was necessary, so the maps are small, and we merge them ourselves.
            final Map dataMap = mergeNonShardedMap(atom);
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

    private Map mergeNonShardedMap(AsyncGroupByAtom atom) {
        final Map destMap = atom.getOwnerParticle().getMap();
        final int perWorkerMapCount = atom.getPerWorkerParticles().size();

        long sizeEstimate = destMap.size();
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Map srcMap = atom.getPerWorkerParticles().getQuick(i).getMap();
            sizeEstimate += srcMap.size();
        }

        if (sizeEstimate > 0) {
            // Pre-size the destination map, so that we don't have to resize it later.
            destMap.setKeyCapacity((int) sizeEstimate);
        }

        for (int i = 0; i < perWorkerMapCount; i++) {
            final Map srcMap = atom.getPerWorkerParticles().getQuick(i).getMap();
            destMap.merge(srcMap, atom.getFunctionUpdater());
        }

        for (int i = 0; i < perWorkerMapCount; i++) {
            final Map srcMap = atom.getPerWorkerParticles().getQuick(i).getMap();
            srcMap.close();
        }
        return destMap;
    }

    private ObjList<Map> mergeShards(AsyncGroupByAtom atom) {
        sharedCircuitBreaker.reset();
        doneLatch.reset();

        // First, make sure to shard all non-sharded maps, if any.
        atom.shardAll();

        // Next, merge each set of partial shard maps into the final shard map. This is done in parallel.
        final int shardCount = atom.getShardCount();
        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
        final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
        final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();

        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;

        try {
            for (int i = 0; i < shardCount; i++) {
                long cursor = pubSeq.next();
                if (cursor < 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                    atom.mergeShard(i);
                    ownCount++;
                } else {
                    queue.get(cursor).of(sharedCircuitBreaker, doneLatch, atom, i);
                    pubSeq.done(cursor);
                    queuedCount++;
                }
                total++;
            }
        } catch (Throwable e) {
            sharedCircuitBreaker.cancel();
            throw e;
        } finally {
            // All done? Great, start consuming the queue we just published.
            // How do we get to the end? If we consume our own queue there is chance we will be consuming
            // aggregation tasks not related to this execution (we work in concurrent environment).
            // To deal with that we need to check our latch.

            while (!doneLatch.done(queuedCount)) {
                if (circuitBreaker.checkIfTripped()) {
                    sharedCircuitBreaker.cancel();
                }

                long cursor = subSeq.next();
                if (cursor > -1) {
                    GroupByMergeShardTask task = queue.get(cursor);
                    GroupByMergeShardJob.run(task, subSeq, cursor);
                    reclaimed++;
                } else {
                    Os.pause();
                }
            }
        }

        if (sharedCircuitBreaker.isCanceled()) {
            throwTimeoutException();
        }

        LOG.debug().$("merge shards done [total=").$(total)
                .$(", ownCount=").$(ownCount)
                .$(", reclaimed=").$(reclaimed)
                .$(", queuedCount=").$(queuedCount).I$();

        return atom.getOwnerParticle().getShardMaps();
    }

    private void throwTimeoutException() {
        throw CairoException.nonCritical().put(AsyncFilteredRecordCursor.exceptionMessage).setInterruption(true);
    }

    void of(PageFrameSequence<AsyncGroupByAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        if (!isOpen) {
            isOpen = true;
            frameSequence.getAtom().reopen();
        }
        this.frameSequence = frameSequence;
        this.circuitBreaker = executionContext.getCircuitBreaker();
        Function.init(recordFunctions, frameSequence.getSymbolTableSource(), executionContext);
        isDataMapBuilt = false;
        frameLimit = -1;
    }
}
