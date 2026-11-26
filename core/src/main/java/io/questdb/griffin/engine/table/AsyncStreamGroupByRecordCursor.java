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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.AlwaysWorkStealingStrategy;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.QueryParallelOwnerLoop;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
import io.questdb.griffin.engine.groupby.StreamGroupByMap;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS;

class AsyncStreamGroupByRecordCursor implements NoRandomAccessRecordCursor {
    private final boolean isIntKey;
    private final int keyIndex;
    private final DirectLongList memory;
    private final MessageBus messageBus;
    private final ObjList<Function> nonGroupByFunctions;
    private final QueryParallelOwnerLoop ownerLoop = new QueryParallelOwnerLoop();
    // Cancels the merge workers.
    private final PostAggregationCircuitBreaker postAggregationCircuitBreaker;
    private final SOUnboundedCountDownLatch postAggregationDoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger postAggregationStartedCounter = new AtomicInteger();
    private final VirtualRecord record;
    private final StreamGroupByMap segmentMap;
    private final FlyweightPackedMapValue source;
    private final IntList spanLanes = new IntList();
    private final ObjList<StreamGroupByMap> spanMaps = new ObjList<>();
    private final long stride;
    private final FlyweightPackedMapValue value;
    // The merge has a strategy of its own: the frame sequence keeps stealing reduce work while a bucket merges.
    private final WorkStealingStrategy workStealingStrategy;
    private long chainFirstKey;
    private long chainLastKey;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean done;
    private MapRecordCursor emitCursor;
    private StreamGroupByMap emitMap;
    private int emitShardIndex;
    private ObjList<Map> emitShards;
    private int frameIndex = -1;
    private int frameLimit = -1;
    private PageFrameSequence<AsyncStreamGroupByAtom> frameSequence;
    private boolean hasOpenKey;
    private boolean hasOpenSpans;
    // The task whose frame the cursor reads, from nextFrame() to collect().
    private int heldTaskIndex = -1;
    private boolean isChainLastKeyInSpan;
    private boolean isChainPending;
    private boolean isChainRowPending;
    private boolean isFirstKeyInSpanPending;
    private boolean isOpenCollected;
    private boolean isShardMerged;
    private long openKey;
    private boolean prepared;
    private long resultOffset;
    private RecordChain resultRecords;
    private DirectLongList results;

    AsyncStreamGroupByRecordCursor(
            CairoEngine engine,
            MessageBus messageBus,
            ColumnTypes stateTypes,
            ObjList<Function> functions,
            StreamGroupByMap segmentMap,
            WorkStealingStrategy workStealingStrategy
    ) {
        this.messageBus = messageBus;
        this.workStealingStrategy = workStealingStrategy;
        this.segmentMap = segmentMap;
        postAggregationCircuitBreaker = new PostAggregationCircuitBreaker(engine);
        keyIndex = stateTypes.getColumnCount() - 1;
        isIntKey = stateTypes.getColumnType(keyIndex) == ColumnType.INT;
        value = new FlyweightPackedMapValue(stateTypes);
        source = new FlyweightPackedMapValue(stateTypes);
        stride = (value.getSizeInBytes() + 7) / 8;
        record = new VirtualRecord(functions);
        nonGroupByFunctions = GroupByUtils.extractNonGroupByFunctions(functions);
        record.of(segmentMap != null ? segmentMap.getRecord() : value);
        memory = new DirectLongList(stride, MemoryTag.NATIVE_FAST_MAP, true);
    }

    @Override
    public void close() {
        try {
            if (frameSequence != null) {
                if (!done) {
                    frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                }
                collect();
                if (prepared) {
                    frameSequence.await();
                }
                frameSequence.reset();
            }
        } finally {
            frameSequence = null;
            results = null;
            resultRecords = null;
            emitCursor = null;
            emitMap = null;
            emitShards = null;
            isShardMerged = false;
            spanMaps.clear();
            spanLanes.clear();
            memory.close();
            Function.cursorClosed(nonGroupByFunctions);
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) record.getFunctions().getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        // Stays small so that a caller's compiled loop can inline it.
        return segmentMap != null ? hasNextSegment() : hasNextNotKeyed();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) record.getFunctions().getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        collect();
        frameSequence.toTop();
        frameSequence.getAtom().toTop();
        releaseSpanMaps();
        frameIndex = -1;
        done = false;
        resetSegments();
    }

    void of(PageFrameSequence<AsyncStreamGroupByAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        this.frameSequence = frameSequence;
        circuitBreaker = executionContext.getCircuitBreaker();
        prepared = done = false;
        frameIndex = frameLimit = -1;
        heldTaskIndex = -1;
        results = null;
        resultRecords = null;
        emitCursor = null;
        emitMap = null;
        emitShards = null;
        isShardMerged = false;
        spanMaps.clear();
        spanLanes.clear();
        resetSegments();
        if (segmentMap == null) {
            memory.ensureCapacity(stride);
            value.of(memory.getAddress());
        }
        Function.init(nonGroupByFunctions, frameSequence.getSymbolTableSource(), executionContext, null);
    }

    private void closeOpenSegment() {
        hasOpenKey = false;
        emitMap = segmentMap;
        emitCursor = null;
        if (hasOpenSpans) {
            final AsyncStreamGroupByAtom atom = frameSequence.getAtom();
            final GroupByFunctionsUpdater updater = atom.getUpdater(-1);
            atom.takeSpanMaps(openKey, spanMaps, spanLanes);
            final int n = spanMaps.size();
            assert n > 0;
            if (isSharded()) {
                mergeShards(atom);
                hasOpenSpans = false;
                isOpenCollected = false;
                return;
            }
            int largest = 0;
            long size = spanMaps.getQuick(0).size();
            for (int i = 1; i < n; i++) {
                size += spanMaps.getQuick(i).size();
                if (spanMaps.getQuick(i).size() > spanMaps.getQuick(largest).size()) {
                    largest = i;
                }
            }
            // A pair costs a task, which pays once the maps average a quarter of the sharding threshold.
            if (n > 2 && size - spanMaps.getQuick(largest).size() > (n - 1) * (long) atom.getSpanRowThreshold() / 4) {
                mergeSpanMapsInParallel(atom);
                largest = 0;
            }
            final StreamGroupByMap dest = spanMaps.getQuick(largest);
            spanMaps.setQuick(largest, spanMaps.getQuick(0));
            spanMaps.setQuick(0, dest);
            final int destLane = spanLanes.getQuick(largest);
            spanLanes.setQuick(largest, spanLanes.getQuick(0));
            spanLanes.setQuick(0, destLane);
            for (int i = spanMaps.size() - 1; i > 0; i--) {
                final StreamGroupByMap map = spanMaps.getQuick(i);
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                dest.merge(map, updater);
                atom.releaseSpanMap(spanLanes.getQuick(i), map);
                spanMaps.setPos(i);
                spanLanes.setPos(i);
            }
            if (isOpenCollected) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                dest.merge(segmentMap, updater);
            }
            emitMap = dest;
        }
        hasOpenSpans = false;
        isOpenCollected = false;
        emitCursor = emitMap.getCursor();
        segmentMap.ofRow(emitCursor.getRecord(), emitMap.getSegmentKey());
    }

    private boolean isSharded() {
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            if (spanMaps.getQuick(i).isSharded()) {
                return true;
            }
        }
        return false;
    }

    // The span maps of a bucket with many keys merge pairwise in parallel, which halves their number each round.
    private void mergeSpanMapsInParallel(AsyncStreamGroupByAtom atom) {
        for (int n = spanMaps.size(); n > 1; n = spanMaps.size()) {
            for (int i = 1; i < n; i++) {
                final StreamGroupByMap map = spanMaps.getQuick(i);
                final int lane = spanLanes.getQuick(i);
                int j = i;
                for (; j > 0 && spanMaps.getQuick(j - 1).size() < map.size(); j--) {
                    spanMaps.setQuick(j, spanMaps.getQuick(j - 1));
                    spanLanes.setQuick(j, spanLanes.getQuick(j - 1));
                }
                spanMaps.setQuick(j, map);
                spanLanes.setQuick(j, lane);
            }
            final int half = (n + 1) / 2;
            GroupByMergeShardJob.mergeShards(
                    atom.openSpanMapMerge(spanMaps, half),
                    ownerLoop,
                    messageBus,
                    AlwaysWorkStealingStrategy.INSTANCE,
                    circuitBreaker,
                    postAggregationCircuitBreaker,
                    postAggregationDoneLatch,
                    postAggregationStartedCounter,
                    true,
                    n - half
            );
            if (postAggregationCircuitBreaker.checkIfTrippedOrYield()) {
                throwPostAggregationException();
            }
            for (int i = n - 1; i >= half; i--) {
                atom.releaseSpanMap(spanLanes.getQuick(i), spanMaps.getQuick(i));
                spanMaps.setPos(i);
                spanLanes.setPos(i);
            }
        }
    }

    // The span maps of a high-cardinality bucket merge shard by shard in parallel.
    private void mergeShards(AsyncStreamGroupByAtom atom) {
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final StreamGroupByMap map = spanMaps.getQuick(i);
            map.shard(atom.getUpdater(-1));
            map.rememberShardSizes();
        }
        isShardMerged = true;
        final ObjList<Map> shards = atom.openShardMerge(spanMaps);
        GroupByMergeShardJob.mergeShards(
                atom,
                ownerLoop,
                messageBus,
                workStealingStrategy,
                circuitBreaker,
                postAggregationCircuitBreaker,
                postAggregationDoneLatch,
                postAggregationStartedCounter,
                // The workers may stay busy reducing later frames, so the owner must not wait for them.
                true,
                NUM_SHARDS
        );
        if (postAggregationCircuitBreaker.checkIfTrippedOrYield()) {
            throwPostAggregationException();
        }
        if (isOpenCollected) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            segmentMap.mergeInto(shards, atom.getUpdater(-1));
        }
        emitMap = spanMaps.getQuick(0);
        emitShards = shards;
        emitShardIndex = -1;
        nextEmitShard();
    }

    // Emits the shards one after another through their own cursors, the cursor type an unsharded bucket emits.
    private boolean nextEmitShard() {
        if (emitShards == null || ++emitShardIndex == emitShards.size()) {
            return false;
        }
        emitCursor = emitShards.getQuick(emitShardIndex).getCursor();
        segmentMap.ofRow(emitCursor.getRecord(), emitMap.getSegmentKey());
        return true;
    }

    // Frees the atom slot of the frame that nextFrame() returned, once the cursor has read its output.
    private void collect() {
        if (heldTaskIndex >= 0) {
            frameSequence.getAtom().releaseResults(frameIndex);
            frameSequence.collect(heldTaskIndex, false);
            heldTaskIndex = -1;
        }
        results = null;
        resultRecords = null;
    }

    private boolean hasNextNotKeyed() {
        if (done) {
            return false;
        }
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        boolean found = false;
        while (nextPartial()) {
            source.of(results.getAddress() + resultOffset * Long.BYTES);
            if (!found) {
                value.copyFrom(source);
                found = true;
            } else if (isIntKey ? value.getInt(keyIndex) == source.getInt(keyIndex) : value.getLong(keyIndex) == source.getLong(keyIndex)) {
                frameSequence.getAtom().getUpdater(-1).merge(value, source);
            } else {
                return true;
            }
            resultOffset += stride;
        }
        done = true;
        return found;
    }

    private boolean hasNextSegment() {
        if (emitCursor != null && emitCursor.hasNext()) {
            return true;
        }
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        while (true) {
            if (emitCursor != null) {
                if (emitCursor.hasNext()) {
                    return true;
                }
                if (nextEmitShard()) {
                    continue;
                }
                releaseSpanMaps();
            }
            if (isFirstKeyInSpanPending) {
                isFirstKeyInSpanPending = false;
                hasOpenKey = true;
                openKey = chainFirstKey;
                hasOpenSpans = true;
                closeOpenSegment();
                continue;
            }
            if (isChainPending) {
                if (isChainRowPending || resultRecords.hasNext()) {
                    isChainRowPending = false;
                    final Record chainRecord = resultRecords.getRecord();
                    final long key = segmentMap.getSegmentKey(chainRecord);
                    if (key != chainLastKey) {
                        segmentMap.ofRow(chainRecord, key);
                        return true;
                    }
                    segmentMap.clear(key);
                    final GroupByFunctionsUpdater updater = frameSequence.getAtom().getUpdater(-1);
                    do {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        segmentMap.merge(chainRecord, updater);
                    } while (resultRecords.hasNext());
                    hasOpenKey = true;
                    openKey = key;
                    isOpenCollected = true;
                }
                isChainPending = false;
                if (isChainLastKeyInSpan) {
                    hasOpenKey = true;
                    openKey = chainLastKey;
                    hasOpenSpans = true;
                }
            }
            if (done) {
                return false;
            }
            if (!nextSegments()) {
                done = true;
                if (hasOpenKey) {
                    closeOpenSegment();
                }
            }
        }
    }

    private boolean nextPartial() {
        if (results != null && resultOffset < results.size()) {
            return true;
        }
        while (nextFrame() >= 0) {
            results = frameSequence.getAtom().getResultValues(frameIndex);
            resultOffset = 0;
            if (results.size() > 0) {
                return true;
            }
        }
        return false;
    }

    private boolean nextSegments() {
        final AsyncStreamGroupByAtom atom = frameSequence.getAtom();
        do {
            if (nextFrame() < 0) {
                return false;
            }
        } while (atom.getSegmentCount(frameIndex) == 0);
        final long first = atom.getFirstSegmentKey(frameIndex);
        if (hasOpenKey && openKey != first) {
            closeOpenSegment();
        }
        if (atom.getSegmentCount(frameIndex) == 1) {
            if (!hasOpenKey) {
                hasOpenKey = true;
                openKey = first;
            }
            hasOpenSpans = true;
            return true;
        }
        resultRecords = atom.getResultRecords(frameIndex);
        chainLastKey = atom.getLastSegmentKey(frameIndex);
        isChainLastKeyInSpan = atom.isLastSegmentKeyInSpan(frameIndex);
        isChainPending = true;
        if (atom.isFirstSegmentKeyInSpan(frameIndex)) {
            // The bucket closed above may still have rows to emit.
            chainFirstKey = first;
            isFirstKeyInSpanPending = true;
            return true;
        }
        if (hasOpenKey) {
            if (!isOpenCollected) {
                segmentMap.clear(first);
                isOpenCollected = true;
            }
            final GroupByFunctionsUpdater updater = atom.getUpdater(-1);
            while (resultRecords.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final Record chainRecord = resultRecords.getRecord();
                if (segmentMap.getSegmentKey(chainRecord) != first) {
                    isChainRowPending = true;
                    break;
                }
                segmentMap.merge(chainRecord, updater);
            }
            closeOpenSegment();
        }
        return true;
    }

    // Returns the next frame in scan order once it is reduced, or -1 after the last one. The frame stays held until
    // the next call, toTop() or close().
    private int nextFrame() {
        collect();
        if (!prepared) {
            frameSequence.prepareForDispatch();
            final AsyncFilterContext filterContext = frameSequence.getAtom().getFilterContext();
            if (filterContext != null) {
                filterContext.initMemoryPools(frameSequence.getPageFrameAddressCache(), frameSequence.getMemoryTracker());
            }
            prepared = true;
            frameLimit = frameSequence.getFrameCount() - 1;
        }
        while (frameIndex < frameLimit) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long taskIndex = frameSequence.next(frameSequence.getAtom().getDispatchLimit(frameIndex + 1), false);
            if (taskIndex >= 0) {
                heldTaskIndex = (int) taskIndex;
                // A stream GROUP BY task holds a single frame.
                frameIndex = frameSequence.getTaskFirstFrame(heldTaskIndex);
                return frameIndex;
            }
            // The cursor reads up to its last frame and never stops the sequence, so a task is always left here.
            assert taskIndex == -1;
            Os.pause();
        }
        if (!frameSequence.isActive()) {
            throw frameSequence.buildInterruptionException();
        }
        return -1;
    }

    private void releaseSpanMaps() {
        emitCursor = null;
        emitMap = null;
        emitShards = null;
        final AsyncStreamGroupByAtom atom = frameSequence.getAtom();
        if (isShardMerged) {
            isShardMerged = false;
            atom.closeShardMerge();
        }
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            atom.releaseSpanMap(spanLanes.getQuick(i), spanMaps.getQuick(i));
        }
        spanMaps.clear();
        spanLanes.clear();
    }

    private void throwPostAggregationException() {
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        if (postAggregationCircuitBreaker.hasError()) {
            throw postAggregationCircuitBreaker.buildError();
        }
        throw frameSequence.buildInterruptionException();
    }

    private void resetSegments() {
        hasOpenKey = false;
        hasOpenSpans = false;
        isOpenCollected = false;
        isChainLastKeyInSpan = false;
        isChainPending = false;
        isChainRowPending = false;
        isFirstKeyInSpanPending = false;
    }
}
