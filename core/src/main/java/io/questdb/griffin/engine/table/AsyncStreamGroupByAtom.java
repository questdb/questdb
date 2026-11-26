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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByShardMerger;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.StreamGroupByMap;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS;

public class AsyncStreamGroupByAtom implements StatefulAtom, PerWorkerLockOwner, GroupByShardMerger {
    // The span flags of a frame's segments, see frameSegments.
    private static final long FIRST_KEY_IN_SPAN = 1;
    private static final long LAST_KEY_IN_SPAN = 2;
    private static final int SEGMENT_COUNT = 0;
    private static final int SEGMENT_FIRST_KEY = 1;
    private static final int SEGMENT_LAST_KEY = 2;
    private static final int SEGMENT_SPAN_FLAGS = 3;
    private static final int SEGMENT_STRIDE = 4;
    private final int[] batchColumnIndexes;
    private final StreamGroupByMap collectorMap;
    private final CairoConfiguration configuration;
    // The merged shards of the span maps of a bucket, from its close to its emission.
    private final ObjList<Map> destShards = new ObjList<>();
    private final AsyncFilterContext filterContext;
    // A frame's segments, by frame index modulo the reduce queue capacity, from its reduce to its collection.
    private final ObjList<RecordChain> frameRecords;
    // A frame's segments as its reduce found them, by frame index modulo the reduce queue capacity: their count, the
    // first and the last key, and whether those keys went to span maps. The frame's completion mark publishes them.
    private final long[] frameSegments;
    private final ObjList<DirectLongList> frameValues;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final int initialDispatchLimit;
    private final boolean isDescending;
    private final Function keyFunction;
    private final int keyFunctionIndex;
    private final ObjList<Function> keyFunctions;
    private final PerWorkerLocks locks;
    private final ObjList<StreamGroupByMap> mergeSources = new ObjList<>();
    private final DirectLongList ownerBatchRows;
    private final StreamGroupByMap ownerMap;
    private final ObjList<DirectLongList> perWorkerBatchRows;
    private final ObjList<ObjList<GroupByFunction>> perWorkerFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeys;
    private final ObjList<StreamGroupByMap> perWorkerMaps;
    private final ObjList<GroupByFunctionsUpdater> perWorkerUpdaters;
    private final ObjList<FlyweightPackedMapValue> perWorkerValues;
    private final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
    private final SpanMapMerger spanMapMerger = new SpanMapMerger();
    // The span maps by lane. A frame aggregates the segments that span its bucket into the maps of its lane, frame
    // index modulo the lane count, whichever thread reduces it. The sequence reduces the frames of a lane one at a
    // time and in order, so a lane adds up the same rows in the same order on every run, and so does the merge of
    // the lanes: floating-point sums come out the same however the workers share the frames. A lane has one frame in
    // flight, and only the owner dispatches the next one, so there are twice as many lanes as threads that reduce
    // frames, the workers and the owner: while the owner reduces a frame, the workers find frames of other lanes queued.
    private final ObjList<SpanMaps> spanMaps;
    private final ArrayColumnTypes stateTypes;
    private final GroupByFunctionsUpdater updater;
    private final FlyweightPackedMapValue value;
    private MemoryTracker memoryTracker;
    private int shardMergeCount;
    private int spanMapMergeCount;

    public AsyncStreamGroupByAtom(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            RecordMetadata baseMetadata,
            ListColumnFilter columnFilter,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            int streamKeyIndex,
            Function keyFunction,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> perWorkerKeys,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> perWorkerFunctions,
            CompiledFilter compiledFilter,
            MemoryCARW bindVarMemory,
            ObjList<Function> bindVarFunctions,
            Function filter,
            IntHashSet filterUsedColumnIndexes,
            ObjList<Function> perWorkerFilters,
            int workerCount,
            boolean isDescending,
            int initialDispatchLimit,
            int maxDispatchLimit
    ) {
        this.configuration = configuration;
        this.initialDispatchLimit = initialDispatchLimit;
        this.frameRecords = new ObjList<>(maxDispatchLimit);
        this.frameRecords.setPos(maxDispatchLimit);
        this.frameSegments = new long[maxDispatchLimit * SEGMENT_STRIDE];
        this.frameValues = new ObjList<>(maxDispatchLimit);
        this.frameValues.setPos(maxDispatchLimit);
        this.keyFunction = keyFunction;
        this.isDescending = isDescending;
        this.keyFunctionIndex = streamKeyIndex - columnFilter.getColumnCount();
        this.keyFunctions = keyFunctions;
        this.perWorkerKeys = perWorkerKeys;
        this.groupByFunctions = groupByFunctions;
        this.perWorkerFunctions = perWorkerFunctions;
        try {
            filterContext = filter == null ? null : new AsyncFilterContext(configuration, compiledFilter,
                    bindVarMemory, bindVarFunctions, filter, filterUsedColumnIndexes, perWorkerFilters,
                    workerCount, workerCount, 0, 0);
            stateTypes = new ArrayColumnTypes().addAll(valueTypes);
            stateTypes.add(keyFunction.getType());
            batchColumnIndexes = new int[groupByFunctions.size()];
            for (int i = 0; i < batchColumnIndexes.length; i++) {
                batchColumnIndexes[i] = AsyncGroupByNotKeyedRecordCursorFactory.computeBatchColumnIndex(groupByFunctions.getQuick(i), baseMetadata);
            }
            locks = new PerWorkerLocks(configuration, workerCount);
            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            updater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);
            value = new FlyweightPackedMapValue(stateTypes);
            perWorkerValues = new ObjList<>(workerCount);
            perWorkerUpdaters = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerValues.add(new FlyweightPackedMapValue(stateTypes));
                perWorkerUpdaters.add(perWorkerFunctions == null ? updater
                        : GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerFunctions.getQuick(i)));
            }
            if (keyTypes.getColumnCount() > 1) {
                final int batchSize = configuration.getGroupByBatchSize();
                ownerBatchRows = new DirectLongList(batchSize, MemoryTag.NATIVE_DEFAULT, true);
                perWorkerBatchRows = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerBatchRows.add(new DirectLongList(batchSize, MemoryTag.NATIVE_DEFAULT, true));
                }
                ownerMap = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes, valueTypes,
                        streamKeyIndex, keyFunctions, groupByFunctions, updater, null);
                collectorMap = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes, valueTypes,
                        streamKeyIndex, keyFunctions, groupByFunctions, updater, ownerMap);
                perWorkerMaps = new ObjList<>(workerCount);
                final int laneCount = 2 * (workerCount + 1);
                spanMaps = new ObjList<>(laneCount);
                for (int i = 0; i < laneCount; i++) {
                    spanMaps.add(new SpanMaps(ownerMap));
                }
                for (int i = 0; i < workerCount; i++) {
                    final StreamGroupByMap map = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes,
                            valueTypes, streamKeyIndex, perWorkerKeys == null ? keyFunctions : perWorkerKeys.getQuick(i),
                            getFunctions(i), getUpdater(i), ownerMap);
                    perWorkerMaps.add(map);
                }
            } else {
                ownerBatchRows = null;
                perWorkerBatchRows = null;
                ownerMap = collectorMap = null;
                perWorkerMaps = null;
                spanMaps = null;
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        releaseAllResults();
        CairoException.rethrowCleanupFailure(closeMaps(null));
        if (filterContext != null) {
            filterContext.clear();
        }
        cursorClosed(groupByFunctions);
        cursorClosed(keyFunctions);
        Misc.clearObjList(groupByFunctions);
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                cursorClosed(perWorkerFunctions.getQuick(i));
                PerWorkerFunctionList.clear(perWorkerFunctions.getQuick(i));
            }
        }
        if (perWorkerKeys != null) {
            for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                cursorClosed(perWorkerKeys.getQuick(i));
            }
        }
    }

    @Override
    public void close() {
        Throwable th = Misc.freeObjListBestEffort(null, keyFunctions);
        th = Misc.freeBestEffort(th, filterContext);
        th = closeMaps(th);
        th = Misc.freeObjListBestEffort(th, frameRecords);
        th = Misc.freeObjListBestEffort(th, frameValues);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerKeys);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerFunctions);
        CairoException.rethrowCleanupFailure(th);
    }

    // The span map of the frame's lane for the bucket. The slot of the reducing thread fills in the empty value of a
    // map it opens.
    public StreamGroupByMap acquireSpanMap(int frameIndex, int slot, long key) {
        return spanMaps.getQuick(frameIndex % spanMaps.size()).acquire(configuration, memoryTracker, key, getUpdater(slot));
    }

    public void closeShardMerge() {
        mergeSources.clear();
    }

    public int[] getBatchColumnIndexes() {
        return batchColumnIndexes;
    }

    public StreamGroupByMap getCollectorMap() {
        return collectorMap;
    }

    public AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    // A LIMIT that the first frame satisfies keeps the small window, which holds a frame for each worker and the owner.
    public int getDispatchLimit(int collectedFrameCount) {
        return collectedFrameCount == 0 ? Math.min(initialDispatchLimit, frameRecords.size()) : frameRecords.size();
    }

    public ObjList<GroupByFunction> getFunctions(int slot) {
        return slot < 0 || perWorkerFunctions == null ? groupByFunctions : perWorkerFunctions.getQuick(slot);
    }

    public Function getKey(int slot) {
        return slot < 0 || perWorkerKeys == null || keyFunctionIndex < 0
                ? keyFunction : perWorkerKeys.getQuick(slot).getQuick(keyFunctionIndex);
    }

    public ObjList<Function> getKeyFunctions() {
        return keyFunctions;
    }

    // The lane count of the span maps, 0 when no frame aggregates into one.
    public int getLaneCount() {
        return spanMaps == null ? 0 : spanMaps.size();
    }

    public DirectLongList getBatchRows(int slot) {
        return slot < 0 ? ownerBatchRows : perWorkerBatchRows.getQuick(slot);
    }

    public StreamGroupByMap getMap(int slot) {
        return slot < 0 ? ownerMap : perWorkerMaps.getQuick(slot);
    }

    @Override
    public PerWorkerLocks getPerWorkerLocks() {
        return locks;
    }

    public int getSpanRowThreshold() {
        return configuration.getGroupByShardingThreshold();
    }

    @TestOnly
    public int getSpanMapKeyCapacity() {
        int capacity = 0;
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            capacity = Math.max(capacity, spanMaps.getQuick(i).getKeyCapacity());
        }
        return capacity;
    }

    @TestOnly
    public int getMergeShardKeyCapacity() {
        int capacity = 0;
        for (int i = 0, n = destShards.size(); i < n; i++) {
            capacity = Math.max(capacity, destShards.getQuick(i).getKeyCapacity());
        }
        return capacity;
    }

    @TestOnly
    public int getShardKeyCapacity() {
        int capacity = 0;
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            capacity = Math.max(capacity, spanMaps.getQuick(i).getShardKeyCapacity());
        }
        return capacity;
    }

    public long getFirstSegmentKey(int frameIndex) {
        return frameSegments[getSegmentOffset(frameIndex) + SEGMENT_FIRST_KEY];
    }

    public long getLastSegmentKey(int frameIndex) {
        return frameSegments[getSegmentOffset(frameIndex) + SEGMENT_LAST_KEY];
    }

    // The chain rows of a reduced frame, which its reduce filled through getResultRecords(PageFrameReduceTask).
    public RecordChain getResultRecords(int frameIndex) {
        return frameRecords.getQuick(frameIndex % frameRecords.size());
    }

    public RecordChain getResultRecords(PageFrameReduceTask task) {
        final int slot = task.getFrameIndex() % frameRecords.size();
        RecordChain records = frameRecords.getQuick(slot);
        if (records == null) {
            records = new RecordChain(ownerMap.getStateTypes(), null, configuration.getSqlSmallMapPageSize(), Integer.MAX_VALUE);
            frameRecords.setQuick(slot, records);
        }
        records.setMemoryTracker(task.getMemoryTracker());
        return records;
    }

    // The group values of a reduced frame, which its reduce filled through getResultValues(PageFrameReduceTask).
    public DirectLongList getResultValues(int frameIndex) {
        return frameValues.getQuick(frameIndex % frameValues.size());
    }

    public DirectLongList getResultValues(PageFrameReduceTask task) {
        final int slot = task.getFrameIndex() % frameValues.size();
        DirectLongList values = frameValues.getQuick(slot);
        if (values == null) {
            values = new DirectLongList(64, MemoryTag.NATIVE_FAST_MAP, true);
            frameValues.setQuick(slot, values);
        }
        values.setMemoryTracker(task.getMemoryTracker());
        return values;
    }

    public long getSegmentCount(int frameIndex) {
        return frameSegments[getSegmentOffset(frameIndex) + SEGMENT_COUNT];
    }

    public boolean isFirstSegmentKeyInSpan(int frameIndex) {
        return (frameSegments[getSegmentOffset(frameIndex) + SEGMENT_SPAN_FLAGS] & FIRST_KEY_IN_SPAN) != 0;
    }

    public boolean isLastSegmentKeyInSpan(int frameIndex) {
        return (frameSegments[getSegmentOffset(frameIndex) + SEGMENT_SPAN_FLAGS] & LAST_KEY_IN_SPAN) != 0;
    }

    public void setSegments(int frameIndex, long count, long firstKey, boolean isFirstKeyInSpan, long lastKey, boolean isLastKeyInSpan) {
        final int offset = getSegmentOffset(frameIndex);
        frameSegments[offset + SEGMENT_COUNT] = count;
        frameSegments[offset + SEGMENT_FIRST_KEY] = firstKey;
        frameSegments[offset + SEGMENT_LAST_KEY] = lastKey;
        frameSegments[offset + SEGMENT_SPAN_FLAGS] = (isFirstKeyInSpan ? FIRST_KEY_IN_SPAN : 0) | (isLastKeyInSpan ? LAST_KEY_IN_SPAN : 0);
    }

    @TestOnly
    public long getShardMergeKeyCapacity() {
        long capacity = 0;
        for (int i = 0, n = destShards.size(); i < n; i++) {
            capacity += destShards.getQuick(i).getKeyCapacity();
        }
        return capacity;
    }

    @TestOnly
    public int getShardMergeCount() {
        return shardMergeCount;
    }

    @TestOnly
    public int getSpanMapMergeCount() {
        return spanMapMergeCount;
    }

    @Override
    public AsyncQueryProgressState getProgressState() {
        return progressState;
    }

    public ArrayColumnTypes getStateTypes() {
        return stateTypes;
    }

    public GroupByFunctionsUpdater getUpdater(int slot) {
        return slot < 0 ? updater : perWorkerUpdaters.getQuick(slot);
    }

    public FlyweightPackedMapValue getValue(int slot) {
        return slot < 0 ? value : perWorkerValues.getQuick(slot);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (filterContext != null) {
            filterContext.initFilters(symbolTableSource, executionContext);
        }
        Function.init(groupByFunctions, symbolTableSource, executionContext, null);
        Function.init(keyFunctions, symbolTableSource, executionContext, null);
        final boolean cloneSymbolTables = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(true);
        try {
            if (perWorkerFunctions != null) {
                for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                    PerWorkerFunctionList.init(perWorkerFunctions.getQuick(i), groupByFunctions, symbolTableSource, executionContext);
                }
            }
            if (perWorkerKeys != null) {
                for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                    PerWorkerFunctionList.init(perWorkerKeys.getQuick(i), keyFunctions, symbolTableSource, executionContext);
                }
            }
        } finally {
            executionContext.setCloneSymbolTables(cloneSymbolTables);
        }
        if (ownerMap != null) {
            try {
                memoryTracker = executionContext.getMemoryTracker();
                ownerMap.reopen(memoryTracker);
                collectorMap.reopen(memoryTracker);
                for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
                    perWorkerMaps.getQuick(i).reopen(memoryTracker);
                }
            } catch (Throwable th) {
                closeMaps(th);
                throw th;
            }
        }
    }

    public boolean isDescending() {
        return isDescending;
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        return workerId == -1 && owner ? -1 : locks.acquireSlot(workerId, circuitBreaker);
    }

    @Override
    public int maybeAcquire(int carrierId, boolean owner, ExecutionCircuitBreaker circuitBreaker) {
        return carrierId == -1 && owner ? -1 : locks.acquireSlot(carrierId, circuitBreaker);
    }

    @Override
    public void mergeShard(int slotId, int shardIndex) {
        final Map dest = destShards.getQuick(shardIndex);
        // The frames of a bucket often repeat their keys, so the sum of the sources would reserve far too much.
        long size = 0;
        for (int i = 0, n = mergeSources.size(); i < n; i++) {
            size = Math.max(size, mergeSources.getQuick(i).getShards().getQuick(shardIndex).size());
        }
        // Otherwise one huge bucket makes every later bucket pay to clear and emit its slots.
        if (dest.getKeyCapacity() > StreamGroupByMap.SHRINK_RATIO * size) {
            dest.restoreInitialCapacity();
        }
        dest.clear();
        if (size > dest.getKeyCapacity()) {
            dest.setKeyCapacity((int) Math.min(size, Integer.MAX_VALUE));
        }
        final GroupByFunctionsUpdater updater = getUpdater(slotId);
        for (int i = 0, n = mergeSources.size(); i < n; i++) {
            final Map source = mergeSources.getQuick(i).getShards().getQuick(shardIndex);
            dest.merge(source, updater);
            source.close();
        }
    }

    public GroupByShardMerger openSpanMapMerge(ObjList<StreamGroupByMap> maps, int half) {
        spanMapMergeCount++;
        spanMapMerger.maps = maps;
        spanMapMerger.half = half;
        return spanMapMerger;
    }

    // The span maps must be sharded.
    public ObjList<Map> openShardMerge(ObjList<StreamGroupByMap> sources) {
        shardMergeCount++;
        mergeSources.clear();
        mergeSources.addAll(sources);
        if (destShards.size() == 0) {
            for (int i = 0; i < NUM_SHARDS; i++) {
                destShards.add(ownerMap.newMergeShard());
            }
        }
        for (int i = 0; i < NUM_SHARDS; i++) {
            final Map shard = destShards.getQuick(i);
            if (!shard.isOpen()) {
                shard.setMemoryTracker(memoryTracker);
                shard.reopen();
            }
        }
        return destShards;
    }

    @Override
    public void release(int slot) {
        locks.releaseSlot(slot);
    }

    public void releaseAllResults() {
        for (int i = 0, n = frameRecords.size(); i < n; i++) {
            releaseResults(i);
        }
    }

    public void releaseResults(int frameIndex) {
        final int slot = frameIndex % frameRecords.size();
        final RecordChain records = frameRecords.getQuick(slot);
        if (records != null) {
            records.close();
            records.setMemoryTracker(null);
        }
        final DirectLongList values = frameValues.getQuick(slot);
        if (values != null) {
            values.close();
            values.setMemoryTracker(null);
        }
    }

    public void releaseSpanMap(int lane, StreamGroupByMap map) {
        spanMaps.getQuick(lane).release(map);
    }

    // Takes the span maps of the bucket in lane order, which fixes the order of their merge.
    public void takeSpanMaps(long key, ObjList<StreamGroupByMap> maps, IntList lanes) {
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            final StreamGroupByMap map = spanMaps.getQuick(i).take(key);
            if (map != null) {
                maps.add(map);
                lanes.add(i);
            }
        }
    }

    public void toTop() {
        releaseAllResults();
        if (filterContext != null) {
            filterContext.toTop();
        }
        if (spanMaps != null) {
            for (int i = 0, n = spanMaps.size(); i < n; i++) {
                spanMaps.getQuick(i).releaseAll();
            }
        }
        GroupByUtils.toTop(groupByFunctions);
        GroupByUtils.toTop(keyFunctions);
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerFunctions.getQuick(i));
            }
        }
        if (perWorkerKeys != null) {
            for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerKeys.getQuick(i));
            }
        }
    }

    private Throwable closeMaps(Throwable th) {
        mergeSources.clear();
        for (int i = 0, n = destShards.size(); i < n; i++) {
            th = Misc.freeBestEffort(th, destShards.getQuick(i));
        }
        th = Misc.freeBestEffort(th, ownerMap);
        th = Misc.freeBestEffort(th, collectorMap);
        th = Misc.freeBestEffort(th, ownerBatchRows);
        th = Misc.freeObjListAndKeepObjectsBestEffort(th, perWorkerBatchRows);
        if (perWorkerMaps != null) {
            for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
                th = Misc.freeBestEffort(th, perWorkerMaps.getQuick(i));
            }
        }
        if (spanMaps != null) {
            for (int i = 0, n = spanMaps.size(); i < n; i++) {
                th = spanMaps.getQuick(i).close(th);
            }
        }
        return th;
    }

    private static void cursorClosed(ObjList<? extends Function> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (PerWorkerFunctionList.isOwned(functions, i)) {
                functions.getQuick(i).cursorClosed();
            }
        }
    }

    // Merges the second half of a bucket's span maps, which the caller sorts largest first, into the first half, a pair per task.
    private int getSegmentOffset(int frameIndex) {
        return (frameIndex % frameRecords.size()) * SEGMENT_STRIDE;
    }

    private class SpanMapMerger implements GroupByShardMerger {
        private int half;
        private ObjList<StreamGroupByMap> maps;

        @Override
        public AsyncQueryProgressState getProgressState() {
            return progressState;
        }

        @Override
        public int maybeAcquire(int carrierId, boolean owner, ExecutionCircuitBreaker circuitBreaker) {
            return AsyncStreamGroupByAtom.this.maybeAcquire(carrierId, owner, circuitBreaker);
        }

        @Override
        public void mergeShard(int slotId, int pairIndex) {
            maps.getQuick(pairIndex).merge(maps.getQuick(half + pairIndex), getUpdater(slotId));
        }

        @Override
        public void release(int slotId) {
            AsyncStreamGroupByAtom.this.release(slotId);
        }
    }

    // The reducing thread and the owner share the lists; a lane only adds to segments the owner has not taken.
    private static class SpanMaps {
        private final ObjList<StreamGroupByMap> freeMaps = new ObjList<>();
        private final ObjList<StreamGroupByMap> maps = new ObjList<>();
        private final LongList openKeys = new LongList();
        private final ObjList<StreamGroupByMap> openMaps = new ObjList<>();
        private final StreamGroupByMap prototype;

        private SpanMaps(StreamGroupByMap prototype) {
            this.prototype = prototype;
        }

        private StreamGroupByMap acquire(CairoConfiguration configuration, MemoryTracker memoryTracker, long key, GroupByFunctionsUpdater updater) {
            final StreamGroupByMap map;
            synchronized (this) {
                final int index = openKeys.indexOf(key);
                if (index > -1) {
                    return openMaps.getQuick(index);
                }
                if (freeMaps.size() > 0) {
                    map = freeMaps.getLast();
                    freeMaps.setPos(freeMaps.size() - 1);
                } else {
                    map = prototype.newInstance(configuration);
                    maps.add(map);
                }
                openKeys.add(key);
                openMaps.add(map);
            }
            if (!map.isOpen()) {
                map.reopen(memoryTracker, updater);
            }
            map.clear(key);
            return map;
        }

        private synchronized Throwable close(Throwable th) {
            for (int i = 0, n = maps.size(); i < n; i++) {
                th = Misc.freeBestEffort(th, maps.getQuick(i));
            }
            openKeys.clear();
            openMaps.clear();
            freeMaps.clear();
            freeMaps.addAll(maps);
            return th;
        }

        private synchronized int getKeyCapacity() {
            int capacity = 0;
            for (int i = 0, n = maps.size(); i < n; i++) {
                capacity = Math.max(capacity, maps.getQuick(i).getKeyCapacity());
            }
            return capacity;
        }

        private synchronized int getShardKeyCapacity() {
            int capacity = 0;
            for (int i = 0, n = maps.size(); i < n; i++) {
                capacity = Math.max(capacity, maps.getQuick(i).getShardKeyCapacity());
            }
            return capacity;
        }

        private synchronized void release(StreamGroupByMap map) {
            freeMaps.add(map);
        }

        private synchronized void releaseAll() {
            freeMaps.addAll(openMaps);
            openKeys.clear();
            openMaps.clear();
        }

        private synchronized StreamGroupByMap take(long key) {
            final int index = openKeys.indexOf(key);
            if (index < 0) {
                return null;
            }
            final StreamGroupByMap map = openMaps.getQuick(index);
            openKeys.removeIndex(index);
            openMaps.remove(index);
            return map;
        }
    }
}
