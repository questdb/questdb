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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapRecordMergeFunction;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.GroupedDistinctFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adaptive physical state for a grouped exact-DISTINCT aggregate.
 *
 * <p>A sample from the first page frames to race through aggregation selects the physical path.
 * Low-cardinality group keys use
 * the flat path: input rows are deduplicated in a key-only map keyed by
 * {@code (group key, distinct argument)} and sharded by the full pair hash. Pair-shard union and
 * collapse are fused: every key newly admitted by the union is reduced immediately into a
 * worker-local output map, without rescanning the merged pair map. Ordinary aggregates, when
 * present, are accumulated from the same page frames. A flat-path marker distinguishes those
 * real states from count-only placeholders created by pair collapse. During merge, the first
 * real state is copied over a placeholder before any ordinary aggregate merge method runs;
 * this is essential because {@code setEmpty()} is not necessarily a merge identity for
 * pointer-backed functions. Samples dominated by unique group keys retain the regular nested
 * aggregate path, whose inline single-value state is substantially cheaper for that shape.</p>
 *
 * <p>Unfiltered raw single-column group keys can use either a compact fixed-width pair layout or
 * the regular map factory and generated record sinks. Filtered input and composite group keys
 * retain the nested aggregate until their flat tuple paths are one-shot competitive; installing
 * them only for repeated executions would make the physical plan depend on execution history.</p>
 */
final class GroupedDistinctContext implements QuietCloseable {
    static final int SAMPLE_BUCKET_COUNT = 8_192;
    static final int SAMPLE_ROW_COUNT = 4_096;
    private static final int FLAT_GROUP_REUSE = 4;
    private static final int MODE_FLAT = 1;
    private static final int MODE_NESTED = 2;
    private static final int MODE_UNDECIDED = 0;
    private static final GroupByFunctionsUpdater PAIR_UPDATER = new GroupByFunctionsUpdater() {
        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            // The pair table has no value columns. A duplicate pair needs no work.
        }

        @Override
        public void setFunctions(ObjList<GroupByFunction> groupByFunctions) {
        }

        @Override
        public void updateEmpty(MapValue value) {
        }

        @Override
        public void updateExisting(MapValue value, Record record, long rowId) {
        }

        @Override
        public void updateNew(MapValue value, Record record, long rowId) {
        }
    };

    private final GroupedDistinctFunction ownerFunction;
    private final boolean specializedPairLayout;
    private final int groupKeyColumnIndex0;
    private final int groupKeyType0;
    private final ObjList<GroupByFunction> ownerOrdinaryFunctions;
    private final PairMergeSink ownerPairMergeSink;
    private final long pairShardingThreshold;
    private final GroupByShardingContext outputShardingCtx;
    private final GroupByShardingContext pairShardingCtx;
    private final ListColumnFilter pairKeyColumnFilter;
    @Nullable
    private final ObjList<GroupedDistinctFunction> perWorkerFunctions;
    @Nullable
    private final ObjList<ObjList<GroupByFunction>> perWorkerOrdinaryFunctions;
    private final ObjList<PairMergeSink> perWorkerPairMergeSinks;
    private final AtomicInteger mode = new AtomicInteger(MODE_UNDECIDED);
    private final AtomicLong unshardedPairCount = new AtomicLong();
    @Nullable
    private RecordSink ownerPairToGroupMapSink;
    @Nullable
    private ObjList<RecordSink> perWorkerPairToGroupMapSinks;

    private GroupedDistinctContext(
            GroupedDistinctFunction ownerFunction,
            boolean specializedPairLayout,
            int groupKeyColumnIndex0,
            int groupKeyType0,
            ObjList<GroupByFunction> ownerOrdinaryFunctions,
            @Nullable ObjList<GroupedDistinctFunction> perWorkerFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerOrdinaryFunctions,
            ListColumnFilter pairKeyColumnFilter,
            long pairShardingThreshold,
            GroupByShardingContext pairShardingCtx,
            GroupByShardingContext outputShardingCtx,
            int workerCount
    ) {
        this.ownerFunction = ownerFunction;
        this.specializedPairLayout = specializedPairLayout;
        this.groupKeyColumnIndex0 = groupKeyColumnIndex0;
        this.groupKeyType0 = groupKeyType0;
        this.ownerOrdinaryFunctions = ownerOrdinaryFunctions;
        this.perWorkerFunctions = perWorkerFunctions;
        this.perWorkerOrdinaryFunctions = perWorkerOrdinaryFunctions;
        this.pairKeyColumnFilter = pairKeyColumnFilter;
        this.pairShardingThreshold = pairShardingThreshold;
        this.pairShardingCtx = pairShardingCtx;
        this.outputShardingCtx = outputShardingCtx;
        this.ownerPairMergeSink = new PairMergeSink(-1);
        this.perWorkerPairMergeSinks = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            perWorkerPairMergeSinks.extendAndSet(i, new PairMergeSink(i));
        }
        pairShardingCtx.setShardMergeReducer(this::preparePairMergeSink);
    }

    @Nullable
    static GroupedDistinctContext tryCreate(
            CairoConfiguration configuration,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter groupKeyColumnFilter,
            ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            ObjList<Function> ownerKeyFunctions,
            PerWorkerLocks perWorkerLocks,
            int workerCount
    ) {
        final int groupKeyColumnCount = keyTypes.getColumnCount();
        if (groupKeyColumnCount != 1
                || groupKeyColumnFilter.getColumnCount() != groupKeyColumnCount
                || ownerKeyFunctions.size() != 0
                || ownerGroupByFunctions.size() == 0) {
            return null;
        }

        int distinctFunctionIndex = -1;
        GroupedDistinctFunction ownerFunction = null;
        for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
            if (ownerGroupByFunctions.getQuick(i) instanceof GroupedDistinctFunction function) {
                if (ownerFunction != null) {
                    // Multiple DISTINCT signatures require separate pair tables.
                    return null;
                }
                distinctFunctionIndex = i;
                ownerFunction = function;
            }
        }
        if (ownerFunction == null
                || !(ownerFunction.getDistinctKeyFunction() instanceof ColumnFunction ownerArg)) {
            return null;
        }

        final ArrayColumnTypes pairKeyTypes = new ArrayColumnTypes().addAll(keyTypes);
        pairKeyTypes.add(ownerFunction.getDistinctKeyType());
        // Keep physical-map eligibility separate from the hand-written sampler/collapse layout.
        // MapFactory may grow additional Unordered16Map shapes without making those shapes safe
        // for the specialized 4-byte group-key access below.
        final boolean specializedPairLayout = isSpecializedPairLayout(pairKeyTypes);

        final ObjList<GroupByFunction> ownerOrdinaryFunctions = ordinaryFunctions(
                ownerGroupByFunctions,
                distinctFunctionIndex
        );
        ObjList<GroupedDistinctFunction> workerFunctions = null;
        ObjList<ObjList<GroupByFunction>> workerOrdinaryFunctions = null;
        if (perWorkerGroupByFunctions != null) {
            workerFunctions = new ObjList<>(workerCount);
            workerOrdinaryFunctions = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                final ObjList<GroupByFunction> functions = perWorkerGroupByFunctions.getQuick(i);
                if (functions.size() != ownerGroupByFunctions.size()
                        || !(functions.getQuick(distinctFunctionIndex) instanceof GroupedDistinctFunction workerFunction)
                        || !(workerFunction.getDistinctKeyFunction() instanceof ColumnFunction workerArg)
                        || workerArg.getColumnIndex() != ownerArg.getColumnIndex()
                        || workerFunction.getDistinctKeyType() != ownerFunction.getDistinctKeyType()) {
                    return null;
                }
                for (int j = 0, n = functions.size(); j < n; j++) {
                    if (j != distinctFunctionIndex && functions.getQuick(j) instanceof GroupedDistinctFunction) {
                        return null;
                    }
                }
                workerFunctions.extendAndSet(i, workerFunction);
                workerOrdinaryFunctions.extendAndSet(i, ordinaryFunctions(functions, distinctFunctionIndex));
            }
        }

        final ListColumnFilter pairKeyColumnFilter = groupKeyColumnFilter.copy();
        pairKeyColumnFilter.add(ownerArg.getColumnIndex() + 1);

        GroupByShardingContext pairCtx = null;
        GroupByShardingContext outputCtx = null;
        try {
            pairCtx = new GroupByShardingContext(
                    configuration,
                    pairKeyTypes,
                    new ArrayColumnTypes(),
                    PAIR_UPDATER,
                    null,
                    perWorkerLocks,
                    workerCount,
                    true
            );

            final ObjList<GroupByFunctionsUpdater> workerOutputUpdaters;
            if (workerFunctions != null) {
                workerOutputUpdaters = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    workerOutputUpdaters.extendAndSet(i, new OutputUpdater(
                            workerFunctions.getQuick(i),
                            workerOrdinaryFunctions.getQuick(i)
                    ));
                }
            } else {
                workerOutputUpdaters = null;
            }
            outputCtx = new GroupByShardingContext(
                    configuration,
                    new ArrayColumnTypes().addAll(keyTypes),
                    new ArrayColumnTypes().addAll(valueTypes),
                    new OutputUpdater(ownerFunction, ownerOrdinaryFunctions),
                    workerOutputUpdaters,
                    perWorkerLocks,
                    workerCount
            );
            return new GroupedDistinctContext(
                    ownerFunction,
                    specializedPairLayout,
                    groupKeyColumnFilter.getColumnIndexFactored(0),
                    keyTypes.getColumnType(0),
                    ownerOrdinaryFunctions,
                    workerFunctions,
                    workerOrdinaryFunctions,
                    pairKeyColumnFilter,
                    configuration.getGroupByShardingThreshold(),
                    pairCtx,
                    outputCtx,
                    workerCount
            );
        } catch (Throwable th) {
            Misc.free(pairCtx);
            Misc.free(outputCtx);
            throw th;
        }
    }

    void clear() {
        unshardedPairCount.set(0);
        pairShardingCtx.clear();
        outputShardingCtx.clear();
    }

    @Override
    public void close() {
        pairShardingCtx.setShardMergeReducer(null);
        Misc.free(pairShardingCtx);
        Misc.free(outputShardingCtx);
    }

    ObjList<Map> getDestShards() {
        return outputShardingCtx.getDestShards();
    }

    ListColumnFilter getPairKeyColumnFilter() {
        return pairKeyColumnFilter;
    }

    GroupByMapFragment getPairFragment(int slotId) {
        return pairShardingCtx.getFragment(slotId);
    }

    GroupByMapFragment getOutputFragment(int slotId) {
        return outputShardingCtx.getFragment(slotId);
    }

    long getSampleGroupHash(Record record, GroupByMapFragment fragment, RecordSink mapSink) {
        if (!specializedPairLayout) {
            // Reuse the regular group map's uncommitted key area as per-slot scratch. commit/hash
            // does not insert or advance the map, so a nested selection immediately reuses the
            // same map without an extra allocation. The generated sink makes this path follow the
            // complete raw single-column group-key schema, including variable-width keys.
            final MapKey key = fragment.reopenMap().withKey();
            mapSink.copy(record, key);
            key.commit();
            return key.hash();
        }
        final int key0 = ColumnType.tagOf(groupKeyType0) == ColumnType.IPv4
                ? record.getIPv4(groupKeyColumnIndex0)
                : record.getInt(groupKeyColumnIndex0);
        // Preserve the original signed int-to-long conversion and therefore the existing
        // adaptive-mode sampling distribution.
        return Hash.hashInt64(key0);
    }

    ObjList<GroupByFunction> getOutputFunctions(int slotId) {
        if (slotId == -1 || perWorkerOrdinaryFunctions == null) {
            return ownerOrdinaryFunctions;
        }
        return perWorkerOrdinaryFunctions.getQuick(slotId);
    }

    GroupByFunctionsUpdater getOutputUpdater(int slotId) {
        return outputShardingCtx.getFunctionUpdater(slotId);
    }

    long getUnshardedPairCount() {
        return unshardedPairCount.get();
    }

    boolean hasOrdinaryFunctions() {
        return ownerOrdinaryFunctions.size() > 0;
    }

    boolean isFlat() {
        return mode.get() == MODE_FLAT;
    }

    boolean isModeUndecided() {
        return mode.get() == MODE_UNDECIDED;
    }

    boolean isPairSharded() {
        return pairShardingCtx.isSharded();
    }

    Map mergeOwnerMap() {
        // The owner map is the unsharded merge destination, so its pre-existing unique pairs do
        // not pass through Map.merge(). Reduce that one fragment first; newly admitted worker
        // pairs are then reduced by the merge callback.
        final PairMergeSink ownerSink = preparePairMergeSink(-1, 0);
        final Map ownerPairMap = pairShardingCtx.getFragment(-1).reopenMap();
        try (MapRecordCursor cursor = ownerPairMap.getCursor()) {
            final MapRecord record = cursor.getRecord();
            while (cursor.hasNext()) {
                ownerSink.mergeNew(record);
            }
        }

        final Map pairMap = pairShardingCtx.mergeOwnerMap();
        pairMap.close();

        final Map outputMap = outputShardingCtx.mergeOwnerMap();
        validateOutputMap(0, outputMap);
        return outputMap;
    }

    ObjList<Map> mergeShards(
            MessageBus messageBus,
            WorkStealingStrategy workStealingStrategy,
            SqlExecutionCircuitBreaker circuitBreaker,
            AtomicBooleanCircuitBreaker postAggregationCircuitBreaker,
            SOUnboundedCountDownLatch postAggregationDoneLatch,
            AtomicInteger postAggregationStartedCounter
    ) {
        pairShardingCtx.mergeShards(
                messageBus,
                workStealingStrategy,
                circuitBreaker,
                postAggregationCircuitBreaker,
                postAggregationDoneLatch,
                postAggregationStartedCounter
        );
        if (postAggregationCircuitBreaker.checkIfTripped()) {
            return outputShardingCtx.getDestShards();
        }

        // Pair destination shards are no longer observable once their unique rows have been
        // folded into count/output fragments. Release them before allocating final group shards.
        pairShardingCtx.closeDestShards();
        outputShardingCtx.forceSharded();
        final ObjList<Map> outputShards = outputShardingCtx.mergeShards(
                messageBus,
                workStealingStrategy,
                circuitBreaker,
                postAggregationCircuitBreaker,
                postAggregationDoneLatch,
                postAggregationStartedCounter
        );
        if (!postAggregationCircuitBreaker.checkIfTripped() && hasOrdinaryFunctions()) {
            for (int shardIndex = 0; shardIndex < outputShards.size(); shardIndex++) {
                validateOutputMap(shardIndex, outputShards.getQuick(shardIndex));
            }
        }
        return outputShards;
    }

    void reopen() {
        // Execution history is not a workload property. Re-sample every eligible execution so a
        // one-shot query gets the same physical-path decision as a cached factory.
        mode.set(MODE_UNDECIDED);
        unshardedPairCount.set(0);
        // Input always starts with a single small output fragment so ordinary aggregates can
        // retain the batched probe path. The output context is sharded only after pair collapse.
        pairShardingCtx.reopen();
    }

    int selectMode(int occupiedGroupBucketCount, int rowCount) {
        // The fixed-size bitmap is an allocation-bounded cardinality estimate. Require at
        // least four sampled rows per occupied bucket before paying for a global pair table;
        // otherwise the nested implementation's inline first value is cheaper. Hash collisions
        // bias the estimate toward flat, so keep twice as many buckets as sampled rows.
        final int candidate = rowCount > 0
                && (long) occupiedGroupBucketCount * FLAT_GROUP_REUSE <= rowCount
                ? MODE_FLAT
                : MODE_NESTED;
        mode.compareAndSet(MODE_UNDECIDED, candidate);
        return mode.get();
    }

    void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        pairShardingCtx.setMemoryTracker(memoryTracker);
        outputShardingCtx.setMemoryTracker(memoryTracker);
    }

    boolean maybeShardPairFragment(GroupByMapFragment fragment, long pairCountIncrement) {
        assert pairCountIncrement >= 0;
        // Count physical pairs across all worker fragments. Callers report deltas only at batch
        // boundaries, so crossing the threshold does not split a vectorized probe batch between
        // the flat map and its shards. A worker that observes another worker's transition shards
        // its own fragment before processing its next batch.
        if (!pairShardingCtx.isSharded()
                && pairCountIncrement > 0
                && unshardedPairCount.addAndGet(pairCountIncrement) > pairShardingThreshold) {
            pairShardingCtx.forceSharded();
        }
        if (pairShardingCtx.isSharded()) {
            fragment.shard();
            return true;
        }
        return false;
    }

    boolean needsPairToGroupMapSink() {
        return !specializedPairLayout;
    }

    void setPairToGroupMapSinks(RecordSink ownerSink, ObjList<RecordSink> perWorkerSinks) {
        assert !specializedPairLayout;
        this.ownerPairToGroupMapSink = ownerSink;
        this.perWorkerPairToGroupMapSinks = perWorkerSinks;
    }

    private GroupedDistinctFunction getFunction(int slotId) {
        return slotId == -1 || perWorkerFunctions == null
                ? ownerFunction
                : perWorkerFunctions.getQuick(slotId);
    }

    private RecordSink getPairToGroupMapSink(int slotId) {
        assert ownerPairToGroupMapSink != null;
        return slotId == -1 || perWorkerPairToGroupMapSinks == null
                ? ownerPairToGroupMapSink
                : perWorkerPairToGroupMapSinks.getQuick(slotId);
    }

    private PairMergeSink preparePairMergeSink(int slotId, int shardIndex) {
        final PairMergeSink sink = slotId == -1
                ? ownerPairMergeSink
                : perWorkerPairMergeSinks.getQuick(slotId);
        sink.prepare();
        return sink;
    }

    private static boolean isSpecializedPairLayout(ArrayColumnTypes pairKeyTypes) {
        final int keyCount = pairKeyTypes.getColumnCount();
        return keyCount == 2
                && Unordered4Map.isSupportedKeyType(pairKeyTypes.getColumnType(0))
                && Unordered8Map.isSupportedKeyType(pairKeyTypes.getColumnType(1));
    }

    private void validateOutputMap(int shardIndex, Map outputMap) {
        if (!hasOrdinaryFunctions()) {
            return;
        }
        try (MapRecordCursor cursor = outputMap.getCursor()) {
            final MapRecord record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (!ownerFunction.isGroupedDistinctStatePresent(record.getValue())) {
                    // Every pair came from an input row that was also reduced into an ordinary
                    // state. Failing the query is safer than exposing untouched value slots.
                    throw CairoException.nonCritical()
                            .put("grouped distinct ordinary state is missing [shard=")
                            .put(shardIndex)
                            .put(']');
                }
            }
        }
    }

    private static ObjList<GroupByFunction> ordinaryFunctions(
            ObjList<GroupByFunction> functions,
            int distinctFunctionIndex
    ) {
        final ObjList<GroupByFunction> ordinaryFunctions = new ObjList<>(functions.size() - 1);
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (i != distinctFunctionIndex) {
                ordinaryFunctions.add(functions.getQuick(i));
            }
        }
        return ordinaryFunctions;
    }

    private final class PairMergeSink implements MapRecordMergeFunction {
        private final GroupedDistinctFunction function;
        private final GroupByMapFragment outputFragment;
        private final int slotId;
        @Nullable
        private RecordSink pairToGroupMapSink;
        private Map targetMap;

        private PairMergeSink(int slotId) {
            this.slotId = slotId;
            this.function = getFunction(slotId);
            this.outputFragment = outputShardingCtx.getFragment(slotId);
        }

        @Override
        public void mergeNew(MapRecord record) {
            reduce(record);
        }

        @Override
        public void mergeNewBatch(MapRecord record, long[] rowIds, int size) {
            for (int i = 0; i < size; i++) {
                record.of(rowIds[i]);
                reduce(record);
            }
        }

        private void reduce(MapRecord record) {
            final MapKey targetKey = targetMap.withKey();
            final int distinctKeyColumnIndex;
            if (specializedPairLayout) {
                targetKey.putInt(record.getInt(0));
                distinctKeyColumnIndex = 1;
            } else {
                assert pairToGroupMapSink != null;
                pairToGroupMapSink.copy(record, targetKey);
                distinctKeyColumnIndex = 1;
            }

            final MapValue targetValue = targetKey.createValue();
            initialize(targetValue);
            if (!function.isDistinctKeyNull(record, distinctKeyColumnIndex)) {
                function.incrementDistinctValue(targetValue);
            }
        }

        private void initialize(MapValue targetValue) {
            if (targetValue.isNew()) {
                // Initialize only the DISTINCT slots. Ordinary slots deliberately remain
                // untouched until a real row-derived state is copied over this placeholder.
                function.setEmpty(targetValue);
                if (hasOrdinaryFunctions()) {
                    function.setGroupedDistinctStatePresent(targetValue, false);
                }
            }
        }

        private void prepare() {
            targetMap = outputFragment.reopenMap();
            pairToGroupMapSink = specializedPairLayout ? null : getPairToGroupMapSink(slotId);
        }
    }

    private static final class OutputUpdater implements GroupByFunctionsUpdater {
        private final GroupedDistinctFunction function;
        private final ObjList<GroupByFunction> ordinaryFunctions;

        private OutputUpdater(GroupedDistinctFunction function, ObjList<GroupByFunction> ordinaryFunctions) {
            this.function = function;
            this.ordinaryFunctions = ordinaryFunctions;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            if (ordinaryFunctions.size() > 0) {
                final boolean srcStatePresent = function.isGroupedDistinctStatePresent(srcValue);
                final boolean destStatePresent = function.isGroupedDistinctStatePresent(destValue);
                if (srcStatePresent && !destStatePresent) {
                    // Map.merge() raw-copies a new key's first value. If that happened to be a
                    // count-only placeholder, replace it wholesale with the first real state;
                    // raw copy is the same ownership model used by Map.merge() itself and is safe
                    // for allocator-backed pointers. Preserve the placeholder's distinct count.
                    final long distinctValue = function.getDistinctValue(destValue);
                    destValue.copyFrom(srcValue);
                    function.mergeDistinctValue(destValue, distinctValue);
                    return;
                }
                if (!srcStatePresent) {
                    function.mergeDistinctValue(destValue, srcValue);
                    return;
                }
            }
            for (int i = 0, n = ordinaryFunctions.size(); i < n; i++) {
                ordinaryFunctions.getQuick(i).merge(destValue, srcValue);
            }
            function.mergeDistinctValue(destValue, srcValue);
        }

        @Override
        public void setFunctions(ObjList<GroupByFunction> groupByFunctions) {
        }

        @Override
        public void updateEmpty(MapValue value) {
            for (int i = 0, n = ordinaryFunctions.size(); i < n; i++) {
                ordinaryFunctions.getQuick(i).setEmpty(value);
            }
            function.setEmpty(value);
            if (ordinaryFunctions.size() > 0) {
                function.setGroupedDistinctStatePresent(value, true);
            }
        }

        @Override
        public void updateExisting(MapValue value, Record record, long rowId) {
            for (int i = 0, n = ordinaryFunctions.size(); i < n; i++) {
                ordinaryFunctions.getQuick(i).computeNext(value, record, rowId);
            }
        }

        @Override
        public void updateNew(MapValue value, Record record, long rowId) {
            function.setEmpty(value);
            for (int i = 0, n = ordinaryFunctions.size(); i < n; i++) {
                ordinaryFunctions.getQuick(i).computeFirst(value, record, rowId);
            }
            if (ordinaryFunctions.size() > 0) {
                function.setGroupedDistinctStatePresent(value, true);
            }
        }
    }
}
