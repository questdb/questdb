/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| |_) |
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
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NullRecord;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.GroupingFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.NullingRecord;
import io.questdb.griffin.engine.groupby.VirtualFunctionSkewedSymbolRecordCursor;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * Asynchronous row-wise GROUPING SETS aggregation. Each page-frame worker
 * updates one private map per grouping set, then the owner merges partial maps
 * and emits the maps in grouping-set order.
 */
public class AsyncGroupingSetsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer AGGREGATE = AsyncGroupingSetsRecordCursorFactory::aggregate;
    private RecordCursorFactory base;
    private AsyncGroupingSetsCursor cursor;
    private UnorderedPageFrameSequence<AsyncGroupingSetsAtom> frameSequence;
    private ObjList<Function> recordFunctions;
    private final int workerCount;
    private final int keyColumnCount;

    public AsyncGroupingSetsRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata metadata,
            @NotNull ListColumnFilter listColumnFilter,
            @NotNull ArrayColumnTypes keyTypes,
            @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> keyFunctions,
            @NotNull ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @NotNull ObjList<IntList> groupingSets,
            @NotNull IntList keyColumnIndices,
            int workerCount
    ) throws SqlException {
        super(metadata);
        try {
            this.base = base;
            this.recordFunctions = recordFunctions;
            this.workerCount = workerCount;
            this.keyColumnCount = keyColumnIndices.size();
            initGroupingFunctions(groupByFunctions, perWorkerGroupByFunctions, groupingSets, keyColumnIndices);
            final AsyncGroupingSetsAtom atom = new AsyncGroupingSetsAtom(
                    asm,
                    configuration,
                    base.getMetadata(),
                    keyTypes,
                    valueTypes,
                    listColumnFilter,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    keyFunctions,
                    perWorkerKeyFunctions,
                    groupingSets,
                    keyColumnIndices,
                    workerCount
            );
            this.frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    AGGREGATE,
                    workerCount
            );
            this.cursor = new AsyncGroupingSetsCursor(
                    recordFunctions,
                    groupingSets.size(),
                    base.getMetadata().getColumnCount(),
                    keyColumnCount
            );
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        frameSequence.of(base, executionContext, order);
        try {
            cursor.of(frameSequence, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean isNonDeterministic() {
        AsyncGroupingSetsAtom atom = frameSequence.getAtom();
        ObjList<Function> keyFunctions = atom.getOwnerKeyFunctions();
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            if (keyFunctions.getQuick(i).isNonDeterministic()) {
                return true;
            }
        }
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            if (recordFunctions.getQuick(i).isNonDeterministic()) {
                return true;
            }
        }
        return base.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        AsyncGroupingSetsAtom atom = frameSequence.getAtom();
        ObjList<Function> keyFunctions = atom.getOwnerKeyFunctions();
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            if (!keyFunctions.getQuick(i).isStableWithinExecution()) {
                return false;
            }
        }
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            if (!recordFunctions.getQuick(i).isStableWithinExecution()) {
                return false;
            }
        }
        return base.isStableWithinExecution();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Group By");
        sink.meta("workers").val(workerCount);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", frameSequence.getAtom().getGroupByFunctions(-1), true);
        sink.attr("vectorized").val(false);
        sink.attr("groupingSets").val(true);
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

    @Override
    protected void _close() {
        RecordCursorFactory base = this.base;
        this.base = null;
        AsyncGroupingSetsCursor cursor = this.cursor;
        this.cursor = null;
        UnorderedPageFrameSequence<AsyncGroupingSetsAtom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, frameSequence);
        ObjList<Function> recordFunctions = this.recordFunctions;
        this.recordFunctions = null;
        failure = Misc.freeObjListBestEffort(failure, recordFunctions);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static void aggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;
        @SuppressWarnings("unchecked") AsyncGroupingSetsAtom atom =
                ((UnorderedPageFrameSequence<AsyncGroupingSetsAtom>) frameSequence).getAtom();
        boolean owner = stealingFrameSequence == frameSequence;
        int slotId = atom.acquireSlot(workerId, owner, circuitBreaker);
        PageFrameMemoryPool memoryPool = null;
        try {
            memoryPool = atom.getFilterContext().getMemoryPool(slotId);
            PageFrameMemory frameMemory = memoryPool.navigateTo(frameIndex);
            record.init(frameMemory);
            long baseRowId = Rows.toRowID(frameIndex, 0);
            RecordSink recordSink = atom.getRecordSink(slotId);
            RecordSink functionSink = atom.getFunctionSink(slotId);
            GroupByFunctionsUpdater updater = atom.getUpdater(slotId);
            int setCount = atom.getSetCount();
            for (long row = 0; row < frameRowCount; row++) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                record.setRowIndex(row);
                for (int setIndex = 0; setIndex < setCount; setIndex++) {
                    NullingRecord keyRecord = atom.getNullingRecord(slotId, setIndex);
                    keyRecord.of(record, atom.getNulledColumns(setIndex));
                    atom.setCurrentGroupingSet(slotId, setIndex);
                    Map map = atom.getMap(slotId, setIndex);
                    MapKey key = map.withKey();
                    recordSink.copy(keyRecord, key);
                    if (functionSink != null) {
                        functionSink.copy(record, key);
                    }
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        updater.updateNew(value, record, baseRowId + row);
                    } else {
                        updater.updateExisting(value, record, baseRowId + row);
                    }
                }
            }
        } finally {
            try {
                if (memoryPool != null) {
                    memoryPool.releaseParquetBuffers();
                }
            } finally {
                atom.releaseSlot(slotId);
            }
        }
    }

    private static void initGroupingFunctions(
            ObjList<GroupByFunction> ownerFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> workerFunctions,
            ObjList<IntList> groupingSets,
            IntList keyColumnIndices
    ) throws SqlException {
        IntList groupingIds = new IntList(groupingSets.size());
        for (int setIndex = 0, setCount = groupingSets.size(); setIndex < setCount; setIndex++) {
            IntList activeKeys = groupingSets.getQuick(setIndex);
            int mask = 0;
            for (int keyIndex = 0, keyCount = keyColumnIndices.size(); keyIndex < keyCount; keyIndex++) {
                if (!containsValue(activeKeys, keyIndex)) {
                    mask |= 1 << (keyCount - 1 - keyIndex);
                }
            }
            groupingIds.add(mask);
        }
        initGroupingFunctions(ownerFunctions, groupingIds, keyColumnIndices);
        if (workerFunctions != null) {
            for (int i = 0, n = workerFunctions.size(); i < n; i++) {
                initGroupingFunctions(workerFunctions.getQuick(i), groupingIds, keyColumnIndices);
            }
        }
    }

    private static void initGroupingFunctions(
            ObjList<GroupByFunction> functions,
            IntList groupingIds,
            IntList keyColumnIndices
    ) throws SqlException {
        for (int i = 0, n = functions.size(); i < n; i++) {
            GroupByFunction function = functions.getQuick(i);
            if (function instanceof GroupingFunction groupingFunction) {
                groupingFunction.init(groupingIds, keyColumnIndices);
            }
        }
    }

    private static boolean containsValue(IntList list, int value) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == value) {
                return true;
            }
        }
        return false;
    }

    private static class AsyncGroupingSetsCursor extends VirtualFunctionSkewedSymbolRecordCursor {
        private final ObjList<Function> nonGroupByFunctions;
        private final IntList allNullColumns;
        private final int keyColumnCount;
        private final int setCount;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private UnorderedPageFrameSequence<AsyncGroupingSetsAtom> frameSequence;
        private boolean isDataMapBuilt;
        private boolean isOpen;
        private int currentSetIndex;
        private MapRecordCursor mapCursor;

        private AsyncGroupingSetsCursor(ObjList<Function> functions, int setCount, int baseColumnCount, int keyColumnCount) {
            super(functions, false);
            this.nonGroupByFunctions = GroupByUtils.extractNonGroupByFunctions(functions);
            this.keyColumnCount = keyColumnCount;
            this.setCount = setCount;
            this.allNullColumns = new IntList(baseColumnCount);
            for (int i = 0; i < baseColumnCount; i++) {
                allNullColumns.add(i);
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            buildMapConditionally();
            if (mapCursor != null) {
                mapCursor.calculateSize(circuitBreaker, counter);
            }
            AsyncGroupingSetsAtom atom = frameSequence.getAtom();
            for (int i = currentSetIndex + 1; i < setCount; i++) {
                counter.add(atom.getOwnerMap(i).size());
            }
            currentSetIndex = setCount;
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
                    mapCursor = null;
                    isOpen = false;
                    super.close();
                }
            }
        }

        @Override
        public boolean hasNext() {
            buildMapConditionally();
            if (baseCursor != null && baseCursor.hasNext()) {
                return true;
            }
            AsyncGroupingSetsAtom atom = frameSequence.getAtom();
            while (++currentSetIndex < setCount) {
                mapCursor = atom.getOwnerMap(currentSetIndex).getCursor();
                of(mapCursor);
                if (baseCursor.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return isDataMapBuilt ? 1 : 0;
        }

        @Override
        public long size() {
            if (!isDataMapBuilt) {
                return -1;
            }
            long size = 0;
            AsyncGroupingSetsAtom atom = frameSequence.getAtom();
            for (int i = 0; i < setCount; i++) {
                size += atom.getOwnerMap(i).size();
            }
            return size;
        }

        @Override
        public void toTop() {
            buildMapConditionally();
            currentSetIndex = 0;
            mapCursor = frameSequence.getAtom().getOwnerMap(0).getCursor();
            of(mapCursor);
            GroupByUtils.toTop(nonGroupByFunctions);
            frameSequence.getAtom().toTop();
        }

        private void buildMap() {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameSequence.prepareForDispatch();
            AsyncGroupingSetsAtom atom = frameSequence.getAtom();
            atom.getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache(), frameSequence.getMemoryTracker());
            frameSequence.dispatchAndAwait();
            for (int setIndex = 0; setIndex < setCount; setIndex++) {
                Map ownerMap = atom.getOwnerMap(setIndex);
                GroupByFunctionsUpdater updater = atom.getUpdater(-1);
                for (int worker = 0; worker < atom.getWorkerCount(); worker++) {
                    Map workerMap = atom.getMapIfExists(worker, setIndex);
                    if (workerMap != null && workerMap.isOpen()) {
                        ownerMap.merge(workerMap, updater);
                        workerMap.close();
                    }
                }
                if (ownerMap.size() == 0
                        && atom.getNulledColumns(setIndex).size() == keyColumnCount) {
                    NullingRecord nullingRecord = atom.getNullingRecord(-1, setIndex);
                    nullingRecord.of(NullRecord.INSTANCE, allNullColumns);
                    MapKey key = ownerMap.withKey();
                    atom.getRecordSink(-1).copy(nullingRecord, key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        updater.updateEmpty(value);
                        atom.setCurrentGroupingSet(-1, setIndex);
                        ObjList<GroupByFunction> groupByFunctions = atom.getGroupByFunctions(-1);
                        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                            GroupByFunction function = groupByFunctions.getQuick(i);
                            if (function instanceof GroupingFunction groupingFunction) {
                                groupingFunction.computeFirst(value, nullingRecord, 0);
                            }
                        }
                    }
                }
            }
            isDataMapBuilt = true;
            currentSetIndex = 0;
            mapCursor = atom.getOwnerMap(0).getCursor();
            of(mapCursor);
        }

        private void buildMapConditionally() {
            if (!isDataMapBuilt) {
                buildMap();
            }
        }

        private void of(UnorderedPageFrameSequence<AsyncGroupingSetsAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
            this.frameSequence = frameSequence;
            if (!isOpen) {
                isOpen = true;
                frameSequence.getAtom().reopen();
            }
            circuitBreaker = executionContext.getCircuitBreaker();
            Function.init(nonGroupByFunctions, frameSequence.getSymbolTableSource(), executionContext, null);
            isDataMapBuilt = false;
            currentSetIndex = -1;
        }
    }
}
