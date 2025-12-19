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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.cairo.vm.api.MemoryCARW;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.prepareBindVarMemory;

/**
 * Atom that manages per-worker resources for parallel markout query execution.
 * <p>
 * This class holds:
 * 1. Per-worker slave cursors for ASOF JOIN lookups
 * 2. Per-worker aggregation maps and group by functions
 * 3. Per-worker ASOF join maps for symbol -> rowId mappings
 */
public class AsyncMarkoutGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledFilter;
    private final RecordSink groupByKeyCopier;
    private final RecordSink masterKeyCopier;
    private final int masterTimestampColumnIndex;
    private final Function ownerFilter;
    private final ObjList<Function> perWorkerFilters;
    private final GroupByAllocator ownerAllocator;
    private final Map ownerAsofJoinMap;
    private final CombinedRecord ownerCombinedRecord;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final Map ownerMap;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<Map> perWorkerAsofJoinMaps;
    private final ObjList<CombinedRecord> perWorkerCombinedRecords;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<Map> perWorkerMaps;
    private final ObjList<RecordCursor> perWorkerSlaveCursors;
    private final int sequenceColumnIndex;
    private final LongList sequenceOffsetValues = new LongList();
    private final RecordCursorFactory slaveFactory;
    private final RecordSink slaveKeyCopier;
    private final int slaveTimestampIndex;
    private RecordCursor ownerSlaveCursor;
    private long sequenceRowCount;
    private SqlExecutionContext storedExecutionContext;

    public AsyncMarkoutGroupByAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            int slaveTimestampIndex,
            @NotNull RecordSink groupByKeyCopier,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndices,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert slaveFactory.supportsTimeFrameCursor();
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.slaveFactory = slaveFactory;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.sequenceColumnIndex = sequenceColumnIndex;

            // Filter resources
            this.compiledFilter = compiledFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
            this.ownerFilter = ownerFilter;
            this.perWorkerFilters = perWorkerFilters;

            // ASOF join lookup resources
            this.masterKeyCopier = masterKeyCopier;
            this.slaveKeyCopier = slaveKeyCopier;
            this.slaveTimestampIndex = slaveTimestampIndex;

            // GROUP BY key copier and column mappings for CombinedRecord
            this.groupByKeyCopier = groupByKeyCopier;

            // Per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            // Per-worker slave cursors (populated lazily)
            this.perWorkerSlaveCursors = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveCursors.add(null);
            }

            // Per-worker ASOF maps
            this.perWorkerAsofJoinMaps = new ObjList<>(slotCount);
            if (asOfJoinKeyTypes != null) {
                ArrayColumnTypes asofValueTypes = new ArrayColumnTypes();
                asofValueTypes.add(io.questdb.cairo.ColumnType.LONG);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerAsofJoinMaps.add(MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asofValueTypes));
                }
                this.ownerAsofJoinMap = MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asofValueTypes);
            } else {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerAsofJoinMaps.add(null);
                }
                this.ownerAsofJoinMap = null;
            }

            // Per-worker aggregation maps
            this.perWorkerMaps = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMaps.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes));
            }
            this.ownerMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);

            // Group by functions and updaters
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

            final Class<GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            this.ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            this.perWorkerFunctionUpdaters = new ObjList<>(slotCount);
            if (perWorkerGroupByFunctions != null) {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.add(GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.add(ownerFunctionUpdater);
                }
            }

            // Allocators
            this.ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            this.perWorkerAllocators = new ObjList<>(slotCount);
            if (perWorkerGroupByFunctions != null) {
                for (int i = 0; i < slotCount; i++) {
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    perWorkerAllocators.add(allocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), allocator);
                }
            } else {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerAllocators.add(null);
                }
            }

            // Per-worker combined records
            this.ownerCombinedRecord = new CombinedRecord();
            this.ownerCombinedRecord.init(columnSources, columnIndices);
            this.perWorkerCombinedRecords = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                CombinedRecord record = new CombinedRecord();
                record.init(columnSources, columnIndices);
                perWorkerCombinedRecords.add(record);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        // Clear sequence data
        sequenceOffsetValues.clear();
        sequenceRowCount = 0;

        // Clear aggregation resources
        Misc.free(ownerMap);
        Misc.freeObjListAndKeepObjects(perWorkerMaps);
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.free(ownerAllocator);
        Misc.freeObjListAndKeepObjects(perWorkerAllocators);

        // Clear ASOF join maps
        if (ownerAsofJoinMap != null) {
            Misc.free(ownerAsofJoinMap);
        }
        Misc.freeObjListAndKeepObjects(perWorkerAsofJoinMaps);

        // Close slave cursors (but NOT the factory)
        if (ownerSlaveCursor != null) {
            ownerSlaveCursor.close();
            ownerSlaveCursor = null;
        }
        for (int i = 0, n = perWorkerSlaveCursors.size(); i < n; i++) {
            RecordCursor cursor = perWorkerSlaveCursors.getQuick(i);
            if (cursor != null) {
                cursor.close();
                perWorkerSlaveCursors.setQuick(i, null);
            }
        }

        storedExecutionContext = null;
    }

    @Override
    public void close() {
        Misc.free(ownerMap);
        Misc.freeObjList(perWorkerMaps);
        Misc.free(ownerAllocator);
        Misc.freeObjList(perWorkerAllocators);
        Misc.freeObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.free(ownerAsofJoinMap);
        Misc.freeObjList(perWorkerAsofJoinMaps);
        for (int i = 0, n = perWorkerSlaveCursors.size(); i < n; i++) {
            Misc.free(perWorkerSlaveCursors.getQuick(i));
        }
        perWorkerSlaveCursors.clear();
        Misc.free(ownerSlaveCursor);
        Misc.free(slaveFactory);
        // Filter resources
        Misc.free(compiledFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(ownerFilter);
        Misc.freeObjList(perWorkerFilters);
    }

    public Map getAsofJoinMap(int slotId) {
        Map map;
        if (slotId == -1) {
            map = ownerAsofJoinMap;
        } else {
            map = perWorkerAsofJoinMaps.getQuick(slotId);
        }
        if (map != null && !map.isOpen()) {
            map.reopen();
        }
        return map;
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public CombinedRecord getCombinedRecord(int slotId) {
        if (slotId == -1) {
            return ownerCombinedRecord;
        }
        return perWorkerCombinedRecords.getQuick(slotId);
    }

    public CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    public Function getFilter(int slotId) {
        if (slotId == -1 || perWorkerFilters == null) {
            return ownerFilter;
        }
        return perWorkerFilters.getQuick(slotId);
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public RecordSink getGroupByKeyCopier() {
        return groupByKeyCopier;
    }

    public Map getMap(int slotId) {
        Map map;
        if (slotId == -1) {
            map = ownerMap;
        } else {
            map = perWorkerMaps.getQuick(slotId);
        }
        if (!map.isOpen()) {
            map.reopen();
        }
        return map;
    }

    public RecordSink getMasterKeyCopier() {
        return masterKeyCopier;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    public ObjList<GroupByFunction> getOwnerGroupByFunctions() {
        return ownerGroupByFunctions;
    }

    /**
     * Get the pre-computed sequence offset value at the given index.
     */
    public long getSequenceOffsetValue(int index) {
        return sequenceOffsetValues.getQuick(index);
    }

    public long getSequenceRowCount() {
        return sequenceRowCount;
    }

    public RecordCursorFactory getSlaveFactory() {
        return slaveFactory;
    }

    public RecordCursor getSlaveCursor(int slotId) throws SqlException {
        if (slotId == -1) {
            if (ownerSlaveCursor == null) {
                ownerSlaveCursor = slaveFactory.getCursor(storedExecutionContext);
            }
            return ownerSlaveCursor;
        }
        RecordCursor cursor = perWorkerSlaveCursors.getQuick(slotId);
        if (cursor == null) {
            cursor = slaveFactory.getCursor(storedExecutionContext);
            perWorkerSlaveCursors.setQuick(slotId, cursor);
        }
        return cursor;
    }

    public RecordSink getSlaveKeyCopier() {
        return slaveKeyCopier;
    }

    public int getSlaveTimestampIndex() {
        return slaveTimestampIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        // Initialize filter functions
        if (ownerFilter != null) {
            ownerFilter.init(symbolTableSource, executionContext);
        }

        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext, ownerFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        // Initialize bind variables for compiled filter
        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }

        // Initialize group by functions
        for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
            ownerGroupByFunctions.getQuick(i).init(symbolTableSource, executionContext);
        }
        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    ObjList<GroupByFunction> functions = perWorkerGroupByFunctions.getQuick(i);
                    for (int j = 0, m = functions.size(); j < m; j++) {
                        functions.getQuick(j).init(symbolTableSource, executionContext);
                    }
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        // Store execution context for lazy cursor creation
        this.storedExecutionContext = executionContext;
    }

    /**
     * Materialize the sequence cursor (offset records) and cache offset values.
     * Must be called once before dispatching tasks.
     */
    public void materializeSequenceCursor(RecordCursor sequenceCursor, SqlExecutionCircuitBreaker circuitBreaker) {
        sequenceOffsetValues.clear();

        Record sequenceRecord = sequenceCursor.getRecord();
        while (sequenceCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            long offsetValue = sequenceRecord.getLong(sequenceColumnIndex);
            sequenceOffsetValues.add(offsetValue);
        }
        sequenceRowCount = sequenceOffsetValues.size();
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    /**
     * Merge all per-worker maps into the owner map.
     */
    public Map mergeOwnerMap() {
        Map destMap = ownerMap;
        if (!destMap.isOpen()) {
            destMap.reopen();
        }

        for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
            Map srcMap = perWorkerMaps.getQuick(i);
            if (srcMap.isOpen() && srcMap.size() > 0) {
                destMap.merge(srcMap, ownerFunctionUpdater);
                srcMap.close();
            }
        }

        return destMap;
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        // Maps will be opened lazily by worker threads
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("AsyncMarkoutGroupByAtom");
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }
}
