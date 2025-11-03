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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.DirectMapValue;
import io.questdb.griffin.engine.groupby.DirectMapValueFactory;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.prepareBindVarMemory;

// TODO:
// * Lookup tables assume up to LOOKUP_TABLE_SIZE symbols in master and slave. We should
//   try using hash tables instead and see if it doesn't affect the performance (likely
//   that it won't affect).
//   These are slaveSymbolLookupTable, ownerSlaveTimestamps, ownerSlaveRowIds, ownerSlaveRowLos,
//   ownerColumnSinkPtrs, and the corresponding perWorker* fields
// * On-heap IntObjHashMaps and LongLists should be swapped with off-heap data structs:
//   ownerSlaveTimestamps, ownerSlaveRowIds, ownerSlaveRowLos, ownerColumnSinkPtrs
//   Our options here are the following:
//   * Use on-heap hash tables with DirectLongList, i.e. IntObjHashMap<DirectLongList>;
//     the downsides are some on-heap objects allocated per symbol key (DirectLongList)
//     and more frequent allocations
//   * Implement a specialized off-heap hash table similar to DirectIntIntHashMap to store
//     int key and a few long values. The values would be pointers for GroupByLongList, i.e.
//     GroupByAllocator will be used to accumulate row ids, timestamps, etc., similar to
//     how it's done with column value copies (GroupByColumnSink). The downside is increased
//     off-heap memory consumption, but the upside is less frequent small allocations, thanks
//     to GroupByLongList.
public class AsyncFastWindowJoinAtom implements StatefulAtom, Plannable {
    private static final int INITIAL_COLUMN_SINK_CAPACITY = 64;
    private static final int LOOKUP_TABLE_SIZE = 1024;
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledMasterFilter;
    // TODO: we could use IntList here, just for the sake of consistency
    private final int[] groupByColumnIndexes;
    private final JoinSymbolTableSource joinSymbolTableSource;
    private final long joinWindowHi;
    private final long joinWindowLo;
    private final int masterSymbolIndex;
    private final int masterTimestampIndex;
    private final long masterTsScale;
    private final GroupByAllocator ownerAllocator;
    private final GroupByColumnSink ownerColumnSink;
    private final long[] ownerColumnSinkPtrs;
    // Note: all function updaters should be used through a getFunctionUpdater() call
    // to properly initialize group by functions' allocator.
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final DirectMapValue ownerGroupByValue;
    private final JoinRecord ownerJoinRecord;
    private final Function ownerMasterFilter;
    private final LongList[] ownerSlaveRowIds;
    private final int[] ownerSlaveRowLos;
    private final AsyncTimeFrameHelper ownerSlaveTimeFrameHelper;
    private final LongList[] ownerSlaveTimestamps;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<long[]> perWorkerColumnSinkPtrs;
    private final ObjList<GroupByColumnSink> perWorkerColumnSinks;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<DirectMapValue> perWorkerGroupByValues;
    private final ObjList<JoinRecord> perWorkerJoinRecords;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<Function> perWorkerMasterFilters;
    private final ObjList<LongList[]> perWorkerSlaveRowIds;
    private final ObjList<int[]> perWorkerSlaveRowLos;
    private final ObjList<AsyncTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    private final ObjList<LongList[]> perWorkerSlaveTimestamps;
    private final int slaveSymbolIndex;
    // slave-to-master symbol key LUT
    private final int[] slaveSymbolLookupTable;
    private final long slaveTsScale;
    private final long valueSizeInBytes;
    private final boolean vectorized;

    public AsyncFastWindowJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterSymbolIndex,
            int slaveSymbolIndex,
            long joinWindowLo,
            long joinWindowHi,
            int columnSplit,
            int masterTimestampIndex,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledMasterFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerMasterFilter,
            @Nullable ObjList<Function> perWorkerMasterFilters,
            long masterTsScale,
            long slaveTsScale,
            int workerCount
    ) {
        assert perWorkerMasterFilters == null || perWorkerMasterFilters.size() == workerCount;

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.masterSymbolIndex = masterSymbolIndex;
            this.slaveSymbolIndex = slaveSymbolIndex;
            this.joinWindowLo = joinWindowLo;
            this.joinWindowHi = joinWindowHi;
            this.masterTimestampIndex = masterTimestampIndex;
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;
            this.compiledMasterFilter = compiledMasterFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
            this.ownerMasterFilter = ownerMasterFilter;
            this.perWorkerMasterFilters = perWorkerMasterFilters;
            this.joinSymbolTableSource = new JoinSymbolTableSource(columnSplit);
            this.masterTsScale = masterTsScale;
            this.slaveTsScale = slaveTsScale;

            this.slaveSymbolLookupTable = new int[LOOKUP_TABLE_SIZE];
            this.ownerSlaveRowIds = new LongList[LOOKUP_TABLE_SIZE];
            this.ownerSlaveTimestamps = new LongList[LOOKUP_TABLE_SIZE];
            this.perWorkerSlaveRowIds = new ObjList<>(slotCount);
            this.perWorkerSlaveTimestamps = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveRowIds.extendAndSet(i, new LongList[LOOKUP_TABLE_SIZE]);
                perWorkerSlaveTimestamps.extendAndSet(i, new LongList[LOOKUP_TABLE_SIZE]);
            }

            this.ownerSlaveRowLos = new int[LOOKUP_TABLE_SIZE];
            this.perWorkerSlaveRowLos = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveRowLos.extendAndSet(i, new int[LOOKUP_TABLE_SIZE]);
            }

            this.ownerSlaveTimeFrameHelper = new AsyncTimeFrameHelper(slaveFactory.newTimeFrameCursor(), configuration.getSqlAsOfJoinLookAhead(), slaveTsScale);
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveTimeFrameHelpers.extendAndSet(i, new AsyncTimeFrameHelper(slaveFactory.newTimeFrameCursor(), configuration.getSqlAsOfJoinLookAhead(), slaveTsScale));
            }

            this.ownerJoinRecord = new JoinRecord(columnSplit);
            this.perWorkerJoinRecords = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerJoinRecords.extendAndSet(i, new JoinRecord(columnSplit));
            }

            final Class<GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            this.ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            if (perWorkerGroupByFunctions != null) {
                this.perWorkerFunctionUpdaters = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.extendAndSet(i, GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                this.perWorkerFunctionUpdaters = null;
            }

            this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            this.ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            // Make sure to set worker-local allocator for the group by functions.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            this.perWorkerAllocators = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                GroupByAllocator workerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                perWorkerAllocators.extendAndSet(i, workerAllocator);
                if (perWorkerGroupByFunctions != null) {
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), workerAllocator);
                }
            }

            ownerGroupByValue = DirectMapValueFactory.createDirectMapValue(valueTypes);
            valueSizeInBytes = ownerGroupByValue.getSizeInBytes();
            perWorkerGroupByValues = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerGroupByValues.extendAndSet(i, DirectMapValueFactory.createDirectMapValue(valueTypes));
            }

            // TODO: validate that all group by function support batch computation and that they
            //  have slave table's columns as arguments; if that's not the case, we should not use
            //  vectorized reducer
            this.vectorized = GroupByUtils.isBatchComputationSupported(ownerGroupByFunctions);
            if (vectorized) {
                // TODO: deduplicate columns we have to copy, i.e. for min(int_col), max(int_col),
                //  we should do a single copy of the int_col
                this.groupByColumnIndexes = new int[ownerGroupByFunctions.size()];
                for (int i = 0, n = groupByColumnIndexes.length; i < n; i++) {
                    groupByColumnIndexes[i] = ownerGroupByFunctions.getQuick(i).getColumnIndex();
                }
                this.ownerColumnSink = new GroupByColumnSink(INITIAL_COLUMN_SINK_CAPACITY);
                ownerColumnSink.setAllocator(ownerAllocator);
                this.perWorkerColumnSinks = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    GroupByColumnSink sink = new GroupByColumnSink(INITIAL_COLUMN_SINK_CAPACITY);
                    sink.setAllocator(perWorkerAllocators.getQuick(i));
                    perWorkerColumnSinks.extendAndSet(i, sink);
                }

                this.ownerColumnSinkPtrs = new long[LOOKUP_TABLE_SIZE * ownerGroupByFunctions.size()];
                this.perWorkerColumnSinkPtrs = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerColumnSinkPtrs.extendAndSet(i, new long[LOOKUP_TABLE_SIZE * ownerGroupByFunctions.size()]);
                }
            } else {
                this.groupByColumnIndexes = null;
                this.ownerColumnSink = null;
                this.perWorkerColumnSinks = null;
                this.ownerColumnSinkPtrs = null;
                this.perWorkerColumnSinkPtrs = null;
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.free(ownerAllocator);
        Misc.freeObjListAndKeepObjects(perWorkerAllocators);
    }

    @Override
    public void close() {
        Misc.free(ownerSlaveTimeFrameHelper);
        Misc.freeObjList(perWorkerSlaveTimeFrameHelpers);
        Misc.free(compiledMasterFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(ownerMasterFilter);
        Misc.freeObjList(perWorkerMasterFilters);
        Misc.free(ownerAllocator);
        Misc.freeObjList(perWorkerAllocators);
        Misc.freeObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    public GroupByAllocator getAllocator(int slotId) {
        if (slotId == -1) {
            return ownerAllocator;
        }
        return perWorkerAllocators.getQuick(slotId);
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public GroupByColumnSink getColumnSink(int slotId) {
        if (slotId == -1) {
            return ownerColumnSink;
        }
        return perWorkerColumnSinks.getQuick(slotId);
    }

    public long[] getColumnSinkPtrs(int slotId) {
        if (slotId == -1) {
            return ownerColumnSinkPtrs;
        }
        return perWorkerColumnSinkPtrs.getQuick(slotId);
    }

    public CompiledFilter getCompiledMasterFilter() {
        return compiledMasterFilter;
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public int[] getGroupByColumnIndexes() {
        return groupByColumnIndexes;
    }

    public ObjList<GroupByFunction> getGroupByFunctions(int slotId) {
        if (slotId == -1 || perWorkerGroupByFunctions == null) {
            return ownerGroupByFunctions;
        }
        return perWorkerGroupByFunctions.getQuick(slotId);
    }

    public JoinRecord getJoinRecord(int slotId) {
        if (slotId == -1) {
            return ownerJoinRecord;
        }
        return perWorkerJoinRecords.getQuick(slotId);
    }

    public long getJoinWindowHi() {
        return joinWindowHi;
    }

    public long getJoinWindowLo() {
        return joinWindowLo;
    }

    public DirectMapValue getMapValue(int slotId) {
        if (slotId == -1) {
            return ownerGroupByValue;
        }
        return perWorkerGroupByValues.getQuick(slotId);
    }

    public Function getMasterFilter(int slotId) {
        if (slotId == -1 || perWorkerMasterFilters == null) {
            return ownerMasterFilter;
        }
        return perWorkerMasterFilters.getQuick(slotId);
    }

    public int getMasterSymbolIndex() {
        return masterSymbolIndex;
    }

    public int getMasterTimestampIndex() {
        return masterTimestampIndex;
    }

    public long getMasterTsScale() {
        return masterTsScale;
    }

    // Thread-unsafe, should be used by query owner thread only.
    public DirectMapValue getOwnerGroupByValue() {
        return ownerGroupByValue;
    }

    public LongList[] getSlaveRowIds(int slotId) {
        if (slotId == -1) {
            return ownerSlaveRowIds;
        }
        return perWorkerSlaveRowIds.getQuick(slotId);
    }

    public int[] getSlaveRowLos(int slotId) {
        if (slotId == -1) {
            return ownerSlaveRowLos;
        }
        return perWorkerSlaveRowLos.getQuick(slotId);
    }

    public int getSlaveSymbolIndex() {
        return slaveSymbolIndex;
    }

    public int[] getSlaveSymbolLookupTable() {
        return slaveSymbolLookupTable;
    }

    public AsyncTimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelper;
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
    }

    public LongList[] getSlaveTimestamps(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimestamps;
        }
        return perWorkerSlaveTimestamps.getQuick(slotId);
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    public long getValueSizeBytes() {
        return valueSizeInBytes;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (ownerMasterFilter != null) {
            ownerMasterFilter.init(symbolTableSource, executionContext);
        }

        if (perWorkerMasterFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerMasterFilters, symbolTableSource, executionContext, ownerMasterFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }

    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor pageFrameCursor,
            PageFrameAddressCache frameAddressCache,
            IntList framePartitionIndexes,
            LongList frameRowCounts,
            LongList partitionTimestamps,
            int frameCount
    ) throws SqlException {
        ownerSlaveTimeFrameHelper.of(
                pageFrameCursor,
                frameAddressCache,
                framePartitionIndexes,
                frameRowCounts,
                partitionTimestamps,
                frameCount
        );
        for (int i = 0, n = perWorkerSlaveTimeFrameHelpers.size(); i < n; i++) {
            perWorkerSlaveTimeFrameHelpers.getQuick(i).of(
                    pageFrameCursor,
                    frameAddressCache,
                    framePartitionIndexes,
                    frameRowCounts,
                    partitionTimestamps,
                    frameCount
            );
        }

        // now we can init join filters
        final SymbolTableSource slaveSymbolTableSource = ownerSlaveTimeFrameHelper.getSymbolTableSource();
        joinSymbolTableSource.of(masterSymbolTableSource, slaveSymbolTableSource);

        Function.init(ownerGroupByFunctions, masterSymbolTableSource, executionContext, null);

        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    Function.init(perWorkerGroupByFunctions.getQuick(i), joinSymbolTableSource, executionContext, null);
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        StaticSymbolTable masterSymbolTable = (StaticSymbolTable) masterSymbolTableSource.getSymbolTable(masterSymbolIndex);
        StaticSymbolTable slaveSymbolTable = (StaticSymbolTable) slaveSymbolTableSource.getSymbolTable(slaveSymbolIndex);
        Arrays.fill(slaveSymbolLookupTable, StaticSymbolTable.VALUE_NOT_FOUND);
        for (int masterKey = 0, n = masterSymbolTable.getSymbolCount(); masterKey < n; masterKey++) {
            final CharSequence masterSym = masterSymbolTable.valueOf(masterKey);
            final int slaveKey = slaveSymbolTable.keyOf(masterSym);
            if (slaveKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                slaveSymbolLookupTable[slaveKey + 1] = masterKey;
            }
        }
        if (masterSymbolTable.containsNullValue() && slaveSymbolTable.containsNullValue()) {
            slaveSymbolLookupTable[0] = StaticSymbolTable.VALUE_IS_NULL;
        }
    }

    public boolean isVectorized() {
        return vectorized;
    }

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original functions anytime.
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.attr("window lo");
        if (joinWindowLo == Long.MAX_VALUE) {
            sink.val("unbounded preceding");
        } else if (joinWindowLo == Long.MIN_VALUE) {
            sink.val("unbounded following");
        } else if (joinWindowLo == 0) {
            sink.val("current row");
        } else if (joinWindowLo < 0) {
            sink.val(Math.abs(joinWindowLo)).val(" following");
        } else {
            sink.val(joinWindowLo).val(" preceding");
        }

        sink.attr("window hi");
        if (joinWindowHi == Long.MAX_VALUE) {
            sink.val("unbounded following");
        } else if (joinWindowHi == Long.MIN_VALUE) {
            sink.val("unbounded preceding");
        } else if (joinWindowHi == 0) {
            sink.val("current row");
        } else if (joinWindowHi < 0) {
            sink.val(Math.abs(joinWindowHi)).val(" preceding");
        } else {
            sink.val(joinWindowHi).val(" following");
        }
    }

    public void toTop() {
        ownerSlaveTimeFrameHelper.toTop();
        for (int i = 0, n = perWorkerSlaveTimeFrameHelpers.size(); i < n; i++) {
            perWorkerSlaveTimeFrameHelpers.getQuick(i).toTop();
        }

        GroupByUtils.toTop(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    private static class JoinSymbolTableSource implements SymbolTableSource {
        private final int columnSplit;
        private SymbolTableSource masterSource;
        private SymbolTableSource slaveSource;

        private JoinSymbolTableSource(int columnSplit) {
            this.columnSplit = columnSplit;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterSource.getSymbolTable(columnIndex);
            }
            return slaveSource.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterSource.newSymbolTable(columnIndex);
            }
            return slaveSource.newSymbolTable(columnIndex - columnSplit);
        }

        public void of(SymbolTableSource masterSource, SymbolTableSource slaveSource) {
            this.masterSource = masterSource;
            this.slaveSource = slaveSource;
        }
    }
}
