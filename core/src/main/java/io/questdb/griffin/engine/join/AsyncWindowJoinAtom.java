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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.DirectMapValue;
import io.questdb.griffin.engine.groupby.DirectMapValueFactory;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByLongList;
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

import static io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.prepareBindVarMemory;

public class AsyncWindowJoinAtom implements StatefulAtom, Plannable {
    private static final int INITIAL_COLUMN_SINK_CAPACITY = 64;
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledMasterFilter;
    private final IntList groupByColumnIndexes;
    private final IntList groupByColumnTypes;
    private final IntList groupByFunctionToColumnIndex;
    private final JoinSymbolTableSource joinSymbolTableSource;
    private final long joinWindowHi;
    private final long joinWindowLo;
    private final int masterTimestampIndex;
    private final long masterTsScale;
    private final LongList ownSlaveData;
    private final GroupByAllocator ownerAllocator;
    private final GroupByColumnSink ownerColumnSink;
    // Note: all function updaters should be used through a getFunctionUpdater() call
    // to properly initialize group by functions' allocator.
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final DirectMapValue ownerGroupByValue;
    private final Function ownerJoinFilter;
    private final JoinRecord ownerJoinRecord;
    private final Function ownerMasterFilter;
    private final GroupByLongList ownerRowIds;
    private final AsyncTimeFrameHelper ownerSlaveTimeFrameHelper;
    private final GroupByLongList ownerTimestamps;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<GroupByColumnSink> perWorkerColumnSinks;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<DirectMapValue> perWorkerGroupByValues;
    private final ObjList<Function> perWorkerJoinFilters;
    private final ObjList<JoinRecord> perWorkerJoinRecords;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<Function> perWorkerMasterFilters;
    private final ObjList<GroupByLongList> perWorkerRowIds;
    private final ObjList<LongList> perWorkerSlaveData;
    private final ObjList<AsyncTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    private final ObjList<GroupByLongList> perWorkerTimestamps;
    private final long slaveTsScale;
    private final long valueSizeInBytes;
    private final boolean vectorized;

    public AsyncWindowJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            @Nullable Function ownerJoinFilter,
            @Nullable ObjList<Function> perWorkerJoinFilters,
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
        assert perWorkerJoinFilters == null || perWorkerJoinFilters.size() == workerCount;
        assert perWorkerMasterFilters == null || perWorkerMasterFilters.size() == workerCount;

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.ownerJoinFilter = ownerJoinFilter;
            this.perWorkerJoinFilters = perWorkerJoinFilters;
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
            this.vectorized = ownerJoinFilter == null && GroupByUtils.isBatchComputationSupported(ownerGroupByFunctions, columnSplit);

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
            ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            if (perWorkerGroupByFunctions != null) {
                perWorkerFunctionUpdaters = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.extendAndSet(i, GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                perWorkerFunctionUpdaters = null;
            }

            perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            // Make sure to set worker-local allocator for the group by functions.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);

            perWorkerAllocators = new ObjList<>(slotCount);
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
            final int groupByFunctionSize = ownerGroupByFunctions.size();

            if (vectorized) {
                this.groupByColumnIndexes = new IntList(groupByFunctionSize);
                this.groupByColumnTypes = new IntList(groupByFunctionSize);
                this.groupByFunctionToColumnIndex = new IntList(groupByFunctionSize);
                for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
                    var func = ownerGroupByFunctions.getQuick(i);
                    int index = func.getColumnIndex();
                    int mappedIndex = groupByColumnIndexes.indexOf(index);
                    if (mappedIndex == -1) {
                        groupByColumnIndexes.add(func.getColumnIndex());
                        var unary = (UnaryFunction) func;
                        groupByColumnTypes.add(unary.getArg().getType());
                        groupByFunctionToColumnIndex.add(groupByColumnIndexes.size() - 1);
                    } else {
                        groupByFunctionToColumnIndex.add(mappedIndex);
                    }
                }
            } else {
                this.groupByColumnIndexes = null;
                this.groupByColumnTypes = null;
                this.groupByFunctionToColumnIndex = null;
            }

            int slaveDataLen = vectorized ? this.groupByColumnIndexes.size() + 1 : 2;
            ownSlaveData = new LongList(slaveDataLen, 0);
            ownSlaveData.setPos(slaveDataLen);
            this.ownerTimestamps = new GroupByLongList(16);
            ownerTimestamps.setAllocator(ownerAllocator);
            if (vectorized) {
                this.ownerColumnSink = new GroupByColumnSink(INITIAL_COLUMN_SINK_CAPACITY);
                ownerColumnSink.setAllocator(ownerAllocator);
                this.ownerRowIds = null;
            } else {
                this.ownerRowIds = new GroupByLongList(16);
                ownerRowIds.setAllocator(ownerAllocator);
                this.ownerColumnSink = null;
            }

            if (vectorized) {
                this.perWorkerColumnSinks = new ObjList<>(slotCount);
                this.perWorkerRowIds = null;
            } else {
                this.perWorkerRowIds = new ObjList<>(slotCount);
                this.perWorkerColumnSinks = null;
            }

            this.perWorkerSlaveData = new ObjList<>(slotCount);
            this.perWorkerTimestamps = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                if (perWorkerColumnSinks != null) {
                    GroupByColumnSink sink = new GroupByColumnSink(INITIAL_COLUMN_SINK_CAPACITY);
                    sink.setAllocator(perWorkerAllocators.getQuick(i));
                    perWorkerColumnSinks.extendAndSet(i, sink);
                }

                if (perWorkerRowIds != null) {
                    GroupByLongList rowIds = new GroupByLongList(16);
                    rowIds.setAllocator(perWorkerAllocators.getQuick(i));
                    perWorkerRowIds.extendAndSet(i, rowIds);
                }

                LongList perWorkerSlave = new LongList(slaveDataLen, 0);
                perWorkerSlave.setPos(slaveDataLen);
                perWorkerSlaveData.extendAndSet(i, perWorkerSlave);
                GroupByLongList timestamps = new GroupByLongList(16);
                timestamps.setAllocator(perWorkerAllocators.getQuick(i));
                perWorkerTimestamps.extendAndSet(i, timestamps);
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
        if (ownSlaveData != null) {
            ownSlaveData.fillWithDefault();
        }
        if (perWorkerSlaveData != null) {
            for (int i = 0, n = perWorkerSlaveData.size(); i < n; i++) {
                perWorkerSlaveData.getQuick(i).fillWithDefault();
            }
        }
    }

    @Override
    public void close() {
        Misc.free(ownerSlaveTimeFrameHelper);
        Misc.freeObjList(perWorkerSlaveTimeFrameHelpers);
        Misc.free(ownerJoinFilter);
        Misc.freeObjList(perWorkerJoinFilters);
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

    public CompiledFilter getCompiledMasterFilter() {
        return compiledMasterFilter;
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public IntList getGroupByColumnIndexes() {
        return groupByColumnIndexes;
    }

    public LongList getGroupByColumnSinkPtrs(int slotId) {
        if (slotId == -1) {
            return ownSlaveData;
        }
        return perWorkerSlaveData.getQuick(slotId);
    }

    public IntList getGroupByColumnTags() {
        return groupByColumnTypes;
    }

    public IntList getGroupByFunctionToColumnIndex() {
        return groupByFunctionToColumnIndex;
    }

    public ObjList<GroupByFunction> getGroupByFunctions(int slotId) {
        if (slotId == -1 || perWorkerGroupByFunctions == null) {
            return ownerGroupByFunctions;
        }
        return perWorkerGroupByFunctions.getQuick(slotId);
    }

    public Function getJoinFilter(int slotId) {
        if (slotId == -1 || perWorkerJoinFilters == null) {
            return ownerJoinFilter;
        }
        return perWorkerJoinFilters.getQuick(slotId);
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

    public GroupByLongList getRowIds(int slotId) {
        if (slotId == -1) {
            return ownerRowIds;
        }
        return perWorkerRowIds.getQuick(slotId);
    }

    public AsyncTimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelper;
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    public GroupByLongList getTimestamps(int slotId) {
        if (slotId == -1) {
            return ownerTimestamps;
        }
        return perWorkerTimestamps.getQuick(slotId);
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
        joinSymbolTableSource.of(masterSymbolTableSource, ownerSlaveTimeFrameHelper.getSymbolTableSource());

        if (ownerJoinFilter != null) {
            ownerJoinFilter.init(joinSymbolTableSource, executionContext);
        }

        if (perWorkerJoinFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerJoinFilters, joinSymbolTableSource, executionContext, ownerJoinFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

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
