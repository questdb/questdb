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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.DirectMapValueFactory;
import io.questdb.griffin.engine.groupby.FlyweightMapValue;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.SelectivityStats;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.AsyncFilterUtils.prepareBindVarMemory;

public class AsyncWindowJoinAtom implements StatefulAtom, Reopenable, Plannable {
    private static final int INITIAL_COLUMN_SINK_CAPACITY = 64;
    private static final int INITIAL_LIST_CAPACITY = 16;
    // kept public for tests
    public static boolean GROUP_BY_VALUE_USE_COMPACT_DIRECT_MAP = true;
    protected final ObjList<Function> ownerGroupByFunctionArgs;
    protected final WindowJoinTimeFrameHelper ownerSlaveTimeFrameHelper;
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledMasterFilter;
    private final IntHashSet filterUsedColumnIndexes;
    private final IntList groupByFunctionToColumnIndex;
    private final IntList groupByFunctionTypes;
    private final boolean includePrevailing;
    private final WindowJoinSymbolTableSource joinSymbolTableSource;
    private final long joinWindowHi;
    private final long joinWindowLo;
    private final int masterTimestampIndex;
    private final long masterTsScale;
    private final GroupByColumnSink ownerColumnSink;
    // Used by group by functions. The memory is released only at the end of the query execution.
    private final GroupByAllocator ownerFunctionAllocator;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final FlyweightMapValue ownerGroupByValue;
    private final Function ownerJoinFilter;
    private final JoinRecord ownerJoinRecord;
    // Holds either row ids or column sink pointers.
    private final GroupByLongList ownerLongList;
    private final Function ownerMasterFilter;
    private final SelectivityStats ownerSelectivityStats = new SelectivityStats();
    private final ConcurrentTimeFrameCursor ownerSlaveTimeFrameCursor;
    // Used by additional data structures such as row id and timestamp lists or symbol hash tables.
    // The memory is released between page frame reduce calls.
    private final GroupByAllocator ownerTemporaryAllocator;
    private final GroupByLongList ownerTimestampList;
    private final ObjList<GroupByColumnSink> perWorkerColumnSinks;
    private final ObjList<GroupByAllocator> perWorkerFunctionAllocators;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<Function>> perWorkerGroupByFunctionArgs;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<FlyweightMapValue> perWorkerGroupByValues;
    private final ObjList<Function> perWorkerJoinFilters;
    private final ObjList<JoinRecord> perWorkerJoinRecords;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<GroupByLongList> perWorkerLongLists;
    private final ObjList<Function> perWorkerMasterFilters;
    private final ObjList<SelectivityStats> perWorkerSelectivityStats;
    private final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    private final ObjList<WindowJoinTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    private final ObjList<GroupByAllocator> perWorkerTemporaryAllocators;
    private final ObjList<GroupByLongList> perWorkerTimestampLists;
    private final long slaveTsScale;
    private final long valueSizeInBytes;
    private final boolean vectorized;
    private boolean skipAggregation = false;

    public AsyncWindowJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            @Nullable Function ownerJoinFilter,
            @Nullable ObjList<Function> perWorkerJoinFilters,
            long joinWindowLo,
            long joinWindowHi,
            boolean includePrevailing,
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
            @Nullable IntHashSet filterUsedColumnIndexes,
            boolean vectorized,
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
            this.includePrevailing = includePrevailing;
            this.masterTimestampIndex = masterTimestampIndex;
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;
            this.compiledMasterFilter = compiledMasterFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
            this.ownerMasterFilter = ownerMasterFilter;
            this.perWorkerMasterFilters = perWorkerMasterFilters;
            this.filterUsedColumnIndexes = filterUsedColumnIndexes;
            this.joinSymbolTableSource = new WindowJoinSymbolTableSource(columnSplit);
            this.masterTsScale = masterTsScale;
            this.slaveTsScale = slaveTsScale;
            this.vectorized = vectorized;

            this.ownerSlaveTimeFrameCursor = slaveFactory.newTimeFrameCursor();
            this.ownerSlaveTimeFrameHelper = new WindowJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTsScale);
            this.perWorkerSlaveTimeFrameCursors = new ObjList<>(slotCount);
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveTimeFrameCursors.extendAndSet(i, slaveFactory.newTimeFrameCursor());
                perWorkerSlaveTimeFrameHelpers.extendAndSet(i, new WindowJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTsScale));
            }

            this.ownerJoinRecord = new JoinRecord(columnSplit);
            this.perWorkerJoinRecords = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerJoinRecords.extendAndSet(i, new JoinRecord(columnSplit));
            }

            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
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

            this.ownerFunctionAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            // Make sure to set worker-local allocator for the group by functions.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerFunctionAllocator);
            if (perWorkerGroupByFunctions != null) {
                this.perWorkerFunctionAllocators = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    GroupByAllocator workerFunctionAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                    perWorkerFunctionAllocators.extendAndSet(i, workerFunctionAllocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), workerFunctionAllocator);
                }
            } else {
                this.perWorkerFunctionAllocators = null;
            }

            this.ownerTemporaryAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            this.ownerLongList = new GroupByLongList(INITIAL_LIST_CAPACITY);
            ownerLongList.setAllocator(ownerTemporaryAllocator);
            this.ownerTimestampList = new GroupByLongList(INITIAL_LIST_CAPACITY);
            ownerTimestampList.setAllocator(ownerTemporaryAllocator);
            this.perWorkerTemporaryAllocators = new ObjList<>(slotCount);
            this.perWorkerLongLists = new ObjList<>(slotCount);
            this.perWorkerTimestampLists = new ObjList<>(slotCount);
            perWorkerSelectivityStats = new ObjList<>(slotCount);

            for (int i = 0; i < slotCount; i++) {
                GroupByAllocator workerTemporaryAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                perWorkerTemporaryAllocators.extendAndSet(i, workerTemporaryAllocator);

                final GroupByLongList workerRowIds = new GroupByLongList(INITIAL_LIST_CAPACITY);
                workerRowIds.setAllocator(workerTemporaryAllocator);
                perWorkerLongLists.extendAndSet(i, workerRowIds);
                final GroupByLongList workerTimestamps = new GroupByLongList(INITIAL_LIST_CAPACITY);
                workerTimestamps.setAllocator(workerTemporaryAllocator);
                perWorkerTimestampLists.extendAndSet(i, workerTimestamps);
                perWorkerSelectivityStats.extendAndSet(i, new SelectivityStats());
            }

            ownerGroupByValue = DirectMapValueFactory.createDirectMapValue(valueTypes, GROUP_BY_VALUE_USE_COMPACT_DIRECT_MAP);
            valueSizeInBytes = ownerGroupByValue.getSizeInBytes();
            perWorkerGroupByValues = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerGroupByValues.extendAndSet(i, DirectMapValueFactory.createDirectMapValue(valueTypes, GROUP_BY_VALUE_USE_COMPACT_DIRECT_MAP));
            }

            if (vectorized) {
                final int groupByFunctionSize = ownerGroupByFunctions.size();

                if (perWorkerGroupByFunctions != null) {
                    this.perWorkerGroupByFunctionArgs = new ObjList<>(slotCount);
                    for (int i = 0; i < slotCount; i++) {
                        // we'll initialize the list a bit later, along with the owner one
                        final ObjList<Function> workerFunctionArgs = new ObjList<>(groupByFunctionSize);
                        perWorkerGroupByFunctionArgs.extendAndSet(i, workerFunctionArgs);
                    }
                } else {
                    this.perWorkerGroupByFunctionArgs = null;
                }

                this.groupByFunctionToColumnIndex = new IntList(groupByFunctionSize);
                this.ownerGroupByFunctionArgs = new ObjList<>(groupByFunctionSize);
                this.groupByFunctionTypes = new IntList(groupByFunctionSize);
                for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
                    final var func = ownerGroupByFunctions.getQuick(i);
                    final var funcArg = func.getComputeBatchArg();
                    final var funcArgType = ColumnType.tagOf(func.getComputeBatchArgType());
                    final int index = findFunctionWithSameArg(ownerGroupByFunctionArgs, groupByFunctionTypes, funcArg, funcArgType);
                    if (index < 0) {
                        groupByFunctionTypes.add(funcArgType);
                        ownerGroupByFunctionArgs.add(funcArg);
                        groupByFunctionToColumnIndex.add(groupByFunctionTypes.size() - 1);
                        // don't forget about per-worker lists
                        if (perWorkerGroupByFunctions != null) {
                            for (int j = 0; j < slotCount; j++) {
                                final ObjList<GroupByFunction> workerGroupByFunctions = perWorkerGroupByFunctions.getQuick(j);
                                final var workerFunc = workerGroupByFunctions.getQuick(i);
                                final var workerFuncArg = workerFunc.getComputeBatchArg();
                                perWorkerGroupByFunctionArgs.getQuick(j).add(workerFuncArg);
                            }
                        }
                    } else {
                        groupByFunctionToColumnIndex.add(index);
                        if (perWorkerGroupByFunctions != null) {
                            for (int j = 0; j < slotCount; j++) {
                                perWorkerGroupByFunctionArgs.getQuick(j).add(null);
                            }
                        }
                    }
                }

                this.ownerColumnSink = new GroupByColumnSink(INITIAL_COLUMN_SINK_CAPACITY);
                ownerColumnSink.setAllocator(ownerTemporaryAllocator);
                this.perWorkerColumnSinks = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    final GroupByColumnSink sink = new GroupByColumnSink(INITIAL_COLUMN_SINK_CAPACITY);
                    sink.setAllocator(perWorkerTemporaryAllocators.getQuick(i));
                    perWorkerColumnSinks.extendAndSet(i, sink);
                }
            } else {
                this.ownerColumnSink = null;
                this.perWorkerColumnSinks = null;
                this.groupByFunctionToColumnIndex = null;
                this.ownerGroupByFunctionArgs = null;
                this.perWorkerGroupByFunctionArgs = null;
                this.groupByFunctionTypes = null;
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.free(ownerSlaveTimeFrameCursor);
        Misc.freeObjListAndKeepObjects(perWorkerSlaveTimeFrameCursors);
        Misc.clear(ownerFunctionAllocator);
        Misc.clearObjList(perWorkerFunctionAllocators);
        Misc.clear(ownerTemporaryAllocator);
        Misc.clearObjList(perWorkerTemporaryAllocators);
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }

        ownerSelectivityStats.clear();
        Misc.clearObjList(perWorkerSelectivityStats);
    }

    public void clearTemporaryData(int slotId) {
        if (slotId == -1) {
            ownerTemporaryAllocator.clear();
        } else {
            perWorkerTemporaryAllocators.getQuick(slotId).clear();
        }
    }

    @Override
    public void close() {
        Misc.free(ownerJoinFilter);
        Misc.freeObjList(perWorkerJoinFilters);
        Misc.free(ownerSlaveTimeFrameCursor);
        Misc.freeObjList(perWorkerSlaveTimeFrameCursors);
        Misc.free(compiledMasterFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(ownerMasterFilter);
        Misc.freeObjList(perWorkerMasterFilters);
        Misc.free(ownerFunctionAllocator);
        Misc.freeObjList(perWorkerFunctionAllocators);
        Misc.free(ownerTemporaryAllocator);
        Misc.freeObjList(perWorkerTemporaryAllocators);
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

    public @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public ObjList<Function> getGroupByFunctionArgs(int slotId) {
        if (slotId == -1 || perWorkerGroupByFunctionArgs == null) {
            return ownerGroupByFunctionArgs;
        }
        return perWorkerGroupByFunctionArgs.getQuick(slotId);
    }

    public IntList getGroupByFunctionToColumnIndex() {
        return groupByFunctionToColumnIndex;
    }

    public IntList getGroupByFunctionTypes() {
        return groupByFunctionTypes;
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

    public GroupByLongList getLongList(int slotId) {
        if (slotId == -1) {
            return ownerLongList;
        }
        return perWorkerLongLists.getQuick(slotId);
    }

    public FlyweightMapValue getMapValue(int slotId) {
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
    public FlyweightMapValue getOwnerGroupByValue() {
        return ownerGroupByValue;
    }

    public SelectivityStats getSelectivityStats(int slotId) {
        if (slotId == -1) {
            return ownerSelectivityStats;
        }
        return perWorkerSelectivityStats.getQuick(slotId);
    }

    public WindowJoinTimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelper;
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    public GroupByLongList getTimestampList(int slotId) {
        if (slotId == -1) {
            return ownerTimestampList;
        }
        return perWorkerTimestampLists.getQuick(slotId);
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
        ownerSlaveTimeFrameCursor.of(
                pageFrameCursor,
                frameAddressCache,
                framePartitionIndexes,
                frameRowCounts,
                partitionTimestamps,
                frameCount
        );
        ownerSlaveTimeFrameHelper.of(ownerSlaveTimeFrameCursor);
        for (int i = 0, n = perWorkerSlaveTimeFrameHelpers.size(); i < n; i++) {
            final ConcurrentTimeFrameCursor workerCursor = perWorkerSlaveTimeFrameCursors.getQuick(i);
            workerCursor.of(
                    pageFrameCursor,
                    frameAddressCache,
                    framePartitionIndexes,
                    frameRowCounts,
                    partitionTimestamps,
                    frameCount
            );
            perWorkerSlaveTimeFrameHelpers.getQuick(i).of(workerCursor);
        }

        // now we can init groupBy functions and join filters
        final SymbolTableSource slaveSymbolTableSource = ownerSlaveTimeFrameHelper.getSymbolTableSource();
        joinSymbolTableSource.of(masterSymbolTableSource, slaveSymbolTableSource);

        Function.init(ownerGroupByFunctions, joinSymbolTableSource, executionContext, null);

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
    }

    public boolean isSkipAggregation() {
        return skipAggregation;
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
    public void reopen() {
        ownerFunctionAllocator.reopen();
        if (perWorkerFunctionAllocators != null) {
            for (int i = 0, n = perWorkerFunctionAllocators.size(); i < n; i++) {
                perWorkerFunctionAllocators.getQuick(i).reopen();
            }
        }

        ownerTemporaryAllocator.reopen();
        for (int i = 0, n = perWorkerTemporaryAllocators.size(); i < n; i++) {
            perWorkerTemporaryAllocators.getQuick(i).reopen();
        }
    }

    public void setSkipAggregation(boolean skipAggregation) {
        this.skipAggregation = skipAggregation;
    }

    public boolean shouldUseLateMaterialization(int slotId, boolean isParquetFrame) {
        if (!isParquetFrame) {
            return false;
        }
        if (filterUsedColumnIndexes == null || filterUsedColumnIndexes.size() == 0) {
            return false;
        }
        return getSelectivityStats(slotId).shouldUseLateMaterialization();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.attr("window lo");
        if (joinWindowLo == 0) {
            sink.val("current row");
        } else if (joinWindowLo < 0) {
            sink.val(Math.abs(joinWindowLo)).val(" following");
        } else {
            sink.val(joinWindowLo).val(" preceding");
        }
        sink.val(includePrevailing ? " (include prevailing)" : " (exclude prevailing)");

        sink.attr("window hi");
        if (joinWindowHi == 0) {
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

    static int findFunctionWithSameArg(ObjList<Function> functions, IntList functionTypes, Function target, int targetType) {
        if (target == null) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (functions.getQuick(i) == null) {
                    return i;
                }
            }
        } else {
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (functionTypes.getQuick(i) == targetType && target.isEquivalentTo(functions.getQuick(i))) {
                    return i;
                }
            }
        }
        return -1;
    }
}
