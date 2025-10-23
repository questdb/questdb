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
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.DirectMapValue;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.prepareBindVarMemory;

public class AsyncWindowJoinAtom implements StatefulAtom, Plannable {
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledFilter;
    private final long joinWindowHi;
    private final long joinWindowLo;
    private final int masterTimestampIndex;
    private final GroupByAllocator ownerAllocator;
    private final Function ownerFilter;
    // Note: all function updaters should be used through a getFunctionUpdater() call
    // to properly initialize group by functions' allocator.
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final Function ownerJoinFilter;
    private final JoinRecord ownerJoinRecord;
    private final DirectMapValue ownerMapValue;
    private final TimeFrameHelper ownerSlaveTimeFrameHelper;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<Function> perWorkerJoinFilters;
    private final ObjList<JoinRecord> perWorkerJoinRecords;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<DirectMapValue> perWorkerMapValues;
    private final ObjList<TimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    private final long valueSizeInBytes;

    public AsyncWindowJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            @Nullable Function ownerJoinFilter,
            @Nullable ObjList<Function> perWorkerJoinFilters,
            long joinWindowLo,
            long joinWindowHi,
            int split,
            int masterTimestampIndex,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert perWorkerJoinFilters == null || perWorkerJoinFilters.size() == workerCount;
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.ownerJoinFilter = ownerJoinFilter;
            this.perWorkerJoinFilters = perWorkerJoinFilters;
            this.joinWindowLo = joinWindowLo;
            this.joinWindowHi = joinWindowHi;
            this.masterTimestampIndex = masterTimestampIndex;
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;
            this.compiledFilter = compiledFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
            this.ownerFilter = ownerFilter;
            this.perWorkerFilters = perWorkerFilters;

            this.ownerSlaveTimeFrameHelper = new TimeFrameHelper(slaveFactory.newTimeFrameCursor());
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveTimeFrameHelpers.extendAndSet(i, new TimeFrameHelper(slaveFactory.newTimeFrameCursor()));
            }

            this.ownerJoinRecord = new JoinRecord(split);
            this.perWorkerJoinRecords = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerJoinRecords.extendAndSet(i, new JoinRecord(split));
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
            perWorkerAllocators = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerAllocators.extendAndSet(i, GroupByAllocatorFactory.createAllocator(configuration));
            }

            final int valueCount = valueTypes.getColumnCount();
            ownerMapValue = new DirectMapValue(valueCount);
            valueSizeInBytes = ownerMapValue.getSizeInBytes();
            perWorkerMapValues = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMapValues.extendAndSet(i, new DirectMapValue(valueCount));
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
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
        Misc.free(ownerJoinFilter);
        Misc.freeObjList(perWorkerJoinFilters);
        Misc.free(compiledFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(ownerFilter);
        Misc.freeObjList(perWorkerFilters);
        Misc.free(ownerAllocator);
        Misc.freeObjList(perWorkerAllocators);
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
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            // Make sure to set worker-local allocator for the functions backed by the returned updater.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            return ownerFunctionUpdater;
        }
        GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(slotId), perWorkerAllocators.getQuick(slotId));
        return perWorkerFunctionUpdaters.getQuick(slotId);
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
            return ownerMapValue;
        }
        return perWorkerMapValues.getQuick(slotId);
    }

    public int getMasterTimestampIndex() {
        return masterTimestampIndex;
    }

    // Thread-unsafe, should be used by query owner thread only.
    public DirectMapValue getOwnerMapValue() {
        return ownerMapValue;
    }

    public TimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelper;
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
    }

    public long getValueSizeInBytes() {
        return valueSizeInBytes;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (ownerJoinFilter != null) {
            ownerJoinFilter.init(symbolTableSource, executionContext);
        }

        if (perWorkerJoinFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerJoinFilters, symbolTableSource, executionContext, ownerJoinFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

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

        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    Function.init(perWorkerGroupByFunctions.getQuick(i), symbolTableSource, executionContext, null);
                }
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
            TablePageFrameCursor pageFrameCursor,
            PageFrameAddressCache frameAddressCache,
            IntList framePartitionIndexes,
            LongList frameRowCounts,
            LongList partitionTimestamps,
            int frameCount
    ) {
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

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, ExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use its own private filter, function updaters, allocator,
            // etc. anytime.
            return -1;
        }
        // All other threads, e.g. worker or work stealing threads, must always acquire a lock
        // to use shared resources.
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.attr("join filter").val(ownerJoinFilter);
        sink.attr("filter").val(ownerJoinFilter);
    }

    public void toTop() {
        ownerSlaveTimeFrameHelper.clear();
        for (int i = 0, n = perWorkerSlaveTimeFrameHelpers.size(); i < n; i++) {
            perWorkerSlaveTimeFrameHelpers.getQuick(i).clear();
        }
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    public static class TimeFrameHelper implements QuietCloseable, Mutable {
        private final Record record;
        private final TimeFrame timeFrame;
        private final ConcurrentTimeFrameCursor timeFrameCursor;

        public TimeFrameHelper(ConcurrentTimeFrameCursor timeFrameCursor) {
            this.timeFrameCursor = timeFrameCursor;
            this.record = timeFrameCursor.getRecord();
            this.timeFrame = timeFrameCursor.getTimeFrame();
        }

        @Override
        public void clear() {
            timeFrameCursor.toTop();
        }

        @Override
        public void close() {
            Misc.free(timeFrameCursor);
        }

        // finds the first row id within the given interval
        public long findRowLo(long timestampLo, long timestampHi) {
            // TODO(puzpuzpuz): !!! this implementation is awful, it only exists to be able to test the whole thing !!!
            timeFrameCursor.toTop();
            while (timeFrameCursor.next()) {
                if (timeFrameCursor.open() == 0) {
                    return Long.MIN_VALUE;
                }
                for (long r = timeFrame.getRowLo(); r < timeFrame.getRowHi(); r++) {
                    recordAt(r);
                    final long timestamp = record.getTimestamp(getTimestampIndex());
                    if (timestamp >= timestampLo && timestamp < timestampHi) {
                        return r;
                    }
                }
            }
            return Long.MIN_VALUE;
        }

        public Record getRecord() {
            return record;
        }

        public long getTimeFrameRowHi() {
            return timeFrameCursor.getTimeFrame().getRowHi();
        }

        public long getTimeFrameRowLo() {
            return timeFrameCursor.getTimeFrame().getRowLo();
        }

        public int getTimestampIndex() {
            return timeFrameCursor.getTimestampIndex();
        }

        public boolean nextFrame(long timestampHi) {
            if (!timeFrameCursor.next()) {
                return false;
            }
            if (timestampHi >= timeFrame.getTimestampEstimateLo()) {
                return timeFrameCursor.open() > 0;
            }
            return false;
        }

        public void of(
                TablePageFrameCursor frameCursor,
                PageFrameAddressCache frameAddressCache,
                IntList framePartitionIndexes,
                LongList frameRowCounts,
                LongList partitionTimestamps,
                int frameCount
        ) {
            timeFrameCursor.of(
                    frameCursor,
                    frameAddressCache,
                    framePartitionIndexes,
                    frameRowCounts,
                    partitionTimestamps,
                    frameCount
            );
        }

        public void recordAt(long rowId) {
            timeFrameCursor.recordAt(record, rowId);
        }
    }
}
