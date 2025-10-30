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
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTable;
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
import io.questdb.griffin.engine.groupby.DirectMapValueFactory;
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
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.prepareBindVarMemory;

public class AsyncWindowJoinAtom implements StatefulAtom, Plannable {
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledMasterFilter;
    private final JoinSymbolTableSource joinSymbolTableSource;
    private final long joinWindowHi;
    private final long joinWindowLo;
    private final int masterTimestampIndex;
    private final GroupByAllocator ownerAllocator;
    // Note: all function updaters should be used through a getFunctionUpdater() call
    // to properly initialize group by functions' allocator.
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final DirectMapValue ownerGroupByValue;
    private final Function ownerJoinFilter;
    private final JoinRecord ownerJoinRecord;
    private final Function ownerMasterFilter;
    private final TimeFrameHelper ownerSlaveTimeFrameHelper;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<DirectMapValue> perWorkerGroupByValues;
    private final ObjList<Function> perWorkerJoinFilters;
    private final ObjList<JoinRecord> perWorkerJoinRecords;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<Function> perWorkerMasterFilters;
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

            this.ownerSlaveTimeFrameHelper = new TimeFrameHelper(slaveFactory.newTimeFrameCursor(), configuration.getSqlAsOfJoinLookAhead());
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveTimeFrameHelpers.extendAndSet(i, new TimeFrameHelper(slaveFactory.newTimeFrameCursor(), configuration.getSqlAsOfJoinLookAhead()));
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
            if (perWorkerGroupByFunctions != null) {
                perWorkerAllocators = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    GroupByAllocator workerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                    perWorkerAllocators.extendAndSet(i, workerAllocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), workerAllocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            ownerGroupByValue = DirectMapValueFactory.createDirectMapValue(valueTypes);
            valueSizeInBytes = ownerGroupByValue.getSizeInBytes();
            perWorkerGroupByValues = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerGroupByValues.extendAndSet(i, DirectMapValueFactory.createDirectMapValue(valueTypes));
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

    public CompiledFilter getCompiledMasterFilter() {
        return compiledMasterFilter;
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
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

    // Thread-unsafe, should be used by query owner thread only.
    public DirectMapValue getOwnerGroupByValue() {
        return ownerGroupByValue;
    }

    public TimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelper;
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
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
        sink.attr("join filter").val(ownerJoinFilter);
        sink.attr("filter").val(ownerJoinFilter);
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

    public static class TimeFrameHelper implements QuietCloseable {
        private final long lookahead;
        private final PageFrameMemoryRecord record;
        private final TimeFrame timeFrame;
        private final ConcurrentTimeFrameCursor timeFrameCursor;
        private final int timestampIndex;
        private int bookmarkedFrameIndex = -1;
        private long bookmarkedRowId = Long.MIN_VALUE;

        public TimeFrameHelper(ConcurrentTimeFrameCursor timeFrameCursor, long lookahead) {
            this.timeFrameCursor = timeFrameCursor;
            this.record = timeFrameCursor.getRecord();
            this.timeFrame = timeFrameCursor.getTimeFrame();
            this.lookahead = lookahead;
            this.timestampIndex = timeFrameCursor.getTimestampIndex();
        }

        @Override
        public void close() {
            Misc.free(timeFrameCursor);
        }

        // finds the first row id within the given interval
        public long findRowLo(long timestampLo, long timestampHi) {
            long rowLo = Long.MIN_VALUE;
            // let's start with the last found frame and row id
            if (bookmarkedFrameIndex != -1) {
                timeFrameCursor.jumpTo(bookmarkedFrameIndex);
                timeFrameCursor.open();
                rowLo = bookmarkedRowId;
            }

            for (; ; ) {
                // find the frame to be scanned
                if (rowLo == Long.MIN_VALUE) {
                    while (timeFrameCursor.next()) {
                        // carry on if the frame is to the left of the interval
                        if (timeFrame.getTimestampEstimateHi() <= timestampLo) {
                            // bookmark the frame, so that next time we search we start with it
                            bookmarkCurrentFrame(0);
                            continue;
                        }
                        // check if the frame intersects with the interval, so it's of our interest
                        if (timeFrame.getTimestampEstimateLo() < timestampHi) {
                            if (timeFrameCursor.open() == 0) {
                                continue;
                            }
                            // now we know the exact boundaries of the frame, let's check them
                            if (timeFrame.getTimestampHi() <= timestampLo) {
                                // the frame is to the left of the interval, so carry on
                                bookmarkCurrentFrame(0);
                                continue;
                            }
                            if (timeFrame.getTimestampLo() < timestampHi) {
                                // yay, it's what we need!
                                if (timeFrame.getTimestampLo() >= timestampLo) {
                                    // we can start with the first row
                                    bookmarkCurrentFrame(timeFrame.getRowLo());
                                    return timeFrame.getRowLo();
                                }
                                // we need to find the first row in the intersection
                                rowLo = timeFrame.getRowLo();
                                break;
                            }
                        }
                        return Long.MIN_VALUE;
                    }
                    if (rowLo == Long.MIN_VALUE) {
                        return Long.MIN_VALUE;
                    }
                }

                // bookmark the frame
                bookmarkCurrentFrame(rowLo);

                // scan the found frame
                // start with a brief linear scan
                final long scanResult = linearScan(timestampLo, timestampHi, rowLo);
                if (scanResult >= 0) {
                    // we've found the row
                    bookmarkCurrentFrame(scanResult);
                    return scanResult;
                } else if (scanResult == Long.MIN_VALUE) {
                    // there are no timestamps in the wanted interval
                    if (timeFrame.getTimestampHi() > timestampHi) {
                        // the interval is contained in the frame, no need to try the next one
                        return Long.MIN_VALUE;
                    }
                    // the next frame may have an intersection with the interval, try it
                    rowLo = Long.MIN_VALUE;
                    continue;
                }
                // ok, the scan gave us nothing, do the binary search
                rowLo = -scanResult - 1;
                final long searchResult = binarySearch(timestampLo, timestampHi, rowLo);
                if (searchResult == Long.MIN_VALUE) {
                    // there are no timestamps in the wanted interval
                    if (timeFrame.getTimestampHi() > timestampHi) {
                        // the interval is contained in the frame, no need to try the next one
                        return Long.MIN_VALUE;
                    }
                    // the next frame may have an intersection with the interval, try it
                    rowLo = Long.MIN_VALUE;
                    continue;
                }
                // we've found the row
                bookmarkCurrentFrame(searchResult);
                return searchResult;
            }
        }

        public Record getRecord() {
            return record;
        }

        public SymbolTableSource getSymbolTableSource() {
            return timeFrameCursor;
        }

        public int getTimeFrameIndex() {
            return timeFrame.getFrameIndex();
        }

        public long getTimeFrameRowHi() {
            return timeFrame.getRowHi();
        }

        public long getTimeFrameRowLo() {
            return timeFrame.getRowLo();
        }

        public int getTimestampIndex() {
            return timestampIndex;
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
            toTop();
        }

        public void recordAtRowIndex(long rowId) {
            timeFrameCursor.recordAtRowIndex(record, rowId);
        }

        public void toTop() {
            timeFrameCursor.toTop();
            record.clear();
            bookmarkedFrameIndex = -1;
            bookmarkedRowId = Long.MIN_VALUE;
        }

        // Finds the first (most-left) value in the given interval.
        private long binarySearch(long timestampLo, long timestampHi, long rowLo) {
            long low = rowLo;
            long high = timeFrame.getRowHi() - 1;
            while (high - low > 65) {
                final long mid = (low + high) >>> 1;
                recordAtRowIndex(mid);
                long midTimestamp = record.getTimestamp(timestampIndex);

                if (midTimestamp < timestampLo) {
                    low = mid;
                } else if (midTimestamp > timestampLo) {
                    high = mid - 1;
                } else {
                    // In case of multiple values equal to timestampLo, find the first one
                    return binarySearchScrollUp(low, mid, timestampLo);
                }
            }

            // scan up
            for (long r = low; r < high + 1; r++) {
                recordAtRowIndex(r);
                long timestamp = record.getTimestamp(timestampIndex);
                if (timestamp >= timestampLo) {
                    if (timestamp < timestampHi) {
                        return r;
                    }
                    return Long.MIN_VALUE;
                }
            }
            return Long.MIN_VALUE;
        }

        private long binarySearchScrollUp(long low, long high, long timestampLo) {
            long timestamp;
            do {
                if (high > low) {
                    high--;
                } else {
                    return low;
                }
                recordAtRowIndex(high);
                timestamp = record.getTimestamp(timestampIndex);
            } while (timestamp == timestampLo);
            return high + 1;
        }

        private void bookmarkCurrentFrame(long rowId) {
            bookmarkedFrameIndex = timeFrame.getFrameIndex();
            bookmarkedRowId = rowId;
        }

        private long linearScan(long timestampLo, long timestampHi, long rowLo) {
            final long scanHi = Math.min(rowLo + lookahead, timeFrame.getRowHi());
            for (long r = rowLo; r < scanHi; r++) {
                recordAtRowIndex(r);
                final long timestamp = record.getTimestamp(timestampIndex);
                if (timestamp >= timestampLo) {
                    if (timestamp < timestampHi) {
                        return r;
                    }
                    return Long.MIN_VALUE;
                }
            }
            return -rowLo - lookahead - 1;
        }
    }
}
