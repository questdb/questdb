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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntMultiLongHashMap;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.join.AsyncWindowJoinAtom.findFunctionWithSameArg;
import static io.questdb.griffin.engine.join.AsyncWindowJoinFastAtom.*;
import static io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory.findPrevailingForMasterRow;

/**
 * Single-threaded WINDOW JOIN factory with symbol-based join key optimization.
 * <p>
 * Iterates over the master cursor and joins against the slave table via time frame
 * cursor. Requires an equality condition on symbol columns between master and slave tables.
 *
 * @see AsyncWindowJoinFastRecordCursorFactory for the multi-threaded variant
 */
public class WindowJoinFastRecordCursorFactory extends AbstractRecordCursorFactory {
    // Index lookahead multiplier. Defines the size of the time interval used to build per-symbol
    // in-memory index with row ids/timestamps to be used by aggregation. Aimed to improve performance
    // for the dense master row timestamps case by not having to rebuild the index for each unique
    // master timestamp.
    private static final int INDEX_LOOKAHEAD = 2;
    private static final int INITIAL_COLUMN_SINK_CAPACITY = 64;
    private static final int INITIAL_LIST_CAPACITY = 16;
    private final AbstractWindowJoinFastRecordCursor cursor;
    private final boolean includePrevailing;
    private final Function joinFilter;
    private final JoinRecordMetadata joinMetadata;
    private final RecordCursorFactory masterFactory;
    private final int masterSymbolIndex;
    private final RecordCursorFactory slaveFactory;
    private final int slaveSymbolIndex;
    private final SimpleMapValue value;
    private final boolean vectorized;
    private final long windowHi;
    private final long windowLo;

    public WindowJoinFastRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull JoinRecordMetadata joinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @Nullable IntList columnIndex,
            boolean includePrevailing,
            long windowLo,
            long windowHi,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ArrayColumnTypes columnTypes,
            int slaveSymbolIndex,
            int masterSymbolIndex,
            @Nullable Function joinFilter,
            boolean vectorized
    ) {
        super(metadata);
        assert slaveFactory.supportsTimeFrameCursor();
        try {
            this.masterFactory = masterFactory;
            this.slaveFactory = slaveFactory;
            this.joinMetadata = joinMetadata;
            this.joinFilter = joinFilter;
            this.includePrevailing = includePrevailing;
            this.windowLo = windowLo;
            this.windowHi = windowHi;
            this.value = new SimpleMapValue(columnTypes.getColumnCount());
            final int columnSplit = masterFactory.getMetadata().getColumnCount();
            var masterMetadata = masterFactory.getMetadata();
            var slaveMetadata = slaveFactory.getMetadata();
            this.vectorized = vectorized;

            final GroupByFunctionsUpdater groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            if (vectorized) {
                var groupByCount = groupByFunctions.size();
                var groupByFunctionToColumnIndex = new IntList(groupByCount);
                ObjList<Function> groupByFunctionArgs = new ObjList<>(groupByCount);
                IntList groupByFunctionTypes = new IntList(groupByCount);
                for (int i = 0; i < groupByCount; i++) {
                    var func = groupByFunctions.getQuick(i);
                    var funcArg = func.getComputeBatchArg();
                    var funcArgType = ColumnType.tagOf(func.getComputeBatchArgType());
                    int index = findFunctionWithSameArg(groupByFunctionArgs, groupByFunctionTypes, funcArg, funcArgType);
                    if (index == -1) {
                        groupByFunctionArgs.add(funcArg);
                        groupByFunctionTypes.add(funcArgType);
                        groupByFunctionToColumnIndex.add(groupByFunctionArgs.size() - 1);
                    } else {
                        groupByFunctionToColumnIndex.add(index);
                    }
                }

                this.cursor = new WindowJoinFastVectRecordCursor(
                        configuration,
                        columnIndex,
                        columnSplit,
                        masterMetadata.getTimestampIndex(),
                        slaveMetadata.getTimestampIndex(),
                        masterMetadata.getTimestampType(),
                        slaveMetadata.getTimestampType(),
                        groupByFunctions,
                        groupByFunctionsUpdater,
                        value,
                        2 + groupByFunctionArgs.size(),
                        groupByFunctionArgs,
                        groupByFunctionTypes,
                        groupByFunctionToColumnIndex
                );
            } else {
                if (includePrevailing) {
                    if (joinFilter != null) {
                        this.cursor = new WindowJoinWithPrevailingAndJoinFilterFastRecordCursor(
                                configuration,
                                columnIndex,
                                columnSplit,
                                masterMetadata.getTimestampIndex(),
                                slaveMetadata.getTimestampIndex(),
                                masterMetadata.getTimestampType(),
                                slaveMetadata.getTimestampType(),
                                groupByFunctions,
                                groupByFunctionsUpdater,
                                value,
                                3
                        );
                    } else {
                        this.cursor = new WindowJoinWithPrevailingFastRecordCursor(
                                configuration,
                                columnIndex,
                                columnSplit,
                                masterMetadata.getTimestampIndex(),
                                slaveMetadata.getTimestampIndex(),
                                masterMetadata.getTimestampType(),
                                slaveMetadata.getTimestampType(),
                                groupByFunctions,
                                groupByFunctionsUpdater,
                                value,
                                3
                        );
                    }
                } else {
                    this.cursor = new WindowJoinFastRecordCursor(
                            configuration,
                            columnIndex,
                            columnSplit,
                            masterMetadata.getTimestampIndex(),
                            slaveMetadata.getTimestampIndex(),
                            masterMetadata.getTimestampType(),
                            slaveMetadata.getTimestampType(),
                            groupByFunctions,
                            groupByFunctionsUpdater,
                            value,
                            3
                    );
                }
            }
            this.slaveSymbolIndex = slaveSymbolIndex;
            this.masterSymbolIndex = masterSymbolIndex;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext);
        } catch (Throwable ex) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            throw ex;
        }
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Window Fast Join");

        sink.attr("vectorized").val(vectorized);
        sink.attr("symbol")
                .val(masterFactory.getMetadata().getColumnName(masterSymbolIndex))
                .val("=")
                .val(slaveFactory.getMetadata().getColumnName(slaveSymbolIndex));

        sink.attr("window lo");
        if (windowLo == 0) {
            sink.val("current row");
        } else if (windowLo < 0) {
            sink.val(Math.abs(windowLo)).val(" following");
        } else {
            sink.val(windowLo).val(" preceding");
        }
        sink.val(includePrevailing ? " (include prevailing)" : " (exclude prevailing)");

        sink.attr("window hi");
        if (windowHi == 0) {
            sink.val("current row");
        } else if (windowHi < 0) {
            sink.val(Math.abs(windowHi)).val(" preceding");
        } else {
            sink.val(windowHi).val(" following");
        }

        if (joinFilter != null) {
            sink.setMetadata(joinMetadata);
            sink.attr("join filter").val(joinFilter);
            sink.setMetadata(null);
        }

        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
        Misc.free(joinFilter);
        Misc.free(joinMetadata);
        Misc.free(value);
    }

    private abstract class AbstractWindowJoinFastRecordCursor implements NoRandomAccessRecordCursor {
        protected final GroupByFunctionsUpdater groupByFunctionsUpdater;
        // Stores metadata about storage of slave underlying records
        // vectorized layout: | timestamp pointer | current row lo | column0 pointer | ... | columnN pointer |
        // un-vectorized layout: | timestamp pointer | row ids pointer | current row lo |
        protected final DirectIntMultiLongHashMap slaveData;
        protected final DirectIntIntHashMap slaveSymbolLookupMap;

        public AbstractWindowJoinFastRecordCursor(GroupByFunctionsUpdater groupByFunctionsUpdater, int valueCount) {
            try {
                this.groupByFunctionsUpdater = groupByFunctionsUpdater;
                this.slaveSymbolLookupMap = new DirectIntIntHashMap(
                        SLAVE_MAP_INITIAL_CAPACITY,
                        SLAVE_MAP_LOAD_FACTOR,
                        0,
                        StaticSymbolTable.VALUE_NOT_FOUND,
                        MemoryTag.NATIVE_UNORDERED_MAP
                );
                this.slaveData = new DirectIntMultiLongHashMap(
                        SLAVE_MAP_INITIAL_CAPACITY,
                        SLAVE_MAP_LOAD_FACTOR,
                        0,
                        0,
                        valueCount,
                        MemoryTag.NATIVE_UNORDERED_MAP
                );
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            Misc.free(slaveData);
            Misc.free(slaveSymbolLookupMap);
        }

        abstract void of(
                RecordCursor masterCursor,
                TimeFrameCursor slaveCursor,
                SqlExecutionContext sqlExecutionContext
        ) throws SqlException;

        protected void setupSlaveLookupMap(RecordCursor masterCursor, TimeFrameCursor slaveCursor) {
            slaveSymbolLookupMap.reopen();
            StaticSymbolTable masterSymbolTable = (StaticSymbolTable) masterCursor.getSymbolTable(masterSymbolIndex);
            StaticSymbolTable slaveSymbolTable = slaveCursor.getSymbolTable(slaveSymbolIndex);
            for (int masterKey = 0, n = masterSymbolTable.getSymbolCount(); masterKey < n; masterKey++) {
                final CharSequence masterSym = masterSymbolTable.valueOf(masterKey);
                final int slaveKey = slaveSymbolTable.keyOf(masterSym);
                if (slaveKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                    slaveSymbolLookupMap.put(slaveKey + KEY_SHIFT, masterKey);
                }
            }
            if (masterSymbolTable.containsNullValue() && slaveSymbolTable.containsNullValue()) {
                slaveSymbolLookupMap.put(NULL_KEY, StaticSymbolTable.VALUE_IS_NULL);
            }
        }
    }

    private class WindowJoinFastRecordCursor extends AbstractWindowJoinFastRecordCursor {
        protected final JoinRecord internalJoinRecord;
        protected final int masterTimestampIndex;
        protected final long masterTimestampScale;
        protected final GroupByAllocator slaveAllocator;
        protected final GroupByLongList slaveRowIds;
        protected final WindowJoinTimeFrameHelper slaveTimeFrameHelper;
        protected final int slaveTimestampIndex;
        protected final long slaveTimestampScale;
        protected final GroupByLongList slaveTimestamps;
        protected final SimpleMapValue value;
        private final GroupByAllocator allocator;
        private final int columnSplit;
        private final @Nullable IntList crossIndex;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final VirtualRecord groupByRecord;
        private final JoinRecord joinRecord;
        private final WindowJoinSymbolTableSource joinSymbolTableSource;
        private final Record record;
        protected SqlExecutionCircuitBreaker circuitBreaker;
        protected long lastSlaveTimestamp;
        protected RecordCursor masterCursor;
        protected Record masterRecord;
        private boolean isOpen;
        private TimeFrameCursor slaveCursor;

        public WindowJoinFastRecordCursor(
                @NotNull CairoConfiguration configuration,
                @Nullable IntList columnIndex,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull GroupByFunctionsUpdater groupByFunctionsUpdater,
                @NotNull SimpleMapValue value,
                int valueCount
        ) {
            super(groupByFunctionsUpdater, valueCount);
            this.crossIndex = columnIndex;
            this.columnSplit = columnSplit;
            this.groupByFunctions = groupByFunctions;
            this.allocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(groupByFunctions, allocator);
            this.value = value;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            if (masterTimestampType == slaveTimestampType) {
                masterTimestampScale = slaveTimestampScale = 1L;
            } else {
                masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
                slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
            }
            this.slaveTimeFrameHelper = new WindowJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTimestampScale);
            this.joinSymbolTableSource = new WindowJoinSymbolTableSource(columnSplit);

            this.internalJoinRecord = new JoinRecord(columnSplit);
            this.groupByRecord = new VirtualRecord(groupByFunctions);
            groupByRecord.of(value);
            this.joinRecord = new JoinRecord(columnSplit);
            if (columnIndex != null) {
                SelectedRecord sr = new SelectedRecord(columnIndex);
                sr.of(joinRecord);
                this.record = sr;
            } else {
                this.record = joinRecord;
            }

            this.slaveAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            this.slaveTimestamps = new GroupByLongList(INITIAL_LIST_CAPACITY);
            this.slaveTimestamps.setAllocator(slaveAllocator);
            this.slaveRowIds = new GroupByLongList(INITIAL_LIST_CAPACITY);
            this.slaveRowIds.setAllocator(slaveAllocator);

            this.lastSlaveTimestamp = Long.MIN_VALUE;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            super.close();
            if (isOpen) {
                isOpen = false;
                Misc.free(allocator);
                Misc.clearObjList(groupByFunctions);
                masterCursor = Misc.free(masterCursor);
                slaveCursor = Misc.free(slaveCursor);
                Misc.free(slaveAllocator);
                slaveTimestamps.resetPtr();
                slaveRowIds.resetPtr();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (crossIndex != null) {
                columnIndex = crossIndex.getQuick(columnIndex);
            }
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            return (SymbolTable) groupByFunctions.getQuick(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (!masterCursor.hasNext()) {
                return false;
            }

            // We build the timestamp interval over which we will aggregate the matching slave rows [slaveTimestampLo; slaveTimestampHi]
            long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            long slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTimestampScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi * INDEX_LOOKAHEAD, masterTimestampScale);
            long masterTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);

            if (masterTimestampHi > lastSlaveTimestamp) {
                slaveData.clear();
                slaveAllocator.clear();
                lastSlaveTimestamp = Long.MIN_VALUE;
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }

                        lastSlaveTimestamp = slaveTimestamp;
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveTimestamps.of(slaveData.get(idx, 1));
                            slaveRowIds.of(slaveData.get(idx, 0));
                            slaveTimestamps.add(slaveTimestamp);
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveData.put(idx, 1, slaveTimestamps.ptr());
                            slaveData.put(idx, 0, slaveRowIds.ptr());
                        }

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }
            }

            groupByFunctionsUpdater.updateEmpty(value);
            value.setNew(true);

            final int masterKey = internalJoinRecord.getInt(masterSymbolIndex);
            final int idx = toSymbolMapKey(masterKey);
            slaveTimestamps.of(slaveData.get(idx, 1));
            slaveRowIds.of(slaveData.get(idx, 0));

            if (slaveTimestamps.size() > 0) {
                long rowLo = slaveData.get(idx, 2);
                rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                slaveData.put(idx, 2, rowLo);
                long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;
                if (rowLo < rowHi) {
                    for (long i = rowLo; i < rowHi; i++) {
                        final long slaveRowId = slaveRowIds.get(i);
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        if (joinFilter == null || joinFilter.getBool(internalJoinRecord)) {
                            if (value.isNew()) {
                                groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                                value.setNew(false);
                            } else {
                                groupByFunctionsUpdater.updateExisting(value, internalJoinRecord, slaveRowId);
                            }
                        }
                    }
                }
            }

            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (crossIndex != null) {
                columnIndex = crossIndex.getQuick(columnIndex);
            }
            if (columnIndex < columnSplit) {
                return masterCursor.newSymbolTable(columnIndex);
            }
            return ((SymbolFunction) groupByFunctions.getQuick(columnIndex - columnSplit)).newSymbolTable();
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveTimeFrameHelper.toTop();
            lastSlaveTimestamp = Long.MIN_VALUE;
            slaveData.clear();
            allocator.clear();
            slaveAllocator.clear();
            slaveTimestamps.resetPtr();
            slaveRowIds.resetPtr();
            GroupByUtils.toTop(groupByFunctions);
        }

        void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                slaveData.reopen();
                allocator.reopen();
                slaveAllocator.reopen();
                setupSlaveLookupMap(masterCursor, slaveCursor);
            }
            this.masterCursor = masterCursor;
            this.masterRecord = masterCursor.getRecord();
            this.slaveCursor = slaveCursor;
            joinRecord.of(masterRecord, groupByRecord);
            slaveTimeFrameHelper.of(slaveCursor);
            internalJoinRecord.of(masterRecord, slaveTimeFrameHelper.getRecord());
            joinSymbolTableSource.of(masterCursor, slaveTimeFrameHelper.getSymbolTableSource());
            if (joinFilter != null) {
                joinFilter.init(joinSymbolTableSource, sqlExecutionContext);
            }
            Function.init(groupByFunctions, joinSymbolTableSource, sqlExecutionContext, null);
            circuitBreaker = sqlExecutionContext.getCircuitBreaker();
            lastSlaveTimestamp = Long.MIN_VALUE;
        }
    }

    private class WindowJoinFastVectRecordCursor extends AbstractWindowJoinFastRecordCursor {
        private final GroupByAllocator allocator;
        private final int columnCount;
        private final GroupByColumnSink columnSink;
        private final int columnSplit;
        private final @Nullable IntList crossIndex;
        private final ObjList<Function> groupByFuncArgs;
        private final IntList groupByFuncTypes;
        private final IntList groupByFunctionToColumnIndex;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final VirtualRecord groupByRecord;
        private final JoinRecord internalJoinRecord;
        private final JoinRecord joinRecord;
        private final WindowJoinSymbolTableSource joinSymbolTableSource;
        private final int masterTimestampIndex;
        private final long masterTimestampScale;
        private final WindowJoinPrevailingCache prevailingCache;
        private final Record record;
        private final GroupByAllocator slaveAllocator;
        private final WindowJoinTimeFrameHelper slaveTimeFrameHelper;
        private final int slaveTimestampIndex;
        private final long slaveTimestampScale;
        private final GroupByLongList timestamps;
        private final SimpleMapValue value;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private long lastSlaveTimestamp = Long.MIN_VALUE;
        private RecordCursor masterCursor;
        private Record masterRecord;
        private TimeFrameCursor slaveCursor;

        public WindowJoinFastVectRecordCursor(
                @NotNull CairoConfiguration configuration,
                @Nullable IntList columnIndex,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull GroupByFunctionsUpdater groupByFunctionsUpdater,
                @NotNull SimpleMapValue value,
                int valueCount,
                @NotNull ObjList<Function> groupByFuncArgs,
                @NotNull IntList groupByFuncTypes,
                @NotNull IntList groupByFunctionToColumnIndex
        ) {
            super(groupByFunctionsUpdater, valueCount);
            try {
                this.crossIndex = columnIndex;
                this.columnSplit = columnSplit;
                this.groupByFunctions = groupByFunctions;
                this.allocator = GroupByAllocatorFactory.createAllocator(configuration);
                GroupByUtils.setAllocator(groupByFunctions, allocator);
                this.value = value;
                this.masterTimestampIndex = masterTimestampIndex;
                this.slaveTimestampIndex = slaveTimestampIndex;
                if (masterTimestampType == slaveTimestampType) {
                    masterTimestampScale = slaveTimestampScale = 1L;
                } else {
                    masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
                    slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
                }
                this.slaveTimeFrameHelper = new WindowJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTimestampScale);
                this.prevailingCache = new WindowJoinPrevailingCache();
                this.joinSymbolTableSource = new WindowJoinSymbolTableSource(columnSplit);

                this.internalJoinRecord = new JoinRecord(columnSplit);
                this.groupByRecord = new VirtualRecord(groupByFunctions);
                groupByRecord.of(value);
                this.joinRecord = new JoinRecord(columnSplit);
                if (columnIndex != null) {
                    SelectedRecord sr = new SelectedRecord(columnIndex);
                    sr.of(joinRecord);
                    this.record = sr;
                } else {
                    this.record = joinRecord;
                }

                this.slaveAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                this.columnSink = new GroupByColumnSink(INITIAL_COLUMN_SINK_CAPACITY);
                this.columnSink.setAllocator(slaveAllocator);
                this.timestamps = new GroupByLongList(INITIAL_LIST_CAPACITY);
                this.timestamps.setAllocator(slaveAllocator);

                this.columnCount = groupByFuncArgs.size();
                this.groupByFuncArgs = groupByFuncArgs;
                this.groupByFuncTypes = groupByFuncTypes;
                this.groupByFunctionToColumnIndex = groupByFunctionToColumnIndex;
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            super.close();
            Misc.free(prevailingCache);
            Misc.free(allocator);
            Misc.free(slaveAllocator);
            if (isOpen) {
                isOpen = false;
                Misc.clearObjList(groupByFunctions);
                masterCursor = Misc.free(masterCursor);
                slaveCursor = Misc.free(slaveCursor);
                timestamps.resetPtr();
                columnSink.resetPtr();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (crossIndex != null) {
                columnIndex = crossIndex.getQuick(columnIndex);
            }
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            return (SymbolTable) groupByFunctions.getQuick(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (!masterCursor.hasNext()) {
                return false;
            }

            // We build the timestamp interval over which we will aggregate the matching slave rows [slaveTimestampLo; slaveTimestampHi]
            long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            long slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTimestampScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi * INDEX_LOOKAHEAD, masterTimestampScale);
            long masterTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);

            final Record slaveRecord = slaveTimeFrameHelper.getRecord();
            if (masterTimestampHi > lastSlaveTimestamp) {
                slaveData.clear();
                slaveAllocator.clear();
                lastSlaveTimestamp = Long.MIN_VALUE;
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, masterTimestampHi, includePrevailing);
                final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
                prevailingCache.of(prevailingFrameIndex, prevailingRowIndex);

                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }

                        lastSlaveTimestamp = slaveTimestamp;
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            timestamps.of(slaveData.get(idx, 0));
                            timestamps.add(slaveTimestamp);
                            slaveData.put(idx, 0, timestamps.ptr());

                            // Copy the column values to be aggregated.
                            for (int i = 0; i < columnCount; i++) {
                                var funcArg = groupByFuncArgs.getQuick(i);
                                if (funcArg != null) {
                                    final long sinkPtr = slaveData.get(idx, i + 2);
                                    columnSink.of(sinkPtr);
                                    if (includePrevailing && sinkPtr == 0) {
                                        // Reserve an extra slot for the prevailing value by writing the first value twice.
                                        columnSink.put(internalJoinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                    }
                                    columnSink.put(internalJoinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                    slaveData.put(idx, i + 2, columnSink.ptr());
                                }
                            }
                        }

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }
            }

            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                groupByFunctions.getQuick(i).setEmpty(value);
            }

            final int masterKey = internalJoinRecord.getInt(masterSymbolIndex);
            final int idx = toSymbolMapKey(masterKey);
            timestamps.of(slaveData.get(idx, 0));
            if (timestamps.size() > 0) {
                long rowLo = slaveData.get(idx, 1);
                rowLo = Vect.binarySearch64Bit(timestamps.dataPtr(), slaveTimestampLo, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                slaveData.put(idx, 1, rowLo);
                long rowHi = Vect.binarySearch64Bit(timestamps.dataPtr(), masterTimestampHi, rowLo, timestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                if (includePrevailing && (rowLo >= rowHi || timestamps.get(rowLo) > slaveTimestampLo)) {
                    // No rows in the time window or the first row has timestamp higher than the low window boundary.
                    // In both cases we need to include the prevailing value.
                    if (rowLo > 0) {
                        // Yay! We already have the prevailing value in the in-memory index.
                        rowLo--;
                    } else {
                        // There are no slave rows corresponding to the time window for the symbol.
                        // Let's find the prevailing value.
                        final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                slaveTimeFrameHelper,
                                slaveRecord,
                                slaveSymbolIndex,
                                slaveSymbolLookupMap,
                                masterKey
                        );
                        if (slaveRowId != Long.MIN_VALUE) {
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            // Copy the column values for the prevailing row to zero slot.
                            for (int i = 0; i < columnCount; i++) {
                                var funcArg = groupByFuncArgs.getQuick(i);
                                if (funcArg != null) {
                                    final long sinkPtr = slaveData.get(idx, i + 2);
                                    assert sinkPtr != 0;
                                    columnSink.of(sinkPtr).putAt(0, internalJoinRecord, funcArg, (short) groupByFuncTypes.getQuick(i));
                                }
                            }
                            // Set rowLo to -1, so that the zero slot value is included into the aggregation.
                            rowLo = -1;
                        }
                    }
                }

                if (rowLo < rowHi) {
                    for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                        int mapIndex = groupByFunctionToColumnIndex.getQuick(i);
                        final long ptr = slaveData.get(idx, mapIndex + 2);
                        if (ptr != 0) {
                            final long typeSize = ColumnType.sizeOfTag((short) groupByFuncTypes.getQuick(mapIndex));
                            // in case of include prevailing column values are shifted by one slot, hence `rowLo + 1`
                            final long efficientRowLo = includePrevailing ? rowLo + 1 : rowLo;
                            groupByFunctions.getQuick(i).computeBatch(value, columnSink.of(ptr).startAddress() + typeSize * efficientRowLo, (int) (rowHi - rowLo));
                        } else { // no-arg function, e.g. count()
                            groupByFunctions.getQuick(i).computeBatch(value, 0, (int) (rowHi - rowLo));
                        }
                    }
                }
            } else if (includePrevailing) {
                // There are no slave rows corresponding to the master page frame for the symbol.
                // Let's find the prevailing value.
                final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                        slaveTimeFrameHelper,
                        slaveRecord,
                        slaveSymbolIndex,
                        slaveSymbolLookupMap,
                        masterKey
                );
                if (slaveRowId != Long.MIN_VALUE) {
                    slaveTimeFrameHelper.recordAt(slaveRowId);
                    groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                }
            }

            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (crossIndex != null) {
                columnIndex = crossIndex.getQuick(columnIndex);
            }
            if (columnIndex < columnSplit) {
                return masterCursor.newSymbolTable(columnIndex);
            }
            return ((SymbolFunction) groupByFunctions.getQuick(columnIndex - columnSplit)).newSymbolTable();
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveTimeFrameHelper.toTop();
            prevailingCache.clear();
            lastSlaveTimestamp = Long.MIN_VALUE;
            allocator.clear();
            slaveAllocator.clear();
            timestamps.resetPtr();
            columnSink.resetPtr();
            slaveData.clear();
            GroupByUtils.toTop(groupByFunctions);
        }

        void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                slaveData.reopen();
                prevailingCache.reopen();
                allocator.reopen();
                slaveAllocator.reopen();
                setupSlaveLookupMap(masterCursor, slaveCursor);
            }
            this.masterCursor = masterCursor;
            this.masterRecord = masterCursor.getRecord();
            this.slaveCursor = slaveCursor;
            joinRecord.of(masterRecord, groupByRecord);
            slaveTimeFrameHelper.of(slaveCursor);
            internalJoinRecord.of(masterRecord, slaveTimeFrameHelper.getRecord());
            joinSymbolTableSource.of(masterCursor, slaveTimeFrameHelper.getSymbolTableSource());
            Function.init(groupByFunctions, joinSymbolTableSource, sqlExecutionContext, null);
            circuitBreaker = sqlExecutionContext.getCircuitBreaker();
            lastSlaveTimestamp = Long.MIN_VALUE;
        }
    }

    private class WindowJoinWithPrevailingAndJoinFilterFastRecordCursor extends WindowJoinFastRecordCursor {
        private int prevailingFrameIndex = -1;
        private long prevailingRowIndex = Long.MIN_VALUE;

        public WindowJoinWithPrevailingAndJoinFilterFastRecordCursor(
                @NotNull CairoConfiguration configuration,
                @Nullable IntList columnIndex,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull GroupByFunctionsUpdater groupByFunctionsUpdater,
                @NotNull SimpleMapValue value,
                int valueCount
        ) {
            super(
                    configuration,
                    columnIndex,
                    columnSplit,
                    masterTimestampIndex,
                    slaveTimestampIndex,
                    masterTimestampType,
                    slaveTimestampType,
                    groupByFunctions,
                    groupByFunctionsUpdater,
                    value,
                    valueCount
            );
        }

        @Override
        public boolean hasNext() {
            if (!masterCursor.hasNext()) {
                return false;
            }

            // We build the timestamp interval over which we will aggregate the matching slave rows [slaveTimestampLo; slaveTimestampHi]
            long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            long slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTimestampScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi * INDEX_LOOKAHEAD, masterTimestampScale);
            long masterTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);

            if (masterTimestampHi > lastSlaveTimestamp) {
                slaveData.clear();
                slaveAllocator.clear();
                lastSlaveTimestamp = Long.MIN_VALUE;
                final Record slaveRecord = slaveTimeFrameHelper.getRecord();
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
                prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }

                        lastSlaveTimestamp = slaveTimestamp;
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveTimestamps.of(slaveData.get(idx, 1));
                            slaveRowIds.of(slaveData.get(idx, 0));
                            slaveTimestamps.add(slaveTimestamp);
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveData.put(idx, 1, slaveTimestamps.ptr());
                            slaveData.put(idx, 0, slaveRowIds.ptr());
                        }

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }
            }

            groupByFunctionsUpdater.updateEmpty(value);
            value.setNew(true);

            final int masterKey = internalJoinRecord.getInt(masterSymbolIndex);
            final int idx = toSymbolMapKey(masterKey);
            final long rowIdsPtr = slaveData.get(idx, 0);
            final long timestampsPtr = slaveData.get(idx, 1);

            boolean needToFindPrevailing = true;
            if (rowIdsPtr != 0) {
                slaveRowIds.of(rowIdsPtr);
                assert slaveRowIds.size() > 0;
                slaveTimestamps.of(timestampsPtr);
                assert slaveTimestamps.size() > 0;

                long rowLo = slaveData.get(idx, 2);
                rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                slaveData.put(idx, 2, rowLo);
                long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                if (rowLo < rowHi) {
                    // First, check if one of the first rows matching the join filter is also at the slaveTimestampLo timestamp.
                    // If so, we don't need to do backward scan to find the prevailing row.
                    long adjustedRowLo = rowLo;
                    for (long i = rowLo; i < rowHi; i++) {
                        if (slaveTimestamps.get(i) > slaveTimestampLo) {
                            break;
                        }
                        adjustedRowLo++;
                        final long slaveRowId = slaveRowIds.get(i);
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        if (joinFilter.getBool(internalJoinRecord)) {
                            groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                            value.setNew(false);
                            needToFindPrevailing = false;
                            break;
                        }
                    }

                    // Do a backward scan to find the prevailing row.
                    if (needToFindPrevailing) {
                        // First check the accumulated row ids.
                        for (long i = rowLo - 1; i >= 0; i--) {
                            final long slaveRowId = slaveRowIds.get(i);
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            if (joinFilter.getBool(internalJoinRecord)) {
                                groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                                value.setNew(false);
                                needToFindPrevailing = false;
                                break;
                            }
                        }
                        // If no luck, do the backward scan.
                        if (needToFindPrevailing) {
                            findPrevailingForMasterRow(
                                    slaveTimeFrameHelper,
                                    slaveTimeFrameHelper.getRecord(),
                                    slaveSymbolIndex,
                                    slaveSymbolLookupMap,
                                    prevailingFrameIndex,
                                    prevailingRowIndex,
                                    masterKey,
                                    joinFilter,
                                    internalJoinRecord,
                                    groupByFunctionsUpdater,
                                    value
                            );
                        }
                    }

                    // At last, process time window rows.
                    for (long i = adjustedRowLo; i < rowHi; i++) {
                        final long slaveRowId = slaveRowIds.get(i);
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        if (joinFilter.getBool(internalJoinRecord)) {
                            if (value.isNew()) {
                                groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                                value.setNew(false);
                            } else {
                                groupByFunctionsUpdater.updateExisting(value, internalJoinRecord, slaveRowId);
                            }
                        }
                    }
                } else {
                    // There are no slave rows corresponding to the time window.
                    // Let's find the prevailing value.

                    // First, check the accumulated row ids.
                    for (long i = rowLo - 1; i >= 0; i--) {
                        final long slaveRowId = slaveRowIds.get(i);
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        if (joinFilter.getBool(internalJoinRecord)) {
                            groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                            value.setNew(false);
                            needToFindPrevailing = false;
                            break;
                        }
                    }
                    // If the prevailing row is not there, we have to do the backward scan.
                    if (needToFindPrevailing) {
                        findPrevailingForMasterRow(
                                slaveTimeFrameHelper,
                                slaveTimeFrameHelper.getRecord(),
                                slaveSymbolIndex,
                                slaveSymbolLookupMap,
                                prevailingFrameIndex,
                                prevailingRowIndex,
                                masterKey,
                                joinFilter,
                                internalJoinRecord,
                                groupByFunctionsUpdater,
                                value
                        );
                    }
                }
            } else {
                // There are no slave rows corresponding to the master page frame for the symbol.
                // Let's find the prevailing value.
                findPrevailingForMasterRow(
                        slaveTimeFrameHelper,
                        slaveTimeFrameHelper.getRecord(),
                        slaveSymbolIndex,
                        slaveSymbolLookupMap,
                        prevailingFrameIndex,
                        prevailingRowIndex,
                        masterKey,
                        joinFilter,
                        internalJoinRecord,
                        groupByFunctionsUpdater,
                        value
                );
            }

            return true;
        }
    }

    private class WindowJoinWithPrevailingFastRecordCursor extends WindowJoinFastRecordCursor {
        private final WindowJoinPrevailingCache prevailingCache;

        public WindowJoinWithPrevailingFastRecordCursor(
                @NotNull CairoConfiguration configuration,
                @Nullable IntList columnIndex,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull GroupByFunctionsUpdater groupByFunctionsUpdater,
                @NotNull SimpleMapValue value,
                int valueCount
        ) {
            super(
                    configuration,
                    columnIndex,
                    columnSplit,
                    masterTimestampIndex,
                    slaveTimestampIndex,
                    masterTimestampType,
                    slaveTimestampType,
                    groupByFunctions,
                    groupByFunctionsUpdater,
                    value,
                    valueCount
            );

            this.prevailingCache = new WindowJoinPrevailingCache();
        }

        @Override
        public void close() {
            super.close();
            Misc.free(prevailingCache);
        }

        @Override
        public boolean hasNext() {
            if (!masterCursor.hasNext()) {
                return false;
            }

            // We build the timestamp interval over which we will aggregate the matching slave rows [slaveTimestampLo; slaveTimestampHi]
            long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            long slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTimestampScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi * INDEX_LOOKAHEAD, masterTimestampScale);
            long masterTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            if (masterTimestampHi > lastSlaveTimestamp) {
                slaveData.clear();
                slaveAllocator.clear();
                lastSlaveTimestamp = Long.MIN_VALUE;
                long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
                final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
                final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
                prevailingCache.of(prevailingFrameIndex, prevailingRowIndex);

                if (slaveRowIndex != Long.MIN_VALUE) {
                    long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                    for (; ; ) {
                        slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (slaveTimestamp > slaveTimestampHi) {
                            break;
                        }

                        lastSlaveTimestamp = slaveTimestamp;
                        final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                        final int matchingMasterKey = slaveSymbolLookupMap.get(toSymbolMapKey(slaveKey));
                        if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                            final int idx = toSymbolMapKey(matchingMasterKey);
                            slaveTimestamps.of(slaveData.get(idx, 1));
                            slaveRowIds.of(slaveData.get(idx, 0));
                            slaveTimestamps.add(slaveTimestamp);
                            slaveRowIds.add(baseSlaveRowId + slaveRowIndex);
                            slaveData.put(idx, 1, slaveTimestamps.ptr());
                            slaveData.put(idx, 0, slaveRowIds.ptr());
                        }

                        if (++slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                            if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                                break;
                            }
                            slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                            baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                            // don't forget to switch the record to the new frame
                            slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                        }
                    }
                }
            }

            groupByFunctionsUpdater.updateEmpty(value);
            value.setNew(true);

            final int masterKey = internalJoinRecord.getInt(masterSymbolIndex);
            final int idx = toSymbolMapKey(masterKey);
            final long rowIdsPtr = slaveData.get(idx, 0);
            final long timestampsPtr = slaveData.get(idx, 1);

            if (rowIdsPtr != 0) {
                slaveRowIds.of(rowIdsPtr);
                assert slaveRowIds.size() > 0;
                slaveTimestamps.of(timestampsPtr);
                assert slaveTimestamps.size() > 0;

                long rowLo = slaveData.get(idx, 2);
                rowLo = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), slaveTimestampLo, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_UP);
                rowLo = rowLo < 0 ? -rowLo - 1 : rowLo;
                slaveData.put(idx, 2, rowLo);
                long rowHi = Vect.binarySearch64Bit(slaveTimestamps.dataPtr(), masterTimestampHi, rowLo, slaveTimestamps.size() - 1, Vect.BIN_SEARCH_SCAN_DOWN);
                rowHi = rowHi < 0 ? -rowHi - 1 : rowHi + 1;

                if (rowLo >= rowHi || slaveTimestamps.get(rowLo) > masterTimestampHi) {
                    // No rows in the time window or the first row has timestamp higher than the low window boundary.
                    // In both cases we need to include the prevailing value.
                    if (rowLo > 0) {
                        // Yay! We already have the prevailing value in the in-memory index.
                        rowLo--;
                    } else {
                        // There are no slave rows corresponding to the time window for the symbol.
                        // Let's find the prevailing value.
                        final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                                slaveTimeFrameHelper,
                                slaveRecord,
                                slaveSymbolIndex,
                                slaveSymbolLookupMap,
                                masterKey
                        );
                        if (slaveRowId != Long.MIN_VALUE) {
                            slaveTimeFrameHelper.recordAt(slaveRowId);
                            groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                            value.setNew(false);
                        }
                    }
                }

                if (rowLo < rowHi) {
                    for (long i = rowLo; i < rowHi; i++) {
                        final long slaveRowId = slaveRowIds.get(i);
                        slaveTimeFrameHelper.recordAt(slaveRowId);
                        if (value.isNew()) {
                            groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                            value.setNew(false);
                        } else {
                            groupByFunctionsUpdater.updateExisting(value, internalJoinRecord, slaveRowId);
                        }
                    }
                }
            } else {
                // There are no slave rows corresponding to the master page frame for the symbol.
                // Let's find the prevailing value.
                final long slaveRowId = prevailingCache.findPrevailingSlaveRowId(
                        slaveTimeFrameHelper,
                        slaveRecord,
                        slaveSymbolIndex,
                        slaveSymbolLookupMap,
                        masterKey
                );
                if (slaveRowId != Long.MIN_VALUE) {
                    slaveTimeFrameHelper.recordAt(slaveRowId);
                    groupByFunctionsUpdater.updateNew(value, internalJoinRecord, slaveRowId);
                }
            }

            return true;
        }

        @Override
        public void toTop() {
            super.toTop();
            prevailingCache.clear();
        }

        @Override
        void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            prevailingCache.reopen();
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
        }
    }
}
