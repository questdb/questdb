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
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory.findPrevailingForMasterRow;

/**
 * Single-threaded WINDOW JOIN factory for general join conditions.
 * <p>
 * The master cursor drives the iteration. For every master row the slave cursor is traversed only
 * within the timestamp window {@code [masterTs - windowLo, masterTs + windowHi]}, with timestamps
 * scaled to nanoseconds when master and slave use different units. Matching slave rows are passed
 * through an optional post-join filter and accumulated by the supplied {@link GroupByFunction}s
 * into a {@link SimpleMapValue} that is exposed as a synthetic slave record via {@link OuterJoinRecord}.
 *
 * @see AsyncWindowJoinRecordCursorFactory for the multi-threaded variant
 */
public class WindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final WindowJoinRecordCursor cursor;
    private final boolean includePrevailing;
    private final Function joinFilter;
    private final JoinRecordMetadata joinMetadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final SimpleMapValue value;
    private final long windowHi;
    private final long windowLo;

    public WindowJoinRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull JoinRecordMetadata joinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            boolean includePrevailing,
            @Nullable IntList columnIndex,
            long windowLo,
            long windowHi,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ArrayColumnTypes columnTypes,
            @Nullable Function joinFilter
    ) {
        super(metadata);
        assert slaveFactory.supportsTimeFrameCursor();
        try {
            this.masterFactory = masterFactory;
            this.slaveFactory = slaveFactory;
            this.joinMetadata = joinMetadata;
            this.joinFilter = joinFilter;
            this.windowLo = windowLo;
            this.windowHi = windowHi;
            this.includePrevailing = includePrevailing;
            this.value = new SimpleMapValue(columnTypes.getColumnCount());
            final int columnSplit = masterFactory.getMetadata().getColumnCount();
            final RecordMetadata masterMetadata = masterFactory.getMetadata();
            final RecordMetadata slaveMetadata = slaveFactory.getMetadata();
            final GroupByFunctionsUpdater groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            if (includePrevailing && joinFilter != null) {
                this.cursor = new WindowJoinWithPrevailingAndJoinFilterRecordCursor(
                        configuration,
                        columnIndex,
                        columnSplit,
                        masterMetadata.getTimestampIndex(),
                        slaveMetadata.getTimestampIndex(),
                        masterMetadata.getTimestampType(),
                        slaveMetadata.getTimestampType(),
                        groupByFunctions,
                        groupByFunctionsUpdater,
                        value
                );
            } else {
                this.cursor = new WindowJoinRecordCursor(
                        configuration,
                        columnIndex,
                        columnSplit,
                        masterMetadata.getTimestampIndex(),
                        slaveMetadata.getTimestampIndex(),
                        masterMetadata.getTimestampType(),
                        slaveMetadata.getTimestampType(),
                        groupByFunctions,
                        groupByFunctionsUpdater,
                        value
                );
            }
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
        sink.type("Window Join");

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

    private class WindowJoinRecordCursor implements NoRandomAccessRecordCursor {
        protected final GroupByFunctionsUpdater groupByFunctionsUpdater;
        protected final JoinRecord internalJoinRecord;
        protected final int masterTimestampIndex;
        protected final long masterTimestampScale;
        protected final WindowJoinTimeFrameHelper slaveTimeFrameHelper;
        protected final int slaveTimestampIndex;
        protected final long slaveTimestampScale;
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
        protected RecordCursor masterCursor;
        protected Record masterRecord;
        private boolean isOpen;
        private TimeFrameCursor slaveCursor;

        public WindowJoinRecordCursor(
                CairoConfiguration configuration,
                @Nullable IntList columnIndex,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull GroupByFunctionsUpdater groupByFunctionsUpdater,
                @NotNull SimpleMapValue value
        ) {
            this.crossIndex = columnIndex;
            this.columnSplit = columnSplit;
            this.groupByFunctions = groupByFunctions;
            this.allocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(groupByFunctions, allocator);
            this.groupByFunctionsUpdater = groupByFunctionsUpdater;
            this.value = value;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            isOpen = true;
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
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            Misc.free(allocator);
            if (isOpen) {
                isOpen = false;
                Misc.clearObjList(groupByFunctions);
                masterCursor = Misc.free(masterCursor);
                slaveCursor = Misc.free(slaveCursor);
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
            long slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);

            groupByFunctionsUpdater.updateEmpty(value);
            value.setNew(true);

            long slaveRowIndex;
            if (includePrevailing) {
                slaveRowIndex = slaveTimeFrameHelper.findRowLoWithPrevailing(slaveTimestampLo, slaveTimestampHi);
            } else {
                slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            }
            if (slaveRowIndex == Long.MIN_VALUE) {
                return true;
            }

            final Record slaveRecord = slaveTimeFrameHelper.getRecord();
            long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
            for (; ; ) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp > slaveTimestampHi) {
                    break;
                }

                if (joinFilter == null || joinFilter.getBool(internalJoinRecord)) {
                    if (value.isNew()) {
                        groupByFunctionsUpdater.updateNew(value, internalJoinRecord, baseSlaveRowId + slaveRowIndex);
                        value.setNew(false);
                    } else {
                        groupByFunctionsUpdater.updateExisting(value, internalJoinRecord, baseSlaveRowId + slaveRowIndex);
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
            GroupByUtils.toTop(groupByFunctions);
            allocator.clear();
        }

        void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                allocator.reopen();
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.masterRecord = masterCursor.getRecord();
            joinRecord.of(masterRecord, groupByRecord);
            slaveTimeFrameHelper.of(slaveCursor);
            internalJoinRecord.of(masterRecord, slaveTimeFrameHelper.getRecord());
            joinSymbolTableSource.of(masterCursor, slaveTimeFrameHelper.getSymbolTableSource());
            if (joinFilter != null) {
                joinFilter.init(joinSymbolTableSource, sqlExecutionContext);
            }
            Function.init(groupByFunctions, joinSymbolTableSource, sqlExecutionContext, null);
            circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        }
    }

    private class WindowJoinWithPrevailingAndJoinFilterRecordCursor extends WindowJoinRecordCursor {

        public WindowJoinWithPrevailingAndJoinFilterRecordCursor(
                CairoConfiguration configuration,
                @Nullable IntList columnIndex,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull GroupByFunctionsUpdater groupByFunctionsUpdater,
                @NotNull SimpleMapValue value
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
                    value
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
            long slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);

            groupByFunctionsUpdater.updateEmpty(value);
            value.setNew(true);

            long slaveRowIndex = slaveTimeFrameHelper.findRowLo(slaveTimestampLo, slaveTimestampHi, true);
            final int prevailingFrameIndex = slaveTimeFrameHelper.getPrevailingFrameIndex();
            final long prevailingRowIndex = slaveTimeFrameHelper.getPrevailingRowIndex();
            if (slaveRowIndex == Long.MIN_VALUE) {
                findPrevailingForMasterRow(
                        slaveTimeFrameHelper,
                        prevailingFrameIndex,
                        prevailingRowIndex,
                        joinFilter,
                        internalJoinRecord,
                        groupByFunctionsUpdater,
                        value
                );
                return true;
            }

            final Record slaveRecord = slaveTimeFrameHelper.getRecord();
            long baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);

            // First, check if one of the first rows matching the join filter is also at the slaveTimestampLo timestamp.
            // If so, we don't need to do backward scan to find the prevailing row.
            boolean needToFindPrevailing = true;
            for (; ; ) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                    if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                        break;
                    }
                    slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                    baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    // don't forget to switch the record to the new frame
                    slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                }
                slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp > slaveTimestampLo) {
                    break;
                }

                slaveRowIndex++;
                if (joinFilter.getBool(internalJoinRecord)) {
                    // - 1 is here to compensate the above increment.
                    groupByFunctionsUpdater.updateNew(value, internalJoinRecord, baseSlaveRowId + slaveRowIndex - 1);
                    value.setNew(false);
                    needToFindPrevailing = false;
                    break;
                }
            }

            // Do a backward scan to find the prevailing row.
            if (needToFindPrevailing) {
                findPrevailingForMasterRow(
                        slaveTimeFrameHelper,
                        prevailingFrameIndex,
                        prevailingRowIndex,
                        joinFilter,
                        internalJoinRecord,
                        groupByFunctionsUpdater,
                        value
                );
            }

            // Aggregate the rows within the time window.
            for (; ; ) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (slaveRowIndex >= slaveTimeFrameHelper.getTimeFrameRowHi()) {
                    if (!slaveTimeFrameHelper.nextFrame(slaveTimestampHi)) {
                        break;
                    }
                    slaveRowIndex = slaveTimeFrameHelper.getTimeFrameRowLo();
                    baseSlaveRowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), 0);
                    // don't forget to switch the record to the new frame
                    slaveTimeFrameHelper.recordAt(baseSlaveRowId);
                }
                slaveTimeFrameHelper.recordAtRowIndex(slaveRowIndex);
                final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp > slaveTimestampHi) {
                    break;
                }

                if (joinFilter.getBool(internalJoinRecord)) {
                    if (value.isNew()) {
                        groupByFunctionsUpdater.updateNew(value, internalJoinRecord, baseSlaveRowId + slaveRowIndex);
                        value.setNew(false);
                    } else {
                        groupByFunctionsUpdater.updateExisting(value, internalJoinRecord, baseSlaveRowId + slaveRowIndex);
                    }
                }
                slaveRowIndex++;
            }

            return true;
        }
    }
}
