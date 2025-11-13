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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Single-threaded WINDOW JOIN factory.
 * <p>
 * The master cursor drives the iteration. For every master row the slave cursor is traversed only
 * within the timestamp window {@code [masterTs - windowLo, masterTs + windowHi]}, with timestamps
 * scaled to nanoseconds when master and slave use different units. Matching slave rows are passed
 * through an optional post-join filter and accumulated by the supplied {@link GroupByFunction}s
 * into a {@link SimpleMapValue} that is exposed as a synthetic slave record via {@link OuterJoinRecord}.
 */
public class WindowJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final WindowJoinRecordCursor cursor;
    private final Function filter;
    private final JoinRecordMetadata joinMetadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final long windowHi;
    private final long windowLo;

    public WindowJoinRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull JoinRecordMetadata joinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            int columnSplit,
            long windowLo,
            long windowHi,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ArrayColumnTypes columnTypes,
            @Nullable Function filter
    ) {
        super(metadata);
        assert slaveFactory.supportsTimeFrameCursor();
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.joinMetadata = joinMetadata;
        this.filter = filter;
        this.windowLo = windowLo;
        this.windowHi = windowHi;
        var masterMetadata = masterFactory.getMetadata();
        var slaveMetadata = slaveFactory.getMetadata();
        final GroupByFunctionsUpdater groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
        this.cursor = new WindowJoinRecordCursor(
                configuration,
                columnSplit,
                NullRecordFactory.getInstance(columnTypes),
                masterMetadata.getTimestampIndex(),
                slaveMetadata.getTimestampIndex(),
                masterMetadata.getTimestampType(),
                slaveMetadata.getTimestampType(),
                groupByFunctions,
                groupByFunctionsUpdater,
                columnTypes
        );
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
        if (windowLo == Long.MAX_VALUE) {
            sink.val("unbounded preceding");
        } else if (windowLo == Long.MIN_VALUE) {
            sink.val("unbounded following");
        } else if (windowLo == 0) {
            sink.val("current row");
        } else if (windowLo < 0) {
            sink.val(Math.abs(windowLo)).val(" following");
        } else {
            sink.val(windowLo).val(" preceding");
        }

        sink.attr("window hi");
        if (windowHi == Long.MAX_VALUE) {
            sink.val("unbounded following");
        } else if (windowHi == Long.MIN_VALUE) {
            sink.val("unbounded preceding");
        } else if (windowHi == 0) {
            sink.val("current row");
        } else if (windowHi < 0) {
            sink.val(Math.abs(windowHi)).val(" preceding");
        } else {
            sink.val(windowHi).val(" following");
        }

        if (filter != null) {
            sink.setMetadata(joinMetadata);
            sink.attr("join filter").val(filter);
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
        Misc.free(filter);
        Misc.free(joinMetadata);
    }

    private class WindowJoinRecordCursor implements NoRandomAccessRecordCursor {
        private final GroupByAllocator allocator;
        private final int columnSplit;
        private final int groupByCount;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private final JoinRecord joinRecord;
        private final int masterTimestampIndex;
        private final long masterTimestampScale;
        private final OuterJoinRecord record;
        private final SimpleMapValue simpleMapValue;
        private final TimeFrameHelper slaveHelper;
        private final int slaveTimestampIndex;
        private final long slaveTimestampScale;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private RecordCursor masterCursor;
        private Record masterRecord;
        private TimeFrameCursor slaveCursor;

        public WindowJoinRecordCursor(
                CairoConfiguration configuration,
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull GroupByFunctionsUpdater groupByFunctionsUpdater,
                @NotNull ArrayColumnTypes columnTypes
        ) {
            this.columnSplit = columnSplit;
            this.groupByFunctions = groupByFunctions;
            this.allocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(groupByFunctions, allocator);
            this.groupByFunctionsUpdater = groupByFunctionsUpdater;
            this.simpleMapValue = new SimpleMapValue(columnTypes.getColumnCount());
            record = new OuterJoinRecord(columnSplit, nullRecord);
            joinRecord = new JoinRecord(columnSplit);
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            isOpen = true;
            if (masterTimestampType == slaveTimestampType) {
                masterTimestampScale = slaveTimestampScale = 1L;
            } else {
                masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
                slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
            }
            this.slaveHelper = new TimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTimestampScale);
            this.groupByCount = groupByFunctions.size();
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(allocator);
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
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            // TODO(puzpuzpuz): this should be
            // return (SymbolTable) groupByFunctions.getQuick(columnIndex - columnSplit);
            return slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (!masterCursor.hasNext()) {
                return false;
            }

            // We build the timestamp interval over which we will aggregate the matching slave rows [slaveTimestampLo; slaveTimestampHi]
            long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            long slaveTimestampLo, slaveTimestampHi;
            if (windowLo == Long.MAX_VALUE) {
                slaveTimestampLo = Long.MIN_VALUE;
            } else {
                slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTimestampScale);
            }
            if (windowHi == Long.MAX_VALUE) {
                slaveTimestampHi = Long.MAX_VALUE;
            } else {
                slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);
            }

            long slaveRowId = slaveHelper.findRowLo(slaveTimestampLo, slaveTimestampHi);
            if (slaveRowId == Long.MIN_VALUE) {
                record.hasSlave(false);
                return true;
            }

            boolean first = true;
            final Record slaveRecord = slaveHelper.getRecord();
            for (; ; ) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                final long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp > slaveTimestampHi) {
                    break;
                }

                // TODO(puzpuzpuz): we never call groupByFunctionsUpdater.updateEmpty() which seems wrong
                if (filter == null || filter.getBool(joinRecord)) {
                    if (first) {
                        groupByFunctionsUpdater.updateNew(simpleMapValue, joinRecord, slaveRowId);
                        first = false;
                    } else {
                        groupByFunctionsUpdater.updateExisting(simpleMapValue, joinRecord, slaveRowId);
                    }
                }

                if (++slaveRowId >= slaveHelper.getTimeFrameRowHi()) {
                    if (!slaveHelper.nextFrame(slaveTimestampHi)) {
                        break;
                    }
                    slaveRowId = slaveHelper.getTimeFrameRowLo();
                    slaveHelper.recordAt(Rows.toRowID(slaveHelper.getTimeFrameIndex(), slaveRowId));
                } else {
                    slaveHelper.recordAtRowIndex(slaveRowId);
                }
            }

            record.hasSlave(!first);
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.newSymbolTable(columnIndex);
            }
            return slaveHelper.newSymbolTable(columnIndex - columnSplit);
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
            slaveHelper.toTop();
        }

        void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
            }
            this.masterCursor = masterCursor;
            masterRecord = masterCursor.getRecord();
            this.slaveCursor = slaveCursor;
            slaveHelper.of(slaveCursor);
            joinRecord.of(masterRecord, slaveHelper.getRecord());
            record.of(masterRecord, simpleMapValue);
            if (filter != null) {
                filter.init(this, sqlExecutionContext);
            }
            for (int i = 0; i < groupByCount; i++) {
                groupByFunctions.getQuick(i).init(this, sqlExecutionContext);
            }
            circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        }
    }
}
