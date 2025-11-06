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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Factory for the "light" window join cursor that feeds a {@link WindowJoinLightRecordCursor}.
 * <p>
 * The master cursor drives the iteration. For every master row the slave cursor is traversed only
 * within the timestamp window {@code [masterTs - windowLo, masterTs + windowHi]}, with timestamps
 * scaled to nanoseconds when master and slave use different units. Matching slave rows are passed
 * through an optional post-join filter and accumulated by the supplied {@link GroupByFunction}s
 * into a {@link SimpleMapValue} that is exposed as a synthetic slave record via {@link OuterJoinRecord}.
 * <p>
 * The cursor keeps a rolling list of slave row ids so the next master row can resume scanning from
 * the earliest candidate row instead of rewinding the slave cursor, keeping the implementation
 * single-pass and memory-light. When the window produces no matches the outer record signals the
 * absence of slave data, effectively yielding an outer join result.
 */
public class WindowJoinLightFilteredRecordCursorFactory extends AbstractRecordCursorFactory {
    private final WindowJoinLightRecordCursor cursor;
    private final Function filter;
    private final JoinRecordMetadata joinMetadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final long windowHi;
    private final long windowLo;

    public WindowJoinLightFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
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
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.joinMetadata = joinMetadata;
        this.filter = filter;
        this.windowLo = windowLo;
        this.windowHi = windowHi;
        var masterMetadata = masterFactory.getMetadata();
        var slaveMetadata = slaveFactory.getMetadata();
        this.cursor = new WindowJoinLightRecordCursor(
                configuration,
                columnSplit,
                NullRecordFactory.getInstance(columnTypes),
                masterMetadata.getTimestampIndex(),
                slaveMetadata.getTimestampIndex(),
                masterMetadata.getTimestampType(),
                slaveMetadata.getTimestampType(),
                groupByFunctions,
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
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
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
        sink.type("Window Join Light");

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

    private class WindowJoinLightRecordCursor extends AbstractJoinCursor {
        private final GroupByAllocator allocator;
        private final int groupByCount;
        private final @NotNull ObjList<GroupByFunction> groupByFunctions;
        private final JoinRecord joinRecord;
        private final MapValue mapValue;
        private final int masterTimestampIndex;
        private final long masterTimestampScale;
        private final OuterJoinRecord record;
        private final GroupByLongList rowIds = new GroupByLongList(16);
        private final int slaveTimestampIndex;
        private final long slaveTimestampScale;
        private boolean isOpen;
        private int lastSlaveHiRowIndex = -1;
        // Bookmark to the first slave row index that could be matched by the next master row
        private int lastSlaveLoRowIndex = -1;
        private Record masterRecord;
        private Record slaveRecord;

        public WindowJoinLightRecordCursor(
                @NotNull CairoConfiguration configuration,
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                @NotNull ObjList<GroupByFunction> groupByFunctions,
                @NotNull ArrayColumnTypes columnTypes
        ) {
            super(columnSplit);
            allocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(groupByFunctions, allocator);
            rowIds.setAllocator(allocator);
            this.groupByFunctions = groupByFunctions;
            this.mapValue = new SimpleMapValue(columnTypes.getColumnCount());
            record = new OuterJoinRecord(columnSplit, nullRecord);
            record.of(masterRecord, mapValue);
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
                allocator.close();
                rowIds.resetPtr();
                Misc.clearObjList(groupByFunctions);
                Misc.free(filter);
                super.close();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }


        @Override
        public boolean hasNext() {
            if (!masterCursor.hasNext()) {
                return false;
            }
            long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            long slaveTimestampLo = scaleTimestamp(masterTimestamp - windowLo, masterTimestampScale);
            long slaveTimestampHi = scaleTimestamp(masterTimestamp + windowHi, masterTimestampScale);
            int slaveRowIndex = lastSlaveLoRowIndex;
            long slaveRowId;
            if (slaveRowIndex == -1) {
                // The rowIds list hasn't been initialized yet, we need to fill it
                if (!slaveCursor.hasNext()) {
                    record.hasSlave(false);
                    return true;
                }
                slaveRowId = slaveRecord.getRowId();
                rowIds.add(slaveRowId);
                lastSlaveLoRowIndex = lastSlaveHiRowIndex = slaveRowIndex = 0;
            } else {
                slaveRowId = rowIds.get(slaveRowIndex);
                slaveCursor.recordAt(slaveRecord, slaveRowId);
            }
            // slaveRecord is now guaranteed to be either the first element or before masterTimestamp - windowLo
            // now we need to iterate until we arrive at the first element that is after masterTimestamp - windowLo

            long slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            while (slaveTimestamp < slaveTimestampLo) {
                if ((slaveRowId = nextSlave(++slaveRowIndex)) == Long.MIN_VALUE) {
                    record.hasSlave(false);
                    return true;
                }
                slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            }
            // todo(raphdal): implement a better data-structure (growable ring buffer?) to quickly clear rowids
            lastSlaveLoRowIndex = slaveRowIndex;

            // Each next row might be part of the current window, we need to check against every
            // record until the end of the window
            boolean first = true;
            while (slaveTimestamp <= slaveTimestampHi) {
                if (filter == null || filter.getBool(joinRecord)) {
                    if (first) {
                        for (int i = 0; i < groupByCount; i++) {
                            groupByFunctions.getQuick(i).computeFirst(mapValue, joinRecord, slaveRowId);
                        }
                        first = false;
                    } else {
                        for (int i = 0; i < groupByCount; i++) {
                            groupByFunctions.getQuick(i).computeNext(mapValue, joinRecord, slaveRowId);
                        }
                    }
                }

                if ((slaveRowId = nextSlave(++slaveRowIndex)) == Long.MIN_VALUE) {
                    break;
                }
                slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            }

            record.hasSlave(!first);

            return true;
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize() + slaveCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveCursor.toTop();
            lastSlaveHiRowIndex = lastSlaveLoRowIndex = -1;
            rowIds.clear();
        }

        /**
         * Retrieve the next slave record that will have its row id stored at {@param slaveRowIndex}
         *
         * @return the slave row id
         */
        private long nextSlave(int slaveRowIndex) {
            if (slaveRowIndex <= lastSlaveHiRowIndex) {
                long rowId = rowIds.get(slaveRowIndex);
                slaveCursor.recordAt(slaveRecord, rowId);
                return rowId;
            } else if (slaveCursor.hasNext()) {
                long rowId = slaveRecord.getRowId();
                rowIds.add(rowId);
                lastSlaveHiRowIndex = slaveRowIndex;
                return rowId;
            } else {
                return Long.MIN_VALUE;
            }
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecord();
            joinRecord.of(masterRecord, slaveRecord);
            record.of(masterRecord, mapValue);
            lastSlaveLoRowIndex = lastSlaveHiRowIndex = -1;
            if (filter != null) {
                filter.init(this, sqlExecutionContext);
            }
            for (int i = 0; i < groupByCount; i++) {
                groupByFunctions.getQuick(i).init(this, sqlExecutionContext);
            }
            rowIds.of(0);
            rowIds.clear();
        }
    }
}
