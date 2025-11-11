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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized ASOF join cursor factory usable when the slave table supports TimeFrameRecordCursor.
 * <p>
 * It conceptually extends {@link AsOfJoinFastRecordCursorFactory} by adding a filter function to the slave records
 * to be applied before the join. It also optionally supports crossindex projection of the slave records.
 * <p>
 * <strong>Important</strong>: the filter is tested <strong>before</strong> applying the crossindex projection!
 */
public final class FilteredAsOfJoinFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final FilteredAsOfJoinKeyedFastRecordCursor cursor;
    private final RecordSink masterKeySink;
    private final SelectedRecordCursorFactory.SelectedTimeFrameCursor selectedTimeFrameCursor;
    private final RecordSink slaveKeySink;
    private final Function slaveRecordFilter;
    private final long toleranceInterval;
    private boolean origHasSlave;

    /**
     * Creates a new instance with filtered slave record support and optional crossindex projection.
     *
     * @param configuration         configuration
     * @param metadata              metadata of the resulting record factory
     * @param masterFactory         master record factory
     * @param masterKeySink         master key sink
     * @param slaveFactory          slave record factory
     * @param slaveKeySink          slave key sink
     * @param slaveRecordFilter     filter function for slave records (applied before projection)
     * @param columnSplit           number of columns to be split between master and slave
     * @param slaveNullRecord       null record to be used when the slave record is not found, projection is NOT applied on this record
     * @param slaveColumnCrossIndex optional crossindex for projecting the slave records, can be null
     * @param slaveTimestampIndex   timestamp column index in slave table after projection
     */
    public FilteredAsOfJoinFastRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordSink masterKeySink,
            @NotNull RecordCursorFactory slaveFactory,
            @NotNull RecordSink slaveKeySink,
            @NotNull Function slaveRecordFilter,
            int columnSplit,
            @NotNull Record slaveNullRecord,
            @Nullable IntList slaveColumnCrossIndex,
            int slaveTimestampIndex,
            long toleranceInterval
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.slaveRecordFilter = slaveRecordFilter;
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        long maxSinkTargetHeapSize = (long) configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
        this.cursor = new FilteredAsOfJoinKeyedFastRecordCursor(
                columnSplit,
                slaveNullRecord,
                masterFactory.getMetadata().getTimestampIndex(),
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                slaveTimestampIndex,
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                masterFactory.getMetadata().getTimestampType(),
                slaveFactory.getMetadata().getTimestampType(),
                configuration.getSqlAsOfJoinLookAhead()
        );
        if (slaveColumnCrossIndex != null && SelectedRecordCursorFactory.isCrossedIndex(slaveColumnCrossIndex)) {
            this.selectedTimeFrameCursor = new SelectedRecordCursorFactory.SelectedTimeFrameCursor(slaveColumnCrossIndex, slaveFactory.recordCursorSupportsRandomAccess());
        } else {
            this.selectedTimeFrameCursor = null;
        }
        this.toleranceInterval = toleranceInterval;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameRecordCursor slaveCursor = null;
        try {
            TimeFrameRecordCursor baseTimeFrameCursor = slaveFactory.getTimeFrameCursor(executionContext);
            Record filterRecord = baseTimeFrameCursor.getRecordB();
            slaveRecordFilter.init(baseTimeFrameCursor, executionContext);
            slaveCursor = selectedTimeFrameCursor == null ? baseTimeFrameCursor : selectedTimeFrameCursor.of(baseTimeFrameCursor);
            cursor.of(masterCursor, slaveCursor, filterRecord, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
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
        sink.type("Filtered AsOf Join Fast");
        sink.attr("filter").val(slaveRecordFilter, slaveFactory);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(slaveRecordFilter);
    }

    private class FilteredAsOfJoinKeyedFastRecordCursor extends AbstractAsOfJoinFastRecordCursor {
        private final SingleRecordSink masterSinkTarget;
        private final SingleRecordSink slaveSinkTarget;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private Record filterRecord;
        private int unfilteredCursorFrameIndex = -1;
        private long unfilteredRecordRowId = -1;

        public FilteredAsOfJoinKeyedFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                SingleRecordSink masterSinkTarget,
                int slaveTimestampIndex,
                SingleRecordSink slaveSinkTarget,
                int masterTimestampType,
                int slaveTimestampType,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
            this.masterSinkTarget = masterSinkTarget;
            this.slaveSinkTarget = slaveSinkTarget;
        }

        @Override
        public void close() {
            super.close();
            masterSinkTarget.close();
            slaveSinkTarget.close();
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (!masterHasNext) {
                return false;
            }

            final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
            TimeFrame timeFrame = slaveTimeFrameCursor.getTimeFrame();
            record.hasSlave(origHasSlave);
            if (unfilteredRecordRowId != -1 && slaveRecB.getRowId() != unfilteredRecordRowId) {
                slaveTimeFrameCursor.recordAt(slaveRecB, unfilteredRecordRowId);
            }
            if (unfilteredCursorFrameIndex != -1 && (timeFrame.getFrameIndex() != unfilteredCursorFrameIndex || !timeFrame.isOpen())) {
                slaveTimeFrameCursor.jumpTo(unfilteredCursorFrameIndex);
                slaveTimeFrameCursor.open();
            }

            if (masterTimestamp >= lookaheadTimestamp) {
                nextSlave(masterTimestamp);
                origHasSlave = record.hasSlave();
                unfilteredRecordRowId = slaveRecB.getRowId();
                unfilteredCursorFrameIndex = timeFrame.getFrameIndex();
            }

            // we have to set the `isMasterHasNextPending` only now since `nextSlave()` may throw DataUnavailableException
            // and in such case we do not want to call `masterCursor.hasNext()` during the next call to `this.hasNext()`.
            // if we are here then it's clear nextSlave() did not throw DataUnavailableException.
            isMasterHasNextPending = true;
            if (!record.hasSlave()) {
                // the non-filtering algo did not find a matching record in the slave table.
                // this means the slave table does not have a single record with a timestamp that is less than or equal
                // to the master record's timestamp.
                return true;
            }

            // ok, the non-keyed matcher found a record with a matching timestamp.
            // we have to make sure the JOIN keys match as well.
            masterSinkTarget.clear();
            masterKeySink.copy(masterRecord, masterSinkTarget);

            // first, we have to set the time frame cursor to the record found by the non-filtering algo
            // and then we have to traverse the slave cursor backwards until we find a match
            long rowId = slaveRecB.getRowId();
            final int initialFilteredFrameIndex = Rows.toPartitionIndex(rowId);
            final long initialFilteredRowId = Rows.toLocalRowID(rowId);

            slaveTimeFrameCursor.jumpTo(initialFilteredFrameIndex);
            slaveTimeFrameCursor.open();

            long currentFrameLo = timeFrame.getRowLo();
            long filteredRowId = initialFilteredRowId;

            for (; ; ) {
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (toleranceInterval != Numbers.LONG_NULL && slaveTimestamp < masterTimestamp - toleranceInterval) {
                    // we are past the tolerance interval, no need to traverse the slave cursor any further
                    record.hasSlave(false);
                    break;
                }

                if (slaveRecordFilter.getBool(filterRecord)) {
                    slaveSinkTarget.clear();
                    slaveKeySink.copy(slaveRecB, slaveSinkTarget);
                    if (masterSinkTarget.memeq(slaveSinkTarget)) {
                        // we have a match, that's awesome, no need to traverse the slave cursor!
                        break;
                    }
                }

                filteredRowId--;
                if (filteredRowId < currentFrameLo) {
                    // ops, we exhausted this frame, let's try the previous one
                    circuitBreaker.statefulThrowExceptionIfTripped(); // check if we are still alive

                    if (!slaveTimeFrameCursor.prev()) {
                        // there is no previous frame, we are done, no match :(
                        // if we are here, chances are we are also pretty slow because we are scanning the entire slave cursor
                        // until we either exhaust the cursor or find a matching record
                        record.hasSlave(false);
                        break;
                    }

                    slaveTimeFrameCursor.open();
                    int filteredFrameIndex = timeFrame.getFrameIndex();
                    filteredRowId = timeFrame.getRowHi() - 1;
                    currentFrameLo = timeFrame.getRowLo();

                    // invariant: when exiting from this branch then either we fully exhausted the slave cursor
                    // or slaveRecB is set to the current filteredFrameIndex so the outside loop can continue
                    // searching for a match by just moving filteredRowId down
                    slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(filteredFrameIndex, filteredRowId));
                }
                slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, filteredRowId);
            }

            return true;
        }

        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, Record filterRecord, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor);
            this.circuitBreaker = circuitBreaker;
            this.filterRecord = filterRecord;
            masterSinkTarget.reopen();
            slaveSinkTarget.reopen();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void toTop() {
            super.toTop();
            slaveRecordFilter.toTop();
            unfilteredRecordRowId = -1;
            unfilteredCursorFrameIndex = -1;
            origHasSlave = false;
        }
    }
}
