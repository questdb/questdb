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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized ASOF join cursor factory usable when the slave table supports TimeFrameRecordCursor.
 * <p>
 * It conceptually extends {@link AsOfJoinNoKeyFastRecordCursorFactory} by adding a filter function to the slave records
 * to be applied before the join. It also optionally supports crossindex projection of the slave records.
 * <p>
 * <strong>Important</strong>: the filter is tested <strong>before</strong> applying the crossindex projection!
 */
public final class FilteredAsOfJoinNoKeyFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final FilteredAsOfJoinKeyedFastRecordCursor cursor;
    private final SelectedRecordCursorFactory.SelectedTimeFrameCursor selectedTimeFrameCursor;
    private final Function slaveRecordFilter;
    private final long toleranceInterval;

    /**
     * Creates a new instance with filtered slave record support and optional crossindex projection.
     *
     * @param configuration         configuration
     * @param metadata              metadata of the resulting record factory
     * @param masterFactory         master record factory
     * @param slaveFactory          slave record factory
     * @param slaveRecordFilter     filter function for slave records (applied before projection)
     * @param columnSplit           number of columns to be split between master and slave
     * @param slaveNullRecord       null record to be used when the slave record is not found, projection is NOT applied on this record
     * @param slaveColumnCrossIndex optional crossindex for projecting the slave records, can be null
     * @param slaveTimestampIndex   timestamp column index in slave table after projection
     */
    public FilteredAsOfJoinNoKeyFastRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
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
        this.cursor = new FilteredAsOfJoinKeyedFastRecordCursor(
                columnSplit,
                slaveNullRecord,
                masterFactory.getMetadata().getTimestampIndex(),
                slaveTimestampIndex,
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
        private SqlExecutionCircuitBreaker circuitBreaker;
        private Record filterRecord;
        private long highestKnownSlaveRowIdWithNoMatch = 0;
        private int unfilteredCursorFrameIndex = -1;
        private long unfilteredRecordRowId = -1;

        public FilteredAsOfJoinKeyedFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
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
            if (masterTimestamp >= lookaheadTimestamp) {
                if (unfilteredRecordRowId != -1 && slaveRecB.getRowId() != unfilteredRecordRowId) {
                    slaveTimeFrameCursor.recordAt(slaveRecB, unfilteredRecordRowId);
                }
                if (unfilteredCursorFrameIndex != -1 && (timeFrame.getFrameIndex() != unfilteredCursorFrameIndex || !timeFrame.isOpen())) {
                    slaveTimeFrameCursor.jumpTo(unfilteredCursorFrameIndex);
                    slaveTimeFrameCursor.open();
                }

                nextSlave(masterTimestamp);

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

            if (toleranceInterval != Numbers.LONG_NULL && scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale) < masterTimestamp - toleranceInterval) {
                // we are past the tolerance interval, no need to traverse the slave cursor any further
                record.hasSlave(false);
                return true;
            }

            if (slaveRecordFilter.getBool(filterRecord)) {
                // we have a match, that's awesome, no need to traverse the slave cursor!
                return true;
            }

            // ok, the current record in the slave cursor does not match the filter, let's try to find a match
            // first, we have to set the time frame cursor to the record found by the non-filtering algo
            // and then we have to traverse the slave cursor backwards until we find a match
            long rowId = slaveRecB.getRowId();
            final int initialFilteredFrameIndex = Rows.toPartitionIndex(rowId);
            final long initialFilteredRowId = Rows.toLocalRowID(rowId);

            slaveTimeFrameCursor.jumpTo(initialFilteredFrameIndex);
            slaveTimeFrameCursor.open();

            long currentFrameLo = timeFrame.getRowLo();
            long filteredRowId = initialFilteredRowId;
            int filteredFrameIndex = initialFilteredFrameIndex;

            int stopAtFrameIndex = Rows.toPartitionIndex(highestKnownSlaveRowIdWithNoMatch);
            long stopUnderRowId = (stopAtFrameIndex == filteredFrameIndex) ? Rows.toLocalRowID(highestKnownSlaveRowIdWithNoMatch) : 0;

            for (; ; ) {
                // let's try to move backwards in the slave cursor until we have a match
                filteredRowId--;

                if (filteredRowId < currentFrameLo) {
                    // ops, we exhausted this frame, let's try the previous one
                    circuitBreaker.statefulThrowExceptionIfTripped(); // check if we are still alive

                    if (!slaveTimeFrameCursor.prev()) {
                        // there is no previous frame, we are done, no match :(
                        // if we are here, chances are we are also pretty slow because we are scanning the entire slave cursor
                        // until we either exhaust the cursor or find a matching record
                        record.hasSlave(false);

                        // Remember that there is no matching slave record for a given initial rowId.
                        // So the next time we can abort the slave cursor traversal before we reach the end of the cursor.
                        // We increment the initial rowId by 1, because the early exit guard is exclusive.
                        highestKnownSlaveRowIdWithNoMatch = Rows.toRowID(initialFilteredFrameIndex, initialFilteredRowId + 1);
                        // note: we do not check for overflow here. localRowId has 44 bits, this is enough to store
                        // 17.5 trillion rows. we don't expect to ever reach this limit within a single partition.
                        break;
                    }

                    slaveTimeFrameCursor.open();
                    filteredFrameIndex = timeFrame.getFrameIndex();
                    filteredRowId = timeFrame.getRowHi() - 1;
                    currentFrameLo = timeFrame.getRowLo();
                    stopUnderRowId = (stopAtFrameIndex == filteredFrameIndex) ? Rows.toLocalRowID(highestKnownSlaveRowIdWithNoMatch) : 0;

                    // invariant: when exiting from this branch then either we fully exhausted the slave cursor
                    // or slaveRecB is set to the current filteredFrameIndex so the outside loop can continue
                    // searching for a match by just moving filteredRowId down
                    slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(filteredFrameIndex, filteredRowId));
                }

                if (filteredRowId < stopUnderRowId) {
                    record.hasSlave(false);
                    highestKnownSlaveRowIdWithNoMatch = Rows.toRowID(initialFilteredFrameIndex, initialFilteredRowId + 1);
                    break;
                }

                slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, filteredRowId);
                if (toleranceInterval != Numbers.LONG_NULL && scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale) < masterTimestamp - toleranceInterval) {
                    // we are past the tolerance interval, no need to traverse the slave cursor any further
                    record.hasSlave(false);
                    highestKnownSlaveRowIdWithNoMatch = Rows.toRowID(initialFilteredFrameIndex, initialFilteredRowId + 1);
                    break;
                }
                if (slaveRecordFilter.getBool(filterRecord)) {
                    // we have a match, that's awesome, no need to traverse the slave cursor!
                    break;
                }
            }

            return true;
        }

        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, Record filterRecord, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor);
            this.circuitBreaker = circuitBreaker;
            this.filterRecord = filterRecord;
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
            highestKnownSlaveRowIdWithNoMatch = 0;
        }
    }
}
