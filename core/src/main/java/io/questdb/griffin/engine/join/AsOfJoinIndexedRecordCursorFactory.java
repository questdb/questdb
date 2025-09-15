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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

/**
 * AsOf Join factory that leverages symbol bitmap indexes for efficient row lookup.
 * This implementation uses the bitmap index to quickly find matching symbol values
 * instead of linear scanning through time-ordered records.
 */
public final class AsOfJoinIndexedRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinIndexedRecordCursor cursor;
    private final RecordSink slaveKeySink;
    private final int slaveSymbolColumnIndex;
    private final long toleranceInterval;

    public AsOfJoinIndexedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            RecordSink slaveKeySink,
            int columnSplit,
            int slaveSymbolColumnIndex,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.slaveKeySink = slaveKeySink;
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        long maxSinkTargetHeapSize = (long) configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
        this.cursor = new AsOfJoinIndexedRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                slaveFactory.getMetadata().getTimestampIndex(),
                new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                configuration.getSqlAsOfJoinLookAhead()
        );
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
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
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
        sink.type("AsOf Join Indexed Scan");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
    }

    private class AsOfJoinIndexedRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {

        public AsOfJoinIndexedRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                SingleRecordSink masterSinkTarget,
                int slaveTimestampIndex,
                SingleRecordSink slaveSinkTarget,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterSinkTarget, slaveTimestampIndex, slaveSinkTarget, lookahead);
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            // Extract the master symbol value we're looking for
            masterSinkTarget.clear();
            masterKeySink.copy(masterRecord, masterSinkTarget);

            // Get the current time frame
            TimeFrame timeFrame = slaveTimeFrameCursor.getTimeFrame();
            int partitionIndex = timeFrame.getFrameIndex();

            // 1. Get the symbol value from the master record
            CharSequence masterSymbolValue = masterRecord.getSymA(slaveSymbolColumnIndex);

            // 2. Get the slave symbol table and convert the symbol to key
            StaticSymbolTable symbolTable = slaveTimeFrameCursor.getSymbolTable(slaveSymbolColumnIndex);
            int symbolKey = symbolTable.keyOf(masterSymbolValue);

            if (symbolKey == StaticSymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                return;
            }

            try {
                // Search through frames backwards until we find a match or exhaust search space
                int cursorFrameIndex = timeFrame.getFrameIndex();
                long currentSlaveRow = slaveFrameRow;

                // slaveFrameRow may be either at the last row that is <= masterTimestamp, or one past that.
                // This is because in nextSlave(), linearScan() finds the first row with timestamp > masterTimestamp.
                slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(partitionIndex, currentSlaveRow));
                if (slaveRecB.getTimestamp(slaveTimestampIndex) > masterTimestamp) {
                    currentSlaveRow--;
                }

                for (; ; ) {
                    // 3. Access bitmap index for this partition and symbol column
                    BitmapIndexReader indexReader = slaveTimeFrameCursor.getBitmapIndexReader(
                            slaveSymbolColumnIndex,
                            BitmapIndexReader.DIR_BACKWARD
                    );

                    // 4. Get cursor for rows matching this symbol in the current time frame
                    timeFrame = slaveTimeFrameCursor.getTimeFrame();
                    int currentPartitionIndex = timeFrame.getFrameIndex();
                    long rowToCheck = currentPartitionIndex == cursorFrameIndex ?
                            currentSlaveRow :
                            timeFrame.getRowHi() - 1; // Last row in other frames

                    if (rowToCheck < timeFrame.getRowLo()) {
                        // No valid row in this frame, try previous frame
                        if (!slaveTimeFrameCursor.prev()) {
                            record.hasSlave(false);
                            slaveTimeFrameCursor.jumpTo(cursorFrameIndex);
                            slaveTimeFrameCursor.open();
                            return;
                        }
                        slaveTimeFrameCursor.open();
                        continue;
                    }

                    RowCursor rowCursor = indexReader.getCursor(false, symbolKey, rowToCheck, rowToCheck);

                    // 5. Check the single row for a match
                    if (rowCursor.hasNext()) {
                        long rowId = rowCursor.next();
                        slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(currentPartitionIndex, rowId));
                        long slaveTimestamp = slaveRecB.getTimestamp(slaveTimestampIndex);
                        if (slaveTimestamp <= masterTimestamp) {
                            // Check tolerance if specified
                            if (toleranceInterval == Numbers.LONG_NULL ||
                                    slaveTimestamp >= masterTimestamp - toleranceInterval
                            ) {
                                // Found our match
                                record.hasSlave(true);
                                return;
                            } else {
                                // Past tolerance interval, no point continuing
                                record.hasSlave(false);
                                return;
                            }
                        }
                    }

                    // No match in this frame, try previous frame
                    if (!slaveTimeFrameCursor.prev()) {
                        record.hasSlave(false);
                        return;
                    }
                    slaveTimeFrameCursor.open();
                }
            } catch (Exception e) {
                // Fallback to linear search if bitmap index access fails
                AbstractKeyedAsOfJoinRecordCursor.findMatchingRowLinear(
                        slaveTimeFrameCursor,
                        slaveRecB,
                        masterTimestamp,
                        toleranceInterval,
                        slaveTimestampIndex,
                        masterSinkTarget,
                        slaveSinkTarget,
                        slaveKeySink,
                        record,
                        circuitBreaker
                );
            }
        }
    }
}
