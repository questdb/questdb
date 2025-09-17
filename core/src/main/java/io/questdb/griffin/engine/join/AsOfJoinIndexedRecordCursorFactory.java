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
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
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
import io.questdb.griffin.engine.table.TimeFrameRecordCursorImpl;
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
    private final int masterSymbolColumnIndex;
    private final int slaveSymbolColumnIndex;
    private final long toleranceInterval;

    public AsOfJoinIndexedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int masterSymbolColumnIndex,
            int slaveSymbolColumnIndex,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.masterSymbolColumnIndex = masterSymbolColumnIndex;
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
            // Look through per-frame symbol indexes backwards, until we find a match or exhaust the search space
            CharSequence masterSymbolValue = masterRecord.getSymA(masterSymbolColumnIndex);
            StaticSymbolTable symbolTable = slaveTimeFrameCursor.getSymbolTable(slaveSymbolColumnIndex);
            int symbolKey = symbolTable.keyOf(masterSymbolValue);
            if (symbolKey == StaticSymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                return;
            }
            // No idea why we have to increment symbolKey here... but that's what works.
            symbolKey++;

            // nextSlave() generally finds the first row with timestamp > masterTimestamp.
            // Ensure rowMax points to a row with timestamp <= masterTimestamp.
            TimeFrame timeFrame = slaveTimeFrameCursor.getTimeFrame();
            slaveTimeFrameCursor.jumpTo(timeFrame.getFrameIndex());
            slaveTimeFrameCursor.open();
            long rowMax = slaveFrameRow;
            int partitionIndex = timeFrame.getFrameIndex();
            if (rowMax == timeFrame.getRowHi()) {
                // slaveFrameRow points to one beyond the end of current frame
                rowMax--;
            } else {
                slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(partitionIndex, rowMax));
                if (slaveRecB.getTimestamp(slaveTimestampIndex) > masterTimestamp) {
                    // slaveFrameRow points to row one beyond the one with timestamp <= masterTimestamp
                    rowMax--;
                }
            }
            if (rowMax < timeFrame.getRowLo()) {
                // Not a valid row in this frame, jump to the previous frame
                if (!slaveTimeFrameCursor.prev()) {
                    record.hasSlave(false);
                    return;
                }
                slaveTimeFrameCursor.open();
            }

            // indexReader.getCursor() takes absolute row IDs, but slaveRecB uses numbering relative to
            // the first row within the BETWEEN ... AND ... range selected by the query.
            PageFrameMemoryRecord pfmRec = (PageFrameMemoryRecord) slaveRecB;
            pfmRec.setRowIndex(0);
            final long rowOffset = Rows.toLocalRowID(pfmRec.getUpdateRowId());
            final int physicalSlaveSymbolColumnIndex = slaveTimeFrameCursor.getPhysicalColumnIndex(slaveSymbolColumnIndex);
            for (; ; ) {
                BitmapIndexReader indexReader = slaveTimeFrameCursor.getBitmapIndexReader(
                        physicalSlaveSymbolColumnIndex,
                        BitmapIndexReader.DIR_BACKWARD
                );
                RowCursor rowCursor = indexReader.getCursor(false, symbolKey, timeFrame.getRowLo() + rowOffset, rowMax + rowOffset);

                // Check the first entry only. They are sorted by timestamp, so other entries are older
                if (rowCursor.hasNext()) {
                    long rowId = rowCursor.next();
                    slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(partitionIndex, rowId));
                    long slaveTimestamp = slaveRecB.getTimestamp(slaveTimestampIndex);
                    if (slaveTimestamp <= masterTimestamp) {
                        // Enforce tolerance limit if specified
                        boolean hasSlave = toleranceInterval == Numbers.LONG_NULL ||
                                slaveTimestamp >= masterTimestamp - toleranceInterval;
                        record.hasSlave(hasSlave);
                        return;
                    }
                }

                // No match in this frame, try the previous frame
                if (!slaveTimeFrameCursor.prev()) {
                    record.hasSlave(false);
                    return;
                }
                slaveTimeFrameCursor.open();
                timeFrame = slaveTimeFrameCursor.getTimeFrame();
                partitionIndex = timeFrame.getFrameIndex();
                rowMax = timeFrame.getRowHi() - 1;
            }
        }
    }
}
