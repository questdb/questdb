/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BoolList;
import io.questdb.std.Misc;
import io.questdb.std.Rows;

public class AsOfJoinFastNoKeyRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinFastRecordCursor cursor;

    public AsOfJoinFastNoKeyRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.cursor = new AsOfJoinFastRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex()
        );
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameRecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            cursor.of(masterCursor, slaveCursor);
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
        sink.type("AsOf Join");
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    private static class AsOfJoinFastRecordCursor implements NoRandomAccessRecordCursor {
        private final int columnSplit;
        private final BoolList enteredFrames = new BoolList();
        private final int masterTimestampIndex;
        private final OuterJoinRecord record;
        private final int slaveTimestampIndex;
        private boolean isMasterHasNextPending;
        private boolean isSlaveEmpty;
        private int latestSlavePartitionIndex = -1;
        private long latestSlaveRow = Long.MIN_VALUE;
        private RecordCursor masterCursor;
        private boolean masterHasNext;
        private Record masterRecord;
        private TimeFrameRecordCursor slaveCursor;
        private Record slaveRecA;
        private Record slaveRecB;
        private long slaveTimestamp = Long.MIN_VALUE;

        public AsOfJoinFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex
        ) {
            this.columnSplit = columnSplit;
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
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
            return slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (masterHasNext) {
                // great, we have a record no matter what
                final long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
                if (masterTimestamp < slaveTimestamp) {
                    isMasterHasNextPending = true;
                    return true;
                }
                nextSlave(masterTimestamp);
                isMasterHasNextPending = true;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.newSymbolTable(columnIndex);
            }
            return slaveCursor.newSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            slaveTimestamp = Long.MIN_VALUE;
            latestSlaveRow = Long.MIN_VALUE;
            latestSlavePartitionIndex = -1;
            record.hasSlave(false);
            masterCursor.toTop();
            slaveCursor.toTop();
            isSlaveEmpty = slaveCursor.next();
            isMasterHasNextPending = true;
            enteredFrames.clear();
        }

        private void findTimeFrame(TimeFrame timeFrame, long masterTimestamp) {
            if (timeFrame.getPartitionTimestamp() > masterTimestamp) {
                // Current time frame is already after the master timestamp,
                // so we should go no further.
                latestSlavePartitionIndex = timeFrame.getPartitionIndex();
                latestSlaveRow = timeFrame.getRowLo();
                markEntered(latestSlavePartitionIndex);
                return;
            }
            // Navigate to the first time frame that contains timestamps
            // after the master timestamp and then enter the previous one.
            while (slaveCursor.next()) {
                if (timeFrame.getPartitionTimestamp() > masterTimestamp) {
                    slaveCursor.prev();
                    latestSlavePartitionIndex = timeFrame.getPartitionIndex();
                    latestSlaveRow = timeFrame.getRowLo();
                    markEntered(latestSlavePartitionIndex);
                    return;
                }
            }
            slaveTimestamp = Long.MAX_VALUE;
        }

        private boolean isEntered(int partitionIndex) {
            return enteredFrames.getQuiet(partitionIndex);
        }

        private void markEntered(int partitionIndex) {
            enteredFrames.extendAndSet(partitionIndex, true);
        }

        private void nextSlave(long masterTimestamp) {
            if (isSlaveEmpty) {
                return;
            }
            final TimeFrame timeFrame = slaveCursor.getTimeFrame();
            while (true) {
                if (latestSlavePartitionIndex >= 0 && latestSlavePartitionIndex == timeFrame.getPartitionIndex()) {
                    if (!timeFrame.isOpen()) {
                        slaveCursor.open();
                    }
                    // TODO we can do binary search for the very first row of entered time frame
                    while (latestSlaveRow < timeFrame.getRowHi()) {
                        record.hasSlave(true);
                        slaveCursor.recordAt(slaveRecB, Rows.toRowID(latestSlavePartitionIndex, latestSlaveRow));
                        slaveCursor.recordAt(slaveRecA, Rows.toRowID(latestSlavePartitionIndex, ++latestSlaveRow));
                        slaveTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);
                        if (slaveTimestamp > masterTimestamp) {
                            return;
                        }
                    }
                }
                findTimeFrame(timeFrame, masterTimestamp);
            }
        }

        private void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            slaveTimestamp = Long.MIN_VALUE;
            latestSlaveRow = Long.MIN_VALUE;
            latestSlavePartitionIndex = -1;
            masterRecord = masterCursor.getRecord();
            slaveRecA = slaveCursor.getRecord();
            slaveRecB = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecB);
            record.hasSlave(false);
            isMasterHasNextPending = true;
            enteredFrames.clear();
        }
    }
}
