/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class AsOfJoinNoKeyRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final AsOfLightJoinRecordCursor cursor;

    public AsOfJoinNoKeyRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit

    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.cursor = new AsOfLightJoinRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex()
        );
    }

    @Override
    public void close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(
                masterFactory.getCursor(executionContext),
                slaveFactory.getCursor(executionContext)
        );
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private static class AsOfLightJoinRecordCursor implements NoRandomAccessRecordCursor {
        private final OuterJoinRecord record;
        private final int columnSplit;
        private final int masterTimestampIndex;
        private final int slaveTimestampIndex;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;
        private Record masterRecord;
        private Record slaveRecB;
        private Record slaveRecA;
        private long slaveTimestamp = Long.MIN_VALUE;
        private long latestSlaveRowID = Long.MIN_VALUE;

        public AsOfLightJoinRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex
        ) {
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.columnSplit = columnSplit;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
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
            if (masterCursor.hasNext()) {
                // great, we have a record no matter what
                final long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
                if (masterTimestamp < slaveTimestamp) {
                    return true;
                }
                nextSlave(masterTimestamp);
                return true;
            }
            return false;
        }

        private void nextSlave(long masterTimestamp) {
            if (slaveCursor.hasNext()) {
                // check where this record falls
                long slaveTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);
                if (slaveTimestamp > masterTimestamp) {
                    positionSlaveRecB();
                    latestSlaveRowID = slaveRecA.getRowId();
                    this.slaveTimestamp = slaveTimestamp;
                } else {
                    overScrollSlave(masterTimestamp, slaveTimestamp);
                }
            } else {
                slaveIsDone();
            }
        }

        private void positionSlaveRecB() {
            if (this.latestSlaveRowID != Long.MIN_VALUE) {
                record.hasSlave(true);
                slaveCursor.recordAt(slaveRecB, latestSlaveRowID);
            }
        }

        private void slaveIsDone() {
            positionSlaveRecB();
            this.slaveTimestamp = Long.MAX_VALUE;
        }

        private void overScrollSlave(long masterTimestamp, long slaveTimestamp) {
            latestSlaveRowID = slaveRecA.getRowId();
            this.slaveTimestamp = slaveTimestamp;

            // scroll slave down
            while (true) {
                if (slaveCursor.hasNext()) {
                    slaveTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);
                    if (slaveTimestamp > masterTimestamp) {
                        record.hasSlave(true);
                        slaveCursor.recordAt(slaveRecB, latestSlaveRowID);
                        latestSlaveRowID = slaveRecA.getRowId();
                        this.slaveTimestamp = slaveTimestamp;
                        break;
                    } else {
                        latestSlaveRowID = slaveRecA.getRowId();
                        this.slaveTimestamp = slaveTimestamp;
                    }
                } else {
                    record.hasSlave(true);
                    slaveCursor.recordAt(slaveRecB, latestSlaveRowID);
                    this.slaveTimestamp = Long.MAX_VALUE;
                    break;
                }
            }
        }

        @Override
        public void toTop() {
            slaveTimestamp = Long.MIN_VALUE;
            latestSlaveRowID = Long.MIN_VALUE;
            record.hasSlave(false);
            masterCursor.toTop();
            slaveCursor.toTop();
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        private void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            slaveTimestamp = Long.MIN_VALUE;
            latestSlaveRowID = Long.MIN_VALUE;
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.masterRecord = masterCursor.getRecord();
            this.slaveRecA = slaveCursor.getRecord();
            this.slaveRecB = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecB);
            record.hasSlave(false);
        }
    }
}
