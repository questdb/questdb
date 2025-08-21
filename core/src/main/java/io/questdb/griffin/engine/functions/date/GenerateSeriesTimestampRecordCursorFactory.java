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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;

public class GenerateSeriesTimestampRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    private final TimestampDriver timestampDriver;
    private GenerateSeriesTimestampRecordCursor cursor;

    public GenerateSeriesTimestampRecordCursorFactory(int timestampType, Function startFunc, Function endFunc, Function stepFunc, IntList argPositions) throws SqlException {
        super(GenerateSeriesTimestampStringRecordCursorFactory.getMetadata(timestampType), startFunc, endFunc, stepFunc, argPositions);
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            cursor = new GenerateSeriesTimestampRecordCursor(timestampDriver, startFunc, endFunc, stepFunc);
        }
        cursor.of(executionContext, stepPosition);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        if (stepFunc.getLong(null) > 0) {
            return SCAN_DIRECTION_FORWARD;
        } else {
            return SCAN_DIRECTION_BACKWARD;
        }
    }

    private static class GenerateSeriesTimestampRecordCursor extends AbstractGenerateSeriesRecordCursor {
        private final GenerateSeriesTimestampRecord recordA = new GenerateSeriesTimestampRecord();
        private final GenerateSeriesTimestampRecord recordB = new GenerateSeriesTimestampRecord();
        private final TimestampDriver timestampDriver;
        private long end;
        private long start;
        private long step;

        public GenerateSeriesTimestampRecordCursor(TimestampDriver driver, Function startFunc, Function endFunc, Function stepFunc) {
            super(startFunc, endFunc, stepFunc);
            this.timestampDriver = driver;
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public boolean hasNext() {
            recordA.curr += step;
            if (step >= 0) {
                return recordA.curr <= end;
            } else {
                return recordA.curr >= end;
            }
        }

        public void of(SqlExecutionContext executionContext, int stepPosition) throws SqlException {
            super.of(executionContext);
            this.start = timestampDriver.from(startFunc.getTimestamp(null), ColumnType.getTimestampType(startFunc.getType()));
            this.end = timestampDriver.from(endFunc.getTimestamp(null), ColumnType.getTimestampType(endFunc.getType()));
            this.step = stepFunc.getLong(null);
            if (step == 0) {
                throw SqlException.$(stepPosition, "step cannot be zero");
            }
            // swap args round transparently if needed
            // so from/to are really a range
            if (start <= end && step < 0
                    || start >= end && step > 0) {
                final long temp = start;
                start = end;
                end = temp;
            }
            toTop();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((GenerateSeriesTimestampRecord) record).curr = start + step * (atRowId - 1);
        }

        @Override
        public long size() {
            return (Math.abs(end - start) / Math.abs(step)) + 1;
        }

        @Override
        public void skipRows(Counter rowCount) throws DataUnavailableException {
            long newRowId = recordA.getRowId() + rowCount.get()
                    - 1 // one-indexed
                    - 1 // we increment at the start of hasNext()
                    ;
            recordAt(recordA, newRowId);
        }

        @Override
        public void toTop() {
            recordA.of(start - step);
        }

        private class GenerateSeriesTimestampRecord implements Record {
            private long curr;

            @Override
            public long getLong(int col) {
                return curr;
            }

            @Override
            public long getRowId() {
                return Math.abs(start - curr) / Math.abs(step) + 1;
            }

            @Override
            public long getTimestamp(int col) {
                return curr;
            }

            public void of(long value) {
                curr = value;
            }
        }
    }
}
