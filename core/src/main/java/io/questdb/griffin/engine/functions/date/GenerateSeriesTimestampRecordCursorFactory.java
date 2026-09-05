/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
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
        // A bind-variable step is only known at runtime, so the order cannot be guaranteed
        // at plan time. Reading getLong() off an unbound function would yield a misleading
        // direction, so report the order only for a constant step.
        if (!stepFunc.isConstant()) {
            return SCAN_DIRECTION_OTHER;
        }
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
        // Rows already handed out. The cursor walks this rather than comparing curr against
        // end: neither end of the series is bounds-checked, so every arithmetic position -
        // toTop()'s start - step included - can run off the end of the long range, and a
        // comparison cannot tell a wrapped value from an in-range one.
        private long rowIndex;
        private long size;
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
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (rowIndex >= size) {
                return false;
            }
            rowIndex++;
            // start + step * (rowIndex - 1) is the row's definition, and every row of the
            // series fits in a long even when the span between the ends does not. The
            // multiply may wrap on the way there; two's complement carries it back, so the
            // sum is exact for every row the count admits.
            recordA.curr = start + step * (rowIndex - 1);
            return true;
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
            // The span between the ends can need the 64th bit, so subtract in the direction
            // that cannot go negative and divide the result as unsigned. Math.abs(step) is
            // likewise the step's magnitude read unsigned, which is what a step of
            // Long.MIN_VALUE needs. Saturate on the +1 rather than wrapping: a count no long
            // can hold must not come back as a negative row count.
            final long steps = Long.divideUnsigned(end >= start ? end - start : start - end, Math.abs(step));
            size = steps < 0 || steps == Long.MAX_VALUE ? Long.MAX_VALUE : steps + 1;
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
            return size;
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            final long rowsToSkip = Math.max(0, Math.min(rowCount.get(), size - rowIndex));
            rowIndex += rowsToSkip;
            // Leaves the record on the last row skipped over, or on toTop()'s sentinel when
            // nothing was skipped, so a skip of 0 stays a positional no-op.
            recordAt(recordA, rowIndex);
            rowCount.dec(rowsToSkip);
        }

        @Override
        public void toTop() {
            rowIndex = 0;
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
                // Derived rather than read off the cursor, because recordB is positioned by
                // recordAt() and carries no index of its own. curr is on-grid at
                // start + step * (rowId - 1), so measuring the offset as an unsigned
                // magnitude keeps a span that overflows a signed long dividing correctly.
                // toTop()'s sentinel is the one position that is not a row, and it can wrap
                // to the far side of start - so recognise it by its exact value rather than
                // by which side of start it appears to fall on.
                if (curr == start - step) {
                    return 0;
                }
                final long offset = curr - start;
                return Long.divideUnsigned(step > 0 ? offset : -offset, Math.abs(step)) + 1;
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
