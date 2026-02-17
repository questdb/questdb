/*******************************************************************************
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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;

public class GenerateSeriesLongRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private GenerateSeriesLongRecordCursor cursor;

    public GenerateSeriesLongRecordCursorFactory(Function startFunc, Function endFunc, Function stepFunc, IntList argPositions) throws SqlException {
        super(METADATA, startFunc, endFunc, stepFunc, argPositions);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            cursor = new GenerateSeriesLongRecordCursor(startFunc, endFunc, stepFunc);
        }
        cursor.of(executionContext, stepPosition);
        return cursor;
    }

    private static class GenerateSeriesLongRecordCursor extends AbstractGenerateSeriesRecordCursor {
        private final GenerateSeriesLongRecord recordA = new GenerateSeriesLongRecord();
        private final GenerateSeriesLongRecord recordB = new GenerateSeriesLongRecord();
        private long end;
        private long start;
        private long step;

        public GenerateSeriesLongRecordCursor(Function startFunc, Function endFunc, Function stepFunc) {
            super(startFunc, endFunc, stepFunc);
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
            this.start = startFunc.getLong(null);
            this.end = endFunc.getLong(null);
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
            ((GenerateSeriesLongRecord) record).curr = start + step * (atRowId - 1);
        }

        @Override
        public long size() {
            return (Math.abs(end - start) / Math.abs(step)) + 1;
        }

        @Override
        public void skipRows(Counter rowCount) {
            long currentRowId = recordA.getRowId()
                    - 1  // one-indexed
                    - 1; // we increment at the start of hasNext()
            long rowsToSkip = Math.min(rowCount.get(), size() - currentRowId);
            long newRowId = currentRowId + rowsToSkip;
            recordAt(recordA, newRowId);
            rowCount.dec(rowsToSkip);
        }

        @Override
        public void toTop() {
            recordA.of(start - step);
        }

        private class GenerateSeriesLongRecord implements Record {
            private long curr;

            @Override
            public long getLong(int col) {
                return curr;
            }

            @Override
            public long getRowId() {
                return Math.abs(start - curr) / Math.abs(step) + 1;
            }

            public void of(long value) {
                curr = value;
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("generate_series", ColumnType.LONG));
        METADATA = metadata;
    }
}
