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
import io.questdb.std.Numbers;

public final class GenerateSeriesDoubleRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private GenerateSeriesDoubleRecordCursor cursor;

    public GenerateSeriesDoubleRecordCursorFactory(Function startFunc, Function endFunc, Function stepFunc, IntList argPositions) throws SqlException {
        super(METADATA, startFunc, endFunc, stepFunc, argPositions);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            cursor = new GenerateSeriesDoubleRecordCursor(startFunc, endFunc, stepFunc);
        }
        cursor.of(executionContext, stepPosition);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private static class GenerateSeriesDoubleRecordCursor extends AbstractGenerateSeriesRecordCursor {
        private final GenerateSeriesDoubleRecord recordA = new GenerateSeriesDoubleRecord();

        private double end;
        private double start;
        private double step;

        public GenerateSeriesDoubleRecordCursor(Function startFunc, Function endFunc, Function stepFunc) {
            super(startFunc, endFunc, stepFunc);
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public boolean hasNext() {
            recordA.kahanInc(step);
            if (Numbers.isNull(recordA.curr)) {
                return false;
            }
            if (step >= 0) {
                return recordA.curr <= end;
            } else {
                return recordA.curr >= end;
            }
        }

        public void of(SqlExecutionContext executionContext, int stepPosition) throws SqlException {
            super.of(executionContext);
            this.start = startFunc.getDouble(null);
            this.end = endFunc.getDouble(null);
            this.step = stepFunc.getDouble(null);
            if (step == 0d) {
                throw SqlException.$(stepPosition, "step cannot be zero");
            }
            // swap args round transparently if needed
            // so from/to are really a range
            if (start <= end && step < 0
                    || start >= end && step > 0) {
                final double temp = start;
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
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            recordA.curr = start;
            recordA.compensation = 0.0;
            recordA.kahanInc(-step);
        }

        private static class GenerateSeriesDoubleRecord implements Record {
            private double compensation;
            private double curr;

            @Override
            public double getDouble(int col) {
                return curr;
            }

            // Kahan summation
            public void kahanInc(double step) {
                final double y = step - compensation;
                final double t = curr + y;
                compensation = t - curr - y;
                curr = t;
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("generate_series", ColumnType.DOUBLE));
        METADATA = metadata;
    }
}
