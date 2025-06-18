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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public class GenerateSeriesLongRecordCursorFactory extends AbstractGenerateSeriesRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private GenerateSeriesLongRecordCursor cursor;

    public GenerateSeriesLongRecordCursorFactory(Function startFunc, Function endFunc, Function stepFunc, int position) throws SqlException {
        super(METADATA, startFunc, endFunc, stepFunc, position);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            cursor = new GenerateSeriesLongRecordCursor(startFunc, endFunc, stepFunc);
        }
        cursor.of(executionContext);
        return cursor;
    }

    private static class GenerateSeriesLongRecordCursor extends AbstractGenerateSeriesRecordCursor {
        private final GenerateSeriesLongRecord record = new GenerateSeriesLongRecord();
        private long curr;
        private long end;
        private long start;
        private long step;

        public GenerateSeriesLongRecordCursor(Function startFunc, Function endFunc, Function stepFunc) {
            super(startFunc, endFunc, stepFunc);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            curr += step;
            if (curr == Long.MIN_VALUE) {
                return false;
            }
            if (step >= 0) {
                return curr <= end;
            } else {
                return curr >= end;
            }
        }

        public void of(SqlExecutionContext executionContext) throws SqlException {
            super.of(executionContext);
            this.start = startFunc.getLong(null);
            this.end = endFunc.getLong(null);
            this.step = stepFunc.getLong(null);
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
        public long size() {
            return (Math.abs(end - start) / Math.abs(step)) + 1;
        }

        @Override
        public void toTop() {
            curr = start - step;
        }

        private class GenerateSeriesLongRecord implements Record {
            @Override
            public long getLong(int col) {
                return curr;
            }

            @Override
            public long getRowId() {
                return Math.abs(start - curr) / Math.abs(step);
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("generate_series", ColumnType.LONG));
        METADATA = metadata;
    }
}
