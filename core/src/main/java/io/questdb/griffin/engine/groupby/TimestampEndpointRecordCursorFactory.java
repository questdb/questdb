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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

/**
 * A keyless {@code min}, {@code max}, {@code first} or {@code last} of a timestamp column over an input of at most
 * one row: the timestamp of that row, or one NULL row when the input is empty. The cursor holds the value of its
 * single row, so it supports random access whatever its input supports.
 */
public class TimestampEndpointRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final int columnIndex;
    private final TimestampEndpointRecordCursor cursor;
    private final String functionName;

    public TimestampEndpointRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory base, String functionName, int columnIndex) {
        super(metadata);
        this.base = base;
        this.functionName = functionName;
        this.columnIndex = columnIndex;
        this.cursor = new TimestampEndpointRecordCursor(columnIndex);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base.getCursor(executionContext), executionContext.getCircuitBreaker());
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Timestamp Endpoint");
        sink.attr("value").val(functionName).val('(').putBaseColumnName(columnIndex).val(')');
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        base.close();
    }

    private static class TimestampEndpointRecordCursor implements RecordCursor {
        private final int columnIndex;
        private final TimestampEndpointRecord record = new TimestampEndpointRecord();
        private final TimestampEndpointRecord recordB = new TimestampEndpointRecord();
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isExhausted;
        private boolean isValueBuilt;
        private long value;

        private TimestampEndpointRecordCursor(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isExhausted) {
                counter.inc();
                isExhausted = true;
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public boolean hasNext() {
            if (isExhausted) {
                return false;
            }
            if (!isValueBuilt) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                value = baseCursor.hasNext() ? baseCursor.getRecord().getTimestamp(columnIndex) : Numbers.LONG_NULL;
                isValueBuilt = true;
            }
            isExhausted = true;
            return true;
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(isValueBuilt);
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            isExhausted = false;
        }

        private void of(RecordCursor baseCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.baseCursor = baseCursor;
            this.circuitBreaker = circuitBreaker;
            isExhausted = false;
            isValueBuilt = false;
        }

        private class TimestampEndpointRecord implements Record {
            @Override
            public long getRowId() {
                return 0;
            }

            @Override
            public long getTimestamp(int col) {
                return value;
            }
        }
    }
}
