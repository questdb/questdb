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

package io.questdb.griffin.engine.functions.test;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.SuspendEvent;
import io.questdb.network.SuspendEventFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class TestDataUnavailableFunctionFactory implements FunctionFactory {

    public static SuspendEventCallback eventCallback;

    @Override
    public String getSignature() {
        return "test_data_unavailable(ll)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        long totalRows = args.getQuick(0).getLong(null);
        long backoffCount = args.getQuick(1).getLong(null);
        return new CursorFunction(new DataUnavailableRecordCursorFactory(totalRows, backoffCount, sqlExecutionContext.getCircuitBreaker()));
    }

    @FunctionalInterface
    public interface SuspendEventCallback {
        void onSuspendEvent(SuspendEvent event);
    }

    private static class DataUnavailableRecordCursor implements NoRandomAccessRecordCursor {

        private final long backoffCount;
        private final SqlExecutionCircuitBreaker circuitBreaker;
        private final LongConstRecord record = new LongConstRecord();
        private final long totalRows;
        private long attempts;
        private long rows;

        public DataUnavailableRecordCursor(long totalRows, long backoffCount, SqlExecutionCircuitBreaker circuitBreaker) {
            this.totalRows = totalRows;
            this.backoffCount = backoffCount;
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            if (rows >= totalRows) {
                return false;
            }
            if (attempts++ < backoffCount) {
                SuspendEvent event = SuspendEventFactory.newInstance(DefaultIODispatcherConfiguration.INSTANCE);
                if (eventCallback != null) {
                    eventCallback.onSuspendEvent(event);
                }
                throw DataUnavailableException.instance(new TableToken("foo", "foo", 1, false, false, false), "2022-01-01", event);
            }
            rows++;
            record.of(rows);
            attempts = 0;
            return true;
        }

        public void reset() {
            rows = 0;
            attempts = 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            reset();
        }
    }

    private static class DataUnavailableRecordCursorFactory extends AbstractRecordCursorFactory {

        private static final RecordMetadata METADATA;
        private final DataUnavailableRecordCursor cursor;

        public DataUnavailableRecordCursorFactory(long totalRows, long backoffCount, SqlExecutionCircuitBreaker circuitBreaker) {
            super(METADATA);
            cursor = new DataUnavailableRecordCursor(totalRows, backoffCount, circuitBreaker);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.reset();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("test_data_unavailable");
            sink.meta("totalRows").val(cursor.totalRows);
            sink.meta("backoffCount").val(cursor.backoffCount);
        }

        static {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(0, new TableColumnMetadata("x", ColumnType.LONG));
            metadata.add(1, new TableColumnMetadata("y", ColumnType.LONG));
            metadata.add(2, new TableColumnMetadata("z", ColumnType.LONG));
            METADATA = metadata;
        }
    }

    private static class LongConstRecord implements Record {
        private long value;

        @Override
        public long getLong(int col) {
            return value;
        }

        @Override
        public long getRowId() {
            return value;
        }

        void of(long value) {
            this.value = value;
        }
    }
}
