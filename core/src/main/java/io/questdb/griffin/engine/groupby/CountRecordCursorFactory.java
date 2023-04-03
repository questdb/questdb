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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class CountRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final GenericRecordMetadata DEFAULT_COUNT_METADATA = new GenericRecordMetadata();
    private final RecordCursorFactory base;
    private final CountRecordCursor cursor = new CountRecordCursor();

    public CountRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory base) {
        super(metadata);
        this.base = base;
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
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Count");
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    protected void _close() {
        base.close();
    }

    private static class CountRecordCursor implements NoRandomAccessRecordCursor {
        private final CountRecord countRecord = new CountRecord();
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long count;
        private boolean hasNext = true;

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return countRecord;
        }

        @Override
        public boolean hasNext() {
            if (baseCursor != null) {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    count++;
                }
                baseCursor = Misc.free(baseCursor);
            }
            if (hasNext) {
                hasNext = false;
                return true;
            }
            return false;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            hasNext = true;
        }

        private void of(RecordCursor baseCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.baseCursor = baseCursor;
            this.circuitBreaker = circuitBreaker;

            final long size = baseCursor.size();
            if (size < 0) {
                count = 0;
            } else {
                count = size;
                this.baseCursor = Misc.free(baseCursor);
            }
            toTop();
        }

        private class CountRecord implements Record {
            @Override
            public long getLong(int col) {
                return count;
            }
        }
    }

    static {
        DEFAULT_COUNT_METADATA.add(new TableColumnMetadata("count", ColumnType.LONG));
    }
}
