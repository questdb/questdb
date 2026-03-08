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

package io.questdb.griffin.engine.functions.test;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

public class TestOwnerCountingFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "test_owner_counter()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CursorFunction(new OwnerCountingCursorFactory(METADATA));
    }

    private static class OwnerCountingCursorFactory extends AbstractRecordCursorFactory {
        private final OwnerCountingRecordCursor cursor = new OwnerCountingRecordCursor();

        public OwnerCountingCursorFactory(RecordMetadata metadata) {
            super(metadata);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return cursor.init();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }

    private static class OwnerCountingRecord implements Record {
        private final AtomicInteger counter = new AtomicInteger();

        public void acquire() {
            counter.incrementAndGet();
        }

        @Override
        public int getInt(int col) {
            return counter.get();
        }

        public void release() {
            counter.decrementAndGet();
        }
    }

    private static class OwnerCountingRecordCursor implements RecordCursor {
        private final OwnerCountingRecord record = new OwnerCountingRecord();
        private int remaining = 1;

        @Override
        public void close() {
            record.release();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            return remaining-- > 0;
        }

        public RecordCursor init() {
            remaining = 1;
            record.acquire();
            return this;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            remaining = 1;
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("owners", ColumnType.INT));
        METADATA = metadata;
    }
}
