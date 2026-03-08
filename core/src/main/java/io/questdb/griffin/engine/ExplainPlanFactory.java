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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.JsonPlanSink;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.model.ExplainModel;

/**
 * Simple stub for returning query execution plan text as result set with one column and one row .
 */
public class ExplainPlanFactory extends AbstractRecordCursorFactory {

    private final static GenericRecordMetadata METADATA;
    private final RecordCursorFactory base;
    private final ExplainPlanRecordCursor cursor;

    private boolean isBaseClosed;

    public ExplainPlanFactory(RecordCursorFactory base, int format) {
        super(METADATA);
        this.base = base;
        this.cursor = new ExplainPlanRecordCursor(format);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base, executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("EXPLAIN");
    }

    @Override
    protected void _close() {
        if (!isBaseClosed) {
            base.close();
            isBaseClosed = true;
        }
    }

    public class ExplainPlanRecord implements Record {
        private final PlanSink planSink;

        public ExplainPlanRecord(PlanSink sink) {
            this.planSink = sink;
        }

        @Override
        public CharSequence getStrA(int col) {
            return planSink.getLine(cursor.row);
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStrA(col);
        }

        @Override
        public int getStrLen(int col) {
            return planSink.getLine(cursor.row).length();
        }
    }

    public class ExplainPlanRecordCursor implements RecordCursor {
        private final PlanSink planSink;
        private final Record record;
        private int row = 0;
        private int rowCount;

        public ExplainPlanRecordCursor(int format) {
            if (format == ExplainModel.FORMAT_JSON) {
                this.planSink = new JsonPlanSink();
            } else {
                this.planSink = new TextPlanSink();
            }
            this.record = new ExplainPlanRecord(planSink);
        }

        @Override
        public void close() {
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
            return row++ < rowCount;
        }

        public void of(RecordCursorFactory base, SqlExecutionContext executionContext) throws SqlException {
            // open the cursor to ensure bind variable types are initialized
            try (RecordCursor ignored = base.getCursor(executionContext)) {
                planSink.of(base, executionContext);
            }
            rowCount = planSink.getLineCount();
            toTop();
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
            return rowCount;
        }

        @Override
        public void toTop() {
            row = 0;
        }
    }

    static {
        METADATA = new GenericRecordMetadata();
        METADATA.add(new TableColumnMetadata("QUERY PLAN", ColumnType.STRING));
    }
}
