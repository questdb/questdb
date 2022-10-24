/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.CharSink;

/**
 * Simple stub for returning query execution plan text as result set with one column and one row .
 */
public class ExplainPlanFactory extends AbstractRecordCursorFactory {

    private final static GenericRecordMetadata METADATA;

    static {
        METADATA = new GenericRecordMetadata();
        METADATA.add(new TableColumnMetadata("QUERY PLAN", 1, ColumnType.STRING));
    }

    private final RecordCursorFactory base;
    private final ExplainPlanRecordCursor cursor;

    public ExplainPlanFactory(RecordCursorFactory base) {
        super(METADATA);
        this.base = base;
        this.cursor = new ExplainPlanRecordCursor();
    }

    @Override
    protected void _close() {
        base.close();
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
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base);
        return cursor;
    }

    public class ExplainPlanRecordCursor implements RecordCursor {
        private final Record record;
        private final PlanSink planSink;
        private int row = 0;
        private int rowCount;

        public ExplainPlanRecordCursor() {
            this.planSink = new PlanSink();
            this.record = new ExplainPlanRecord(planSink);
        }

        public void of(RecordCursorFactory base) {
            planSink.clear();
            base.toPlan(planSink);
            planSink.end();
            rowCount = planSink.getLineCount();
            toTop();
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
            return row++ < rowCount;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            row = 0;
        }

        @Override
        public long size() {
            return rowCount;
        }
    }

    public class ExplainPlanRecord implements Record {
        private final PlanSink planSink;

        public ExplainPlanRecord(PlanSink sink) {
            this.planSink = sink;
        }

        @Override
        public CharSequence getStr(int col) {
            return planSink.getLine(cursor.row);
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(col);
        }

        @Override
        public void getStr(int col, CharSink sink) {
            sink.put(planSink.getLine(cursor.row));
        }

        @Override
        public int getStrLen(int col) {
            return planSink.getLine(cursor.row).length();
        }
    }
}
