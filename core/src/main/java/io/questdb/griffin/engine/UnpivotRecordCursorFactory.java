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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class UnpivotRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final UnpivotRecordCursor cursor;
    private GenericRecordMetadata metadata;

    public UnpivotRecordCursorFactory(RecordCursorFactory base, Function pivotForFunc,  ObjList<Function> pivotColumnFuncs) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new UnpivotRecordCursor(pivotForFunc, pivotColumnFuncs);
        this.metadata = new GenericRecordMetadata();

        // specify metadata based on the for and column funcs

    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base, executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return SCAN_DIRECTION_OTHER; // we are generating new rows, ordering is not guaranteed
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Unpivot")
                .attr("for: ").val(cursor.pivotForFunc)
                .attr("columns: ").val(cursor.pivotColumnFuncs);

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

    public class UnpivotRecordCursor implements RecordCursor {
        Function pivotForFunc;
        ObjList<Function> pivotColumnFuncs;

        public UnpivotRecordCursor(Function pivotForFunc, ObjList<Function> pivotColumnFuncs) {

        }

        public void of(RecordCursorFactory base, SqlExecutionContext executionContext) {

        }

        @Override
        public void close() {

        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            return false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {

        }

        @Override
        public long size() throws DataUnavailableException {
            return 0;
        }

        @Override
        public void toTop() {

        }
    }
}
