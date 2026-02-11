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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

/**
 * Handles ORDER BY + LIMIT N on a single LONG or TIMESTAMP column.
 */
public class LongTopKRecordCursorFactory extends AbstractRecordCursorFactory {
    private final boolean ascending;
    private final RecordCursorFactory base;
    private final int columnIndex;
    private final LongTopKRecordCursor cursor;
    private final int lo;

    public LongTopKRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory base,
            int columnIndex,
            int lo,
            boolean ascending
    ) {
        super(metadata);
        assert lo > 0;
        assert base.recordCursorSupportsLongTopK(columnIndex);
        this.base = base;
        this.columnIndex = columnIndex;
        this.lo = lo;
        this.ascending = ascending;
        this.cursor = new LongTopKRecordCursor(columnIndex, lo, ascending);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return ascending ? SCAN_DIRECTION_FORWARD : SCAN_DIRECTION_BACKWARD;
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Long Top K");
        sink.meta("lo").val(lo);
        sink.attr("keys").val('[');
        sink.putBaseColumnName(columnIndex);
        sink.val(" ");
        if (ascending) {
            sink.val("asc");
        } else {
            sink.val("desc");
        }
        sink.val(']');
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
        Misc.free(base);
        Misc.free(cursor);
    }
}
