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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;

public class SortedLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final SortedLightRecordCursor cursor;
    private final ListColumnFilter sortColumnFilter;

    public SortedLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            RecordComparator comparator,
            ListColumnFilter sortColumnFilter
    ) {
        super(metadata);
        LongTreeChain chain = new LongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxPages(),
                configuration
                        .getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxPages());
        this.base = base;
        this.cursor = new SortedLightRecordCursor(chain, comparator);
        this.sortColumnFilter = sortColumnFilter;
    }

    public static void addSortKeys(PlanSink sink, ListColumnFilter filter) {
        sink.attr("keys").val('[');
        filter.toPlan(sink);
        sink.val(']');
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable ex) {
            baseCursor.close();
            cursor.close();
            throw ex;
        }
    }

    @Override
    public int getScanDirection() {
        return SortedRecordCursorFactory.getScanDirection(sortColumnFilter);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sort light");
        addSortKeys(sink, sortColumnFilter);
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
        cursor.close();
    }
}
