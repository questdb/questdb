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

package io.questdb.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.DirectIntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class SortedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ListColumnFilter sortColumnFilter;
    private RecordCursorFactory base;
    private SortedRecordCursor cursor;

    public SortedRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull RecordComparator comparator,
            @NotNull ListColumnFilter sortColumnFilter
    ) {
        super(metadata);
        this.base = base;
        this.sortColumnFilter = sortColumnFilter;
        RecordTreeChain chain = null;
        ObjList<DirectIntList> rankMaps = null;
        try {
            // Lazy variant: the chain skeleton is constructed but the
            // MemoryPages key heap is not allocated until the first cursor's
            // of() binds a MemoryTracker and calls reopen(). RecordChain is
            // lazy by construction. This keeps malloc/free symmetric on the
            // per-query counter from the very first cursor.
            chain = new RecordTreeChain(
                    metadata,
                    recordSink,
                    comparator,
                    configuration.getSqlSortKeyPageSize(),
                    configuration.getSqlSortKeyMaxBytes(),
                    configuration.getSqlSortValuePageSize(),
                    configuration.getSqlSortValueMaxBytes(),
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                    PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath(),
                    false
            );
            // Hoist rankMaps into a named local so the catch can free the
            // (native-memory-owning) list if the cursor ctor below throws after
            // createRankMaps succeeds. On success, ownership passes to the cursor.
            rankMaps = SortKeyEncoder.createRankMaps(metadata, sortColumnFilter);
            this.cursor = new SortedRecordCursor(chain, comparator, rankMaps);
        } catch (Throwable th) {
            Misc.free(chain);
            Misc.freeObjList(rankMaps);
            close();
            throw th;
        }
    }

    public static int getScanDirection(ListColumnFilter sortColumnFilter) {
        assert sortColumnFilter.size() > 0;
        return SortedRecordCursorFactory.toOrder(sortColumnFilter.get(0));
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
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return getScanDirection(sortColumnFilter);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sort");
        SortedLightRecordCursorFactory.addSortKeys(sink, sortColumnFilter);
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

    private static int toOrder(int filter) {
        if (filter >= 0) {
            return SCAN_DIRECTION_FORWARD;
        } else {
            return SCAN_DIRECTION_BACKWARD;
        }
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final SortedRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }
}
