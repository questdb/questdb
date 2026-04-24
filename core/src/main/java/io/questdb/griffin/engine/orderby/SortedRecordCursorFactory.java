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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
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
    // `base` is non-final because the constructor sets it to null in the catch
    // block so `_close()` does not double-free a caller-owned factory when
    // construction throws after the field has been assigned.
    private RecordCursorFactory base;
    private final SortedRecordCursor cursor;
    private final ListColumnFilter sortColumnFilter;

    public SortedRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull RecordComparator comparator,
            @NotNull ListColumnFilter sortColumnFilter
    ) {
        super(metadata);
        this.sortColumnFilter = sortColumnFilter;
        RecordTreeChain chain = null;
        ObjList<DirectIntList> rankMaps = null;
        try {
            chain = new RecordTreeChain(
                    metadata,
                    recordSink,
                    comparator,
                    configuration.getSqlSortKeyPageSize(),
                    configuration.getSqlSortKeyMaxPages(),
                    configuration.getSqlSortValuePageSize(),
                    configuration.getSqlSortValueMaxPages()
            );
            // Assign this.base only after RecordTreeChain construction succeeds,
            // so that if the allocation above throws, the catch block sees
            // this.base == null and the cascaded close() does not free the
            // caller-owned base factory. The caller retains ownership of base
            // on any constructor-throw path and frees it through its own
            // error handling.
            this.base = base;
            // Hoist rankMaps into a named local so the catch can free the
            // (native-memory-owning) list if the cursor ctor below throws after
            // createRankMaps succeeds. On success, ownership passes to the cursor.
            rankMaps = SortKeyEncoder.createRankMaps(metadata, sortColumnFilter);
            this.cursor = new SortedRecordCursor(chain, comparator, rankMaps);
            // Ownership of chain and rankMaps has transferred to the cursor.
            // Null the locals so the catch block does not double-free them if
            // any future statement between here and the closing brace throws;
            // the cursor's close() (reached via close() -> _close() -> Misc.free(cursor))
            // will handle their release.
            chain = null;
            rankMaps = null;
        } catch (Throwable th) {
            // Null the field so _close() does not double-free the caller-owned
            // base factory if the assignment above completed before the throw.
            this.base = null;
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
        Misc.free(base);
        Misc.free(cursor);
    }
}
