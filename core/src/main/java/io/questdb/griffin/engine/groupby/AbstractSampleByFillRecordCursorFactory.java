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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractSampleByFillRecordCursorFactory extends AbstractSampleByRecordCursorFactory {
    protected ObjList<GroupByFunction> groupByFunctions;
    // factory keeps a reference but allocation lifecycle is governed by cursor
    protected Map map;
    protected final RecordSink mapSink;

    public AbstractSampleByFillRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            Function timezoneNameFunc,
            Function offsetFunc,
            Function sampleFromFunc,
            Function sampleToFunc
    ) {
        super(base, groupByMetadata, recordFunctions, timezoneNameFunc, offsetFunc, sampleFromFunc, sampleToFunc);
        try {
            this.groupByFunctions = groupByFunctions;
            // sink will be storing record columns to map key
            mapSink = RecordSinkFactory.getInstance(configuration, asm, base.getMetadata(), listColumnFilter);
            // this is the map itself, which we must not forget to free when factory closes.
            // Lazy variant (openOnInit=false): the native backing is allocated by the first
            // reopen() in getCursor(), after the per-query MemoryTracker is bound, so the
            // map's malloc and the matching free at cursor close balance on the per-query counter.
            map = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes, false);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        AbstractNoRecordSampleByCursor cursor = null;
        try {
            cursor = getRawCursor();
            // Bind the active workload's MemoryTracker before reopen() so the map's
            // initial allocation is charged to it; the matching free at cursor close
            // keeps the per-query counter balanced.
            map.setMemoryTracker(executionContext.getMemoryTracker());
            if (cursor instanceof Reopenable) {
                ((Reopenable) cursor).reopen();
            }
        } catch (Throwable th) {
            Misc.free(cursor);
            throw th;
        }

        final RecordCursor baseCursor;
        try {
            baseCursor = base.getCursor(executionContext);
        } catch (Throwable th) {
            // The map was already reopened above; free the cursor so its native
            // backing is released under the bound tracker rather than lingering
            // until factory close.
            Misc.free(cursor);
            throw th;
        }
        try {
            // Init all record functions for this cursor, in case functions require metadata and/or symbol tables.
            Function.init(recordFunctions, baseCursor, executionContext, null);
        } catch (Throwable th) {
            try {
                Misc.free(baseCursor);
            } catch (Throwable cleanupTh) {
                if (cleanupTh != th) {
                    th.addSuppressed(cleanupTh);
                }
            }
            // The cursor's map was reopened before record-function initialization. Release it on
            // init failure so the tracker stays balanced and the cached cursor can reopen safely.
            try {
                Misc.free(cursor);
            } catch (Throwable cleanupTh) {
                if (cleanupTh != th) {
                    th.addSuppressed(cleanupTh);
                }
            }
            throw th;
        }

        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor);
            throw th;
        }
    }

    @Override
    protected void _close() {
        final AbstractNoRecordSampleByCursor cursor = detachRawCursor();
        this.groupByFunctions = null; // groupByFunctions are included in recordFunctions
        this.map = null; // the cursor owns the map allocation lifecycle
        Throwable failure = closeSampleByOwnersBestEffort(null);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }
}
