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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates table backwards and finds the latest values for a single symbol column.
 * When the LATEST BY is applied to symbol column QuestDB knows all distinct symbol values
 * and in many cases can stop before scanning all the data when it finds all the expected values
 */
public class LatestByDeferredListValuesFilteredRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    private final int columnIndex;
    private final IntList excludedSymbolKeyCache;
    private final IntList includedSymbolKeyCache;
    private LatestByValueListRecordCursor cursor;
    private ObjList<Function> excludedSymbolFuncs;
    private Function filter;
    private ObjList<Function> includedSymbolFuncs;

    public LatestByDeferredListValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @Transient @Nullable ObjList<Function> includedSymbolFuncs,
            @Transient @Nullable ObjList<Function> excludedSymbolFuncs,
            @Nullable Function filter,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.includedSymbolFuncs = includedSymbolFuncs != null ? new ObjList<>(includedSymbolFuncs) : null;
        this.excludedSymbolFuncs = excludedSymbolFuncs != null ? new ObjList<>(excludedSymbolFuncs) : null;
        this.includedSymbolKeyCache = newSymbolKeyCache(includedSymbolFuncs);
        this.excludedSymbolKeyCache = newSymbolKeyCache(excludedSymbolFuncs);
        this.filter = filter;
        this.columnIndex = columnIndex;
        cursor = new LatestByValueListRecordCursor(
                configuration,
                metadata,
                columnIndex,
                filter,
                configuration.getDefaultSymbolCapacity(),
                includedSymbolFuncs != null && includedSymbolFuncs.size() > 0,
                excludedSymbolFuncs != null && excludedSymbolFuncs.size() > 0
        );
    }

    public LatestByDeferredListValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int latestByIndex,
            Function filter,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        this(configuration, metadata, partitionFrameCursorFactory, latestByIndex, null, null, filter, columnIndexes, columnSizeShifts);
    }

    @Override
    public boolean usesCompiledFilter() {
        return filter instanceof LatestByCompiledFilter;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestByDeferredListValuesFiltered");
        LatestByCompiledFilter.addJitAttr(sink, filter);
        sink.optAttr("filter", filter);
        sink.optAttr("includedSymbols", includedSymbolFuncs);
        sink.optAttr("excludedSymbols", excludedSymbolFuncs);
        sink.child(partitionFrameCursorFactory);
    }

    private static @Nullable IntList newSymbolKeyCache(@Nullable ObjList<Function> functions) {
        if (functions == null) {
            return null;
        }
        final IntList cache = new IntList(functions.size());
        cache.setAll(functions.size(), SymbolTable.VALUE_NOT_FOUND);
        return cache;
    }

    private void lookupDeferredSymbols(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        if (excludedSymbolFuncs != null) {
            resolveSymbolKeys(excludedSymbolFuncs, excludedSymbolKeyCache, cursor.getExcludedSymbolKeys(), null, true, pageFrameCursor, executionContext);
        }
        if (includedSymbolFuncs != null) {
            final IntHashSet excludedKeys = excludedSymbolFuncs != null ? cursor.getExcludedSymbolKeys() : null;
            resolveSymbolKeys(includedSymbolFuncs, includedSymbolKeyCache, cursor.getIncludedSymbolKeys(), excludedKeys, false, pageFrameCursor, executionContext);
        }
    }

    private void resolveSymbolKeys(
            ObjList<Function> functions,
            IntList keyCache,
            IntHashSet keys,
            @Nullable IntHashSet excludedKeys,
            boolean isNullKeyKept,
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        keys.clear();
        final StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(columnIndex);
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            function.init(pageFrameCursor, executionContext);
            final int key = symbolTable.keyOf(function.getStrA(null), keyCache.getQuick(i));
            keyCache.setQuick(i, key);
            if (key != SymbolTable.VALUE_NOT_FOUND
                    && (isNullKeyKept || key != SymbolTable.VALUE_IS_NULL || symbolTable.containsNullValue())
                    && (excludedKeys == null || excludedKeys.excludes(key))) {
                keys.add(key);
            }
        }
    }

    @Override
    protected void _close() {
        final LatestByValueListRecordCursor cursor = this.cursor;
        this.cursor = null;
        final ObjList<Function> excludedSymbolFuncs = this.excludedSymbolFuncs;
        this.excludedSymbolFuncs = null;
        final Function filter = this.filter;
        this.filter = null;
        final ObjList<Function> includedSymbolFuncs = this.includedSymbolFuncs;
        this.includedSymbolFuncs = null;
        Throwable failure = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, filter);
        failure = Misc.freeObjListBestEffort(failure, excludedSymbolFuncs);
        failure = Misc.freeObjListBestEffort(failure, includedSymbolFuncs);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        lookupDeferredSymbols(pageFrameCursor, executionContext);
        try {
            cursor.of(pageFrameCursor, executionContext);
        } catch (Throwable th) {
            // free partial allocations under the still-bound per-query tracker on a failed open
            cursor.close();
            throw th;
        }
        return cursor;
    }
}
