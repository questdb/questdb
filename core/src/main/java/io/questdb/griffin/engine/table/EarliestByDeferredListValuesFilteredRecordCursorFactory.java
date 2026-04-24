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
 * Iterates table forwards and finds the earliest values for a single symbol column.
 * When the EARLIEST BY is applied to a symbol column QuestDB knows all distinct symbol values
 * and in many cases can stop before scanning all the data when it finds all the expected values.
 */
public class EarliestByDeferredListValuesFilteredRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    private final int columnIndex;
    private final EarliestByValueListRecordCursor cursor;
    private final ObjList<Function> excludedSymbolFuncs;
    private final Function filter;
    private final ObjList<Function> includedSymbolFuncs;

    public EarliestByDeferredListValuesFilteredRecordCursorFactory(
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
        // Normalize empty lists to null so lookupDeferredSymbols and the cursor's
        // restriction flags stay in agreement (otherwise the cursor would leave its
        // symbol key set unallocated while lookupDeferredSymbols dereferences it).
        this.includedSymbolFuncs = includedSymbolFuncs != null && includedSymbolFuncs.size() > 0
                ? new ObjList<>(includedSymbolFuncs)
                : null;
        this.excludedSymbolFuncs = excludedSymbolFuncs != null && excludedSymbolFuncs.size() > 0
                ? new ObjList<>(excludedSymbolFuncs)
                : null;
        this.filter = filter;
        this.columnIndex = columnIndex;
        cursor = new EarliestByValueListRecordCursor(
                configuration,
                metadata,
                columnIndex,
                filter,
                configuration.getDefaultSymbolCapacity(),
                this.includedSymbolFuncs != null,
                this.excludedSymbolFuncs != null
        );
    }

    public EarliestByDeferredListValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int earliestByIndex,
            Function filter,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        this(configuration, metadata, partitionFrameCursorFactory, earliestByIndex, null, null, filter, columnIndexes, columnSizeShifts);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("EarliestByDeferredListValuesFiltered");
        sink.optAttr("filter", filter);
        sink.optAttr("includedSymbols", includedSymbolFuncs);
        sink.optAttr("excludedSymbols", excludedSymbolFuncs);
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    protected void _close() {
        super._close();
        // This factory takes ownership of the cloned deferred symbol function lists;
        // release any closeable resources they hold.
        Misc.freeObjList(includedSymbolFuncs);
        Misc.freeObjList(excludedSymbolFuncs);
        Misc.free(filter);
        Misc.free(cursor);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        lookupDeferredSymbols(pageFrameCursor, executionContext);
        cursor.of(pageFrameCursor, executionContext);
        return cursor;
    }

    private void lookupDeferredSymbols(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        if (includedSymbolFuncs != null) {
            IntHashSet symbolKeys = cursor.getIncludedSymbolKeys();
            symbolKeys.clear();
            StaticSymbolTable symbolMapReader = pageFrameCursor.getSymbolTable(columnIndex);
            for (int i = 0, n = includedSymbolFuncs.size(); i < n; i++) {
                Function symbolFunc = includedSymbolFuncs.getQuick(i);
                symbolFunc.init(pageFrameCursor, executionContext);
                int key = symbolMapReader.keyOf(symbolFunc.getStrA(null));
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(key);
                }
            }
        }
        if (excludedSymbolFuncs != null) {
            IntHashSet symbolKeys = cursor.getExcludedSymbolKeys();
            symbolKeys.clear();
            final StaticSymbolTable symbolMapReader = pageFrameCursor.getSymbolTable(columnIndex);
            for (int i = 0, n = excludedSymbolFuncs.size(); i < n; i++) {
                Function symbolFunc = excludedSymbolFuncs.getQuick(i);
                symbolFunc.init(pageFrameCursor, executionContext);
                int key = symbolMapReader.keyOf(symbolFunc.getStrA(null));
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(key);
                }
            }
        }
    }
}
