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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates table backwards and finds the latest values for a single symbol column.
 * When the LATEST BY is applied to symbol column QuestDB knows all distinct symbol values
 * and in many cases can stop before scanning all the data when it finds all the expected values
 */
public class LatestByDeferredListValuesFilteredRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    private final int columnIndex;
    private final LatestByValueListRecordCursor cursor;
    private final ObjList<Function> excludedSymbolFuncs;
    private final Function filter;
    private final ObjList<Function> includedSymbolFuncs;

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
        super(configuration, metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.includedSymbolFuncs = includedSymbolFuncs != null ? new ObjList<>(includedSymbolFuncs) : null;
        this.excludedSymbolFuncs = excludedSymbolFuncs != null ? new ObjList<>(excludedSymbolFuncs) : null;
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
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestByDeferredListValuesFiltered");
        sink.optAttr("filter", filter);
        sink.optAttr("includedSymbols", includedSymbolFuncs);
        sink.optAttr("excludedSymbols", excludedSymbolFuncs);
        sink.child(partitionFrameCursorFactory);
    }

    private void lookupDeferredSymbols(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        // If symbol values are restricted by a list in the query by syntax
        // sym in ('val1', 'val2', 'val3')
        // or similar we need to resolve string values into int symbol keys to search the table faster.
        // Resolve values to int keys and save them in cursor.getSymbolKeys() set.
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
        // Do the same with not in keys.
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

    @Override
    protected void _close() {
        super._close();
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
}
