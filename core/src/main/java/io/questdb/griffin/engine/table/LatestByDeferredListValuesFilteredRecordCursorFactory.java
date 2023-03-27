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
public class LatestByDeferredListValuesFilteredRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final LatestByValueListRecordCursor cursor;
    private final ObjList<Function> excludedSymbolFuncs;
    private final Function filter;
    private final int frameSymbolIndex;
    private final ObjList<Function> includedSymbolFuncs;

    public LatestByDeferredListValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Transient @Nullable ObjList<Function> includedSymbolFuncs,
            @Transient @Nullable ObjList<Function> excludedSymbolFuncs,
            @Nullable Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory);
        this.includedSymbolFuncs = includedSymbolFuncs != null ? new ObjList<>(includedSymbolFuncs) : null;
        this.excludedSymbolFuncs = excludedSymbolFuncs != null ? new ObjList<>(excludedSymbolFuncs) : null;
        this.filter = filter;
        frameSymbolIndex = columnIndexes.getQuick(columnIndex);
        cursor = new LatestByValueListRecordCursor(
                columnIndex,
                filter,
                columnIndexes,
                configuration.getDefaultSymbolCapacity(),
                includedSymbolFuncs != null && includedSymbolFuncs.size() > 0,
                excludedSymbolFuncs != null && excludedSymbolFuncs.size() > 0
        );
    }

    public LatestByDeferredListValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            RecordMetadata metadata,
            DataFrameCursorFactory dataFrameCursorFactory,
            int latestByIndex,
            Function filter,
            IntList columnIndexes
    ) {
        this(configuration, metadata, dataFrameCursorFactory, latestByIndex, null, null, filter, columnIndexes);
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
        sink.child(dataFrameCursorFactory);
    }

    private void lookupDeferredSymbols(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        // If symbol values are restricted by a list in the query by syntax
        // sym in ('val1', 'val2', 'val3')
        // or similar we need to resolve string values into int symbol keys to search the table faster.
        // Resolve values to int keys and save them in cursor.getSymbolKeys() set.
        if (includedSymbolFuncs != null) {
            IntHashSet symbolKeys = cursor.getIncludedSymbolKeys();
            symbolKeys.clear();
            StaticSymbolTable symbolMapReader = dataFrameCursor.getSymbolTable(frameSymbolIndex);
            for (int i = 0, n = includedSymbolFuncs.size(); i < n; i++) {
                Function symbolFunc = includedSymbolFuncs.getQuick(i);
                symbolFunc.init(dataFrameCursor, executionContext);
                int key = symbolMapReader.keyOf(symbolFunc.getStr(null));
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(key);
                }
            }
        }
        // Do the same with not in keys.
        if (excludedSymbolFuncs != null) {
            IntHashSet symbolKeys = cursor.getExcludedSymbolKeys();
            symbolKeys.clear();
            StaticSymbolTable symbolMapReader = dataFrameCursor.getSymbolTable(frameSymbolIndex);
            for (int i = 0, n = excludedSymbolFuncs.size(); i < n; i++) {
                Function symbolFunc = excludedSymbolFuncs.getQuick(i);
                symbolFunc.init(dataFrameCursor, executionContext);
                int key = symbolMapReader.keyOf(symbolFunc.getStr(null));
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
        this.cursor.destroy();
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        lookupDeferredSymbols(dataFrameCursor, executionContext);
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }
}
