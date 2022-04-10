/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates table backwards and finds latest values by single symbol column.
 * When the LATEST BY is applied to symbol column QuestDB knows all distinct symbol values
 * and in many cases can stop before scanning all the data when it finds all the expected values
 */
public class LatestByDeferredListValuesFilteredRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final ObjList<Function> symbolFunctions;
    private final Function filter;
    private final LatestByValueListRecordCursor cursor;
    private final int frameSymbolIndex;

    public LatestByDeferredListValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Nullable ObjList<Function> symbolFunctions,
            @Nullable Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory);
        this.symbolFunctions = symbolFunctions;
        this.filter = filter;
        this.frameSymbolIndex = columnIndexes.getQuick(columnIndex);
        this.cursor = new LatestByValueListRecordCursor(columnIndex, filter, columnIndexes, configuration.getDefaultSymbolCapacity(), symbolFunctions != null);
    }

    public LatestByDeferredListValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            RecordMetadata metadata,
            DataFrameCursorFactory dataFrameCursorFactory,
            int latestByIndex,
            Function filter,
            IntList columnIndexes
    ) {
        this(configuration, metadata, dataFrameCursorFactory, latestByIndex, null, filter, columnIndexes);
    }

    @Override
    public void close() {
        super.close();
        if (filter != null) {
            filter.close();
        }
        this.cursor.destroy();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        lookupDeferredSymbol(dataFrameCursor, executionContext);
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }

    private void lookupDeferredSymbol(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        // If symbol values are restricted by a list in the qyert by syntax
        // sym in ('val1', 'val2', 'val3')
        // or similar we need to resolve string values into int symbol keys to search the table faster.
        // Resolve values to int keys and save them in cursor.getSymbolKeys() set.
        if (symbolFunctions != null) {
            // Re-evaluate symbol key resolution if not all known already
            IntHashSet symbolKeys = cursor.getSymbolKeys();
            if (symbolKeys.size() < symbolFunctions.size()) {
                symbolKeys.clear();
                StaticSymbolTable symbolMapReader = dataFrameCursor.getSymbolTable(frameSymbolIndex);
                for (int i = 0, n = symbolFunctions.size(); i < n; i++) {
                    Function symbolFunc = symbolFunctions.getQuick(i);
                    symbolFunc.init(dataFrameCursor, executionContext);
                    int key = symbolMapReader.keyOf(symbolFunc.getStr(null));
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        symbolKeys.add(key);
                    }
                }
            }
        }
    }
}
