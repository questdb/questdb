/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterOnSubQueryRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final int columnIndex;
    private final Function filter;
    private final ObjList<RowCursorFactory> cursorFactories;
    private final IntObjHashMap<RowCursorFactory> factoriesA = new IntObjHashMap<>(64, 0.5, -5);
    private final IntObjHashMap<RowCursorFactory> factoriesB = new IntObjHashMap<>(64, 0.5, -5);
    private final RecordCursorFactory recordCursorFactory;
    private IntObjHashMap<RowCursorFactory> factories;
    private final Record.CharSequenceFunction func;
    private final IntList columnIndexes;

    public FilterOnSubQueryRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull RecordCursorFactory recordCursorFactory,
            int columnIndex,
            @Nullable Function filter,
            @NotNull Record.CharSequenceFunction func,
            @NotNull IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory);
        this.recordCursorFactory = recordCursorFactory;
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.factories = factoriesA;
        cursorFactories = new ObjList<>();
        this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories), false, filter, columnIndexes);
        this.func = func;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public void close() {
        Misc.free(filter);
        recordCursorFactory.close();
        factoriesA.clear();
        factoriesB.clear();
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
        StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
        IntObjHashMap<RowCursorFactory> targetFactories;
        if (factories == factoriesA) {
            targetFactories = factoriesB;
        } else {
            targetFactories = factoriesA;
        }

        cursorFactories.clear();
        targetFactories.clear();

        try (RecordCursor cursor = recordCursorFactory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final CharSequence symbol = func.get(record, 0);
                int symbolKey = symbolTable.keyOf(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {

                    final int targetIndex = targetFactories.keyIndex(symbolKey);
                    if (targetIndex > -1) {
                        final RowCursorFactory rowCursorFactory;
                        final int index = factories.keyIndex(symbolKey);
                        if (index < 0) {
                            rowCursorFactory = factories.valueAtQuick(index);
                        } else {
                            // we could be constantly re-hashing factories, which is why
                            // we cannot reliably tell that one of them could be using cursor that
                            // belongs to index reader
                            if (filter == null) {
                                rowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, false, BitmapIndexReader.DIR_FORWARD, null);
                            } else {
                                rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(columnIndex, symbolKey, filter, false, BitmapIndexReader.DIR_FORWARD, columnIndexes, null);
                            }
                        }

                        targetFactories.putAt(targetIndex, symbolKey, rowCursorFactory);
                        cursorFactories.add(rowCursorFactory);
                    }
                }
            }
        }

        factories.clear();
        factories = targetFactories;

        if (targetFactories.size() == 0) {
            dataFrameCursor.close();
            return EmptyTableRandomRecordCursor.INSTANCE;
        }

        this.cursor.of(dataFrameCursor, executionContext);
        if (filter != null) {
            filter.init(cursor, executionContext);
        }
        return this.cursor;
    }
}
