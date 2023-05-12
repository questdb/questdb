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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterOnSubQueryRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {

    private final int columnIndex;
    private final IntList columnIndexes;
    private final DataFrameRecordCursorWrapper cursor;
    private final ObjList<RowCursorFactory> cursorFactories;
    private final int[] cursorFactoriesIdx;
    private final IntObjHashMap<RowCursorFactory> factoriesA = new IntObjHashMap<>(64, 0.5, -5);
    private final IntObjHashMap<RowCursorFactory> factoriesB = new IntObjHashMap<>(64, 0.5, -5);
    private final Function filter;
    private final Record.CharSequenceFunction func;
    private final RecordCursorFactory recordCursorFactory;

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
        this.filter = filter;
        this.func = func;
        cursorFactories = new ObjList<>();
        cursorFactoriesIdx = new int[]{0};
        final DataFrameRecordCursorImpl dataFrameRecordCursor = new DataFrameRecordCursorImpl(
                new HeapRowCursorFactory(cursorFactories, cursorFactoriesIdx),
                false,
                filter,
                columnIndexes
        );
        cursor = new DataFrameRecordCursorWrapper(dataFrameRecordCursor);
        this.columnIndex = columnIndex;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("FilterOnSubQuery");
        sink.optAttr("filter", filter);
        sink.child(recordCursorFactory);
        sink.child(dataFrameCursorFactory);
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(filter);
        recordCursorFactory.close();
        factoriesA.clear();
        factoriesB.clear();
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }

    private class DataFrameRecordCursorWrapper implements RecordCursor {

        private final DataFrameRecordCursor delegate;
        private RecordCursor baseCursor;
        private IntObjHashMap<RowCursorFactory> factories;
        private IntObjHashMap<RowCursorFactory> targetFactories;

        private DataFrameRecordCursorWrapper(DataFrameRecordCursor delegate) {
            this.delegate = delegate;
            this.factories = factoriesA;
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            delegate.close();
        }

        @Override
        public Record getRecord() {
            return delegate.getRecord();
        }

        @Override
        public Record getRecordB() {
            return delegate.getRecordB();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (baseCursor != null) {
                buildFactories();
                baseCursor = Misc.free(baseCursor);
            }
            return delegate.hasNext();
        }

        @Override
        public boolean isUsingIndex() {
            return delegate.isUsingIndex();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return delegate.newSymbolTable(columnIndex);
        }

        public void of(DataFrameCursor cursor, SqlExecutionContext executionContext) throws SqlException {
            if (baseCursor != null) {
                baseCursor = Misc.free(baseCursor);
            }
            baseCursor = recordCursorFactory.getCursor(executionContext);
            if (factories == factoriesA) {
                targetFactories = factoriesB;
            } else {
                targetFactories = factoriesA;
            }
            cursorFactories.clear();
            targetFactories.clear();
            delegate.of(cursor, executionContext);
            if (filter != null) {
                filter.init(delegate, executionContext);
            }
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            delegate.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public boolean skipTo(long rowCount) {
            return delegate.skipTo(rowCount);
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }

        private void buildFactories() {
            final StaticSymbolTable symbolTable = delegate.getDataFrameCursor().getSymbolTable(columnIndex);
            final Record record = baseCursor.getRecord();
            while (baseCursor.hasNext()) {
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
                                rowCursorFactory = new SymbolIndexRowCursorFactory(
                                        columnIndex,
                                        symbolKey,
                                        false,
                                        BitmapIndexReader.DIR_FORWARD,
                                        null
                                );
                            } else {
                                rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(
                                        columnIndex,
                                        symbolKey,
                                        filter,
                                        false,
                                        BitmapIndexReader.DIR_FORWARD,
                                        columnIndexes,
                                        null
                                );
                            }
                        }

                        targetFactories.putAt(targetIndex, symbolKey, rowCursorFactory);
                        cursorFactories.add(rowCursorFactory);
                    }
                }
            }

            factories.clear();
            factories = targetFactories;
            cursorFactoriesIdx[0] = cursorFactories.size();
        }
    }
}
