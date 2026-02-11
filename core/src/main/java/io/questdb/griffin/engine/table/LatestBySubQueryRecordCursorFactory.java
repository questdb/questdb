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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestBySubQueryRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    private final int columnIndex;
    private final Function filter;
    private final Record.CharSequenceFunction func;
    private final boolean indexed;
    private final RecordCursorFactory recordCursorFactory;
    private final IntHashSet symbolKeys;

    public LatestBySubQueryRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @NotNull RecordCursorFactory recordCursorFactory,
            @Nullable Function filter,
            boolean indexed,
            @NotNull Record.CharSequenceFunction func,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(configuration, metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);

        try {
            // this instance is shared between factory and cursor
            // factory will be resolving symbols for cursor and if successful
            // symbol keys will be added to this hash set
            symbolKeys = new IntHashSet();
            this.indexed = indexed;
            PageFrameRecordCursor cursor;
            if (indexed) {
                if (filter != null) {
                    cursor = new LatestByValuesIndexedFilteredRecordCursor(configuration, metadata, columnIndex, rows, symbolKeys, null, filter);
                } else {
                    cursor = new LatestByValuesIndexedRecordCursor(configuration, metadata, columnIndex, symbolKeys, null, rows);
                }
            } else {
                if (filter != null) {
                    cursor = new LatestByValuesFilteredRecordCursor(configuration, metadata, columnIndex, rows, symbolKeys, null, filter);
                } else {
                    cursor = new LatestByValuesRecordCursor(configuration, metadata, columnIndex, rows, symbolKeys, null);
                }
            }
            this.cursor = new PageFrameRecordCursorWrapper(cursor);
            this.recordCursorFactory = recordCursorFactory;
            this.filter = filter;
            this.columnIndex = columnIndex;
            this.func = func;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestBySubQuery");
        sink.child("Subquery", recordCursorFactory);
        sink.child(cursor);
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return indexed;
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(recordCursorFactory);
        Misc.free(filter);
        Misc.free(cursor);
    }

    private class PageFrameRecordCursorWrapper implements PageFrameRecordCursor {
        private final PageFrameRecordCursor delegate;
        private RecordCursor baseCursor;

        private PageFrameRecordCursorWrapper(PageFrameRecordCursor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (baseCursor != null) {
                buildSymbolKeys();
                baseCursor = Misc.free(baseCursor);
            }

            delegate.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            delegate.close();
        }

        @Override
        public PageFrameCursor getPageFrameCursor() {
            return delegate.getPageFrameCursor();
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
                buildSymbolKeys();
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

        @Override
        public void of(PageFrameCursor cursor, SqlExecutionContext executionContext) throws SqlException {
            if (baseCursor != null) {
                baseCursor = Misc.free(baseCursor);
            }
            baseCursor = recordCursorFactory.getCursor(executionContext);
            symbolKeys.clear();
            delegate.of(cursor, executionContext);
        }

        @Override
        public long preComputedStateSize() {
            return baseCursor == null ? 1 : 0;
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
        public void skipRows(Counter rowCount) {
            if (baseCursor != null) {
                buildSymbolKeys();
                baseCursor = Misc.free(baseCursor);
            }
            delegate.skipRows(rowCount);
        }

        @Override
        public void toPlan(PlanSink sink) {
            delegate.toPlan(sink);
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }

        private void buildSymbolKeys() {
            final StaticSymbolTable symbolTable = delegate.getSymbolTable(columnIndex);
            final Record record = baseCursor.getRecord();
            StringSink sink = Misc.getThreadLocalSink();
            while (baseCursor.hasNext()) {
                int symbolKey = symbolTable.keyOf(func.get(record, 0, sink));
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(TableUtils.toIndexKey(symbolKey));
                }
            }
        }
    }
}
