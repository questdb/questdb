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

package io.questdb.test.griffin.engine;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.union.UnionSymbolCastRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;

/**
 * Shared scaffolding for the {@link UnionSymbolCastRecordCursorFactory} unit tests: a query memory
 * tracker helper, the projection builder, and a configurable STRING cursor double backed by a
 * caller-supplied {@link StaticSymbolTable}.
 */
abstract class AbstractUnionSymbolCastTest extends AbstractCairoTest {

    protected static ObjList<Function> functions(Function... functions) {
        final ObjList<Function> result = new ObjList<>(functions.length);
        for (int i = 0; i < functions.length; i++) {
            result.add(functions[i]);
        }
        return result;
    }

    /**
     * Wraps {@code base} so every base column is re-exposed as SYMBOL, mapping result column i to
     * {@code functions.getQuick(i)}.
     */
    protected static UnionSymbolCastRecordCursorFactory newSymbolProjection(
            RecordCursorFactory base,
            ObjList<Function> functions
    ) {
        final int columnCount = base.getMetadata().getColumnCount();
        final GenericRecordMetadata resultMetadata = new GenericRecordMetadata();
        final IntList columnToFunctionIndex = new IntList(columnCount);
        for (int i = 0; i < columnCount; i++) {
            resultMetadata.add(new TableColumnMetadata("s" + i, ColumnType.SYMBOL, IndexType.NONE, 0, false, null));
            columnToFunctionIndex.add(i);
        }
        return new UnionSymbolCastRecordCursorFactory(resultMetadata, base, columnToFunctionIndex, functions);
    }

    private static GenericRecordMetadata stringMetadata(int columnCount) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            metadata.add(new TableColumnMetadata("s" + i, ColumnType.STRING));
        }
        return metadata;
    }

    protected MemoryTracker acquireTracker() {
        return acquireTracker(1L << 20);
    }

    protected MemoryTracker acquireTracker(long limitBytes) {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limitBytes);
        final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                sqlExecutionContext.getSecurityContext(),
                1L,
                MemoryTrackerWorkload.QUERY
        );
        sqlExecutionContext.setMemoryTracker(tracker);
        return tracker;
    }

    protected void releaseTracker(MemoryTracker tracker) {
        sqlExecutionContext.setMemoryTracker(null);
        tracker.close();
    }

    /**
     * A single reused, non-random-access STRING cursor backed by a caller-supplied static symbol
     * table, mirroring a union leg. Exposes the lookup/close counters and failure switches the
     * union-symbol tests drive; the switches default to off, so a test that only supplies rows sees
     * a plain source.
     */
    protected static class StaticSymbolCursorFactory extends AbstractRecordCursorFactory {
        final StaticSymbolCursor cursor;

        protected StaticSymbolCursorFactory(StaticSymbolTable symbolTable, String[][] values) {
            super(stringMetadata(values[0].length));
            cursor = new StaticSymbolCursor(symbolTable, values);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        int getSymbolTableLookupCount() {
            return cursor.symbolTableLookupCount;
        }
    }

    protected static class StaticSymbolCursor implements NoRandomAccessRecordCursor {
        final StaticSymbolTable symbolTable;
        int closeCount;
        boolean isNativeKeyAccessForbidden;
        boolean isSymbolTableUnsupported;
        int symbolTableFailureColumn = -1;
        int symbolTableLookupCount;
        private final Record record = new Record() {
            @Override
            public int getInt(int col) {
                if (isNativeKeyAccessForbidden) {
                    throw new AssertionError("native source key path must not be used");
                }
                return symbolTable.keyOf(values[rowIndex][col]);
            }

            @Override
            public long getRowId() {
                return rowIndex;
            }

            @Override
            public CharSequence getStrA(int col) {
                return values[rowIndex][col];
            }

            @Override
            public CharSequence getStrB(int col) {
                // A distinct instance with equal content, so an assertion can tell the B slot
                // apart from the A slot the way a real record's A/B flyweights do.
                final String value = values[rowIndex][col];
                return value != null ? new String(value) : null;
            }

            @Override
            public int getStrLen(int col) {
                final CharSequence value = values[rowIndex][col];
                return value != null ? value.length() : -1;
            }
        };
        private final String[][] values;
        private boolean isOpen;
        private int rowIndex = -1;

        protected StaticSymbolCursor(StaticSymbolTable symbolTable, String[][] values) {
            this.symbolTable = symbolTable;
            this.values = values;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                closeCount++;
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            symbolTableLookupCount++;
            if (isSymbolTableUnsupported) {
                throw new UnsupportedOperationException("injected unsupported symbol table");
            }
            if (columnIndex == symbolTableFailureColumn) {
                throw CairoException.nonCritical().put("injected source state construction failure");
            }
            return symbolTable;
        }

        @Override
        public boolean hasNext() {
            return ++rowIndex < values.length;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return values.length;
        }

        @Override
        public void toTop() {
            isOpen = true;
            rowIndex = -1;
        }
    }
}
