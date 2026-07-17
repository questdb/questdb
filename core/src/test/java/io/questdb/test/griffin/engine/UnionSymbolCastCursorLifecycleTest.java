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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.union.UnionSymbolCastRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnionSymbolCastCursorLifecycleTest extends AbstractCairoTest {

    @Test
    public void testFunctionInitFailureClosesBaseCursorAndFunctions() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final TrackingCursorFactory base = new TrackingCursorFactory(new String[][]{{"alpha", "beta"}});
            final TrackingSymbolFunction functionA = new TrackingSymbolFunction(new StrColumn(0));
            final TrackingSymbolFunction functionB = new FailingInitSymbolFunction(new StrColumn(1));
            final ObjList<Function> functions = functions(functionA, functionB);
            try (UnionSymbolCastRecordCursorFactory factory = newFactory(base, functions)) {
                try {
                    factory.getCursor(sqlExecutionContext);
                    Assert.fail("expected injected function initialization failure");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "injected function initialization failure");
                }
                assertCursorClosed(base, tracker, functionA, functionB);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testPartialSourceStateFailureClosesCursorAndFunctions() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final TrackingCursorFactory base = new TrackingCursorFactory(new String[][]{{"alpha", "beta"}});
            base.cursor.symbolTableFailureColumn = 1;
            final TrackingSymbolFunction functionA = new TrackingSymbolFunction(new StrColumn(0));
            final TrackingSymbolFunction functionB = new TrackingSymbolFunction(new StrColumn(1));
            final ObjList<Function> functions = functions(functionA, functionB);
            try (UnionSymbolCastRecordCursorFactory factory = newFactory(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    try {
                        cursor.getRecord().getInt(0);
                        Assert.fail("expected injected source state construction failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected source state construction failure");
                    }
                }
                Assert.assertEquals(2, base.cursor.symbolTableLookupCount);
                assertCursorClosed(base, tracker, functionA, functionB);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testSourceStateRegistrationFailureIsCleanAndRetryable() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final TrackingCursorFactory base = new TrackingCursorFactory(new String[][]{{"alpha"}});
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try (UnionSymbolCastRecordCursorFactory factory = newFactory(base, functions)) {
                final int[] registrationCount = {0};
                factory.setCursorTestHook(() -> {
                    if (registrationCount[0]++ == 0) {
                        throw CairoException.nonCritical().put("injected source state registration failure");
                    }
                });
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final Record record = cursor.getRecord();
                    try {
                        record.getInt(0);
                        Assert.fail("expected injected source state registration failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected source state registration failure");
                    }

                    final int key = record.getInt(0);
                    Assert.assertEquals(0, key);
                    TestUtils.assertEquals("alpha", cursor.getSymbolTable(0).valueOf(key));
                    Assert.assertTrue(tracker.getUsed() > 0);
                }
                Assert.assertEquals(2, registrationCount[0]);
                assertCursorClosed(base, tracker, function);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    @Test
    public void testUnsupportedSourceSymbolTableFallsBackToTextKeys() throws Exception {
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = acquireTracker();
            final TrackingCursorFactory base = new TrackingCursorFactory(new String[][]{
                    {"alpha"},
                    {null},
                    {"beta"},
                    {"alpha"}
            });
            base.cursor.isSymbolTableUnsupported = true;
            base.cursor.isNativeKeyAccessForbidden = true;
            final TrackingSymbolFunction function = new TrackingSymbolFunction(new StrColumn(0));
            final ObjList<Function> functions = functions(function);
            try (UnionSymbolCastRecordCursorFactory factory = newFactory(base, functions)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    final int[] expectedKeys = {0, SymbolTable.VALUE_IS_NULL, 1, 0};
                    final String[] expectedValues = {"alpha", null, "beta", "alpha"};
                    for (int i = 0; i < expectedKeys.length; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        TestUtils.assertEquals(expectedValues[i], record.getSymA(0));
                        TestUtils.assertEquals(expectedValues[i], record.getSymB(0));
                        final int key = record.getInt(0);
                        Assert.assertEquals(expectedKeys[i], key);
                        TestUtils.assertEquals(expectedValues[i], symbolTable.valueOf(key));
                        TestUtils.assertEquals(expectedValues[i], symbolTable.valueBOf(key));
                    }
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertTrue(tracker.getUsed() > 0);
                }
                Assert.assertEquals(1, base.cursor.symbolTableLookupCount);
                assertCursorClosed(base, tracker, function);
            } finally {
                releaseTracker(tracker);
            }
        });
    }

    private MemoryTracker acquireTracker() {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1L << 20);
        final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                sqlExecutionContext.getSecurityContext(),
                1L,
                MemoryTrackerWorkload.QUERY
        );
        sqlExecutionContext.setMemoryTracker(tracker);
        return tracker;
    }

    private static void assertCursorClosed(
            TrackingCursorFactory base,
            MemoryTracker tracker,
            TrackingSymbolFunction... functions
    ) {
        Assert.assertEquals(1, base.cursor.closeCount);
        for (int i = 0; i < functions.length; i++) {
            Assert.assertEquals(1, functions[i].cursorClosedCount);
        }
        Assert.assertEquals(0, tracker.getUsed());
    }

    private static ObjList<Function> functions(TrackingSymbolFunction... functions) {
        final ObjList<Function> result = new ObjList<>(functions.length);
        for (int i = 0; i < functions.length; i++) {
            result.add(functions[i]);
        }
        return result;
    }

    private static GenericRecordMetadata metadata(int columnCount, int columnType) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            metadata.add(new TableColumnMetadata("s" + i, columnType));
        }
        return metadata;
    }

    private static UnionSymbolCastRecordCursorFactory newFactory(
            TrackingCursorFactory base,
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

    private void releaseTracker(MemoryTracker tracker) {
        sqlExecutionContext.setMemoryTracker(null);
        tracker.close();
    }

    private static class FailingInitSymbolFunction extends TrackingSymbolFunction {
        private FailingInitSymbolFunction(Function arg) {
            super(arg);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            intern("init-allocation");
            throw SqlException.$(0, "injected function initialization failure");
        }
    }

    private static class TrackingCursorFactory extends AbstractRecordCursorFactory {
        private final TrackingRecordCursor cursor;

        private TrackingCursorFactory(String[][] values) {
            super(metadata(values[0].length, ColumnType.STRING));
            cursor = new TrackingRecordCursor(values);
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
    }

    private static class TrackingRecordCursor implements NoRandomAccessRecordCursor {
        private static final StaticSymbolTable SYMBOL_TABLE = new StaticSymbolTable() {
            @Override
            public boolean containsNullValue() {
                return true;
            }

            @Override
            public int getSymbolCount() {
                return 2;
            }

            @Override
            public int keyOf(CharSequence value) {
                if (value == null) {
                    return VALUE_IS_NULL;
                }
                if ("alpha".contentEquals(value)) {
                    return 100;
                }
                if ("beta".contentEquals(value)) {
                    return 200;
                }
                return VALUE_NOT_FOUND;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return valueOf(key);
            }

            @Override
            public CharSequence valueOf(int key) {
                return switch (key) {
                    case 100 -> "alpha";
                    case 200 -> "beta";
                    default -> null;
                };
            }
        };
        private final Record record = new Record() {
            @Override
            public int getInt(int col) {
                if (isNativeKeyAccessForbidden) {
                    throw new AssertionError("native source key path must not be used");
                }
                return SYMBOL_TABLE.keyOf(values[rowIndex][col]);
            }

            @Override
            public CharSequence getStrA(int col) {
                return values[rowIndex][col];
            }

            @Override
            public CharSequence getStrB(int col) {
                return values[rowIndex][col];
            }

            @Override
            public int getStrLen(int col) {
                final CharSequence value = values[rowIndex][col];
                return value != null ? value.length() : -1;
            }
        };
        private final String[][] values;
        private int closeCount;
        private boolean isNativeKeyAccessForbidden;
        private boolean isOpen;
        private boolean isSymbolTableUnsupported;
        private int rowIndex = -1;
        private int symbolTableFailureColumn = -1;
        private int symbolTableLookupCount;

        private TrackingRecordCursor(String[][] values) {
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
            return SYMBOL_TABLE;
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

    private static class TrackingSymbolFunction extends CastStrToSymbolFunctionFactory.Func {
        private int cursorClosedCount;

        private TrackingSymbolFunction(Function arg) {
            super(arg);
        }

        @Override
        public void cursorClosed() {
            try {
                super.cursorClosed();
            } finally {
                cursorClosedCount++;
            }
        }
    }
}
