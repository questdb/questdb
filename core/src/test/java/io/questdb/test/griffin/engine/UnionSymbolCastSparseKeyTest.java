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

public class UnionSymbolCastSparseKeyTest extends AbstractCairoTest {
    private static final int SPARSE_SOURCE_KEY = 100_000_000;

    @Test
    public void testStringAccessDoesNotInitializeSourceSymbolState() throws Exception {
        assertMemoryLeak(() -> {
            final SingleSparseSymbolCursorFactory base = newSourceFactory();
            try (RecordCursorFactory factory = newFactory(base)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    TestUtils.assertEquals("sparse", cursor.getRecord().getSymA(0));
                    Assert.assertEquals(0, base.getSymbolTableLookupCount());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testSparseSourceKeyUsesCardinalitySizedTrackedCache() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    1L,
                    MemoryTrackerWorkload.QUERY
            );
            sqlExecutionContext.setMemoryTracker(tracker);
            try (RecordCursorFactory factory = newFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    final Record record = cursor.getRecord();
                    final SymbolTable symbolTable = cursor.getSymbolTable(0);
                    final int resultKey = record.getInt(0);
                    Assert.assertEquals(0, resultKey);
                    TestUtils.assertEquals("sparse", symbolTable.valueOf(resultKey));
                    Assert.assertTrue(tracker.getUsed() > 0);
                    Assert.assertTrue(tracker.getUsed() < 1024 * 1024L);

                    final long used = tracker.getUsed();
                    Assert.assertEquals(resultKey, record.getInt(0));
                    Assert.assertEquals("the cached translation must not allocate", used, tracker.getUsed());
                    Assert.assertFalse(cursor.hasNext());
                }
                Assert.assertEquals("cursor close must release the tracked cache", 0, tracker.getUsed());
            } finally {
                sqlExecutionContext.setMemoryTracker(null);
                tracker.close();
            }
        });
    }

    private static RecordCursorFactory newFactory() {
        return newFactory(newSourceFactory());
    }

    private static RecordCursorFactory newFactory(RecordCursorFactory base) {
        final GenericRecordMetadata resultMetadata = new GenericRecordMetadata();
        resultMetadata.add(new TableColumnMetadata("s", ColumnType.SYMBOL, IndexType.NONE, 0, false, null));
        final IntList columnToFunctionIndex = new IntList(1);
        columnToFunctionIndex.add(0);
        final ObjList<Function> functions = new ObjList<>(1);
        functions.add(new CastStrToSymbolFunctionFactory.Func(new StrColumn(0)));
        return new UnionSymbolCastRecordCursorFactory(resultMetadata, base, columnToFunctionIndex, functions);
    }

    private static SingleSparseSymbolCursorFactory newSourceFactory() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("s", ColumnType.STRING));
        return new SingleSparseSymbolCursorFactory(metadata);
    }

    private static class SingleSparseSymbolCursorFactory extends AbstractRecordCursorFactory {
        private final SingleSparseSymbolCursor cursor = new SingleSparseSymbolCursor();

        private SingleSparseSymbolCursorFactory(GenericRecordMetadata metadata) {
            super(metadata);
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

        private int getSymbolTableLookupCount() {
            return cursor.symbolTableLookupCount;
        }
    }

    private static class SingleSparseSymbolCursor implements NoRandomAccessRecordCursor {
        private static final StaticSymbolTable SYMBOL_TABLE = new StaticSymbolTable() {
            @Override
            public boolean containsNullValue() {
                return false;
            }

            @Override
            public int getSymbolCount() {
                return 1;
            }

            @Override
            public int keyOf(CharSequence value) {
                return "sparse".contentEquals(value) ? SPARSE_SOURCE_KEY : VALUE_NOT_FOUND;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return valueOf(key);
            }

            @Override
            public CharSequence valueOf(int key) {
                return key == SPARSE_SOURCE_KEY ? "sparse" : null;
            }
        };
        private final Record record = new Record() {
            @Override
            public int getInt(int col) {
                return SPARSE_SOURCE_KEY;
            }

            @Override
            public CharSequence getStrA(int col) {
                return "sparse";
            }

            @Override
            public CharSequence getStrB(int col) {
                return "sparse";
            }

            @Override
            public int getStrLen(int col) {
                return 6;
            }
        };
        private boolean hasNext;
        private int symbolTableLookupCount;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            symbolTableLookupCount++;
            return SYMBOL_TABLE;
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                hasNext = false;
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            hasNext = true;
        }
    }
}
