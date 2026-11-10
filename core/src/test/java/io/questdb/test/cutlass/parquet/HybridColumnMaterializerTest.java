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

package io.questdb.test.cutlass.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cutlass.parquet.HybridColumnMaterializer;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class HybridColumnMaterializerTest extends AbstractCairoTest {

    @Test
    public void testCursorWithoutSymbolTablesExportsSymbolAsString() throws Exception {
        // A cursor that serves no symbol tables gives the export no symbol map to resolve keys through, so the export
        // writes its static SYMBOL column as STRING, from the text of each row.
        assertMemoryLeak(() -> assertSymbolWrittenAsText(null));
    }

    @Test
    public void testSetUpForgetsKeptSymbolColumnsOfPreviousExport() throws Exception {
        // The exporters reuse one materializer: an export that keeps a SYMBOL column at column 0 leaves no symbol map
        // behind for the INT column 0 of the next export.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, v INT)");
            execute("INSERT INTO t VALUES ('a', 1), ('b', 2)");
            try (
                    HybridColumnMaterializer materializer = new HybridColumnMaterializer();
                    DirectLongList columnData = new DirectLongList(14, MemoryTag.NATIVE_PARQUET_EXPORTER);
                    RecordCursorFactory symbolFirstFactory = select("SELECT t.sym, t.v FROM t CROSS JOIN long_sequence(1)");
                    RecordCursorFactory intFirstFactory = select("SELECT t.v, t.sym FROM t CROSS JOIN long_sequence(1)")
            ) {
                try (RecordCursor cursor = symbolFirstFactory.getCursor(sqlExecutionContext)) {
                    materializer.setUp(symbolFirstFactory.getMetadata(), cursor);
                    Assert.assertEquals(ColumnType.SYMBOL, materializer.getAdjustedMetadata().getColumnType(0));
                    Assert.assertEquals(2, materializer.buildColumnDataFromCursor(cursor, columnData, 10));
                    assertKeptSymbolColumnData(columnData, 0);
                    assertNoSymbolMap(columnData, 1);
                }
                materializer.clear();
                try (RecordCursor cursor = intFirstFactory.getCursor(sqlExecutionContext)) {
                    materializer.setUp(intFirstFactory.getMetadata(), cursor);
                    Assert.assertEquals(ColumnType.INT, materializer.getAdjustedMetadata().getColumnType(0));
                    Assert.assertEquals(ColumnType.SYMBOL, materializer.getAdjustedMetadata().getColumnType(1));
                    Assert.assertEquals(2, materializer.buildColumnDataFromCursor(cursor, columnData, 10));
                    assertNoSymbolMap(columnData, 0);
                    assertKeptSymbolColumnData(columnData, 1);
                }
            }
        });
    }

    @Test
    public void testSymbolTableWithoutTableSymbolMapExportsSymbolAsString() throws Exception {
        // The export keeps a SYMBOL column only when its keys resolve through a table's symbol map with mapped memory,
        // which the cursor serves itself or through column references with static symbol tables. Otherwise it writes
        // the text of each row.
        assertMemoryLeak(() -> {
            try (MemoryCARWImpl valuesMemory = new MemoryCARWImpl(1024, 1, MemoryTag.NATIVE_DEFAULT)) {
                final SymbolTable[] symbolTables = {
                        // a symbol map with values but without offsets
                        new EmptySymbolMapReader() {
                            @Override
                            public MemoryR getSymbolValuesColumn() {
                                return valuesMemory;
                            }
                        },
                        // a symbol map without memory
                        EmptySymbolMapReader.INSTANCE,
                        // a column reference whose symbol table is not static
                        new SymbolColumn(0, false)
                };
                for (SymbolTable symbolTable : symbolTables) {
                    assertSymbolWrittenAsText(symbolTable);
                }
            }
        });
    }

    // The 7 longs of a kept SYMBOL column: the keys a and b have, 0 and 1, and the two-symbol map of t.sym.
    private static void assertKeptSymbolColumnData(DirectLongList columnData, int columnIndex) {
        final long base = columnIndex * 7L;
        Assert.assertEquals(0, columnData.get(base));
        Assert.assertEquals(2 * Integer.BYTES, columnData.get(base + 2));
        Assert.assertEquals(0, Unsafe.getInt(columnData.get(base + 1)));
        Assert.assertEquals(1, Unsafe.getInt(columnData.get(base + 1) + Integer.BYTES));
        Assert.assertNotEquals(0, columnData.get(base + 3));
        Assert.assertNotEquals(0, columnData.get(base + 5));
        Assert.assertEquals(2, columnData.get(base + 6));
    }

    // The 7 longs of a fixed-size column carry no aux vector and no symbol map.
    private static void assertNoSymbolMap(DirectLongList columnData, int columnIndex) {
        final long base = columnIndex * 7L;
        for (int i = 3; i < 7; i++) {
            Assert.assertEquals("long " + i + " of column " + columnIndex, 0, columnData.get(base + i));
        }
    }

    // Exports a static SYMBOL column whose cursor serves symbolTable, or no symbol table when it is null, and asserts
    // that the export writes it as STRING, from the text of each row.
    private static void assertSymbolWrittenAsText(SymbolTable symbolTable) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("sym", ColumnType.SYMBOL, IndexType.NONE, 0, true, null));
        try (
                HybridColumnMaterializer materializer = new HybridColumnMaterializer();
                DirectLongList columnData = new DirectLongList(7, MemoryTag.NATIVE_PARQUET_EXPORTER);
                SymbolTextCursor cursor = new SymbolTextCursor(symbolTable, "abc", null, "de")
        ) {
            materializer.setUp(metadata, cursor);
            Assert.assertEquals(ColumnType.STRING, materializer.getAdjustedMetadata().getColumnType(0));

            Assert.assertEquals(3, materializer.buildColumnDataFromCursor(cursor, columnData, 10));
            // [column top, data address, data size, aux address, aux size, 0, 0]
            Assert.assertEquals(7, columnData.size());
            Assert.assertEquals(0, columnData.get(0));
            Assert.assertEquals(0, columnData.get(5));
            Assert.assertEquals(0, columnData.get(6));
            // A STRING value is its UTF-16 length and chars, -1 for NULL.
            final long dataAddress = columnData.get(1);
            final long dataSize = columnData.get(2);
            final StringSink sink = new StringSink();
            long offset = 0;
            while (offset < dataSize) {
                final int length = Unsafe.getInt(dataAddress + offset);
                offset += Integer.BYTES;
                if (length < 0) {
                    sink.put("null");
                } else {
                    for (int i = 0; i < length; i++) {
                        sink.put(Unsafe.getChar(dataAddress + offset));
                        offset += Character.BYTES;
                    }
                }
                sink.put('\n');
            }
            Assert.assertEquals("abc\nnull\nde\n", sink.toString());
            // The aux vector holds the offset of each value.
            Assert.assertEquals(3 * Long.BYTES, columnData.get(4));
            Assert.assertEquals(0, Unsafe.getLong(columnData.get(3)));
            Assert.assertEquals(Integer.BYTES + 3 * Character.BYTES, Unsafe.getLong(columnData.get(3) + Long.BYTES));
            Assert.assertEquals(2 * Integer.BYTES + 3 * Character.BYTES, Unsafe.getLong(columnData.get(3) + 2 * Long.BYTES));
        }
    }

    // A cursor over one SYMBOL column that serves the given symbol table, or, when that is null, keeps RecordCursor's
    // default getSymbolTable(), which serves no symbol table.
    private static class SymbolTextCursor implements NoRandomAccessRecordCursor {
        private final Record record = new Record() {
            @Override
            public int getInt(int col) {
                throw new UnsupportedOperationException("the export must read the text of the symbol");
            }

            @Override
            public CharSequence getSymA(int col) {
                return symbols[row];
            }
        };
        private final SymbolTable symbolTable;
        private final String[] symbols;
        private int row = -1;

        private SymbolTextCursor(SymbolTable symbolTable, String... symbols) {
            this.symbolTable = symbolTable;
            this.symbols = symbols;
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbolTable != null ? symbolTable : NoRandomAccessRecordCursor.super.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return ++row < symbols.length;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return symbols.length;
        }

        @Override
        public void toTop() {
            row = -1;
        }
    }
}
