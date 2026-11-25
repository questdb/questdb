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

package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.parquet.HybridColumnMaterializer;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.groupby.MapSymbolColumn;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntKeyType;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.LongKeyType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharKeyType;
import io.questdb.test.griffin.fuzz.types.VarcharType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the SYMBOL columns that QueryFuzzTest's generated queries hand to the exported symbol key check of
 * {@link QueryRunner}: columns a CURSOR_BASED Parquet export keeps as SYMBOL, whose cursor serves a table's
 * symbol map, a {@link SymbolColumn} or a {@link MapSymbolColumn} over one, and columns it writes as STRING
 * because an aggregate computes them. A generator that stopped reaching one of them would leave that path
 * unchecked while the fuzz run stays green. The generator produces no set operation and the fuzz run keeps
 * function memoization off, so it reaches neither a UNION nor a memoizer; CopyExportTest covers both.
 * {@link #testKeyCheckResolvesKeysThroughExportedMaps} pins that the check resolves the keys of each path through
 * the table symbol map the export hands to the encoder, so that a wrong map fails it, and
 * {@link #testKeyCheckFailsQueryOnWrongMap} that a wrong map fails the fuzz query.
 * <p>
 * The generator is random, so the assertions run over a corpus; the seed is fixed so a failure is
 * reproducible.
 */
public class ExportSymbolShapeCoverageTest extends AbstractCairoTest {
    private static final int CORPUS_SIZE = 600;

    @Test
    public void testGeneratedQueriesReachSymbolExportPaths() throws Exception {
        assertMemoryLeak(() -> {
            // The columns of FuzzTableFactory's tables: the join keys sym, k, lk and vk, other columns, and ts last.
            final ObjList<FuzzTable> tables = new ObjList<>();
            for (int t = 0; t < 2; t++) {
                final String tableName = "fuzz_t" + t;
                execute("CREATE TABLE " + tableName + " (sym SYMBOL, k INT, lk LONG, vk VARCHAR, c0 SYMBOL, c1 INT, c2 DOUBLE, c3 VARCHAR, ts TIMESTAMP)"
                        + " TIMESTAMP(ts) PARTITION BY DAY");
                execute("INSERT INTO " + tableName + " SELECT"
                        + " CASE WHEN x % 5 = 0 THEN NULL ELSE 'a' || (x % 4) END, (x % 16)::INT, x % 16, 'v' || (x % 16),"
                        + " 'b' || (x % 3), x::INT, x / 3.0, 'w' || x, timestamp_sequence('2024-01-01', 3_600_000_000L)"
                        + " FROM long_sequence(48)");
                final ObjList<FuzzColumn> columns = new ObjList<>();
                columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
                columns.add(new FuzzColumn("k", IntKeyType.INSTANCE));
                columns.add(new FuzzColumn("lk", LongKeyType.INSTANCE));
                columns.add(new FuzzColumn("vk", VarcharKeyType.INSTANCE));
                columns.add(new FuzzColumn("c0", SymbolType.INSTANCE));
                columns.add(new FuzzColumn("c1", IntType.INSTANCE));
                columns.add(new FuzzColumn("c2", DoubleType.INSTANCE));
                columns.add(new FuzzColumn("c3", VarcharType.INSTANCE));
                columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
                tables.add(new FuzzTable(tableName, columns, "ts"));
            }

            final Rnd rnd = new Rnd(0x5eed, 0xe5b0);
            // A Rnd of its own keeps the config's draws out of the generator's stream.
            final FuzzConfig config = new FuzzConfig(new Rnd(0x5eed, 0xc0f1));
            int keptThroughSymbolMap = 0;
            int keptThroughSymbolColumn = 0;
            int keptThroughMapSymbolColumn = 0;
            int writtenAsStringThroughAggregate = 0;
            for (int i = 0; i < CORPUS_SIZE; i++) {
                final String sql = QueryGenerator.generate(rnd, tables, null, false, true, true, true, true, true, true, config).sql();
                try (
                        RecordCursorFactory factory = select(sql);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                        HybridColumnMaterializer materializer = new HybridColumnMaterializer()
                ) {
                    final RecordMetadata metadata = factory.getMetadata();
                    materializer.setUp(metadata, cursor);
                    for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
                        if (!ColumnType.isSymbol(metadata.getColumnType(c))) {
                            continue;
                        }
                        final SymbolTable symbolTable = symbolTableOf(cursor, c);
                        if (ColumnType.isSymbol(materializer.getAdjustedMetadata().getColumnType(c))) {
                            if (symbolTable instanceof SymbolMapReader) {
                                keptThroughSymbolMap++;
                            } else if (symbolTable instanceof SymbolColumn) {
                                keptThroughSymbolColumn++;
                            } else if (symbolTable instanceof MapSymbolColumn) {
                                keptThroughMapSymbolColumn++;
                            }
                        } else if (symbolTable instanceof GroupByFunction) {
                            writtenAsStringThroughAggregate++;
                        }
                    }
                } catch (SqlException | CairoException e) {
                    // QueryRunner skips the generated queries the engine rejects as well.
                }
            }
            final String counts = "symbol map: " + keptThroughSymbolMap + ", SymbolColumn: " + keptThroughSymbolColumn
                    + ", MapSymbolColumn: " + keptThroughMapSymbolColumn + ", aggregate: " + writtenAsStringThroughAggregate;
            Assert.assertTrue("a kept column whose cursor serves the table's symbol map, " + counts, keptThroughSymbolMap > 0);
            Assert.assertTrue("a kept column through a SymbolColumn, " + counts, keptThroughSymbolColumn > 0);
            Assert.assertTrue("a kept column through a MapSymbolColumn, " + counts, keptThroughMapSymbolColumn > 0);
            Assert.assertTrue("an aggregate written as STRING, " + counts, writtenAsStringThroughAggregate > 0);
        });
    }

    @Test
    public void testKeyCheckFailsQueryOnWrongMap() throws Exception {
        assertMemoryLeak(() -> {
            createKeyCheckTable();
            final GeneratedQuery query = new GeneratedQuery("SELECT a, b FROM t WHERE v = 2", true);
            final QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, false, null, true, new ObjList<>(), null);
            final QueryRunner.Result result = runner.run(query);
            Assert.assertFalse("must not fail: " + (result.getFailure() != null ? result.getFailure().getMessage() : ""), result.isFailed());
            Assert.assertFalse("must not be skipped: " + result.getSkipReason(), result.isSkipped());

            // The check resolves a through b's map, which holds no key past b0's 0, so a1's key 1 resolves to NULL.
            final QueryRunner runnerWithWrongMap = new QueryRunner(engine, sqlExecutionContext, false, false, false, null, true, new ObjList<>(), null) {
                @Override
                void onExportedSymbolMapsCollected(IntList columns, ObjList<SymbolMapReader> symbolMaps) {
                    Assert.assertEquals("[0,1]", columns.toString());
                    symbolMaps.setQuick(0, symbolMaps.getQuick(1));
                }
            };
            final QueryRunner.Result wrongMapResult = runnerWithWrongMap.run(query);
            Assert.assertTrue("must fail: " + wrongMapResult.getSkipReason(), wrongMapResult.isFailed());
            Assert.assertFalse(wrongMapResult.isSkipped());
            Assert.assertEquals(query.sql(), wrongMapResult.getSql());
            Assert.assertEquals(
                    "the key of an exported SYMBOL column does not resolve to its symbol [column=a, row=0, key=1, symbol=a1, resolved=null]\n"
                            + "  sql: SELECT a, b FROM t WHERE v = 2",
                    wrongMapResult.getFailure().getMessage()
            );
        });
    }

    @Test
    public void testKeyCheckReportsSymbolWithNullKey() throws Exception {
        assertMemoryLeak(() -> {
            // No real cursor returns a symbol with the NULL key, so a cursor of one such row stands in for a broken one.
            final Record record = new Record() {
                @Override
                public int getInt(int col) {
                    return Numbers.INT_NULL;
                }

                @Override
                public CharSequence getSymA(int col) {
                    return "x";
                }
            };
            final RecordCursor cursor = new RecordCursor() {
                private boolean hasRow = true;

                @Override
                public void close() {
                }

                @Override
                public Record getRecord() {
                    return record;
                }

                @Override
                public Record getRecordB() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean hasNext() {
                    final boolean hadRow = hasRow;
                    hasRow = false;
                    return hadRow;
                }

                @Override
                public long preComputedStateSize() {
                    return 0;
                }

                @Override
                public void recordAt(Record record, long atRowId) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long size() {
                    return 1;
                }

                @Override
                public void toTop() {
                    hasRow = true;
                }
            };
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("a", ColumnType.SYMBOL, IndexType.NONE, 0, true, null));
            final IntList columns = new IntList();
            columns.add(0);
            // the check resolves a key below zero through no map
            final ObjList<SymbolMapReader> symbolMaps = new ObjList<>();
            symbolMaps.add(null);
            Assert.assertEquals(
                    "[column=a, row=0, key=-2147483648, symbol=x, resolved=null]",
                    QueryRunner.findExportedSymbolKeyViolation(cursor, metadata, columns, symbolMaps)
            );
        });
    }

    @Test
    public void testKeyCheckResolvesKeysThroughExportedMaps() throws Exception {
        assertMemoryLeak(() -> {
            createKeyCheckTable();
            // For a and b, the cursors of these queries serve the table's symbol maps, a SymbolColumn over each map, and
            // a MapSymbolColumn over each map.
            final String[] queries = {
                    "SELECT a, b FROM t WHERE v = ",
                    "SELECT a, b, v + 1 w FROM t WHERE v = ",
                    "SELECT DISTINCT a, b FROM t WHERE v = "
            };
            final Class<?>[] servedSymbolTables = {SymbolMapReader.class, SymbolColumn.class, MapSymbolColumn.class};
            // Per v from 2 to 4: what the check reports for a resolved through b's map, which holds no key past 0, and
            // for b resolved through a's map. A NULL key resolves to NULL through any map.
            final String[] violationsOfAThroughB = {
                    "[column=a, row=0, key=1, symbol=a1, resolved=null]",
                    "[column=a, row=0, key=2, symbol=a2, resolved=null]",
                    "[column=a, row=0, key=3, symbol=, resolved=null]"
            };
            final String[] violationsOfBThroughA = {
                    "[column=b, row=0, key=0, symbol=b0, resolved=a0]",
                    null,
                    "[column=b, row=0, key=0, symbol=b0, resolved=a0]"
            };
            final IntList columns = new IntList();
            final ObjList<SymbolMapReader> symbolMaps = new ObjList<>();
            final IntList swappedColumns = new IntList();
            final ObjList<SymbolMapReader> swappedMaps = new ObjList<>();
            for (int q = 0; q < queries.length; q++) {
                for (int v = 2; v <= 4; v++) {
                    final String sql = queries[q] + v;
                    try (
                            RecordCursorFactory factory = select(sql);
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                            HybridColumnMaterializer materializer = new HybridColumnMaterializer();
                            HybridColumnMaterializer cursorBackedMaterializer = new HybridColumnMaterializer();
                            DirectLongList columnData = new DirectLongList(32, MemoryTag.NATIVE_DEFAULT)
                    ) {
                        final RecordMetadata metadata = factory.getMetadata();
                        Assert.assertTrue(sql, servedSymbolTables[q].isInstance(cursor.getSymbolTable(0)));
                        Assert.assertTrue(sql, servedSymbolTables[q].isInstance(cursor.getSymbolTable(1)));
                        QueryRunner.collectExportedSymbolMaps(cursor, metadata, columns, symbolMaps);
                        Assert.assertEquals(sql, "[0,1]", columns.toString());
                        Assert.assertEquals(sql, 2, symbolMaps.size());

                        // The exporters set up on the fresh cursor, with setUpCursorBacked() for a virtual factory and
                        // with setUp() for any other, and the encoder resolves the keys through the maps the check holds.
                        final RecordCursorFactory exportedFactory = ParquetExportMode.unwrapFactory(factory);
                        Assert.assertEquals(sql, q == 1, exportedFactory instanceof VirtualRecordCursorFactory);
                        materializer.setUp(metadata, cursor);
                        if (exportedFactory instanceof VirtualRecordCursorFactory virtualFactory) {
                            cursorBackedMaterializer.setUpCursorBacked(virtualFactory, cursor);
                        }
                        Assert.assertEquals(sql, 1, materializer.buildColumnDataFromCursor(cursor, columnData, 10));
                        assertSymbolValuesOfMaps(sql, columnData, columns, symbolMaps);
                        if (exportedFactory instanceof VirtualRecordCursorFactory) {
                            cursor.toTop();
                            Assert.assertEquals(sql, 1, cursorBackedMaterializer.buildColumnDataFromCursor(cursor, columnData, 10));
                            assertSymbolValuesOfMaps(sql, columnData, columns, symbolMaps);
                        }

                        Assert.assertNull(sql, QueryRunner.findExportedSymbolKeyViolation(cursor, metadata, columns, symbolMaps));
                        swappedColumns.clear();
                        swappedColumns.add(0);
                        swappedMaps.clear();
                        swappedMaps.add(symbolMaps.getQuick(1));
                        Assert.assertEquals(
                                sql,
                                violationsOfAThroughB[v - 2],
                                QueryRunner.findExportedSymbolKeyViolation(cursor, metadata, swappedColumns, swappedMaps)
                        );
                        swappedColumns.clear();
                        swappedColumns.add(1);
                        swappedMaps.clear();
                        swappedMaps.add(symbolMaps.getQuick(0));
                        Assert.assertEquals(
                                sql,
                                violationsOfBThroughA[v - 2],
                                QueryRunner.findExportedSymbolKeyViolation(cursor, metadata, swappedColumns, swappedMaps)
                        );
                    }
                }
            }

            // Two rows: a2 with a NULL b, then '' with b0. Through a's map for both columns, the first row holds and
            // the second resolves b0's key to a0.
            try (
                    RecordCursorFactory factory = select("SELECT a, b FROM t WHERE v > 2");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final RecordMetadata metadata = factory.getMetadata();
                QueryRunner.collectExportedSymbolMaps(cursor, metadata, columns, symbolMaps);
                Assert.assertEquals("[0,1]", columns.toString());
                Assert.assertNull(QueryRunner.findExportedSymbolKeyViolation(cursor, metadata, columns, symbolMaps));
                swappedMaps.clear();
                swappedMaps.add(symbolMaps.getQuick(0));
                swappedMaps.add(symbolMaps.getQuick(0));
                Assert.assertEquals(
                        "[column=b, row=1, key=0, symbol=b0, resolved=a0]",
                        QueryRunner.findExportedSymbolKeyViolation(cursor, metadata, columns, swappedMaps)
                );
            }
        });
    }

    // The column data holds 7 longs per column, the address of the symbol values at long 3 of a SYMBOL column.
    private static void assertSymbolValuesOfMaps(String sql, DirectLongList columnData, IntList columns, ObjList<SymbolMapReader> symbolMaps) {
        for (int k = 0, n = columns.size(); k < n; k++) {
            Assert.assertEquals(sql, symbolMaps.getQuick(k).getSymbolValuesColumn().addressOf(0), columnData.get(columns.getQuick(k) * 7L + 3));
        }
    }

    // A symbol map numbers its symbols densely in the order they first appear, and a NULL takes the NULL key:
    // a holds a0 as 0, a1 as 1, a2 as 2 and '' as 3, and b holds b0 alone, as 0.
    private static void createKeyCheckTable() throws SqlException {
        execute("CREATE TABLE t (a SYMBOL, b SYMBOL, v INT)");
        execute("INSERT INTO t VALUES ('a0', 'b0', 1), ('a1', 'b0', 2), ('a2', NULL, 3), ('', 'b0', 4)");
    }

    private static SymbolTable symbolTableOf(RecordCursor cursor, int columnIndex) {
        try {
            return cursor.getSymbolTable(columnIndex);
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }
}
