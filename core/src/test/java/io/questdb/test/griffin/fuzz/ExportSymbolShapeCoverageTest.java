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
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.parquet.HybridColumnMaterializer;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.groupby.MapSymbolColumn;
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

    private static SymbolTable symbolTableOf(RecordCursor cursor, int columnIndex) {
        try {
            return cursor.getSymbolTable(columnIndex);
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }
}
