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


package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.map.Map;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * A parquet row group above Map.BATCH_ROW_INDEX_MASK + 1 rows (2^24) is split into sub-frames so the
 * 24-bit batched GROUP BY row index cannot overflow. One 17M-row group is converted to parquet and every
 * result is compared with the native (pre-conversion) oracle, around the split boundary and across it.
 * <p>
 * Row x (1-based) has v = x and ts = 2024-01-01 + (x - 1) microseconds, so the split lands between
 * v = 2^24 and v = 2^24 + 1 only if the row group is cut at the first 2^24 rows. A sentinel row in
 * 2024-01-02 keeps 2024-01-01 non-active so it can be converted.
 */
public class ParquetSubFrameLargeRowGroupTest extends AbstractCairoTest {
    private static final long ROW_COUNT = 17_000_000L;
    private static final long SPLIT_ROWS = Map.BATCH_ROW_INDEX_MASK + 1L;

    @Before
    public void setUp() {
        // a single row group holds the whole partition
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 20_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testRowGroupAboveBatchRowIndexSplitsMatchesNative() throws Exception {
        assertMemoryLeak(() -> executeWithPool(
                new WorkerPool(() -> 4),
                (engine, compiler, ctx) -> {
                    ctx.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    engine.execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
                    engine.execute(
                            "INSERT INTO x SELECT x, timestamp_sequence('2024-01-01', 1) FROM long_sequence(" + ROW_COUNT + ")",
                            ctx
                    );
                    engine.execute("INSERT INTO x VALUES (-1, '2024-01-02T00:00:00.000000Z')", ctx);

                    final String day = " FROM x WHERE ts IN '2024-01-01'";
                    // ts of row 2^24 is 2^24 - 1 microseconds after midnight; the window holds rows 2^24 - 10 .. 2^24 + 10
                    final String window = "SELECT v, ts FROM x WHERE ts BETWEEN '2024-01-01T00:00:16.777205Z' AND '2024-01-01T00:00:16.777225Z'";
                    final String windowDesc = window + " ORDER BY ts DESC";
                    final String aggregates = "SELECT count(), sum(v), min(v), max(v)" + day;
                    // matches are v = 1, 2^24 - 1, 2^24 and 2^24 + 1: one on each side of the split and two on its edge
                    final String filtered = "SELECT v, ts" + day + " AND v % " + SPLIT_ROWS + " IN (0, 1, " + (SPLIT_ROWS - 1) + ")";
                    final String filteredAgg = "SELECT count(), sum(v)" + day + " AND v % 7 = 3";
                    // the keys span the boundary: v / 10_000_000 = 1 covers 10M .. 17M
                    final String keyed = "SELECT v / 10000000 k, count(), sum(v), min(v), max(v)" + day + " ORDER BY k";
                    final String modulo = "SELECT v % 3 k, count(), sum(v), min(v), max(v)" + day + " ORDER BY k";
                    // count_distinct is not vectorized: this is the Map-batching Async Group By
                    final String batched = "SELECT v / 10000000 k, count_distinct(v) cd, sum(v) sv" + day + " ORDER BY k";

                    final String[] queries = {
                            aggregates, window, windowDesc, filtered, filteredAgg, keyed, modulo, batched
                    };
                    final String[] expected = new String[queries.length];
                    for (int i = 0; i < queries.length; i++) {
                        final StringSink sink = new StringSink();
                        TestUtils.printSql(compiler, ctx, queries[i], sink);
                        expected[i] = sink.toString();
                    }

                    engine.execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'", ctx);

                    assertPlanContains(compiler, ctx, aggregates, "vectorized: true");
                    assertPlanContains(compiler, ctx, keyed, "Async Group By");
                    assertPlanContains(compiler, ctx, batched, "Async Group By");
                    for (int i = 0; i < queries.length; i++) {
                        final var assertion = assertQuery(queries[i]).noLeakCheck().withEngine(engine).withCompiler(compiler).withContext(ctx);
                        if (queries[i] == window || queries[i] == filtered) {
                            assertion.timestamp("ts");
                        } else if (queries[i] == windowDesc) {
                            assertion.timestampDesc("ts");
                        } else if (queries[i] == aggregates || queries[i] == filteredAgg) {
                            // a keyless GROUP BY has one row and no random access
                            assertion.noRandomAccess().expectSize();
                        } else if (queries[i] == keyed || queries[i] == modulo || queries[i] == batched) {
                            assertion.expectSize();
                        }
                        assertion.returns(expected[i]);
                    }
                }
        ));
    }

    private static void assertPlanContains(SqlCompiler compiler, SqlExecutionContext ctx, String sql, String fragment) throws Exception {
        final StringSink sink = new StringSink();
        TestUtils.printSql(compiler, ctx, "EXPLAIN " + sql, sink);
        TestUtils.assertContains(sink, fragment);
    }
}
