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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Encoded sorts past the parallel threshold rewrite a one-word key into a word that spreads over
 * the radix sort's first byte (SortKeyEncoder.spreadKeyWords). The order must be the one the
 * sort gives without it: each query is run with the threshold above its row count (no rewrite,
 * the native sort's sequential path) and at 2 (rewrite, parallel path), light and materialized,
 * with row ids ascending (ordinals) and descending (shift only), narrow and full key ranges,
 * duplicates, NULLs, and a single distinct key.
 */
public class EncodedSortSpreadKeysTest extends AbstractCairoTest {
    private static final String[] KEYS = {
            "f", "f desc", "i", "i desc", "l", "l desc", "d", "d desc", "s", "s desc", "b", "b desc",
            "ts", "ts desc", "wide", "wide desc", "c", "c desc", "fn", "fn desc", "sym", "sym desc", "fo", "fo desc", "io", "io desc"
    };

    @Test
    public void testSpreadKeysKeepTheOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table t as (select " +
                            "(200 + rnd_int(0, 5000, 0) / 100.0)::float f, " +
                            "rnd_int(1000, 1100, 0) i, " +
                            "rnd_long(-5, 5, 0) l, " +
                            "rnd_double(0) d, " +
                            "rnd_short(10, 20) s, " +
                            "rnd_boolean() b, " +
                            "rnd_long(-9223372036854775807L, 9223372036854775807L, 0) wide, " +
                            "7 c, " +
                            "rnd_float(3) fn, " +
                            "rnd_symbol('a', 'b', 'c', null) sym, " +
                            // a narrow bulk with outliers on both sides of it
                            "case when x % 97 = 0 then 0.0 when x % 89 = 0 then 70 + x % 5 when x % 83 = 0 then 1e6 " +
                            "else 130 + rnd_int(0, 1500, 0) / 100.0 end::float fo, " +
                            "case when x % 101 = 0 then -2_000_000_000 when x % 103 = 0 then 2_000_000_000 " +
                            "else rnd_int(0, 300, 0) end io, " +
                            "timestamp_sequence('2026-04-01', 1_000_000L) ts " +
                            "from long_sequence(30000)) timestamp(ts) partition by hour"
            );
            int sorted = 0;
            for (String key : KEYS) {
                final String col = key.split(" ")[0];
                final String[] queries = {
                        // light, row ids ascending
                        "select ts, " + col + " from t where i > 1003 order by " + key,
                        // light over a descending scan: row ids descending
                        "select ts, " + col + " from (select * from t order by ts desc) order by " + key,
                        // materialized (a union is not random access)
                        "select ts, " + col + " from (select ts, " + col + " from t where i > 1003 union all " +
                                "select ts, " + col + " from t where i < 1010) order by " + key,
                };
                for (String query : queries) {
                    setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, 1_000_000);
                    final String expected = rows(query);
                    setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, 2);
                    final StringSink plan = new StringSink();
                    printSql("explain " + query, plan);
                    if (Chars.contains(plan, "Encode sort")) {
                        sorted++;
                    }
                    TestUtils.assertEquals(query, expected, rows(query));
                }
            }
            Assert.assertTrue("no encoded sort", sorted > KEYS.length);
        });
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // as a server compiles: an ORDER BY over a projected column memoizes it
        allowFunctionMemoization();
    }

    @Test
    public void testCapacityHintAcrossExecutions() throws Exception {
        // a cursor sized from its previous execution, smaller or larger than this one's, past and
        // below the count at which it grows to the previous execution's
        assertMemoryLeak(() -> {
            execute(
                    "create table t as (select rnd_int(0, 1000, 0) i, rnd_double(0) d, rnd_str(3, 8, 1) s, " +
                            "timestamp_sequence('2026-04-01', 100_000L) ts from long_sequence(200000)) timestamp(ts) partition by hour"
            );
            final String[] queries = {
                    "select ts, d, s from t where i > $1 order by d desc",
                    "select ts, d, s from (select ts, d, s from t where i > $1 union all select ts, d, s from t where i < 0) order by d",
            };
            final int[] bounds = {10, 900, 990, 0, 500, 1000, 3, 700, 2};
            for (String query : queries) {
                final String[] expected = new String[bounds.length];
                for (int j = 0; j < bounds.length; j++) {
                    bindVariableService.clear();
                    bindVariableService.setInt(0, bounds[j]);
                    try (io.questdb.cairo.sql.RecordCursorFactory factory = select(query)) {
                        expected[j] = rowsOf(factory);
                    }
                }
                try (io.questdb.cairo.sql.RecordCursorFactory factory = select(query)) {
                    for (int j = 0; j < bounds.length; j++) {
                        bindVariableService.clear();
                        bindVariableService.setInt(0, bounds[j]);
                        TestUtils.assertEquals(query + " $1=" + bounds[j], expected[j], rowsOf(factory));
                    }
                }
            }
        });
    }

    @Test
    public void testMaterializedFromBlocksOnTestContext() throws Exception {
        assertMemoryLeak(() -> assertMaterializedFromBlocks(engine, sqlExecutionContext));
    }

    @Test
    public void testMaterializedFromBlocksOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final io.questdb.mp.WorkerPool pool = new io.questdb.test.mp.TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> assertMaterializedFromBlocks(engine, ctx), configuration, LOG);
        });
    }

    // A materialized sort (the sorted rows of a union, which is not random access) fills its chain
    // from the union's branch blocks when every column is of a fixed-size type: the same rows,
    // in the same order, as the light sort of the branch alone.
    private static void assertMaterializedFromBlocks(io.questdb.cairo.CairoEngine engine, io.questdb.griffin.SqlExecutionContext ctx) throws Exception {
        engine.execute(
                "create table q as (select rnd_symbol('A', 'B', 'C') sym, rnd_float() bid, rnd_float() ask, rnd_int(1, 1000, 3) bsize, " +
                        "rnd_boolean() b, rnd_short() sh, rnd_byte() by, rnd_char() ch, rnd_long(0, 100, 2) l, " +
                        "(x * 3_000_000L)::timestamp ts from long_sequence(60000)) timestamp(ts) partition by day bypass wal",
                ctx
        );
        final String[] projections = {
                "ts, (bid + ask) / 2 mid",
                "ts, bid, bsize, b, sh, by, ch, l, ts::date d",
                "ts, (bid + ask) / 2 mid, bsize * 2 b2",
        };
        final String[] orders = {"mid desc", "bid, ts desc", "b2, mid desc"};
        final String[] filters = {"sym = 'A'", "bsize > 10", "sym in ('A', 'B') and bid > 0.2"};
        int compared = 0;
        for (int i = 0; i < projections.length; i++) {
            for (String filter : filters) {
                for (String dispatch : new String[]{"false", "true"}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, dispatch);
                    final String light = "select " + projections[i] + " from q where " + filter + " order by " + orders[i];
                    final String materialized = "select * from (select " + projections[i] + " from q where " + filter +
                            " union all select " + projections[i] + " from q where 1 = 0) order by " + orders[i];
                    final String expected;
                    try (io.questdb.cairo.sql.RecordCursorFactory factory = engine.select(light, ctx)) {
                        expected = rowsOf(factory, ctx);
                    }
                    try (io.questdb.cairo.sql.RecordCursorFactory factory = engine.select(materialized, ctx)) {
                        for (int run = 0; run < 2; run++) {
                            TestUtils.assertEquals(materialized + " [" + dispatch + ", run " + run + "]", expected, rowsOf(factory, ctx));
                        }
                    }
                    compared++;
                }
            }
        }
        Assert.assertEquals(18, compared);
    }

    private static String rowsOf(io.questdb.cairo.sql.RecordCursorFactory factory, io.questdb.griffin.SqlExecutionContext ctx) throws Exception {
        final StringSink sink = new StringSink();
        try (io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(ctx)) {
            println(factory.getMetadata(), cursor, sink);
        }
        return sink.toString();
    }

    private static String rowsOf(io.questdb.cairo.sql.RecordCursorFactory factory) throws Exception {
        final StringSink sink = new StringSink();
        try (io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            println(factory.getMetadata(), cursor, sink);
        }
        return sink.toString();
    }

    private static String rows(String query) throws Exception {
        final StringSink sink = new StringSink();
        printSql(query, sink);
        return sink.toString();
    }
}
