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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.cairo.fuzz.ParallelGroupByFuzzTest.assertQueries;

// Tests ORDER BY + LIMIT (top K) parallel execution.
// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
@RunWith(Parameterized.class)
public class ParallelTopKFuzzTest extends AbstractCairoTest {
    private static final String[] FIXED8_COLUMNS = {
            "col_bool", "col_byte", "col_short", "col_char", "col_int", "col_float",
            "col_sym", "col_sym_null", "col_ipv4", "col_long", "col_double", "col_date",
            "col_geobyte", "col_geoshort", "col_geoint", "col_geolong",
            "col_dec8", "col_dec16", "col_dec32", "col_dec64",
    };
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private static final String[] WIDE_COLUMNS = {"col_dec128", "col_dec256"};
    private final boolean convertToParquet;
    private final boolean enableJitCompiler;
    private final boolean enableParallelTopK;

    public ParallelTopKFuzzTest(boolean enableParallelTopK, boolean enableJitCompiler, boolean convertToParquet) {
        this.enableParallelTopK = enableParallelTopK;
        this.enableJitCompiler = enableJitCompiler;
        this.convertToParquet = convertToParquet;
    }

    @Parameterized.Parameters(name = "parallel={0} JIT={1} parquet={2}")
    public static Collection<Object[]> data() {
        // only run a single combination per CI run
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // make sure to have a run with all equal flags occasionally
        if (rnd.nextInt(100) >= 90) {
            boolean flag = rnd.nextBoolean();
            return Arrays.asList(new Object[][]{{flag, flag, flag}});
        }
        return Arrays.asList(new Object[][]{{rnd.nextBoolean(), rnd.nextBoolean(), rnd.nextBoolean()}});
        // uncomment to run all combinations
//        return Arrays.asList(new Object[][]{
//                {true, true, true},
//                {true, true, false},
//                {true, false, true},
//                {true, false, false},
//                {false, true, true},
//                {false, true, false},
//                {false, false, true},
//                {false, false, false},
//        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        // Set the sharding threshold to a small value to test sharding.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, String.valueOf(enableParallelTopK));
        super.setUp();
    }

    @Test
    public void testParallelTopK() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelTopK(
                "SELECT * FROM tab ORDER BY key DESC, price ASC LIMIT 3;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-01-01T00:57:36.000000Z\tk4\t4.0\t4\tnull\n" +
                        "1970-01-01T02:09:36.000000Z\tk4\t9.0\t9\tnull\n" +
                        "1970-01-01T03:21:36.000000Z\tk4\t14.0\t14\tnull\n"
        );
    }

    /**
     * Validates the parallel encoded top-K against the serial tree-chain
     * reference. Single-column keys exercise the frame batch encoder for every
     * fixed-width-8 type; projecting only the sort column keeps the comparison
     * deterministic on duplicate keys, while the unique col_id covers full-row
     * emission. Multi-word decimal keys cover the per-row generic encoder.
     */
    @Test
    public void testParallelTopKEncodedTypes() throws Exception {
        Assume.assumeTrue(enableParallelTopK);
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        final int rowCount = 3_000 + rnd.nextInt(7_000);
                        createTypeMatrixTable(engine, sqlExecutionContext, rowCount);
                        final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;

                        for (String col : FIXED8_COLUMNS) {
                            final String desc = rnd.nextBoolean() ? " DESC" : "";
                            final int k = 1 + rnd.nextInt(200);
                            assertTopKMatch(engine, ctx, "SELECT " + col + " FROM tab ORDER BY " + col + desc + " LIMIT " + k);
                        }
                        for (String col : WIDE_COLUMNS) {
                            final String desc = rnd.nextBoolean() ? " DESC" : "";
                            final int k = 1 + rnd.nextInt(200);
                            assertTopKMatch(engine, ctx, "SELECT " + col + " FROM tab ORDER BY " + col + desc + " LIMIT " + k);
                        }

                        // Unique keys make the full emitted rows deterministic.
                        assertTopKMatch(engine, ctx, "SELECT * FROM tab ORDER BY col_id LIMIT " + (1 + rnd.nextInt(200)));
                        assertTopKMatch(engine, ctx, "SELECT * FROM tab ORDER BY col_id DESC LIMIT " + (1 + rnd.nextInt(200)));

                        // The filter reducer feeds the batch encoder its filtered row list.
                        assertTopKMatch(
                                engine, ctx,
                                "SELECT col_long FROM tab WHERE col_long >= 3 ORDER BY col_long LIMIT " + (1 + rnd.nextInt(200))
                        );
                        assertTopKMatch(
                                engine, ctx,
                                "SELECT col_double FROM tab WHERE col_long >= 3 ORDER BY col_double DESC LIMIT " + (1 + rnd.nextInt(200))
                        );

                        // Multi-column keys take the per-row generic encoder.
                        assertTopKMatch(
                                engine, ctx,
                                "SELECT * FROM tab ORDER BY col_int, col_sym, col_id LIMIT " + (1 + rnd.nextInt(200))
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelTopKFilter() throws Exception {
        testParallelTopK(
                "SELECT * FROM tab WHERE key = 'k0' ORDER BY price DESC LIMIT 1;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-10T12:00:00.000000Z\tk0\t4050.0\t4050\t4050.0\n"
        );
    }

    @Test
    public void testParallelTopKIntrinsicsFilter() throws Exception {
        testParallelTopK(
                "SELECT * FROM tab WHERE ts in '1970-02' ORDER BY colTop LIMIT 3;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-01T00:00:00.000000Z\tk0\t3100.0\t3100\t3100.0\n" +
                        "1970-02-01T00:14:24.000000Z\tk1\t3101.0\t3101\t3101.0\n" +
                        "1970-02-01T00:28:48.000000Z\tk2\t3102.0\t3102\t3102.0\n"
        );
    }

    @Test
    public void testParallelTopKThreadUnsafeOrderByExpression() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // The query won't use the parallel factory due to virtual base factory.
        testParallelTopK(
                "SELECT * FROM tab ORDER BY concat(key, 'foobar'), ts DESC LIMIT 3;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-10T12:00:00.000000Z\tk0\t4050.0\t4050\t4050.0\n" +
                        "1970-02-10T10:48:00.000000Z\tk0\t4045.0\t4045\t4045.0\n" +
                        "1970-02-10T09:36:00.000000Z\tk0\t4040.0\t4040\t4040.0\n"
        );
    }

    @Test
    public void testParallelTopKWithBindVariablesInFilter() throws Exception {
        testParallelTopK(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("asym", "k0");
                },
                "SELECT * FROM tab WHERE key = :asym ORDER BY price DESC LIMIT 1;",
                "ts\tkey\tprice\tquantity\tcolTop\n" +
                        "1970-02-10T12:00:00.000000Z\tk0\t4050.0\t4050\t4050.0\n"
        );
    }

    private void assertTopKMatch(CairoEngine engine, SqlExecutionContextImpl ctx, String query) throws Exception {
        ctx.setParallelTopKEnabled(true);
        final StringSink plan = new StringSink();
        TestUtils.printSql(engine, ctx, "EXPLAIN " + query, plan);
        TestUtils.assertContains(plan, "Async");
        TestUtils.assertContains(plan, "Top K");

        final StringSink actual = new StringSink();
        TestUtils.printSql(engine, ctx, query, actual);

        ctx.setParallelTopKEnabled(false);
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        try {
            final StringSink expected = new StringSink();
            TestUtils.printSql(engine, ctx, query, expected);
            TestUtils.assertEquals(query, expected, actual);
        } finally {
            node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
            ctx.setParallelTopKEnabled(true);
        }
    }

    private void createTypeMatrixTable(CairoEngine engine, SqlExecutionContext ctx, int rowCount) throws SqlException {
        engine.execute(
                "CREATE TABLE tab AS (SELECT" +
                        " rnd_boolean() col_bool," +
                        " rnd_byte() col_byte," +
                        " rnd_short() col_short," +
                        " rnd_char() col_char," +
                        " rnd_int(0, 10, 2) col_int," +
                        " rnd_float(2) col_float," +
                        " rnd_symbol(16, 2, 6, 0) col_sym," +
                        " rnd_symbol(16, 2, 6, 2) col_sym_null," +
                        " rnd_ipv4() col_ipv4," +
                        " rnd_long(0, 10, 2) col_long," +
                        " rnd_double(2) col_double," +
                        " rnd_date(0, 100_000_000_000L, 2) col_date," +
                        " rnd_geohash(5) col_geobyte," +
                        " rnd_geohash(10) col_geoshort," +
                        " rnd_geohash(20) col_geoint," +
                        " rnd_geohash(40) col_geolong," +
                        " rnd_decimal(2, 1, 2) col_dec8," +
                        " rnd_decimal(4, 2, 2) col_dec16," +
                        " rnd_decimal(9, 3, 2) col_dec32," +
                        " rnd_decimal(18, 4, 2) col_dec64," +
                        " rnd_decimal(38, 5, 2) col_dec128," +
                        " rnd_decimal(76, 6, 2) col_dec256," +
                        " x col_id," +
                        " timestamp_sequence(0, 1_000_000) ts" +
                        " FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts) PARTITION BY HOUR",
                ctx
        );
        if (convertToParquet) {
            // A row in a later partition makes the generated partitions convertible.
            engine.execute("INSERT INTO tab(ts) VALUES ('2000-01-01')", ctx);
            engine.execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts < '2000-01-01'", ctx);
        }
    }

    private void testParallelTopK(String... queriesAndExpectedResults) throws Exception {
        testParallelTopK(null, queriesAndExpectedResults);
    }

    private void testParallelTopK(BindVariablesInitializer initializer, String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key SYMBOL," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x, x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        engine.execute(
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, " +
                                        "  'k' || ((50 + x) % 5), 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private interface BindVariablesInitializer {
        void init(SqlExecutionContext sqlExecutionContext) throws SqlException;
    }
}
