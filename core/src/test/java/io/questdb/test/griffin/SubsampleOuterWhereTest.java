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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.junit.Test;

public class SubsampleOuterWhereTest extends AbstractCairoTest {
    private static final String HEADER = "ts\tx\tv\n";
    private static final String FIRST_ROW = "2024-01-01T00:00:00.000000Z\t0\t10.0\n";
    private static final String LAST_ROW = "2024-01-01T04:00:00.000000Z\t5\t50.0\n";
    private static final ObjList<String> METHODS = new ObjList<>(
            "uniform(2)",
            "cadence(4)",
            "m4(v, 2)",
            "minmax(v, 2)",
            "lttb(v, 2)",
            "lttb(v, 2, '1d')",
            "sdt(v, 0.0)"
    );
    private static final String SAMPLE = "SELECT ts, x, v FROM t SUBSAMPLE lttb(v, 2)";

    @Test
    public void testAliasesAndComputedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertFusedQuery("SELECT * FROM (SELECT ts, x AS qty, v AS value FROM t SUBSAMPLE lttb(value, 2)) WHERE qty > 0")
                    .timestamp("ts").returns((HEADER + LAST_ROW).replace("ts\tx\tv", "ts\tqty\tvalue"));
            assertFusedQuery("SELECT * FROM (SELECT ts, x + 1 AS qty, v * 2 AS value FROM t SUBSAMPLE lttb(value, 2)) WHERE qty > 1")
                    .timestamp("ts").returns("""
                            ts\tqty\tvalue
                            2024-01-01T04:00:00.000000Z\t6\t100.0
                            """);
            assertFusedQuery("SELECT value FROM (SELECT ts, x AS qty, v AS value FROM t SUBSAMPLE lttb(value, 2)) q WHERE q.qty > 0")
                    .returns("value\n50.0\n");
        });
    }

    @Test
    public void testAllAlgorithmsAndFullWindowFallback() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                final boolean isLight = mode == 1;
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                for (int i = 0; i < METHODS.size(); i++) {
                    final String sample = "SELECT ts, x, v FROM t SUBSAMPLE " + METHODS.getQuick(i);
                    // Each algorithm selects the endpoints of this linear series. Filtering first
                    // would add the second input row, so the golden also pins operation order.
                    assertQuery(sample).timestamp("ts").returns(HEADER + FIRST_ROW + LAST_ROW);
                    final QueryAssertion query = assertQuery("SELECT * FROM (" + sample + ") WHERE x > 0");
                    if (isLight) {
                        query.withPlanContaining("CachedWindowLightSelect")
                                .withPlanNotContaining("__keep_subsample and");
                    } else {
                        query.withPlanContaining("CachedWindow")
                                .withPlanNotContaining("CachedWindowLight");
                    }
                    query.timestamp("ts").returns(HEADER + LAST_ROW);
                }
            }
        });
    }

    @Test
    public void testCompilerReuseAfterInvalidOuterPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final String invalid = "SELECT * FROM (" + SAMPLE + ") WHERE missing > 0";
                assertQuery(invalid).withCompiler(compiler).fails(invalid.indexOf("missing"), "Invalid column: missing");
                assertQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0")
                        .withCompiler(compiler).timestamp("ts").returns(HEADER + LAST_ROW);
                assertQuery("SELECT * FROM (SELECT ts, x, v FROM t) WHERE x = 3")
                        .withCompiler(compiler).timestamp("ts").returns("""
                                ts\tx\tv
                                2024-01-01T02:00:00.000000Z\t3\t30.0
                                """);
            }
        });
    }

    @Test
    public void testConjunctionDisjunctionAndNestedPredicates() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0 AND v < 100")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 3 OR v < 0")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0 AND x < 5")
                    .timestamp("ts").returns(HEADER);
            assertFusedQuery("SELECT * FROM (SELECT * FROM (" + SAMPLE + ") WHERE x > 0) WHERE v < 100")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
            assertFusedQuery("WITH s AS (" + SAMPLE + ") SELECT * FROM s WHERE x > 0")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
        });
    }

    @Test
    public void testEmptyAndAllNullSampleInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG, v DOUBLE) TIMESTAMP(ts)");
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0")
                    .timestamp("ts").returns(HEADER);
            execute("INSERT INTO t SELECT x::timestamp, x, NULL::double FROM long_sequence(5)");
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0")
                    .timestamp("ts").returns(HEADER);
        });
    }

    @Test
    public void testHandWrittenKeepFlagStillMaterializesItsBoolean() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // The spelling matches the internal helper, but no desugaring marker authorizes fusion.
            assertQuery("""
                    SELECT * FROM (
                        SELECT ts, x, v, lttb(ts, v, 2) OVER (ORDER BY ts) AS __keep_subsample FROM t
                    ) WHERE __keep_subsample AND x > 0
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Filter", "CachedWindowLight")
                    .withPlanNotContaining("CachedWindowLightSelect")
                    .returns("""
                            ts\tx\tv\t__keep_subsample
                            2024-01-01T04:00:00.000000Z\t5\t50.0\ttrue
                            """);
        });
    }

    @Test
    public void testInnerWhereStillPushesIntoScan() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String sample = "SELECT ts, x, v FROM (SELECT ts, x, v FROM t) WHERE x > 0 SUBSAMPLE lttb(v, 2)";
            assertFusedQuery(sample).timestamp("ts")
                    .withPlanContaining("CachedWindowLightSelect", "Async ", "filter: 0<x")
                    .returns("""
                            ts\tx\tv
                            2024-01-01T01:00:00.000000Z\t2\t20.0
                            2024-01-01T04:00:00.000000Z\t5\t50.0
                            """);
            assertFusedQuery("SELECT * FROM (" + sample + ") WHERE x < 5")
                    .timestamp("ts").withPlanContaining("CachedWindowLightSelect", "Async ", "filter: 0<x")
                    .returns("""
                            ts\tx\tv
                            2024-01-01T01:00:00.000000Z\t2\t20.0
                            """);
            // An ordinary subquery has no barrier and retains scan-level filtering.
            assertQuery("SELECT * FROM (SELECT ts, x, v FROM t) WHERE x = 3")
                    .timestamp("ts").withPlanContaining("Async ", "filter: x=3")
                    .returns("""
                            ts\tx\tv
                            2024-01-01T02:00:00.000000Z\t3\t30.0
                            """);
        });
    }

    @Test
    public void testJoinBranchRetainsFusionAndPostJoinFiltering() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String expected = """
                    ts\tx\tv\trx
                    2024-01-01T04:00:00.000000Z\t5\t50.0\t5
                    """;
            assertFusedQuery("SELECT s.ts, s.x, s.v, r.x AS rx FROM (" + SAMPLE + ") s ASOF JOIN t r WHERE s.x > 0")
                    .timestamp("ts").noRandomAccess().returns(expected);
            // A predicate on the nullable side must remain a post-join filter, not just a right scan filter.
            assertFusedQuery("SELECT s.ts, s.x, s.v, r.x AS rx FROM (" + SAMPLE + ") s LEFT JOIN t r ON s.x = r.x WHERE r.x > 0")
                    .timestamp("ts").noRandomAccess().returns(expected);
        });
    }

    @Test
    public void testLimitAndOrderBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertFusedQuery("SELECT * FROM (" + SAMPLE + " LIMIT 5) WHERE x > 0")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
            // LIMIT 1 keeps the first sampled row, which the outer predicate removes.
            assertFusedQuery("SELECT * FROM (" + SAMPLE + " LIMIT 1) WHERE x > 0")
                    .timestamp("ts").returns(HEADER);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0 LIMIT 1")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + " ORDER BY ts DESC) WHERE x >= 0")
                    .timestampDesc("ts").returns(HEADER + LAST_ROW + FIRST_ROW);
            assertFusedQuery("SELECT * FROM (SELECT ts, x, v FROM (SELECT * FROM t ORDER BY ts DESC) SUBSAMPLE lttb(v, 2)) WHERE x >= 0")
                    .timestampDesc("ts").returns(HEADER + LAST_ROW + FIRST_ROW);
            // The hidden input sort key must survive the new residual-filter placement.
            assertFusedQuery("SELECT * FROM (SELECT ts, v FROM t SUBSAMPLE lttb(v, 2) ORDER BY x DESC) WHERE v > 0")
                    .returns("""
                            ts\tv
                            2024-01-01T04:00:00.000000Z\t50.0
                            2024-01-01T00:00:00.000000Z\t10.0
                            """);
        });
    }

    @Test
    public void testLttbOuterWhereRetainsFusion() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0")
                    .timestamp("ts").withPlanContaining("CachedWindowLightSelect", "Filter filter: 0<x")
                    .returns(HEADER + LAST_ROW);
        });
    }

    @Test
    public void testNullPredicateAndSampleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG, v DOUBLE) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', NULL, 10.0),
                    ('2024-01-01T01:00:00.000000Z', -1, 20.0),
                    ('2024-01-01T02:00:00.000000Z', 3, NULL),
                    ('2024-01-01T03:00:00.000000Z', 4, 40.0),
                    ('2024-01-01T04:00:00.000000Z', 5, 50.0)
                    """);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > 0")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x IS NULL")
                    .timestamp("ts").returns("""
                            ts\tx\tv
                            2024-01-01T00:00:00.000000Z\tnull\t10.0
                            """);
            assertFusedQuery("SELECT * FROM (SELECT ts, x, v FROM t SUBSAMPLE uniform(3)) WHERE v IS NULL")
                    .timestamp("ts").returns("""
                            ts\tx\tv
                            2024-01-01T02:00:00.000000Z\t3\tnull
                            """);
        });
    }

    @Test
    public void testOuterWhereOverJoinUsesFullWindowFallback() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("""
                    SELECT * FROM (
                        SELECT a.ts, a.x, a.v, b.x AS bx FROM t a ASOF JOIN t b SUBSAMPLE lttb(v, 2)
                    ) WHERE x > 0
                    """)
                    .timestamp("ts")
                    .withPlanContaining("CachedWindow", "AsOf Join")
                    .withPlanNotContaining("CachedWindowLight")
                    .returns("""
                            ts\tx\tv\tbx
                            2024-01-01T04:00:00.000000Z\t5\t50.0\t5
                            """);
        });
    }

    @Test
    public void testOuterWhereOverSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertFusedQuery("SELECT * FROM (SELECT ts, avg(v) AS value FROM t SAMPLE BY 1h SUBSAMPLE lttb(value, 2)) WHERE value > 10")
                    .timestamp("ts").withPlanContaining("CachedWindowLightSelect", "Group By")
                    .returns("""
                            ts\tvalue
                            2024-01-01T04:00:00.000000Z\t50.0
                            """);
            assertFusedQuery("SELECT * FROM (SELECT DISTINCT ts, x, v FROM t SUBSAMPLE uniform(2)) WHERE x > 0")
                    .returns(HEADER + LAST_ROW);
        });
    }

    @Test
    public void testOuterWhereOverWindowProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // An ordered cached window supplies random access, so the keep window can still fuse.
            assertFusedQuery("""
                    SELECT * FROM (
                        SELECT ts, x, v, row_number() OVER (ORDER BY v DESC) AS rn FROM t SUBSAMPLE lttb(v, 2)
                    ) WHERE rn < 5
                    """)
                    .timestamp("ts").returns("""
                            ts\tx\tv\trn
                            2024-01-01T04:00:00.000000Z\t5\t50.0\t1
                            """);
            // A streaming window cannot supply random access. The keep window materializes rows,
            // with or without an outer predicate, and must still expose the original row numbers.
            final String sample = "SELECT ts, x, v, row_number() OVER (ORDER BY ts) AS rn FROM t SUBSAMPLE lttb(v, 2)";
            assertQuery(sample).timestamp("ts")
                    .withPlanContaining("CachedWindow", "functions: [row_number()]")
                    .withPlanNotContaining("CachedWindowLight")
                    .returns("""
                            ts\tx\tv\trn
                            2024-01-01T00:00:00.000000Z\t0\t10.0\t1
                            2024-01-01T04:00:00.000000Z\t5\t50.0\t5
                            """);
            assertQuery("SELECT * FROM (" + sample + ") WHERE rn > 1").timestamp("ts")
                    .withPlanContaining("CachedWindow", "functions: [row_number()]")
                    .withPlanNotContaining("CachedWindowLight")
                    .returns("""
                            ts\tx\tv\trn
                            2024-01-01T04:00:00.000000Z\t5\t50.0\t5
                            """);
        });
    }

    @Test
    public void testTimestampPredicateStaysAfterSampling() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE ts >= '2024-01-01T01:00:00.000000Z'")
                    .timestamp("ts").withPlanNotContaining("Interval forward scan")
                    .returns(HEADER + LAST_ROW);
            bindVariableService.setLong(0, 0);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > $1")
                    .timestamp("ts").returns(HEADER + LAST_ROW);
            bindVariableService.setLong(0, 5);
            assertFusedQuery("SELECT * FROM (" + SAMPLE + ") WHERE x > $1")
                    .timestamp("ts").returns(HEADER);
        });
    }

    @Test
    public void testUnionBranchesAndSharedCte() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertFusedQuery("SELECT * FROM ((" + SAMPLE + ") UNION ALL (" + SAMPLE + ")) WHERE x > 0")
                    .noRandomAccess().returns(HEADER + LAST_ROW + LAST_ROW);
            assertFusedQuery("WITH s AS (" + SAMPLE + ") SELECT * FROM s WHERE x > 0 UNION ALL SELECT * FROM s WHERE x = 0")
                    .noRandomAccess().returns(HEADER + LAST_ROW + FIRST_ROW);
        });
    }

    @Test
    public void testUserKeepColumnNamesDoNotControlBarrier() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE c AS (SELECT ts, x > 0 AS __keep_subsample, x > 3 AS __keep_subsample1, v FROM t) TIMESTAMP(ts)");
            assertFusedQuery("SELECT * FROM (SELECT * FROM c SUBSAMPLE lttb(v, 2)) WHERE __keep_subsample AND __keep_subsample1")
                    .timestamp("ts").returns("""
                            ts\t__keep_subsample\t__keep_subsample1\tv
                            2024-01-01T04:00:00.000000Z\ttrue\ttrue\t50.0
                            """);
            assertFusedQuery("SELECT * FROM (SELECT ts, x AS __KEEP_SUBSAMPLE, v FROM t SUBSAMPLE lttb(v, 2)) WHERE __KEEP_SUBSAMPLE > 0")
                    .timestamp("ts").returns((HEADER + LAST_ROW).replace("ts\tx\tv", "ts\t__KEEP_SUBSAMPLE\tv"));
            // A user's boolean with the helper's spelling must not block ordinary pushdown.
            assertQuery("SELECT * FROM (SELECT * FROM c) WHERE __keep_subsample AND v = 30")
                    .timestamp("ts").withPlanContaining("Async ")
                    .returns("""
                            ts\t__keep_subsample\t__keep_subsample1\tv
                            2024-01-01T02:00:00.000000Z\ttrue\tfalse\t30.0
                            """);
        });
    }

    private static void createTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, x LONG, v DOUBLE) TIMESTAMP(ts)");
        execute("""
                INSERT INTO t VALUES
                ('2024-01-01T00:00:00.000000Z', 0, 10.0),
                ('2024-01-01T01:00:00.000000Z', 2, 20.0),
                ('2024-01-01T02:00:00.000000Z', 3, 30.0),
                ('2024-01-01T03:00:00.000000Z', 4, 40.0),
                ('2024-01-01T04:00:00.000000Z', 5, 50.0)
                """);
    }

    private QueryAssertion assertFusedQuery(String query) {
        return assertQuery(query).withPlanContaining("CachedWindowLightSelect");
    }
}
