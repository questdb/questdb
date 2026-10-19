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
import org.junit.Test;

public class SubsampleOrderByColumnTest extends AbstractCairoTest {
    private static final ObjList<String> METHODS = new ObjList<>();
    private static final String SAMPLE = "SELECT ts, v FROM t SUBSAMPLE uniform(3)";
    private static final String SAMPLED_V_ORDER = """
            ts\tv
            1970-01-01T00:00:00.000000Z\t10
            1970-01-01T02:00:00.000000Z\t20
            1970-01-01T04:00:00.000000Z\t30
            """;
    private static final String SAMPLED_X_ORDER = """
            ts\tv
            1970-01-01T04:00:00.000000Z\t30
            1970-01-01T02:00:00.000000Z\t20
            1970-01-01T00:00:00.000000Z\t10
            """;

    @Test
    public void testAggregationBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("SELECT DISTINCT ts, v FROM t SUBSAMPLE uniform(3) ORDER BY ^x",
                    "ORDER BY expressions must appear in select list");
            assertError("SELECT ts, sum(v) AS v FROM t GROUP BY ts SUBSAMPLE uniform(3) ORDER BY ^x",
                    "ORDER BY expressions must appear in select list");
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT DISTINCT ts, v FROM t SUBSAMPLE uniform(5) ORDER BY v").returns("""
                        ts\tv
                        1970-01-01T00:00:00.000000Z\t10
                        1970-01-01T02:00:00.000000Z\t20
                        1970-01-01T04:00:00.000000Z\t30
                        1970-01-01T03:00:00.000000Z\t40
                        1970-01-01T01:00:00.000000Z\t50
                        """);
            }
        });
    }

    @Test
    public void testAliasShadowingAndQualification() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                // Bare x names the output alias; t.x explicitly names the source column instead.
                final String query = "SELECT ts, v AS x FROM t SUBSAMPLE uniform(3)";
                assertQuery(query + " ORDER BY x").returns(SAMPLED_V_ORDER.replace("ts\tv", "ts\tx"));
                assertQuery(query + " ORDER BY x + 1").returns(SAMPLED_V_ORDER.replace("ts\tv", "ts\tx"));
                assertQuery(query + " ORDER BY t.x").returns(SAMPLED_X_ORDER.replace("ts\tv", "ts\tx"));
                assertQuery("SELECT ts, v AS value FROM t SUBSAMPLE uniform(3) ORDER BY v")
                        .returns(SAMPLED_V_ORDER.replace("ts\tv", "ts\tvalue"));
                assertQuery("SELECT ts, v AS __order_subsample FROM t SUBSAMPLE uniform(3) ORDER BY x")
                        .returns(SAMPLED_X_ORDER.replace("ts\tv", "ts\t__order_subsample"));
                assertQuery("SELECT ts, v AS __keep_subsample FROM t SUBSAMPLE uniform(3) ORDER BY x")
                        .returns(SAMPLED_X_ORDER.replace("ts\tv", "ts\t__keep_subsample"));
                assertQuery("SELECT ts, v FROM (SELECT ts, v, x, v AS __order_subsample FROM t) q "
                        + "SUBSAMPLE uniform(3) ORDER BY x % 2, __order_subsample").returns(SAMPLED_V_ORDER);
            }
        });
    }

    @Test
    public void testAllAlgorithms() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                for (int i = 0; i < METHODS.size(); i++) {
                    // Each method keeps all five rows of this non-collinear fixture.
                    assertQuery("SELECT ts, v FROM t SUBSAMPLE " + METHODS.getQuick(i) + " ORDER BY x").returns("""
                            ts\tv
                            1970-01-01T01:00:00.000000Z\t50
                            1970-01-01T03:00:00.000000Z\t40
                            1970-01-01T04:00:00.000000Z\t30
                            1970-01-01T02:00:00.000000Z\t20
                            1970-01-01T00:00:00.000000Z\t10
                            """);
                }
                assertQuery("SELECT ts, v FROM t SUBSAMPLE lttb(v, 2) ORDER BY x").returns("""
                        ts\tv
                        1970-01-01T04:00:00.000000Z\t30
                        1970-01-01T00:00:00.000000Z\t10
                        """);
            }
        });
    }

    @Test
    public void testCompilerReuseAfterInvalidSort() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int mode = 0; mode < 2; mode++) {
                    setWindowMode(mode);
                    final String invalid = SAMPLE + " ORDER BY missing";
                    assertQuery(invalid).withCompiler(compiler).fails(invalid.indexOf("missing"), "Invalid column: missing");
                    assertQuery(SAMPLE + " ORDER BY x").withCompiler(compiler).returns(SAMPLED_X_ORDER);
                    assertQuery(SAMPLE).withCompiler(compiler).timestamp("ts").returns(SAMPLED_V_ORDER);
                }
            }
        });
    }

    @Test
    public void testCompositeExpressionsAndOrdinals() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery(SAMPLE + " ORDER BY x % 2, v DESC").returns(SAMPLED_X_ORDER);
                assertQuery(SAMPLE + " ORDER BY x + v, t.x").returns(SAMPLED_X_ORDER);
                assertQuery(SAMPLE + " ORDER BY coalesce(NULL, x, 0), 2 DESC").returns(SAMPLED_X_ORDER);
                assertQuery(SAMPLE + " ORDER BY x DESC, 2").returns(SAMPLED_V_ORDER);
                assertQuery(SAMPLE + " ORDER BY 2").returns(SAMPLED_V_ORDER);
                assertQuery(SAMPLE + " ORDER BY 1, x").timestamp("ts").returns(SAMPLED_V_ORDER);
                final String tail = """
                        ts\tv
                        1970-01-01T02:00:00.000000Z\t20
                        1970-01-01T00:00:00.000000Z\t10
                        """;
                assertQuery(SAMPLE + " ORDER BY x LIMIT 1, 3").expectSize().returns(tail);
                assertQuery(SAMPLE + " ORDER BY x LIMIT -2").expectSize().returns(tail);
                assertError(SAMPLE + " ORDER BY x, ^3", "order column position is out of range [max=2]");
            }
        });
    }

    @Test
    public void testHiddenColumnsDoNotWidenSubsampleArguments() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^x, 3) ORDER BY x", "column not found in SELECT list: x");
            assertError("SELECT ts, v FROM t SUBSAMPLE lttb(^__order_subsample, 3) ORDER BY x",
                    "column not found in SELECT list: __order_subsample");
            assertError("SELECT v FROM t ^SUBSAMPLE uniform(3) ORDER BY ts",
                    "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertError("SELECT ts + 1 AS ts, v FROM t ^SUBSAMPLE uniform(3) ORDER BY t.ts",
                    "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
        });
    }

    @Test
    public void testJoinScopeAndWildcardDeduplication() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("SELECT a.ts, a.v FROM t a ASOF JOIN t b SUBSAMPLE uniform(3) ORDER BY ^x", "Ambiguous column");
            assertError("SELECT a.ts AS time, a.v FROM t a ASOF JOIN t b SUBSAMPLE uniform(3) ORDER BY ^ts", "Ambiguous column");
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT a.ts, a.v FROM t a ASOF JOIN t b SUBSAMPLE uniform(3) ORDER BY b.x")
                        .returns(SAMPLED_X_ORDER);
                assertQuery("SELECT a.* FROM (SELECT ts, v FROM t) a ASOF JOIN t b SUBSAMPLE uniform(3) ORDER BY b.x")
                        .returns(SAMPLED_X_ORDER);
                assertQuery("SELECT a.ts, a.v FROM t a JOIN LATERAL (SELECT x FROM t b WHERE b.v = a.v) b ON true "
                        + "SUBSAMPLE uniform(3) ORDER BY b.x").returns(SAMPLED_X_ORDER);
                assertQuery("SELECT * FROM t a ASOF JOIN t b SUBSAMPLE uniform(3) ORDER BY x1").returns("""
                        ts\tv\tx\tts1\tv1\tx1
                        1970-01-01T04:00:00.000000Z\t30\t10\t1970-01-01T04:00:00.000000Z\t30\t10
                        1970-01-01T02:00:00.000000Z\t20\t30\t1970-01-01T02:00:00.000000Z\t20\t30
                        1970-01-01T00:00:00.000000Z\t10\t50\t1970-01-01T00:00:00.000000Z\t10\t50
                        """);
            }
        });
    }

    @Test
    public void testNestedOrderAdviceDoesNotSkipFinalSort() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                // The descending table scan follows the INNER ts advice, not the final v sort.
                // Check a projected key too: fusing the keep-filter must preserve its boundary.
                assertQuery("SELECT ts, v FROM (SELECT * FROM t ORDER BY ts DESC) q SUBSAMPLE uniform(3) ORDER BY v")
                        .withPlanContaining("keys: [v]").returns(SAMPLED_V_ORDER);
            }
        });
    }

    @Test
    public void testNullAndVariableWidthSortColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE n AS (SELECT ts, v, nullif(x, 30) AS k, nullif(x, 30)::VARCHAR AS s FROM t) TIMESTAMP(ts)");
            final String expected = """
                    ts\tv
                    1970-01-01T02:00:00.000000Z\t20
                    1970-01-01T04:00:00.000000Z\t30
                    1970-01-01T00:00:00.000000Z\t10
                    """;
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT ts, v FROM n SUBSAMPLE uniform(3) ORDER BY k").returns(expected);
                assertQuery("SELECT ts, v FROM n SUBSAMPLE uniform(3) ORDER BY s").returns(expected);
            }
        });
    }

    @Test
    public void testQualifiedTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery(SAMPLE + " ORDER BY t.ts").timestamp("ts").returns(SAMPLED_V_ORDER);
                final String renamed = SAMPLED_V_ORDER.replace("ts\tv", "time\tv");
                assertQuery("SELECT ts AS time, v FROM t SUBSAMPLE uniform(3) ORDER BY ts")
                        .timestamp("time").returns(renamed);
                assertQuery("SELECT a.ts AS time, a.v FROM t a ASOF JOIN t b SUBSAMPLE uniform(3) ORDER BY a.ts")
                        .timestamp("time").returns(renamed);
                assertQuery("SELECT source_ts AS time, v FROM (SELECT ts AS source_ts, v FROM t) q "
                        + "SUBSAMPLE uniform(3) ORDER BY q.source_ts").timestamp("time").returns(renamed);
                assertQuery("SELECT 0 AS ts, t.* FROM t SUBSAMPLE uniform(3) ORDER BY t.ts")
                        .timestamp("ts1").returns("""
                                ts\tts1\tv\tx
                                0\t1970-01-01T00:00:00.000000Z\t10\t50
                                0\t1970-01-01T02:00:00.000000Z\t20\t30
                                0\t1970-01-01T04:00:00.000000Z\t30\t10
                                """);
            }
        });
    }

    @Test
    public void testQuotedSortColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String source = " FROM (SELECT ts, v, x AS \"sort.key\", x AS \"in\", x AS \"sort key\" FROM t) q";
            // Match ordinary ORDER BY resolution: dotted names need qualification in this scope.
            assertError("SELECT ts, v" + source + " ORDER BY ^\"sort.key\"", "Invalid table name or alias");
            assertError("SELECT ts, v" + source + " SUBSAMPLE uniform(3) ORDER BY ^\"sort.key\"", "Invalid table name or alias");
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT ts, v" + source + " SUBSAMPLE uniform(3) ORDER BY q.\"sort.key\"").returns(SAMPLED_X_ORDER);
                assertQuery("SELECT ts, v" + source + " SUBSAMPLE uniform(3) ORDER BY \"in\"").returns(SAMPLED_X_ORDER);
                assertQuery("SELECT ts, v" + source + " SUBSAMPLE uniform(3) ORDER BY \"sort key\"").returns(SAMPLED_X_ORDER);
                assertQuery("SELECT ts, v AS \"v.dot\" FROM t SUBSAMPLE uniform(3) ORDER BY x")
                        .returns(SAMPLED_X_ORDER.replace("ts\tv", "ts\tv.dot"));
            }
        });
    }

    @Test
    public void testSubqueryScopeAndOutputMetadata() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertError("SELECT ts, v FROM (SELECT ts, v FROM t) q SUBSAMPLE uniform(3) ORDER BY ^x", "Invalid column: x");
            assertError(SAMPLE + " ORDER BY ^absent.x", "Invalid table name or alias");
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT ts, v FROM (SELECT * FROM t ORDER BY ts DESC) q SUBSAMPLE uniform(3) ORDER BY q.x")
                        .returns(SAMPLED_X_ORDER);
                assertQuery("SELECT * FROM (" + SAMPLE + " ORDER BY x)").returns(SAMPLED_X_ORDER);
                assertQuery("SELECT * FROM (" + SAMPLE + " ORDER BY x) ORDER BY ts").timestamp("ts").returns(SAMPLED_V_ORDER);
                assertQuery("WITH q AS (" + SAMPLE + " ORDER BY x) SELECT * FROM q UNION ALL SELECT * FROM q")
                        .noRandomAccess().returns(SAMPLED_X_ORDER + SAMPLED_X_ORDER.substring("ts\tv\n".length()));
                assertQuery("WITH q AS (SELECT * FROM t) SELECT ts, v FROM q SUBSAMPLE uniform(3) ORDER BY x")
                        .returns(SAMPLED_X_ORDER);
            }
        });
    }

    @Test
    public void testWildcardHelperNameCollisions() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE c AS (SELECT ts, v, x AS __order_subsample, x AS __keep_subsample FROM t) TIMESTAMP(ts)");
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT * FROM c SUBSAMPLE uniform(3) ORDER BY c.__order_subsample").returns("""
                        ts\tv\t__order_subsample\t__keep_subsample
                        1970-01-01T04:00:00.000000Z\t30\t10\t10
                        1970-01-01T02:00:00.000000Z\t20\t30\t30
                        1970-01-01T00:00:00.000000Z\t10\t50\t50
                        """);
            }
        });
    }

    @Test
    public void testWindowProjectionAndInputLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT ts, v, row_number() OVER (ORDER BY v) AS rn FROM t SUBSAMPLE uniform(3) ORDER BY x")
                        .returns("""
                                ts\tv\trn
                                1970-01-01T04:00:00.000000Z\t30\t3
                                1970-01-01T02:00:00.000000Z\t20\t2
                                1970-01-01T00:00:00.000000Z\t10\t1
                                """);
                assertQuery("SELECT ts, v FROM (SELECT * FROM t LIMIT 4) q SUBSAMPLE uniform(2) ORDER BY x LIMIT 1")
                        .expectSize().returns("""
                                ts\tv
                                1970-01-01T03:00:00.000000Z\t40
                                """);
            }
        });
    }

    private static void createTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT, x INT) TIMESTAMP(ts)");
        execute("""
                INSERT INTO t VALUES
                ('1970-01-01T00:00:00.000000Z', 10, 50),
                ('1970-01-01T01:00:00.000000Z', 50, 1),
                ('1970-01-01T02:00:00.000000Z', 20, 30),
                ('1970-01-01T03:00:00.000000Z', 40, 2),
                ('1970-01-01T04:00:00.000000Z', 30, 10)
                """);
    }

    private static void setWindowMode(int mode) {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, mode == 1 ? "true" : "false");
    }

    private void assertError(String markedSql, String message) throws Exception {
        final int position = markedSql.indexOf('^');
        assertQuery(markedSql.replace("^", "")).fails(position, message);
    }

    static {
        METHODS.add("uniform(5)");
        METHODS.add("cadence(1)");
        METHODS.add("lttb(v, 5)");
        METHODS.add("minmax(v, 10)");
        METHODS.add("m4(v, 20)");
        METHODS.add("sdt(v, 0.0)");
    }
}
