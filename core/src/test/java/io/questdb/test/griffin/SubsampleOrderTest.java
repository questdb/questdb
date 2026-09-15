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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SubsampleOrderTest extends AbstractCairoTest {
    private static final String ASCENDING = """
            ts\tv
            1970-01-01T00:00:00.000000Z\t30
            1970-01-01T01:00:00.000000Z\t10
            1970-01-01T02:00:00.000000Z\t40
            1970-01-01T03:00:00.000000Z\t20
            """;
    private static final String DESCENDING = """
            ts\tv
            1970-01-01T03:00:00.000000Z\t20
            1970-01-01T02:00:00.000000Z\t40
            1970-01-01T01:00:00.000000Z\t10
            1970-01-01T00:00:00.000000Z\t30
            """;
    private static final ObjList<String> METHODS = new ObjList<>();
    private static final String PIVOT_QUERY = """
            SELECT *
            FROM (SELECT * FROM p ORDER BY v LIMIT 4)
            PIVOT (sum(v) FOR c IN ('a') GROUP BY ts)
            SUBSAMPLE uniform(4)
            """;
    private static final String SOURCE = "SELECT ts, v FROM (SELECT ts, v FROM t ORDER BY v) SUBSAMPLE ";
    private static final String VALUE_ORDER = """
            ts\tv
            1970-01-01T01:00:00.000000Z\t10
            1970-01-01T03:00:00.000000Z\t20
            1970-01-01T00:00:00.000000Z\t30
            1970-01-01T02:00:00.000000Z\t40
            """;

    @Test
    public void testAllAlgorithmsPreserveInputOrderMetadata() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                for (int i = 0; i < METHODS.size(); i++) {
                    // The window orders its computation by ts, not the returned rows. The fluent
                    // builder also checks that value-ordered output has no designated timestamp.
                    assertQuery(SOURCE + METHODS.getQuick(i)).returns(VALUE_ORDER);
                }
            }
        });
    }

    @Test
    public void testCompilerReusePreservesExplicitTimestampDeclarations() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0; i < 3; i++) {
                    assertQuery(SOURCE + "uniform(4)").withCompiler(compiler).returns(VALUE_ORDER);
                    assertQuery("SELECT * FROM (SELECT x::TIMESTAMP AS ts FROM long_sequence(2)) TIMESTAMP(ts)")
                            .withCompiler(compiler)
                            .timestamp("ts")
                            .expectSize()
                            .returns("""
                                    ts
                                    1970-01-01T00:00:00.000001Z
                                    1970-01-01T00:00:00.000002Z
                                    """);
                }
            }
        });
    }

    @Test
    public void testDifferentTimestampSortKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String query = """
                    SELECT ts, other_ts, v
                    FROM (SELECT ts, v::TIMESTAMP AS other_ts, v FROM t ORDER BY other_ts)
                    SUBSAMPLE uniform(4)
                    """;
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                // Inherit the actual designation, even when it names another timestamp column.
                assertQuery(query).timestamp("other_ts").returns("""
                        ts\tother_ts\tv
                        1970-01-01T01:00:00.000000Z\t1970-01-01T00:00:00.000010Z\t10
                        1970-01-01T03:00:00.000000Z\t1970-01-01T00:00:00.000020Z\t20
                        1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000030Z\t30
                        1970-01-01T02:00:00.000000Z\t1970-01-01T00:00:00.000040Z\t40
                        """);
                assertQuery("SELECT ts, v FROM (" + query + ") ORDER BY ts")
                        .timestamp("ts").returns(ASCENDING);
            }
        });
    }

    @Test
    public void testNanosecondOuterTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE n AS (SELECT ts::TIMESTAMP_NS AS ts, v FROM t) TIMESTAMP(ts)");
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT * FROM (SELECT ts, v FROM (SELECT * FROM n ORDER BY v) SUBSAMPLE uniform(4)) ORDER BY ts")
                        .timestamp("ts").returns(ASCENDING.replace(".000000Z", ".000000000Z"));
            }
        });
    }

    @Test
    public void testNestedSubsamplesWithQuotedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("""
                        SELECT * FROM (
                            SELECT * FROM (
                                SELECT ts AS "clock.ts", v
                                FROM (SELECT * FROM t ORDER BY v)
                                SUBSAMPLE uniform(4)
                            ) SUBSAMPLE uniform(4)
                        ) s ORDER BY s."clock.ts"
                        """)
                        .timestamp("clock.ts").returns(ASCENDING.replace("ts\tv", "clock.ts\tv"));
            }
        });
    }

    @Test
    public void testNoSubsampleControl() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM (SELECT ts, v FROM (SELECT ts, v FROM t ORDER BY v)) ORDER BY ts")
                    .timestamp("ts").expectSize().returns(ASCENDING);
        });
    }

    @Test
    public void testNullValuesWithOuterTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String source = "SELECT ts, v FROM (SELECT ts, nullif(v, 10) AS v FROM t ORDER BY v) SUBSAMPLE ";
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT * FROM (" + source + "uniform(4)) ORDER BY ts")
                        .timestamp("ts").returns(ASCENDING.replace("Z\t10", "Z\tnull"));
                assertQuery("SELECT * FROM (" + source + "lttb(v, 4)) ORDER BY ts")
                        .timestamp("ts").returns("""
                                ts\tv
                                1970-01-01T00:00:00.000000Z\t30
                                1970-01-01T02:00:00.000000Z\t40
                                1970-01-01T03:00:00.000000Z\t20
                                """);
            }
        });
    }

    @Test
    public void testOuterTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                for (int i = 0; i < METHODS.size(); i++) {
                    final String query = "SELECT * FROM (" + SOURCE + METHODS.getQuick(i) + ")";
                    assertQuery(query + " ORDER BY ts").timestamp("ts").returns(ASCENDING);
                    assertQuery(query + " ORDER BY ts").assertsPlanContaining("keys: [ts]");
                    assertQuery(query + " ORDER BY ts DESC").timestampDesc("ts").returns(DESCENDING);
                    assertQuery(query + " ORDER BY ts, v").timestamp("ts").returns(ASCENDING);
                }
            }
        });
    }

    @Test
    public void testPivotDoesNotClaimTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                // PIVOT uses hash GROUP BY: even a timestamp key does not establish output order.
                // Check metadata directly, then assert the rows under a deterministic value sort.
                try (RecordCursorFactory factory = select(PIVOT_QUERY)) {
                    Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
                }
                assertQuery("SELECT * FROM (" + PIVOT_QUERY + ") ORDER BY a")
                        .returns(VALUE_ORDER.replace("ts\tv", "ts\ta"));
            }
        });
    }

    @Test
    public void testPivotOuterTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createPivotTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                // LIMIT keeps the input value sort below the hash aggregation. The old rewrite
                // advertised timestamp order over the shuffled groups and omitted this outer sort.
                assertQuery("SELECT * FROM (" + PIVOT_QUERY + ") ORDER BY ts")
                        .timestamp("ts").returns(ASCENDING.replace("ts\tv", "ts\ta"));
                assertQuery("SELECT * FROM (" + PIVOT_QUERY + ") ORDER BY ts")
                        .assertsPlanContaining("keys: [ts]");
            }
        });
    }

    @Test
    public void testSharedCtePreservesTimestampOrderMetadata() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("""
                        WITH q AS (
                            SELECT ts AS "clock.ts", v
                            FROM (SELECT * FROM t ORDER BY v)
                            SUBSAMPLE uniform(4)
                        )
                        SELECT * FROM (
                            SELECT * FROM q
                            UNION ALL
                            SELECT * FROM q WHERE v < 0
                        ) s ORDER BY s."clock.ts"
                        """)
                        .timestamp("clock.ts").returns(ASCENDING.replace("ts\tv", "clock.ts\tv"));
            }
        });
    }

    @Test
    public void testSortedSubsampleAsTemporalJoinOperand() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String right = "(SELECT * FROM (" + SOURCE + "uniform(2)) ORDER BY ts) r";
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT l.ts, l.v, r.v AS rv FROM t l ASOF JOIN " + right)
                        .timestamp("ts").expectSize().noRandomAccess().returns("""
                                ts\tv\trv
                                1970-01-01T00:00:00.000000Z\t30\t30
                                1970-01-01T01:00:00.000000Z\t10\t30
                                1970-01-01T02:00:00.000000Z\t40\t30
                                1970-01-01T03:00:00.000000Z\t20\t20
                                """);
                assertQuery("SELECT l.ts, l.v, r.v AS rv FROM t l LT JOIN " + right)
                        .timestamp("ts").expectSize().noRandomAccess().returns("""
                                ts\tv\trv
                                1970-01-01T00:00:00.000000Z\t30\tnull
                                1970-01-01T01:00:00.000000Z\t10\t30
                                1970-01-01T02:00:00.000000Z\t40\t30
                                1970-01-01T03:00:00.000000Z\t20\t30
                                """);
            }
        });
    }

    @Test
    public void testSortedSubsampleSupportsSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT ts, sum(v) AS v FROM (SELECT * FROM (" + SOURCE + "uniform(4)) ORDER BY ts) SAMPLE BY 1h")
                        .timestamp("ts").noRandomAccess().returns(ASCENDING);
            }
        });
    }

    @Test
    public void testSubsetAndLimitPreserveInputOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String ascendingSubset = """
                    ts\tv
                    1970-01-01T00:00:00.000000Z\t30
                    1970-01-01T03:00:00.000000Z\t20
                    """;
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                final String query = SOURCE + "uniform(2)";
                assertQuery(query).returns("""
                        ts\tv
                        1970-01-01T03:00:00.000000Z\t20
                        1970-01-01T00:00:00.000000Z\t30
                        """);
                assertQuery(query + " LIMIT 1").returns("ts\tv\n1970-01-01T03:00:00.000000Z\t20\n");
                assertQuery("SELECT * FROM (" + query + ") ORDER BY ts")
                        .timestamp("ts").returns(ascendingSubset);
                assertQuery("SELECT * FROM (" + query + ") ORDER BY ts LIMIT 1")
                        .timestamp("ts").expectSize().returns("ts\tv\n1970-01-01T00:00:00.000000Z\t30\n");
                assertQuery("SELECT * FROM (" + query + ") ORDER BY ts, v LIMIT 2")
                        .timestamp("ts").expectSize().returns(ascendingSubset);
            }
        });
    }

    @Test
    public void testTimestampOrderedInputsNeedNoExtraSort() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (int mode = 0; mode < 2; mode++) {
                setWindowMode(mode);
                assertQuery("SELECT ts, v FROM t SUBSAMPLE uniform(4)")
                        .timestamp("ts").withPlanNotContaining("keys: [ts]").returns(ASCENDING);
                assertQuery("SELECT ts, v FROM (SELECT * FROM t ORDER BY ts DESC) SUBSAMPLE uniform(4)")
                        .timestampDesc("ts").withPlanNotContaining("keys: [ts]").returns(DESCENDING);
            }
        });
    }

    private static void createPivotTable() throws SqlException {
        createTable();
        execute("CREATE TABLE p AS (SELECT ts, 'a'::SYMBOL AS c, v FROM t) TIMESTAMP(ts)");
    }

    private static void createTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts)");
        execute("""
                INSERT INTO t VALUES
                ('1970-01-01T00:00:00.000000Z', 30),
                ('1970-01-01T01:00:00.000000Z', 10),
                ('1970-01-01T02:00:00.000000Z', 40),
                ('1970-01-01T03:00:00.000000Z', 20)
                """);
    }

    private static void setWindowMode(int mode) {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, mode == 1 ? "true" : "false");
    }

    static {
        METHODS.add("uniform(4)");
        METHODS.add("cadence(1)");
        METHODS.add("lttb(v, 4)");
        METHODS.add("minmax(v, 4)");
        METHODS.add("m4(v, 4)");
        METHODS.add("sdt(v, 0.0)");
    }
}
