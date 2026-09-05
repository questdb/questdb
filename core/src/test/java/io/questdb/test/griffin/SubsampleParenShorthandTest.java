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

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises SUBSAMPLE through the parenthesized table shorthand {@code FROM (t SUBSAMPLE ...)}.
 * <p>
 * The shorthand is parsed as an artificial-star sub-query which
 * {@code SqlParser#parseFromClause} collapses back to a plain table reference when the
 * sub-query carries no clauses. The collapse guard must treat SUBSAMPLE like WHERE or
 * ORDER BY: a sub-query that owns a SUBSAMPLE clause must survive, otherwise the clause
 * is discarded silently and the query returns raw rows. Every test here pins the
 * shorthand to a reference spelling that never enters the collapse path, or pins the
 * exact expected output.
 * <p>
 * The data set is deliberately discriminating: 10 rows against target sizes of 2..6, so
 * a dropped clause always changes the result. Avoid shapes where the target size is
 * greater than or equal to the row count - those are passthroughs and prove nothing.
 */
public class SubsampleParenShorthandTest extends AbstractCairoTest {

    private static final String PINNED_UNIFORM_4 = "v\tts\n" +
            "1.0\t1970-01-01T00:00:00.000000Z\n" +
            "4.0\t1970-01-01T00:00:03.000000Z\n" +
            "7.0\t1970-01-01T00:00:06.000000Z\n" +
            "10.0\t1970-01-01T00:00:09.000000Z\n";

    // -------------------------------------------------------------------------------------------
    // Spelling equivalence: the shorthand must return exactly what the plain spelling returns,
    // for every SUBSAMPLE method.
    // -------------------------------------------------------------------------------------------

    @Test
    public void testAliasedShorthandMatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE uniform(4)",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(4)) x"
            );
        });
    }

    @Test
    public void testExplicitSelectSpellingMatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE uniform(4)",
                    "SELECT v, ts FROM (SELECT * FROM t SUBSAMPLE uniform(4))"
            );
        });
    }

    @Test
    public void testShorthandCadenceMatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE cadence(3)",
                    "SELECT v, ts FROM (t SUBSAMPLE cadence(3))"
            );
        });
    }

    @Test
    public void testShorthandLttbMatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE lttb(v, 4)",
                    "SELECT v, ts FROM (t SUBSAMPLE lttb(v, 4))"
            );
        });
    }

    @Test
    public void testShorthandLttbWithBucketMatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE lttb(v, 4, '2s')",
                    "SELECT v, ts FROM (t SUBSAMPLE lttb(v, 4, '2s'))"
            );
        });
    }

    @Test
    public void testShorthandM4MatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE m4(v, 4)",
                    "SELECT v, ts FROM (t SUBSAMPLE m4(v, 4))"
            );
        });
    }

    @Test
    public void testShorthandMinmaxMatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE minmax(v, 4)",
                    "SELECT v, ts FROM (t SUBSAMPLE minmax(v, 4))"
            );
        });
    }

    @Test
    public void testShorthandUniformMatchesPlainSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE uniform(4)",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(4))"
            );
        });
    }

    // -------------------------------------------------------------------------------------------
    // Pinned results: catch "both spellings equally wrong" regressions that pure
    // cross-spelling equivalence cannot see.
    // -------------------------------------------------------------------------------------------

    @Test
    public void testShorthandKeepsDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the un-collapsed sub-query must propagate the designated timestamp to the outer model
            assertQuery("SELECT v, ts FROM (t SUBSAMPLE uniform(4))")
                    .timestamp("ts")
                    .returns(PINNED_UNIFORM_4);
        });
    }

    @Test
    public void testShorthandPinnedCountValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a dropped clause would count all 10 raw rows
            assertQuery("SELECT count() c FROM (t SUBSAMPLE uniform(4))").noRandomAccess().expectSize().returns("c\n4\n");
        });
    }

    @Test
    public void testShorthandPinnedFullClauseSandwich() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // WHERE + SUBSAMPLE + ORDER BY all inside the shorthand parentheses
            assertQuery("SELECT v, ts FROM (t WHERE v > 0 SUBSAMPLE uniform(2) ORDER BY ts)")
                    .timestamp("ts")
                    .returns("v\tts\n" +
                            "1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "10.0\t1970-01-01T00:00:09.000000Z\n");
        });
    }

    // -------------------------------------------------------------------------------------------
    // Follower matrix: every token that stops alias parsing used to trigger the collapse and
    // silently discard the clause. Each shape is pinned to the explicit-SELECT spelling with
    // an identical follower.
    // -------------------------------------------------------------------------------------------

    @Test
    public void testShorthandDoubleParens() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE uniform(4)",
                    "SELECT v, ts FROM ((t SUBSAMPLE uniform(4)))"
            );
        });
    }

    @Test
    public void testShorthandInCte() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE uniform(4)",
                    "WITH q AS (t SUBSAMPLE uniform(4)) SELECT v, ts FROM q"
            );
        });
    }

    @Test
    public void testShorthandInsideExplicitSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM t SUBSAMPLE uniform(4)",
                    "SELECT * FROM (SELECT v, ts FROM (t SUBSAMPLE uniform(4)))"
            );
        });
    }

    @Test
    public void testShorthandThenAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT * FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) ASOF JOIN u2",
                    "SELECT * FROM (t SUBSAMPLE uniform(4)) ASOF JOIN u2"
            );
        });
    }

    @Test
    public void testShorthandThenChainedSubsample() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // SUBSAMPLE is itself an alias-stop token: 10 rows -> uniform(6) -> uniform(3)
            assertSqlCursors(
                    "SELECT v, ts FROM (SELECT v, ts FROM t SUBSAMPLE uniform(6)) SUBSAMPLE uniform(3)",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(6)) SUBSAMPLE uniform(3)"
            );
            assertQuery("SELECT v, ts FROM (t SUBSAMPLE uniform(6)) SUBSAMPLE uniform(3)")
                    .timestamp("ts")
                    .returns("v\tts\n" +
                            "1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "6.0\t1970-01-01T00:00:05.000000Z\n" +
                            "10.0\t1970-01-01T00:00:09.000000Z\n");
        });
    }

    @Test
    public void testShorthandThenCrossJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT * FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) CROSS JOIN u2",
                    "SELECT * FROM (t SUBSAMPLE uniform(4)) CROSS JOIN u2"
            );
        });
    }

    @Test
    public void testShorthandThenGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, count() c FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) GROUP BY v ORDER BY v",
                    "SELECT v, count() c FROM (t SUBSAMPLE uniform(4)) GROUP BY v ORDER BY v"
            );
        });
    }

    @Test
    public void testShorthandThenLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT * FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) LATEST ON ts PARTITION BY v",
                    "SELECT * FROM (t SUBSAMPLE uniform(4)) LATEST ON ts PARTITION BY v"
            );
        });
    }

    @Test
    public void testShorthandThenLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) LIMIT 2",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(4)) LIMIT 2"
            );
        });
    }

    @Test
    public void testShorthandThenOrderByDesc() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) ORDER BY ts DESC",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(4)) ORDER BY ts DESC"
            );
        });
    }

    @Test
    public void testShorthandThenSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT avg(v) a FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) SAMPLE BY 5s",
                    "SELECT avg(v) a FROM (t SUBSAMPLE uniform(4)) SAMPLE BY 5s"
            );
        });
    }

    @Test
    public void testShorthandThenTimestampClause() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // "timestamp" never triggered the collapse; pinned as a control. Note: a timestamp()
            // suffix on a sub-query reorders output columns timestamp-first for every spelling,
            // so the reference must carry the same suffix.
            assertSqlCursors(
                    "SELECT v, ts FROM (SELECT * FROM t SUBSAMPLE uniform(4)) timestamp(ts)",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(4)) timestamp(ts)"
            );
        });
    }

    @Test
    public void testShorthandThenUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) UNION ALL SELECT v, ts FROM t",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(4)) UNION ALL SELECT v, ts FROM t"
            );
        });
    }

    @Test
    public void testShorthandThenWhere() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the outer WHERE filters rows the sub-query already down-sampled
            assertSqlCursors(
                    "SELECT v, ts FROM (SELECT v, ts FROM t SUBSAMPLE uniform(4)) WHERE v > 2",
                    "SELECT v, ts FROM (t SUBSAMPLE uniform(4)) WHERE v > 2"
            );
        });
    }

    // -------------------------------------------------------------------------------------------
    // Plan canaries: the shorthand must produce the same plan as the plain spelling and must
    // contain the window node. A missing window node means the clause was dropped again.
    // -------------------------------------------------------------------------------------------

    @Test
    public void testAliasedShorthandPlanMatchesShorthandPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "EXPLAIN SELECT v, ts FROM (t SUBSAMPLE uniform(4)) x",
                    "EXPLAIN SELECT v, ts FROM (t SUBSAMPLE uniform(4))"
            );
        });
    }

    @Test
    public void testShorthandPlanMatchesPlainSpellingPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "EXPLAIN SELECT v, ts FROM t SUBSAMPLE uniform(4)",
                    "EXPLAIN SELECT v, ts FROM (t SUBSAMPLE uniform(4))"
            );
        });
    }

    @Test
    public void testShorthandPlanUsesWindowPath() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            printSql("EXPLAIN SELECT v, ts FROM (t SUBSAMPLE uniform(4))");
            final String plan = sink.toString();
            Assert.assertTrue("shorthand SUBSAMPLE must use the window path: " + plan, plan.contains("CachedWindow"));
            Assert.assertTrue("shorthand SUBSAMPLE must retain the method call: " + plan, plan.contains("uniform(4)"));
        });
    }

    // -------------------------------------------------------------------------------------------
    // Collapse preservation: the guard must not stop the collapse for sub-queries without a
    // SUBSAMPLE clause, and clauses that already blocked the collapse must keep doing so.
    // -------------------------------------------------------------------------------------------

    @Test
    public void testParenWithWhereStillNotCollapsed() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT v, ts FROM (SELECT * FROM t WHERE v > 5)",
                    "SELECT v, ts FROM (t WHERE v > 5)"
            );
        });
    }

    @Test
    public void testPlainParenTableStillCollapses() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // no SUBSAMPLE inside: the sub-query must still collapse to a bare table scan
            assertSqlCursors(
                    "EXPLAIN SELECT v, ts FROM t",
                    "EXPLAIN SELECT v, ts FROM (t)"
            );
            printSql("EXPLAIN SELECT v, ts FROM (t)");
            final String plan = sink.toString();
            Assert.assertFalse("plain (t) must stay collapsed: " + plan, plan.contains("CachedWindow"));
        });
    }

    // -------------------------------------------------------------------------------------------
    // Error surface: malformed SUBSAMPLE inside the shorthand must report the same diagnostics,
    // at positions inside the parentheses, instead of being collapsed away or reported at the
    // closing brace.
    // -------------------------------------------------------------------------------------------

    @Test
    public void testShorthandDuplicateSubsampleClause() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException(
                    "SELECT v FROM (t SUBSAMPLE uniform(2) SUBSAMPLE uniform(3))",
                    38,
                    "duplicate SUBSAMPLE clause"
            );
        });
    }

    @Test
    public void testShorthandEmptyArgList() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException(
                    "SELECT v FROM (t SUBSAMPLE uniform())",
                    35,
                    "expression expected"
            );
        });
    }

    @Test
    public void testShorthandMethodWithoutParens() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException(
                    "SELECT v FROM (t SUBSAMPLE uniform)",
                    35,
                    "'(' expected after subsample method name"
            );
        });
    }

    @Test
    public void testShorthandMissingMethodName() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException(
                    "SELECT v FROM (t SUBSAMPLE)",
                    27,
                    "subsample method name expected"
            );
        });
    }

    @Test
    public void testShorthandSubsampleBeforeWhere() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException(
                    "SELECT v FROM (t SUBSAMPLE uniform(2) WHERE v > 0)",
                    38,
                    "SUBSAMPLE must be placed after the WHERE, LATEST ON, SAMPLE BY, GROUP BY and WINDOW clauses"
            );
        });
    }

    @Test
    public void testShorthandUnclosedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertException(
                    "SELECT v FROM (t SUBSAMPLE uniform(2)",
                    37,
                    "')' expected"
            );
        });
    }

    // -------------------------------------------------------------------------------------------

    private static void createTables() throws Exception {
        execute("CREATE TABLE t AS (" +
                "SELECT x::double v, timestamp_sequence(0, 1000000) ts FROM long_sequence(10)) TIMESTAMP(ts)");
        execute("CREATE TABLE u2 AS (" +
                "SELECT x::double w, timestamp_sequence(500000, 1000000) ts FROM long_sequence(10)) TIMESTAMP(ts)");
    }
}
