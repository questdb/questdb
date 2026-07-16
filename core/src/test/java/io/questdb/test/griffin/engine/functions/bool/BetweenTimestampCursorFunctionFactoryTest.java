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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Exercises the scalar-sub-query BETWEEN overloads: {@code between(NCC)}, {@code between(NCN)}
 * and {@code between(NNC)}. These cover every context where the designated-timestamp interval
 * intrinsic does not apply: projections, CASE expressions, non-designated timestamp filters and
 * OR-residual designated filters. The results must match both the {@code between(NNN)} runtime
 * semantics (reversed-bound normalization, NULL evaluates to false) and the interval-intrinsic
 * results for identical data.
 */
public class BetweenTimestampCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCaseWhenDualCursor() throws Exception {
        // note: base-table column references resolve inside the expression (x in THEN), but a
        // bare base-table column cannot sit next to a scalar-sub-query expression in the same
        // projection; that is a pre-existing SqlOptimiser limitation shared with = and >
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT CASE WHEN ts BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b) THEN x ELSE -1 END f FROM t")
                    .expectSize()
                    .returns("""
                            f
                            -1
                            1
                            2
                            3
                            -1
                            """);
        });
    }

    @Test
    public void testDualCursorInProjection() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT ts BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b) f FROM t")
                    .expectSize()
                    .returns("""
                            f
                            false
                            true
                            true
                            true
                            false
                            """);
        });
    }

    @Test
    public void testEmptySubQueryYieldsNoRows() throws Exception {
        // an empty scalar sub-query produces a NULL bound; between with a NULL bound is false,
        // matching both between(NNN) and the interval-intrinsic path
        assertMemoryLeak(() -> {
            createBaseTables();
            // function path: non-designated timestamp column
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b WHERE 1 <> 1) AND (SELECT hi FROM b)")
                    .returns("x\n");
            // intrinsic path must agree on identical data
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b WHERE 1 <> 1) AND (SELECT hi FROM b)")
                    .returns("x\n");
            // projection: NULL bound evaluates to false, not NULL
            assertQuery("SELECT ts BETWEEN (SELECT lo FROM b WHERE 1 <> 1) AND (SELECT hi FROM b) f FROM t")
                    .expectSize()
                    .returns("""
                            f
                            false
                            false
                            false
                            false
                            false
                            """);
        });
    }

    @Test
    public void testIntrinsicControlStillUsesIntervalScan() throws Exception {
        // regression guard: a conjunctive designated-timestamp BETWEEN must keep using the
        // interval scan intrinsic instead of the new cursor-bound between() function
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b)")
                    .withPlanContaining("Interval forward scan")
                    .withPlanNotContaining("between")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testInvalidStringValueInSubQueryFails() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            final String loQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT 'hello') AND (SELECT hi FROM b)";
            assertQuery(loQuery)
                    .fails(loQuery.indexOf("(SELECT 'hello'") + 1, "the cursor selected invalid timestamp value: hello");
            final String hiQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND (SELECT 'hello'::varchar)";
            assertQuery(hiQuery)
                    .fails(hiQuery.indexOf("(SELECT 'hello'") + 1, "the cursor selected invalid timestamp value: hello");
        });
    }

    @Test
    public void testLeftOperandNotTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            final String query = "SELECT x FROM t WHERE x BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b)";
            assertQuery(query)
                    .fails(query.indexOf("x BETWEEN"), "left operand must be a TIMESTAMP, found: INT");
        });
    }

    @Test
    public void testMixedConstAndCursorBounds() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            // between(NCN): cursor lower bound, constant upper bound
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND '2020-01-01T03:00:00.000000Z'")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            // between(NNC): constant lower bound, cursor upper bound
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN '2020-01-01T01:00:00.000000Z' AND (SELECT hi FROM b)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testMultiColumnSubQueryFails() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            final String loQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo, hi FROM b) AND (SELECT hi FROM b)";
            assertQuery(loQuery)
                    .fails(loQuery.indexOf("(SELECT lo, hi") + 1, "select must provide exactly one column");
            final String hiQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND (SELECT lo, hi FROM b)";
            assertQuery(hiQuery)
                    .fails(hiQuery.indexOf("(SELECT lo, hi") + 1, "select must provide exactly one column");
        });
    }

    @Test
    public void testMultiRowSubQueryFails() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            // lower bound sub-query returns two rows
            final String loQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT ts FROM t LIMIT 2) AND (SELECT hi FROM b)";
            assertQuery(loQuery)
                    .fails(loQuery.indexOf("(SELECT ts") + 1, "scalar sub-query returned more than one row");
            // upper bound sub-query returns two rows
            final String hiQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND (SELECT ts FROM t LIMIT 2)";
            assertQuery(hiQuery)
                    .fails(hiQuery.indexOf("(SELECT ts") + 1, "scalar sub-query returned more than one row");
            // projection context fails during init as well; the ownership of both cursor
            // factories must survive the failed init without leaking
            final String projectionQuery = "SELECT ts BETWEEN (SELECT ts FROM t LIMIT 2) AND (SELECT hi FROM b) f FROM t";
            assertQuery(projectionQuery)
                    .fails(projectionQuery.indexOf("(SELECT ts") + 1, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testNonDesignatedTimestampFilter() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b)")
                    .withPlanContaining("between")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testNonTimestampCursorColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            final String loQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT x FROM t LIMIT 1) AND (SELECT hi FROM b)";
            assertQuery(loQuery)
                    .fails(loQuery.indexOf("(SELECT x") + 1, "cannot compare TIMESTAMP and INT");
            final String hiQuery = "SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND (SELECT x FROM t LIMIT 1)";
            assertQuery(hiQuery)
                    .fails(hiQuery.indexOf("(SELECT x") + 1, "cannot compare TIMESTAMP and INT");
        });
    }

    @Test
    public void testNotBetweenDualCursor() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b)")
                    .returns("""
                            x
                            0
                            4
                            """);
        });
    }

    @Test
    public void testNsColumnWithMicroCursorBounds() throws Exception {
        // a nanosecond column with microsecond cursor bounds converts the bounds up exactly
        assertMemoryLeak(() -> {
            createNanoTables();
            assertQuery("SELECT x FROM t_ns WHERE ts2n BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            // intrinsic path must agree on identical data
            assertQuery("SELECT x FROM t_ns WHERE tsn BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            // nanosecond cursor bounds on the nanosecond column need no conversion
            assertQuery("SELECT x FROM t_ns WHERE ts2n BETWEEN (SELECT lo FROM b_ns) AND (SELECT hi FROM b_ns)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testNullLeftOperand() throws Exception {
        // a NULL left operand evaluates to false, same as between(NNN)
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT null BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b) f")
                    .expectSize()
                    .returns("""
                            f
                            false
                            """);
        });
    }

    @Test
    public void testNullValueInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            execute("CREATE TABLE b_null (lo TIMESTAMP, hi TIMESTAMP)");
            execute("INSERT INTO b_null VALUES (null, '2020-01-01T03:00:00.000000Z')");
            // NULL bound value evaluates to false for every row
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null)")
                    .returns("x\n");
            // NOT BETWEEN with a NULL bound is the negation of false: every row matches
            assertQuery("SELECT x FROM t WHERE ts2 NOT BETWEEN (SELECT lo FROM b_null) AND (SELECT hi FROM b_null)")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
            // an explicit (SELECT null) bound behaves the same
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT null) AND (SELECT hi FROM b)")
                    .returns("x\n");
        });
    }

    @Test
    public void testOrResidualMatchesIntrinsicScan() throws Exception {
        // under OR the designated-timestamp BETWEEN cannot be extracted as an intrinsic; the
        // residual between() function must produce rows consistent with the interval scan
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b) AND (SELECT hi FROM b) OR x = 0")
                    .withPlanContaining("between")
                    .withPlanNotContaining("Interval forward scan")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testPerRowScalarHiBoundWithCursorLo() throws Exception {
        // the non-cursor bound of a mixed signature stays a per-row expression, and reversed
        // bounds normalize per row via min/max exactly like between(NNN): with the upper bound
        // dateadd('h', -2, ts) the range flips whenever ts - 2h < lo, so only the first two
        // rows fall inside their own range; the between(NNN) control with a constant lower
        // bound of the same value must return the identical rows
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND dateadd('h', -2, ts)")
                    .returns("""
                            x
                            0
                            1
                            """);
            // between(NNN) control: identical bounds without the sub-query
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN '2020-01-01T01:00:00.000000Z' AND dateadd('h', -2, ts)")
                    .returns("""
                            x
                            0
                            1
                            """);
            // the upper bound equal to the value itself always matches non-null rows, same
            // as between(NNN): the per-row range is [min(lo, ts), max(lo, ts)] and ts is
            // always inside it
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b) AND ts")
                    .returns("""
                            x
                            0
                            1
                            2
                            3
                            4
                            """);
        });
    }

    @Test
    public void testReversedCursorBounds() throws Exception {
        // between(NNN) normalizes reversed bounds via min/max; the cursor overloads and the
        // interval intrinsic must do the same
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT hi FROM b) AND (SELECT lo FROM b)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT hi FROM b) AND (SELECT lo FROM b)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testStringAndVarcharCursorBounds() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTables();
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT '2020-01-01T01') AND (SELECT '2020-01-01T03'::varchar)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testUsColumnWithNanoCursorBounds() throws Exception {
        // nanosecond cursor bounds on a microsecond column floor to microsecond precision, the
        // exact conversion the interval intrinsic performs; both paths must return the same rows
        assertMemoryLeak(() -> {
            createBaseTables();
            createNanoBoundsWithSubMicroOffsets();
            assertQuery("SELECT x FROM t WHERE ts2 BETWEEN (SELECT lo FROM b_ns_frac) AND (SELECT hi FROM b_ns_frac)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            assertQuery("SELECT x FROM t WHERE ts BETWEEN (SELECT lo FROM b_ns_frac) AND (SELECT hi FROM b_ns_frac)")
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
        });
    }

    private void createBaseTables() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                  ('2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z', 0),
                  ('2020-01-01T01:00:00.000000Z', '2020-01-01T01:00:00.000000Z', 1),
                  ('2020-01-01T02:00:00.000000Z', '2020-01-01T02:00:00.000000Z', 2),
                  ('2020-01-01T03:00:00.000000Z', '2020-01-01T03:00:00.000000Z', 3),
                  ('2020-01-01T04:00:00.000000Z', '2020-01-01T04:00:00.000000Z', 4)
                """);
        execute("CREATE TABLE b (lo TIMESTAMP, hi TIMESTAMP)");
        execute("INSERT INTO b VALUES ('2020-01-01T01:00:00.000000Z', '2020-01-01T03:00:00.000000Z')");
    }

    private void createNanoBoundsWithSubMicroOffsets() throws SqlException {
        execute("CREATE TABLE b_ns_frac (lo TIMESTAMP_NS, hi TIMESTAMP_NS)");
        execute("INSERT INTO b_ns_frac VALUES ('2020-01-01T01:00:00.000000500Z', '2020-01-01T03:00:00.000000700Z')");
    }

    private void createNanoTables() throws SqlException {
        createBaseTables();
        execute("CREATE TABLE t_ns (tsn TIMESTAMP_NS, ts2n TIMESTAMP_NS, x INT) TIMESTAMP(tsn) PARTITION BY DAY");
        execute("""
                INSERT INTO t_ns VALUES
                  ('2020-01-01T00:00:00.000000000Z', '2020-01-01T00:00:00.000000000Z', 0),
                  ('2020-01-01T01:00:00.000000000Z', '2020-01-01T01:00:00.000000000Z', 1),
                  ('2020-01-01T02:00:00.000000000Z', '2020-01-01T02:00:00.000000000Z', 2),
                  ('2020-01-01T03:00:00.000000000Z', '2020-01-01T03:00:00.000000000Z', 3),
                  ('2020-01-01T04:00:00.000000000Z', '2020-01-01T04:00:00.000000000Z', 4)
                """);
        execute("CREATE TABLE b_ns (lo TIMESTAMP_NS, hi TIMESTAMP_NS)");
        execute("INSERT INTO b_ns VALUES ('2020-01-01T01:00:00.000000000Z', '2020-01-01T03:00:00.000000000Z')");
    }
}
