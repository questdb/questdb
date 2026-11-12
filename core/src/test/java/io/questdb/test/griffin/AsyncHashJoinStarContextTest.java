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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Stacked and swapped stars of the parallel hash join in the SQL contexts around them: under
 * LATEST ON, in UNION, EXCEPT and INTERSECT, as a CTE and a sub-query, as the master and as the
 * slave of ASOF and LT joins, and as the source of CREATE TABLE AS SELECT and INSERT INTO SELECT,
 * with and without a batch size, into WAL and non-WAL tables. Every query compares with the ordinary
 * plan of the same SQL, in order, and every written table with the one that the ordinary plan wrote.
 */
public class AsyncHashJoinStarContextTest extends AbstractCairoTest {
    // A stack of three joins over t: an INNER join, a LEFT join with an ON residual and a translated
    // SYMBOL key.
    private static final String STAR = "SELECT t.ts, t.sym, t.k, t.v, d.w, e.ev, g.gw FROM t JOIN d ON t.k = d.k"
            + " LEFT JOIN e ON t.k = e.k AND e.ev > t.v % 5 LEFT JOIN g ON t.sym = g.sym";
    // A stack whose first join, a LEFT one, keeps t's order, and with it t's designated timestamp,
    // which a time-series join over or under the stack needs.
    private static final String STAR_LEFT = "SELECT t.ts, t.sym, t.k, t.v, d.w, e.ev, g.gw FROM t LEFT JOIN d ON t.k = d.k"
            + " JOIN e ON t.k = e.k AND e.ev > t.v % 5 LEFT JOIN g ON t.sym = g.sym";
    // A stack whose first join swaps: d, the smaller input, is its left input.
    private static final String SWAPPED = "SELECT d.w, t.ts, t.v, e.ev FROM d JOIN t ON d.k = t.k LEFT JOIN e ON d.k = e.k";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCreateTableAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            int table = 0;
            for (String wal : new String[]{" BYPASS WAL", " WAL"}) {
                for (String sql : new String[]{STAR, STAR + " WHERE t.v % 3 = 0", SWAPPED}) {
                    final String timestamp = sql.equals(SWAPPED) ? "" : " TIMESTAMP(ts) PARTITION BY HOUR" + wal;
                    final String ordinaryTable = "c" + table++;
                    final String operatorTable = "c" + table++;
                    sqlExecutionContext.setParallelHashJoinEnabled(false);
                    execute("CREATE TABLE " + ordinaryTable + " AS (" + sql + ")" + timestamp);
                    sqlExecutionContext.setParallelHashJoinEnabled(true);
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    execute("CREATE TABLE " + operatorTable + " AS (" + sql + ")" + timestamp);
                    drainWalQueue();
                    assertSameRows(ordinaryTable, operatorTable);
                }
            }
        });
    }

    @Test
    public void testInsertIntoSelect() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            int table = 0;
            for (String wal : new String[]{" BYPASS WAL", " WAL"}) {
                for (String insert : new String[]{"INSERT INTO ", "INSERT BATCH 7 INTO "}) {
                    final String ordinaryTable = "i" + table++;
                    final String operatorTable = "i" + table++;
                    for (String target : new String[]{ordinaryTable, operatorTable}) {
                        execute("CREATE TABLE " + target + " (ts TIMESTAMP, sym SYMBOL, k INT, v LONG, w LONG, ev LONG, gw LONG) TIMESTAMP(ts) PARTITION BY HOUR" + wal);
                    }
                    sqlExecutionContext.setParallelHashJoinEnabled(false);
                    execute(insert + ordinaryTable + " " + STAR);
                    sqlExecutionContext.setParallelHashJoinEnabled(true);
                    assertParallelHashJoin(STAR, sqlExecutionContext);
                    execute(insert + operatorTable + " " + STAR);
                    drainWalQueue();
                    assertSameRows(ordinaryTable, operatorTable);
                }
            }
        });
    }

    @Test
    public void testLatestOnOverTheJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql : new String[]{
                    "SELECT * FROM (" + STAR + ") LATEST ON ts PARTITION BY sym",
                    "SELECT * FROM (" + STAR + ") LATEST ON ts PARTITION BY k, sym",
                    "SELECT * FROM (" + STAR + " WHERE t.v % 2 = 0) LATEST ON ts PARTITION BY ev",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testSetOperationsSubQueriesAndCtes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            final String star = "SELECT t.k, d.w, g.gw FROM t JOIN d ON t.k = d.k LEFT JOIN g ON t.sym = g.sym";
            final String swapped = "SELECT d.k, d.w, e.ev FROM d JOIN t ON d.k = t.k LEFT JOIN e ON d.k = e.k";
            for (String sql : new String[]{
                    star + " UNION ALL " + swapped,
                    "(" + star + " UNION " + swapped + ") ORDER BY 1, 2, 3",
                    "(" + star + " EXCEPT " + swapped + ") ORDER BY 1, 2, 3",
                    "(" + star + " INTERSECT " + swapped + ") ORDER BY 1, 2, 3",
                    "WITH j AS (" + STAR + ") SELECT k, w, ev FROM j WHERE v % 2 = 0",
                    "WITH j AS (" + SWAPPED + ") SELECT j.w, j.ev FROM j WHERE j.v > 10",
                    "SELECT * FROM (" + STAR + ") WHERE gw > 2 OR gw IS NULL",
                    "SELECT count(*) FROM (" + STAR + ")",
                    "SELECT count(*) FROM (" + SWAPPED + ")",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testTimeSeriesJoinsOverAndUnderTheJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE q AS (SELECT ('s' || (x % 4))::SYMBOL sym, x qv, timestamp_sequence('2024-01-01', 450_000_000) qts FROM long_sequence(40)) TIMESTAMP(qts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql : new String[]{
                    // The star as the master of ASOF and LT joins.
                    "SELECT j.ts, j.k, j.w, j.ev, q.qv FROM (" + STAR_LEFT + ") j ASOF JOIN q ON (sym)",
                    "SELECT j.ts, j.k, j.w, q.qv FROM (" + STAR_LEFT + ") j LT JOIN q ON (sym)",
                    // The star as their slave, whose rows the time-series join reads in the order of
                    // the timestamp that the sub-query declares.
                    "SELECT q.qts, q.qv, j.w, j.ev FROM q ASOF JOIN ((" + STAR_LEFT + ") timestamp(ts)) j ON (sym)",
                    "SELECT q.qts, q.qv, j.w, j.ev FROM q LT JOIN ((" + STAR_LEFT + ") timestamp(ts)) j ON (sym)",
                    "SELECT q.qts, q.qv, j.w FROM q ASOF JOIN ((" + STAR_LEFT + ") timestamp(ts)) j",
            }) {
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                Assert.assertTrue(plan, plan.contains("AsOf Join") || plan.contains("Lt Join"));
            }
        });
    }

    private static void assertSameRows(String expectedTable, String actualTable) throws Exception {
        final String expected;
        try (RecordCursorFactory factory = select("SELECT * FROM " + expectedTable)) {
            expected = result(factory, sqlExecutionContext);
        }
        try (RecordCursorFactory factory = select("SELECT * FROM " + actualTable)) {
            final String actual = result(factory, sqlExecutionContext);
            TestUtils.assertEquals(actualTable, expected, actual);
        }
        TestUtils.assertContains(expected, "\n");
    }

    // t holds 36 rows over six hours, d 15 rows, e 9 and g 5, with NULL keys in t and d.
    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 6)::INT END k, x v, ('s' || (x % 4))::SYMBOL sym,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(36)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE d AS (
                    SELECT CASE WHEN x % 7 = 0 THEN NULL ELSE (x % 5)::INT END k, x w
                    FROM long_sequence(15)
                )
                """);
        execute("CREATE TABLE e AS (SELECT (x % 6)::INT k, x ev FROM long_sequence(9))");
        execute("CREATE TABLE g AS (SELECT ('s' || (x % 3))::SYMBOL sym, x gw FROM long_sequence(5))");
    }
}
