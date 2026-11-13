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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The LIMIT of a query over joins reaches the filter of the join's first table as advice, and a filter
 * that takes it stops its scan at the LIMIT. The first N rows of the scan yield the first N rows of the
 * joins, and its last N rows the last N rows of the joins, only when each join returns at least one row
 * for every row of the scan, in the order of the scan, and no filter after a join drops rows. So the
 * filter of t takes no LIMIT ahead of an INNER join, UNNEST or a filter after a join, see
 * RightFullJoinLimitTest for the RIGHT and FULL joins, and keeps it ahead of LEFT, ASOF, LT and CROSS joins
 * without a filter after them.
 * <p>
 * Data: t holds ten rows over three days. Rows 1, 2 and 3 come first and rows 9 and 10 last, and none of
 * them matches a w row, has a q row with v > 0 as of its timestamp, or holds an array element in ta. Row 5
 * fails x > 1. Rows 4, 6, 7 and 8 match w rows 10, 11, 10 and 12, find q rows with v 5, 6, 6 and 6, and
 * hold two array elements each. So the first and the last rows of t that pass the filter yield no row of
 * the joins, and a scan that stops at the LIMIT returns too few rows. Every expected row follows by hand
 * from the data.
 */
public class JoinScanLimitTest extends AbstractCairoTest {
    private static final String ASOF_WHERE_Q = "SELECT t.id, q.v FROM t ASOF JOIN q WHERE t.x > 1 AND q.v > 0";
    // the rows of ASOF_WHERE_Q and LT_WHERE_Q
    private static final String[] ASOF_ROWS = {"4\t5\n", "6\t6\n", "7\t6\n", "8\t6\n"};
    private static final String HEADER = "id\tk\n";
    private static final String INNER = "SELECT t.id, w.k FROM t JOIN w ON t.k = w.k WHERE t.x > 1";
    private static final String LEFT_WHERE_W = "SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k WHERE t.x > 1 AND w.k > 0";
    private static final String LT_WHERE_Q = "SELECT t.id, q.v FROM t LT JOIN q WHERE t.x > 1 AND q.v > 0";
    private static final String Q_HEADER = "id\tv\n";
    // the rows of INNER and LEFT_WHERE_W
    private static final String[] ROWS = {"4\t10\n", "6\t11\n", "7\t10\n", "8\t12\n"};

    @Test
    public void testAsOfAndLtJoinsWithWhereOnJoinedTable() throws Exception {
        // the conjunct on q filters after the join, which keeps one row for each row of t
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{ASOF_WHERE_Q, LT_WHERE_Q}) {
                assertWhereLimits(sql, Q_HEADER, ASOF_ROWS);
                assertNoScanLimit(sql + " LIMIT 2", "filter: 0<q.v");
                assertNoScanLimit(sql + " LIMIT -2", "filter: 0<q.v");
            }
        });
    }

    @Test
    public void testBindVariableLimitOnOneFactory() throws Exception {
        // the same factory runs with a positive, a negative, a NULL and a positive LIMIT again
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{INNER + " LIMIT $1", LEFT_WHERE_W + " LIMIT $1"}) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 2);
                try (RecordCursorFactory factory = select(sql)) {
                    assertBoundRows(factory, 2, HEADER + rows(0, 2));
                    assertBoundRows(factory, -2, HEADER + rows(2, 4));
                    assertBoundRows(factory, 1, HEADER + rows(0, 1));
                    assertBoundRows(factory, -1, HEADER + rows(3, 4));
                    assertBoundRows(factory, -10, HEADER + rows(0, 4));
                    // a NULL LIMIT returns every row
                    assertBoundRows(factory, Numbers.LONG_NULL, HEADER + rows(0, 4));
                    assertBoundRows(factory, 3, HEADER + rows(0, 3));
                }
            }
        });
    }

    @Test
    public void testCommaAndCrossJoinsWithJoinKeyInWhere() throws Exception {
        // the WHERE turns the CROSS join into an INNER join on k
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{
                    "SELECT t.id, w.k FROM t, w WHERE t.k = w.k AND t.x > 1",
                    "SELECT t.id, w.k FROM t CROSS JOIN w WHERE t.k = w.k AND t.x > 1"
            }) {
                assertWhereLimits(sql, HEADER, ROWS);
                assertNoScanLimit(sql + " LIMIT 2", "Hash Join");
            }
        });
    }

    @Test
    public void testCoveringIndexScan() throws Exception {
        // The covering index returns the rows of the IN list in timestamp order, so the rows of a, b and c
        // come first and match no w row.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE tc (id INT, sym SYMBOL INDEX TYPE POSTING INCLUDE (id, x), x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tc SELECT id, sym, x, ts FROM t");
            final String sql = "SELECT t.id, w.sym FROM tc t JOIN w ON t.sym = w.sym"
                    + " WHERE t.sym IN ('a', 'b', 'c', 'p', 'q', 'r') AND t.x > 1";
            assertQuery(sql + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlanContaining("CoveringIndex on: sym", "filter: 1<x")
                    .withPlanNotContaining("limit:")
                    .returns("id\tsym\n4\tp\n6\tq\n");
            assertQuery(sql + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns("id\tsym\n7\tp\n8\tr\n");
        });
    }

    @Test
    public void testEmptyAndUnmatchedTables() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // no w row matches the rows of t that pass the filter
            assertRows(INNER.replace("t.x > 1", "t.x > 1 AND t.k = 99") + " LIMIT 2", "");
            assertRows(LEFT_WHERE_W.replace("t.x > 1", "t.x > 1 AND t.k = 99") + " LIMIT -2", "");
            // every row of t that passes the filter matches a w row
            assertRows(INNER.replace("t.x > 1", "t.x > 7 AND t.x < 12") + " LIMIT 2", rows(0, 2));
            assertRows(INNER.replace("t.x > 1", "t.x > 7 AND t.x < 12") + " LIMIT -2", rows(2, 4));
            execute("TRUNCATE TABLE w");
            assertRows(INNER + " LIMIT 2", "");
            assertRows(INNER + " LIMIT -2", "");
            assertRows(LEFT_WHERE_W + " LIMIT 2", "");
            execute("TRUNCATE TABLE t");
            assertRows(INNER + " LIMIT 2", "");
            assertRows(INNER + " LIMIT -2", "");
        });
    }

    @Test
    public void testExecutionModes() throws Exception {
        assertExecutionModes();
    }

    @Test
    public void testExecutionModesOverOneRowPageFrames() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1);
        assertExecutionModes();
    }

    @Test
    public void testFirstTableInSubQueryAndCte() throws Exception {
        // the filter of the sub-query reads the rows of t that the INNER join drops
        assertMemoryLeak(() -> {
            createTables();
            assertWhereLimits("SELECT t.id, w.k FROM (SELECT * FROM t WHERE x > 1) t JOIN w ON t.k = w.k", HEADER, ROWS);
            assertWhereLimits("WITH f AS (SELECT * FROM t WHERE x > 1) SELECT f.id, w.k FROM f JOIN w ON f.k = w.k", HEADER, ROWS);
            assertWhereLimits("SELECT t.id, w.k FROM (SELECT id, k, x + 1 y FROM t WHERE x > 1) t JOIN w ON t.k = w.k", HEADER, ROWS);
            // the LIMIT of the sub-query applies to the rows of t: rows 1, 2 and 3 match no w row
            assertRows("SELECT t.id, w.k FROM (SELECT * FROM t WHERE x > 1 LIMIT 3) t JOIN w ON t.k = w.k LIMIT 2", "");
            assertRows("SELECT t.id, w.k FROM (SELECT * FROM t WHERE x > 1 LIMIT 4) t JOIN w ON t.k = w.k LIMIT 2", rows(0, 1));
        });
    }

    @Test
    public void testInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertWhereLimits(INNER, HEADER, ROWS);
            assertNoScanLimit(INNER + " LIMIT 1", "Hash Join");
            assertNoScanLimit(INNER + " LIMIT -1", "Hash Join");
        });
    }

    @Test
    public void testInnerJoinAfterRowPreservingJoins() throws Exception {
        // the ASOF and LEFT joins keep every row of t, the INNER join after them drops the rows that match
        // no w row
        assertMemoryLeak(() -> {
            createTables();
            assertWhereLimits("SELECT t.id, w.k FROM t ASOF JOIN q JOIN w ON t.k = w.k WHERE t.x > 1", HEADER, ROWS);
            assertWhereLimits("SELECT t.id, w.k FROM t LEFT JOIN u ON t.k = u.k JOIN w ON t.k = w.k WHERE t.x > 1", HEADER, ROWS);
            assertNoScanLimit("SELECT t.id, w.k FROM t ASOF JOIN q JOIN w ON t.k = w.k WHERE t.x > 1 LIMIT -2", "AsOf Join");
        });
    }

    @Test
    public void testInnerJoinOnConjunctOfFirstTable() throws Exception {
        // the ON conjunct of the INNER join on t filters the scan of t
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, w.k FROM t JOIN w ON t.k = w.k AND t.x > 1";
            assertWhereLimits(sql, HEADER, ROWS);
            assertNoScanLimit(sql + " LIMIT 2", "filter: 1<x");
        });
    }

    @Test
    public void testInnerJoinOnSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, w.sym FROM t JOIN w ON t.sym = w.sym WHERE t.x > 1";
            assertQuery(sql + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlanContaining("symbolKeyJoin: true")
                    .withPlanNotContaining("limit:")
                    .returns("id\tsym\n4\tp\n6\tq\n");
            assertQuery(sql + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns("id\tsym\n7\tp\n8\tr\n");
        });
    }

    @Test
    public void testLateralJoins() throws Exception {
        // the INNER LATERAL join runs as an INNER join, and the LEFT LATERAL join as a LEFT join that the
        // conjunct on l filters after
        assertMemoryLeak(() -> {
            createTables();
            final String inner = "SELECT t.id, l.k FROM t JOIN LATERAL (SELECT w.k FROM w WHERE w.k = t.k) l ON true WHERE t.x > 1";
            final String left = "SELECT t.id, l.k FROM t LEFT JOIN LATERAL (SELECT w.k FROM w WHERE w.k = t.k) l ON true WHERE t.x > 1 AND l.k > 0";
            for (String sql : new String[]{inner, left}) {
                assertWhereLimits(sql, HEADER, ROWS);
                assertNoScanLimit(sql + " LIMIT 2", "filter: 1<x");
            }
        });
    }

    @Test
    public void testLeftJoinsWithWhereOnJoinedTable() throws Exception {
        // the conjunct on w filters after the join, which NULL-extends the rows of t that match nothing; OR
        // makes the ON clause non-equi, so that join runs as a nested loop, and w.k + 1_000 matches no row
        assertMemoryLeak(() -> {
            createTables();
            final String nestedLoop = "SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k OR t.k = w.k + 1_000 WHERE t.x > 1 AND w.k > 0";
            final String notNull = "SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k WHERE t.x > 1 AND w.k IS NOT NULL";
            for (String sql : new String[]{LEFT_WHERE_W, nestedLoop, notNull}) {
                assertWhereLimits(sql, HEADER, ROWS);
                assertNoScanLimit(sql + " LIMIT 2", "filter: 1<x");
            }
            assertNoScanLimit(nestedLoop + " LIMIT -2", "Nested Loop Left Join");
        });
    }

    @Test
    public void testLimitBeyondMaxNegativeLimit() throws Exception {
        // The negative LIMIT of a filter that takes it scans backward and keeps at most
        // cairo.sql.max.negative.limit rows. The Limit above the joins counts the rows instead.
        setProperty(PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 2);
        assertMemoryLeak(() -> {
            createTables();
            assertRows(INNER + " LIMIT -3", rows(1, 4));
            assertRows(LEFT_WHERE_W + " LIMIT -3", rows(1, 4));
        });
    }

    @Test
    public void testLimitInSubQueryCteAndUnion() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertRows("SELECT * FROM (" + INNER + " LIMIT 2)", rows(0, 2));
            assertRows("WITH r AS (" + INNER + " LIMIT -2) SELECT * FROM r", rows(2, 4));
            assertRows("SELECT * FROM (" + INNER + " LIMIT 2) LIMIT 1", rows(0, 1));
            assertRows("SELECT * FROM (" + INNER + " LIMIT 1) UNION ALL SELECT * FROM (" + LEFT_WHERE_W + " LIMIT -1)", rows(0, 1) + rows(3, 4));
            // an ORDER BY takes no LIMIT advice, whatever the joins
            assertQuery(INNER + " ORDER BY t.id DESC LIMIT 2")
                    .noLeakCheck()
                    .returns(HEADER + "8\t12\n7\t10\n");
        });
    }

    @Test
    public void testNonEquiInnerJoin() throws Exception {
        // OR makes the ON clause non-equi, so the join runs as a CROSS join and a filter after it
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, w.k FROM t JOIN w ON t.k = w.k OR t.k = w.k + 1_000 WHERE t.x > 1";
            assertWhereLimits(sql, HEADER, ROWS);
            assertNoScanLimit(sql + " LIMIT 2", "Cross Join");
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-07'");
            assertWhereLimits(INNER, HEADER, ROWS);
            assertWhereLimits(LEFT_WHERE_W, HEADER, ROWS);
            assertWhereLimits(ASOF_WHERE_Q, Q_HEADER, ASOF_ROWS);
        });
    }

    @Test
    public void testProjectionOverJoin() throws Exception {
        // a computed column puts a model between the LIMIT and the joins, which passes the LIMIT advice on
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, w.k, t.x + 1 y FROM t JOIN w ON t.k = w.k WHERE t.x > 1";
            assertQuery(sql + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlanContaining("VirtualRecord")
                    .withPlanNotContaining("limit:")
                    .returns("id\tk\ty\n4\t10\t9\n6\t11\t10\n");
            assertQuery(sql + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns("id\tk\ty\n7\t10\t11\n8\t12\t12\n");
        });
    }

    @Test
    public void testRowPreservingJoinsKeepScanLimit() throws Exception {
        // Without a filter after them, LEFT, ASOF, LT and CROSS joins return at least one row for every row
        // of t in the order of t, so the filter of t still takes the LIMIT. The LEFT joins NULL-extend rows
        // 1, 2 and 3, the ASOF and LT joins pair them with q row 1, and the CROSS join with every u row.
        assertMemoryLeak(() -> {
            createTables();
            assertScanLimit("SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k WHERE t.x > 1",
                    HEADER + "1\tnull\n2\tnull\n", HEADER + "9\tnull\n10\tnull\n");
            // OR makes the ON clause non-equi, so the join runs as a nested loop
            assertScanLimit("SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k OR t.k = w.k + 1_000 WHERE t.x > 1",
                    HEADER + "1\tnull\n2\tnull\n", HEADER + "9\tnull\n10\tnull\n");
            // the ON conjunct on t keeps the rows of t that fail it, NULL-extended
            assertScanLimit("SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k AND t.x > 7 WHERE t.x > 1",
                    HEADER + "1\tnull\n2\tnull\n", HEADER + "9\tnull\n10\tnull\n");
            assertScanLimit("SELECT t.id, q.v FROM t ASOF JOIN q WHERE t.x > 1",
                    Q_HEADER + "1\t0\n2\t0\n", Q_HEADER + "9\t0\n10\t0\n");
            assertScanLimit("SELECT t.id, q.v FROM t LT JOIN q WHERE t.x > 1",
                    Q_HEADER + "1\t0\n2\t0\n", Q_HEADER + "9\t0\n10\t0\n");
            assertScanLimit("SELECT t.id, u.k FROM t CROSS JOIN u WHERE t.x > 1",
                    HEADER + "1\t1\n1\t2\n", HEADER + "10\t12\n10\t99\n");
            assertScanLimit("SELECT t.id, w.k, q.v FROM t LEFT JOIN w ON t.k = w.k ASOF JOIN q CROSS JOIN u WHERE t.x > 1",
                    "id\tk\tv\n1\tnull\t0\n1\tnull\t0\n", "id\tk\tv\n10\tnull\t0\n10\tnull\t0\n");
        });
    }

    @Test
    public void testSymbolPatternIndex() throws Exception {
        // A LIKE on an indexed symbol runs through the adaptive symbol pattern factory, which a negative
        // LIMIT on the filter used to route around, to a backward scan of the table.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE ti (id INT, sym SYMBOL INDEX, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti SELECT id, sym, x, ts FROM t");
            final String sql = "SELECT t.id, w.sym FROM ti t JOIN w ON t.sym = w.sym WHERE t.sym LIKE '_'";
            assertQuery(sql + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlanContaining("AdaptiveSymbolPattern")
                    .withPlanNotContaining("limit:")
                    .returns("id\tsym\n7\tp\n8\tr\n");
            assertQuery(sql + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns("id\tsym\n4\tp\n5\tq\n");
        });
    }

    @Test
    public void testUnnest() throws Exception {
        // UNNEST returns no row for an empty or a NULL array: rows 1, 2, 3 and 10 hold an empty array and
        // row 9 a NULL one
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (id INT, x INT, arr DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ta VALUES"
                    + " (1, 5, ARRAY[]::DOUBLE[], '2024-01-05T01:00:00.000000Z'),"
                    + " (2, 6, ARRAY[]::DOUBLE[], '2024-01-05T02:00:00.000000Z'),"
                    + " (3, 7, ARRAY[]::DOUBLE[], '2024-01-05T03:00:00.000000Z'),"
                    + " (4, 8, ARRAY[4.0, 4.5], '2024-01-05T04:00:00.000000Z'),"
                    + " (5, 0, ARRAY[5.0, 5.5], '2024-01-05T05:00:00.000000Z'),"
                    + " (6, 9, ARRAY[6.0, 6.5], '2024-01-06T01:00:00.000000Z'),"
                    + " (7, 10, ARRAY[7.0, 7.5], '2024-01-06T02:00:00.000000Z'),"
                    + " (8, 11, ARRAY[8.0, 8.5], '2024-01-06T03:00:00.000000Z'),"
                    + " (9, 12, NULL, '2024-01-07T01:00:00.000000Z'),"
                    + " (10, 13, ARRAY[]::DOUBLE[], '2024-01-07T02:00:00.000000Z')");
            final String sql = "SELECT t.id, u.val FROM ta t, UNNEST(t.arr) u(val) WHERE t.x > 1";
            final String header = "id\tval\n";
            assertRowsWithHeader(sql + " LIMIT 3", header + "4\t4.0\n4\t4.5\n6\t6.0\n");
            assertRowsWithHeader(sql + " LIMIT -3", header + "7\t7.5\n8\t8.0\n8\t8.5\n");
            assertRowsWithHeader(sql + " LIMIT 1, 3", header + "4\t4.5\n6\t6.0\n");
            assertRowsWithHeader(sql + " LIMIT 10", header + "4\t4.0\n4\t4.5\n6\t6.0\n6\t6.5\n7\t7.0\n7\t7.5\n8\t8.0\n8\t8.5\n");
            assertNoScanLimit(sql + " LIMIT 3", "Unnest");
            assertNoScanLimit(sql + " LIMIT -3", "Unnest");
        });
    }

    @Test
    public void testWalTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, k INT, k2 INT, sym SYMBOL, x INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE w (k INT, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertRows();
            drainWalQueue();
            assertWhereLimits(INNER, HEADER, ROWS);
            assertWhereLimits(LEFT_WHERE_W, HEADER, ROWS);
            assertWhereLimits(ASOF_WHERE_Q, Q_HEADER, ASOF_ROWS);
        });
    }

    @Test
    public void testWindowJoinKeepsEveryRow() throws Exception {
        // The WINDOW join returns a row for every row of t, with a NULL max where no q row falls into the
        // window. It takes the filter of t over, and the LIMIT above it stops it.
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, max(q.v) v FROM t WINDOW JOIN q RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING"
                    + " EXCLUDE PREVAILING WHERE t.x > 1";
            assertQuery(sql + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlanContaining("master filter: 1<x")
                    .returns(Q_HEADER + "1\tnull\n2\tnull\n");
            assertQuery(sql + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns(Q_HEADER + "9\tnull\n10\tnull\n");
        });
    }

    private static String rows(String[] rows, int from, int to) {
        final StringBuilder sb = new StringBuilder();
        for (int i = from; i < to; i++) {
            sb.append(rows[i]);
        }
        return sb.toString();
    }

    private static String rows(int from, int to) {
        return rows(ROWS, from, to);
    }

    private void assertBoundRows(RecordCursorFactory factory, long limit, String expected) throws Exception {
        bindVariableService.setLong(0, limit);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursorTwoPass(expected, cursor, factory.getMetadata());
        }
    }

    // The filter of t runs as an async JIT filter, an async Java filter and a serial filter.
    private void assertExecutionModes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final int callerJitMode = sqlExecutionContext.getJitMode();
            final boolean callerParallel = sqlExecutionContext.isParallelFilterEnabled();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    for (boolean isParallel : new boolean[]{false, true}) {
                        sqlExecutionContext.setJitMode(jitMode);
                        sqlExecutionContext.setParallelFilterEnabled(isParallel);
                        assertWhereLimits(INNER, HEADER, ROWS);
                        assertWhereLimits(LEFT_WHERE_W, HEADER, ROWS);
                        assertWhereLimits(ASOF_WHERE_Q, Q_HEADER, ASOF_ROWS);
                        // the regular expression runs in the Java filter whether JIT is on or off
                        assertWhereLimits("SELECT t.id, w.k FROM t JOIN w ON t.k = w.k WHERE t.s ~ '^n'", HEADER, ROWS);
                        if (isParallel) {
                            final String filter = jitMode == SqlJitMode.JIT_MODE_ENABLED ? "Async JIT Filter" : "Async Filter";
                            assertNoScanLimit(INNER + " LIMIT 2", filter);
                            assertNoScanLimit(INNER + " LIMIT -2", filter);
                            assertNoScanLimit(LEFT_WHERE_W + " LIMIT 2", filter);
                            assertNoScanLimit(ASOF_WHERE_Q + " LIMIT -2", filter);
                            // the LEFT join keeps every row of t, so the filter of t still takes the LIMIT
                            assertQuery("SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k WHERE t.x > 1 LIMIT -2")
                                    .noLeakCheck()
                                    .assertsPlanContaining(filter, "limit: 2");
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
                sqlExecutionContext.setParallelFilterEnabled(callerParallel);
            }
        });
    }

    // the plan holds the fragment, and no filter in it takes the LIMIT
    private void assertNoScanLimit(String sql, String planFragment) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .assertsPlanContaining(planFragment);
        assertQuery(sql)
                .noLeakCheck()
                .assertsPlanNotContaining("limit:");
    }

    private void assertRows(String sql, String expectedRows) throws Exception {
        assertRowsWithHeader(sql, HEADER + expectedRows);
    }

    private void assertRowsWithHeader(String sql, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .sizeMayVary()
                .returns(expected);
    }

    // the filter of t takes LIMIT 2 and LIMIT -2, which return the expected rows
    private void assertScanLimit(String sql, String expectedFirstRows, String expectedLastRows) throws Exception {
        assertQuery(sql + " LIMIT 2")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .sizeMayVary()
                .withPlanContaining("filter: 1<x", "limit: 2")
                .returns(expectedFirstRows);
        assertQuery(sql + " LIMIT -2")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .sizeMayVary()
                .withPlanContaining("filter: 1<x", "limit: 2")
                .returns(expectedLastRows);
    }

    private void assertWhereLimits(String sql, String header, String[] rows) throws Exception {
        assertRowsWithHeader(sql + " LIMIT 2", header + rows(rows, 0, 2));
        assertRowsWithHeader(sql + " LIMIT -2", header + rows(rows, 2, 4));
        assertRowsWithHeader(sql + " LIMIT 1", header + rows(rows, 0, 1));
        assertRowsWithHeader(sql + " LIMIT -1", header + rows(rows, 3, 4));
        assertRowsWithHeader(sql + " LIMIT 1, 3", header + rows(rows, 1, 3));
        assertRowsWithHeader(sql + " LIMIT 10", header + rows(rows, 0, 4));
        assertRowsWithHeader(sql + " LIMIT -10", header + rows(rows, 0, 4));
        assertRowsWithHeader(sql + " LIMIT 0", header);
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE t (id INT, k INT, k2 INT, sym SYMBOL, x INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE w (k INT, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        insertRows();
        // every k of t, once
        execute("CREATE TABLE u (k INT)");
        execute("INSERT INTO u VALUES (1), (2), (3), (10), (11), (12), (99)");
    }

    private void insertRows() throws Exception {
        execute("INSERT INTO t VALUES"
                + " (1, 1, 1, 'a', 5, 'n1', '2024-01-05T01:00:00.000000Z'),"
                + " (2, 2, 2, 'b', 6, 'n2', '2024-01-05T02:00:00.000000Z'),"
                + " (3, 3, 3, 'c', 7, 'n3', '2024-01-05T03:00:00.000000Z'),"
                + " (4, 10, 10, 'p', 8, 'n4', '2024-01-05T04:00:00.000000Z'),"
                + " (5, 11, 10, 'q', 0, 'm5', '2024-01-05T05:00:00.000000Z'),"
                + " (6, 11, 11, 'q', 9, 'n6', '2024-01-06T01:00:00.000000Z'),"
                + " (7, 10, 10, 'p', 10, 'n7', '2024-01-06T02:00:00.000000Z'),"
                + " (8, 12, 12, 'r', 11, 'n8', '2024-01-06T03:00:00.000000Z'),"
                + " (9, 99, 99, 'z', 12, 'n9', '2024-01-07T01:00:00.000000Z'),"
                + " (10, 99, 99, 'z', 13, 'n10', '2024-01-07T02:00:00.000000Z')");
        execute("INSERT INTO w VALUES (10, 'p', '2024-01-01T00:00:00.000000Z'), (11, 'q', '2024-01-01T00:00:01.000000Z'),"
                + " (12, 'r', '2024-01-01T00:00:02.000000Z'), (13, 's', '2024-01-01T00:00:03.000000Z')");
        // as of the rows of t, v is 0 for rows 1, 2, 3, 9 and 10, 5 for rows 4 and 5, and 6 for rows 6, 7 and 8
        execute("INSERT INTO q VALUES (0, '2024-01-01T00:00:00.000000Z'), (5, '2024-01-05T03:30:00.000000Z'),"
                + " (6, '2024-01-06T00:30:00.000000Z'), (0, '2024-01-06T03:30:00.000000Z')");
    }
}
