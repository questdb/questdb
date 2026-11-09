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
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The LIMIT of a query over joins reaches the filter of the join's first table as advice, and a filter
 * that takes it stops its scan at the LIMIT. That is correct only when every row of the table yields a
 * row of the join before any row that depends on the rows the scan skips. A RIGHT join drops the rows of
 * the table that match nothing, and RIGHT and FULL joins return their unmatched rows after reading the
 * whole table, so the filter of a join level that holds a RIGHT or FULL join takes no LIMIT; the Limit
 * above the joins implements it. Such filters hold copies of WHERE conjuncts, see
 * RightJoinWhereScanFilterTest, and the equality that two ON keys of a RIGHT join imply between two
 * columns of the table.
 * <p>
 * Data: t holds ten rows over three days. Rows 1, 2 and 3 come first and rows 9 and 10 last, and none of
 * them matches a w row. Row 5 fails both x > 1 and k = k2. Rows 4, 6, 7 and 8 match w rows 10, 11, 10 and
 * 12, and no t row matches w row 13. So the first and the last rows of t that pass the filter match
 * nothing, and a scan that stops at the LIMIT returns too few rows, or NULL-extends rows that have a
 * match. The joins return the matched rows in the order of t, then the unmatched rows of w, and every
 * expected row follows by hand from the data.
 */
public class RightFullJoinLimitTest extends AbstractCairoTest {
    private static final String FULL_WHERE_BOTH = "SELECT t.id, w.k FROM t FULL JOIN w ON t.k = w.k WHERE t.x > 1 AND w.k > 0";
    private static final String HEADER = "id\tk\n";
    // the equality k = k2 that the two ON keys imply filters the scan of t
    private static final String RIGHT_IMPLIED_KEY = "SELECT t.id, w.k FROM t RIGHT JOIN w ON t.k = w.k AND t.k2 = w.k";
    private static final String RIGHT_WHERE = "SELECT t.id, w.k FROM t RIGHT JOIN w ON t.k = w.k WHERE t.x > 1";
    // the rows of RIGHT_WHERE and FULL_WHERE_BOTH
    private static final String[] WHERE_ROWS = {"4\t10\n", "6\t11\n", "7\t10\n", "8\t12\n"};

    @Test
    public void testAsOfJoinBeforeRightJoin() throws Exception {
        // the ASOF join keeps every row of t, so the rows are those of RIGHT_WHERE
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO q VALUES (1, '2024-01-01T00:00:00.000000Z')");
            final String sql = "SELECT t.id, w.k FROM t ASOF JOIN q RIGHT JOIN w ON t.k = w.k WHERE t.x > 1";
            assertRows(sql + " LIMIT 2", rows(0, 2));
            assertRows(sql + " LIMIT -2", rows(2, 4));
            assertNoScanLimit(sql + " LIMIT 2", "AsOf Join");
        });
    }

    @Test
    public void testBindVariableLimitOnOneFactory() throws Exception {
        // the same factory runs with a positive, a negative and a positive LIMIT again
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{RIGHT_WHERE + " LIMIT $1", FULL_WHERE_BOTH + " LIMIT $1"}) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 2);
                try (RecordCursorFactory factory = select(sql)) {
                    assertBoundRows(factory, 2, HEADER + rows(0, 2));
                    assertBoundRows(factory, -2, HEADER + rows(2, 4));
                    assertBoundRows(factory, 3, HEADER + rows(0, 3));
                    assertBoundRows(factory, -10, HEADER + rows(0, 4));
                }
            }
        });
    }

    @Test
    public void testCoveringIndexScan() throws Exception {
        // The covering index returns the rows of the IN list in timestamp order, so the rows of a, b and c
        // come first and match nothing.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE tc (id INT, sym SYMBOL INDEX TYPE POSTING INCLUDE (id, x), x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tc SELECT id, sym, x, ts FROM t");
            final String sql = "SELECT t.id, w.sym FROM tc t RIGHT JOIN w ON t.sym = w.sym"
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
    public void testEmptyTables() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // no t row: RIGHT_WHERE drops every NULL-extended w row, and the implied key keeps them
            execute("TRUNCATE TABLE t");
            assertRows(RIGHT_WHERE + " LIMIT 2", "");
            assertRows(FULL_WHERE_BOTH + " LIMIT -2", "");
            assertRowsWithHeader("SELECT count(*) FROM (" + RIGHT_IMPLIED_KEY + " LIMIT 3)", "count\n3\n");
            // no w row: a RIGHT join returns no row
            createTables("t2", "w2");
            execute("TRUNCATE TABLE w2");
            assertRows("SELECT t.id, w.k FROM t2 t RIGHT JOIN w2 w ON t.k = w.k WHERE t.x > 1 LIMIT 2", "");
            assertRows("SELECT t.id, w.k FROM t2 t RIGHT JOIN w2 w ON t.k = w.k AND t.k2 = w.k LIMIT -2", "");
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
    public void testExecutionModesOverRampedPageFrames() throws Exception {
        // the parallel filter ahead of the RIGHT or FULL join starts the scans of a LIMIT n and a
        // LIMIT lo, hi with one-row page frames, and grows the frames from there
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_RAMP_START_ROWS, 1);
        assertExecutionModes();
    }

    @Test
    public void testFullJoinKeepsRowsOfFirstTable() throws Exception {
        // Without a conjunct on w, the FULL join returns every row of t that passes x > 1, matched or not,
        // ahead of the unmatched rows of w, which the post-join filter drops.
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, w.k FROM t FULL JOIN w ON t.k = w.k WHERE t.x > 1";
            assertRows(sql + " LIMIT 2", "1\tnull\n2\tnull\n");
            assertRows(sql + " LIMIT -2", "9\tnull\n10\tnull\n");
            assertRows(sql + " LIMIT 3, 6", "4\t10\n6\t11\n7\t10\n");
            assertNoScanLimit(sql + " LIMIT 2", "filter: 1<x");
        });
    }

    @Test
    public void testInnerJoinBeforeRightJoin() throws Exception {
        // The ON conjunct t.x > 1 of the INNER join filters the scan of t. Every t row matches a u row,
        // and the RIGHT join returns w row 13 NULL-extended, as no WHERE drops it.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE u (k INT)");
            execute("INSERT INTO u VALUES (1), (2), (3), (10), (11), (12), (99)");
            final String sql = "SELECT t.id, w.k FROM t JOIN u ON t.k = u.k AND t.x > 1 RIGHT JOIN w ON t.k = w.k";
            assertRows(sql + " LIMIT 2", rows(0, 2));
            assertRows(sql + " LIMIT -2", "8\t12\nnull\t13\n");
            assertRows(sql + " LIMIT 10", rows(0, 4) + "null\t13\n");
            assertNoScanLimit(sql + " LIMIT 2", "filter: 1<x");
        });
    }

    @Test
    public void testLeftAndAsOfJoinsKeepScanLimit() throws Exception {
        // A LEFT or ASOF join returns a row for every row of t, so the filter of t still takes the LIMIT.
        // The LEFT join returns rows 1, 2 and 3 NULL-extended, the ASOF join pairs each row with q row 1.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO q VALUES (1, '2024-01-01T00:00:00.000000Z')");
            assertQuery("SELECT t.id, w.k FROM t LEFT JOIN w ON t.k = w.k WHERE t.x > 1 LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlanContaining("limit: 2")
                    .returns("id\tk\n1\tnull\n2\tnull\n");
            assertQuery("SELECT t.id, q.v FROM t ASOF JOIN q WHERE t.x > 1 LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlanContaining("limit: 2")
                    .returns("id\tv\n9\t1\n10\t1\n");
        });
    }

    @Test
    public void testLimitBeyondMaxNegativeLimit() throws Exception {
        // The negative LIMIT of a filter that takes it scans backward and keeps at most
        // cairo.sql.max.negative.limit rows. The Limit above the joins counts the rows instead.
        setProperty(PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 2);
        assertMemoryLeak(() -> {
            createTables();
            assertRows(RIGHT_WHERE + " LIMIT -3", rows(1, 4));
            assertRows(RIGHT_IMPLIED_KEY + " LIMIT -3", "7\t10\n8\t12\nnull\t13\n");
            assertRows(FULL_WHERE_BOTH + " LIMIT -3", rows(1, 4));
        });
    }

    @Test
    public void testLimitInSubQueryCteAndUnion() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertRows("SELECT * FROM (" + RIGHT_WHERE + " LIMIT 2)", rows(0, 2));
            assertRows("WITH r AS (" + RIGHT_WHERE + " LIMIT -2) SELECT * FROM r", rows(2, 4));
            assertRows("SELECT * FROM (" + RIGHT_WHERE + " LIMIT 1) UNION ALL SELECT * FROM (" + FULL_WHERE_BOTH + " LIMIT -1)", rows(0, 1) + rows(3, 4));
            // a LIMIT over the sub-query also implements the LIMIT above the joins
            assertRows("SELECT * FROM (" + RIGHT_IMPLIED_KEY + ") LIMIT 3", rows(0, 3));
            // an ORDER BY takes no LIMIT advice, whatever the joins
            assertQuery(RIGHT_WHERE + " ORDER BY t.id DESC LIMIT 2")
                    .noLeakCheck()
                    .returns(HEADER + "8\t12\n7\t10\n");
        });
    }

    @Test
    public void testNonEquiRightAndFullJoins() throws Exception {
        // OR makes the ON clause non-equi, so the joins run as nested loops; w.k + 1_000 matches no t row
        assertMemoryLeak(() -> {
            createTables();
            final String on = " ON t.k = w.k OR t.k = w.k + 1_000 WHERE t.x > 1";
            for (String join : new String[]{"RIGHT", "FULL"}) {
                final String sql = "SELECT t.id, w.k FROM t " + join + " JOIN w" + on + (join.equals("FULL") ? " AND w.k > 0" : "");
                assertRowsWithHeader("SELECT count(*) FROM (" + sql + " LIMIT 3)", "count\n3\n");
                assertRowsWithHeader("SELECT count(*) FROM (" + sql + " LIMIT -3)", "count\n3\n");
                assertRowsWithHeader("SELECT count(*) FROM (" + sql + " LIMIT 10)", "count\n4\n");
                assertNoScanLimit(sql + " LIMIT 3", "Nested Loop");
            }
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-07'");
            assertWhereLimits(RIGHT_WHERE);
            assertImpliedKeyLimits();
        });
    }

    @Test
    public void testSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.id, w.sym FROM t RIGHT JOIN w ON t.sym = w.sym WHERE t.x > 1";
            assertQuery(sql + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns("id\tsym\n4\tp\n6\tq\n");
            assertQuery(sql + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns("id\tsym\n7\tp\n8\tr\n");
            assertNoScanLimit(sql + " LIMIT 2", "filter: 1<x");
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
            final String sql = "SELECT t.id, w.sym FROM ti t RIGHT JOIN w ON t.sym = w.sym WHERE t.sym LIKE '_'";
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
    public void testWalTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, k INT, k2 INT, sym SYMBOL, x INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE w (k INT, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertRows("t", "w");
            drainWalQueue();
            assertWhereLimits(RIGHT_WHERE);
            assertWhereLimits(FULL_WHERE_BOTH);
            assertImpliedKeyLimits();
        });
    }

    private static String rows(int from, int to) {
        final StringBuilder sb = new StringBuilder();
        for (int i = from; i < to; i++) {
            sb.append(WHERE_ROWS[i]);
        }
        return sb.toString();
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
                        assertWhereLimits(RIGHT_WHERE);
                        assertWhereLimits(FULL_WHERE_BOTH);
                        // the regular expression runs in the Java filter whether JIT is on or off
                        assertWhereLimits("SELECT t.id, w.k FROM t RIGHT JOIN w ON t.k = w.k WHERE t.s ~ '^n'");
                        assertImpliedKeyLimits();
                        if (isParallel) {
                            final String filter = jitMode == SqlJitMode.JIT_MODE_ENABLED ? "Async JIT Filter" : "Async Filter";
                            assertNoScanLimit(RIGHT_WHERE + " LIMIT 2", filter);
                            assertNoScanLimit(RIGHT_WHERE + " LIMIT -2", filter);
                            assertNoScanLimit(FULL_WHERE_BOTH + " LIMIT 2", filter);
                            assertNoScanLimit(RIGHT_IMPLIED_KEY + " LIMIT 3", "filter: k=k2");
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
                sqlExecutionContext.setParallelFilterEnabled(callerParallel);
            }
        });
    }

    private void assertImpliedKeyLimits() throws Exception {
        // RIGHT_IMPLIED_KEY returns the rows of RIGHT_WHERE, then w row 13 NULL-extended
        assertRows(RIGHT_IMPLIED_KEY + " LIMIT 3", rows(0, 3));
        assertRows(RIGHT_IMPLIED_KEY + " LIMIT -2", "8\t12\nnull\t13\n");
        assertRows(RIGHT_IMPLIED_KEY + " LIMIT 10", rows(0, 4) + "null\t13\n");
        assertRows(RIGHT_IMPLIED_KEY + " LIMIT 1, 3", rows(1, 3));
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

    private void assertWhereLimits(String sql) throws Exception {
        assertRows(sql + " LIMIT 2", rows(0, 2));
        assertRows(sql + " LIMIT -2", rows(2, 4));
        assertRows(sql + " LIMIT 1, 3", rows(1, 3));
        assertRows(sql + " LIMIT 10", rows(0, 4));
        assertRows(sql + " LIMIT -10", rows(0, 4));
        assertRows(sql + " LIMIT 0", "");
    }

    private void createTables() throws Exception {
        createTables("t", "w");
    }

    private void createTables(String t, String w) throws Exception {
        execute("CREATE TABLE " + t + " (id INT, k INT, k2 INT, sym SYMBOL, x INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE " + w + " (k INT, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        insertRows(t, w);
    }

    private void insertRows(String t, String w) throws Exception {
        execute("INSERT INTO " + t + " VALUES"
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
        execute("INSERT INTO " + w + " VALUES (10, 'p', '2024-01-01T00:00:00.000000Z'), (11, 'q', '2024-01-01T00:00:01.000000Z'),"
                + " (12, 'r', '2024-01-01T00:00:02.000000Z'), (13, 's', '2024-01-01T00:00:03.000000Z')");
    }
}
