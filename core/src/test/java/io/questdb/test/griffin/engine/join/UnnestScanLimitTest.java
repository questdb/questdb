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
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A negative LIMIT over the UNNEST of the first table of a join level reaches the filter of that table as a
 * fan-out limit: the filter scans the table backward, counts the rows that UNNEST returns for each row that
 * it reads, skips the rows whose arrays are empty or NULL, and stops once the rows it kept unnest into the
 * LIMIT rows. The Limit above the level then takes the last rows of those.
 * <p>
 * Data: ta holds ten rows over three days. Rows 1 and 10 hold an empty array, rows 2 and 9 a NULL one, row 6
 * an empty one, and row 4 fails x > 1. Rows 3, 5, 7 and 8 hold 1, 3, 2 and 1 elements. So the last rows that
 * pass the filter unnest into no row, and a scan that stops at the LIMIT rows of the table returns too few
 * rows. Every expected row follows by hand from the data.
 */
public class UnnestScanLimitTest extends AbstractCairoTest {
    private static final String HEADER = "id\tval\n";
    // the rows of SQL, in the order of the table
    private static final String[] ROWS = {
            "3\t3.0\n",
            "5\t5.0\n",
            "5\t5.5\n",
            "5\t5.75\n",
            "7\t7.0\n",
            "7\t7.5\n",
            "8\t8.0\n"
    };
    private static final String SQL = "SELECT t.id, u.val FROM ta t, UNNEST(t.arr) u(val) WHERE t.x > 1";

    @Test
    public void testBindVariableLimitOnOneFactory() throws Exception {
        // the same factory runs with negative, positive, NULL and zero limits, in turn
        setProperty(PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 5);
        assertMemoryLeak(() -> {
            createTable();
            bindVariableService.clear();
            bindVariableService.setLong(0, -2);
            try (RecordCursorFactory factory = select(SQL + " LIMIT $1")) {
                for (int i = 0; i < 2; i++) {
                    assertBoundRows(factory, -2, rows(5, 7));
                    assertBoundRows(factory, 2, rows(0, 2));
                    assertBoundRows(factory, -4, rows(3, 7));
                    // beyond cairo.sql.max.negative.limit, the scan reads every row
                    assertBoundRows(factory, -6, rows(1, 7));
                    assertBoundRows(factory, -100, rows(0, 7));
                    // a NULL LIMIT returns every row
                    assertBoundRows(factory, Numbers.LONG_NULL, rows(0, 7));
                    assertBoundRows(factory, 0, "");
                    assertBoundRows(factory, -1, rows(6, 7));
                }
            }
        });
    }

    @Test
    public void testEmptyAndUnnestingNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // every row that passes the filter holds an empty or a NULL array
            assertRows(SQL.replace("t.x > 1", "t.x > 11") + " LIMIT -2", "");
            assertRows(SQL.replace("t.x > 1", "t.x > 11") + " LIMIT -100", "");
            // no row passes the filter
            assertRows(SQL.replace("t.x > 1", "t.x > 100") + " LIMIT -2", "");
            // the last row that passes the filter holds the only elements
            assertRows(SQL.replace("t.x > 1", "t.x > 1 AND t.x < 8") + " LIMIT -2", rows(0, 1));
            execute("TRUNCATE TABLE ta");
            assertRows(SQL + " LIMIT -2", "");
        });
    }

    @Test
    public void testErrorInUnnestSource() throws Exception {
        // The scan counts the rows of row 3 first, and the matrix product of its arrays fails there, as it
        // does when a scan without a LIMIT reaches the row last.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (id INT, x INT, m DOUBLE[][], n DOUBLE[][], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tc VALUES
                    (1, 5, ARRAY[[1.0, 2.0]], ARRAY[[1.0], [1.0]], '2024-01-05T01:00:00.000000Z'),
                    (2, 6, ARRAY[[1.0, 2.0], [3.0, 4.0]], ARRAY[[1.0], [1.0]], '2024-01-06T01:00:00.000000Z'),
                    (3, 7, ARRAY[[1.0, 2.0]], ARRAY[[1.0]], '2024-01-07T01:00:00.000000Z')
                    """);
            final String sql = "SELECT t.id, u.r FROM tc t, UNNEST(matmul(t.m, t.n)) u(r) WHERE t.x > 1";
            final String unlimitedError = getError(sql);
            TestUtils.assertContains(unlimitedError, "left array row length doesn't match right array column length");
            Assert.assertEquals(unlimitedError, getError(sql + " LIMIT -2"));
            assertQuery(sql + " LIMIT -2")
                    .noLeakCheck()
                    .assertsPlanContaining("fan-out limit: 2");
            // the table answers once the row is fixed
            execute("UPDATE tc SET n = ARRAY[[1.0], [1.0]] WHERE id = 3");
            assertRowsWithHeader(sql + " LIMIT -2", "id\tr\n2\t[7.0]\n3\t[3.0]\n");
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
    public void testFanOutLimitPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(SQL + " LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlan("""
                            Limit value: -2 skip-rows: baseRows-2 take-rows-max: 2
                                SelectedRecord
                                    Unnest
                                      columns: [val]
                                        Async JIT Filter workers: 1
                                          fan-out limit: 2
                                          filter: 1<x
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: ta
                            """)
                    .returns(HEADER + rows(5, 7));
            // a positive LIMIT stops the forward scan from above, through UNNEST
            assertQuery(SQL + " LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .withPlan("""
                            Limit value: 2 skip-rows-max: 0 take-rows-max: 2
                                SelectedRecord
                                    Unnest
                                      columns: [val]
                                        Async JIT Filter workers: 1
                                          filter: 1<x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: ta
                            """)
                    .returns(HEADER + rows(0, 2));
        });
    }

    @Test
    public void testFuzzAgainstModel() throws Exception {
        // random arrays of zero to three elements, a NULL array in one row of five and a NULL x in one row of
        // ten; a model of the table gives the rows of every LIMIT
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1 + rnd.nextInt(4));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4 + rnd.nextInt(60));
        assertMemoryLeak(() -> {
            for (int iteration = 0; iteration < 20; iteration++) {
                // the JIT and the Java filter collect the rows with the same cursor, from different reducers
                sqlExecutionContext.setJitMode(rnd.nextBoolean() ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                execute("DROP TABLE IF EXISTS tf");
                execute("CREATE TABLE tf (x INT, arr DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                final int rowCount = 1 + rnd.nextInt(200);
                final int threshold = rnd.nextInt(10);
                final StringBuilder insert = new StringBuilder("INSERT INTO tf VALUES ");
                final StringBuilder expected = new StringBuilder();
                final int[] expectedRowStarts = new int[rowCount * 3 + 1];
                int expectedRowCount = 0;
                for (int r = 0; r < rowCount; r++) {
                    final boolean isNullX = rnd.nextInt(10) == 0;
                    final int x = rnd.nextInt(15);
                    final boolean isNullArray = rnd.nextInt(5) == 0;
                    final int len = rnd.nextInt(4);
                    if (r > 0) {
                        insert.append(',');
                    }
                    insert.append('(').append(isNullX ? "NULL" : Integer.toString(x)).append(',');
                    if (isNullArray) {
                        insert.append("NULL");
                    } else if (len == 0) {
                        insert.append("ARRAY[]::DOUBLE[]");
                    } else {
                        insert.append("ARRAY[");
                        for (int e = 0; e < len; e++) {
                            if (e > 0) {
                                insert.append(',');
                            }
                            insert.append(r).append('.').append(5);
                        }
                        insert.append(']');
                    }
                    // one row an hour, so the table spans several DAY partitions
                    insert.append(",").append((long) r * 3_600_000_000L).append("::TIMESTAMP)");
                    if (!isNullX && x > threshold && !isNullArray) {
                        for (int e = 0; e < len; e++) {
                            expectedRowStarts[expectedRowCount++] = expected.length();
                            expected.append(x).append('\t').append(r).append(".5\n");
                        }
                    }
                }
                expectedRowStarts[expectedRowCount] = expected.length();
                execute(insert);
                final String sql = "SELECT t.x, u.val FROM tf t, UNNEST(t.arr) u(val) WHERE t.x > " + threshold;
                for (int k = 0; k < 4; k++) {
                    final int n = 1 + rnd.nextInt(expectedRowCount + 3);
                    final int from = Math.max(0, expectedRowCount - n);
                    assertRowsWithHeader(
                            sql + " LIMIT -" + n,
                            "x\tval\n" + expected.substring(expectedRowStarts[from], expectedRowStarts[expectedRowCount])
                    );
                    final int to = Math.min(n, expectedRowCount);
                    assertRowsWithHeader(
                            sql + " LIMIT " + n,
                            "x\tval\n" + expected.substring(0, expectedRowStarts[to])
                    );
                }
            }
        });
    }

    @Test
    public void testInvalidLimit() throws Exception {
        // the filter takes no fan-out limit of a LIMIT that does not compile, and the Limit above the level
        // reports the error, as it does without UNNEST
        assertMemoryLeak(() -> {
            createTable();
            assertException(SQL + " LIMIT 'abc'", SQL.length() + 7, "invalid type: STRING");
            assertException(SQL + " LIMIT true", SQL.length() + 7, "LIMIT expressions must be convertible to INT");
        });
    }

    @Test
    public void testJoinsAfterUnnestKeepFanOutLimit() throws Exception {
        // LEFT, CROSS and ASOF joins return at least one row for each row of UNNEST, in its order
        assertMemoryLeak(() -> {
            createTable();
            // w holds row 8 twice
            execute("CREATE TABLE w (k INT)");
            execute("INSERT INTO w VALUES (8), (5), (8)");
            execute("CREATE TABLE c (s SYMBOL)");
            execute("INSERT INTO c VALUES ('a'), ('b')");
            execute("CREATE TABLE ce (s SYMBOL)");
            execute("CREATE TABLE q (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO q VALUES (100, '2024-01-06T00:00:00.000000Z'), (200, '2024-01-07T00:00:00.000000Z')");
            assertFanOutLimit(
                    "SELECT t.id, u.val, w.k FROM ta t, UNNEST(t.arr) u(val) LEFT JOIN w ON w.k = t.id WHERE t.x > 1 LIMIT -3",
                    """
                            id\tval\tk
                            7\t7.5\tnull
                            8\t8.0\t8
                            8\t8.0\t8
                            """
            );
            assertFanOutLimit(
                    "SELECT t.id, u.val, c.s FROM ta t, UNNEST(t.arr) u(val) CROSS JOIN c WHERE t.x > 1 LIMIT -3",
                    """
                            id\tval\ts
                            7\t7.5\tb
                            8\t8.0\ta
                            8\t8.0\tb
                            """
            );
            assertFanOutLimit(
                    "SELECT t.id, u.val, c.s FROM ta t, UNNEST(t.arr) u(val) CROSS JOIN ce c WHERE t.x > 1 LIMIT -3",
                    "id\tval\ts\n"
            );
            assertFanOutLimit(
                    "SELECT t.id, u.val, q.v FROM ta t, UNNEST(t.arr) u(val) ASOF JOIN q WHERE t.x > 1 LIMIT -3",
                    """
                            id\tval\tv
                            7\t7.0\t100
                            7\t7.5\t100
                            8\t8.0\t200
                            """
            );
        });
    }

    @Test
    public void testLatestOnScanTakesNoLimit() throws Exception {
        // LATEST ON reads every row of the filter below it, a bind variable residual of a sub-query here, so
        // the filter takes neither a LIMIT nor a fan-out limit. x differs in every row of ta, so every row is
        // the latest of its key.
        assertMemoryLeak(() -> {
            createTable();
            bindVariableService.clear();
            bindVariableService.setStr("b0", "2024-01-01T00:00:00.000000Z");
            assertQuery("WITH c AS (SELECT id, x, ts FROM ta) SELECT id FROM c WHERE :b0::timestamp < '2024-09-22'::date"
                    + " LATEST ON ts PARTITION BY x ORDER BY ts LIMIT -2")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining("limit:")
                    .returns("id\n9\n10\n");
        });
    }

    @Test
    public void testLimitBeyondMaxNegativeLimit() throws Exception {
        // The fan-out limit keeps at most cairo.sql.max.negative.limit rows. Beyond it, the scan reads every
        // row and the Limit above the level counts the rows, with no error.
        setProperty(PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 2);
        assertMemoryLeak(() -> {
            createTable();
            assertRows(SQL + " LIMIT -3", rows(4, 7));
            assertQuery(SQL + " LIMIT -3")
                    .noLeakCheck()
                    .assertsPlanNotContaining("limit:");
            assertRows(SQL + " LIMIT -2", rows(5, 7));
            assertQuery(SQL + " LIMIT -2")
                    .noLeakCheck()
                    .assertsPlanContaining("fan-out limit: 2", "Row backward scan");
        });
    }

    @Test
    public void testLimitThatFailsToEvaluate() throws Exception {
        // The plan evaluates the LIMIT to show the fan-out limit, and shows none for a LIMIT that fails to
        // evaluate, such as a bind variable that is no longer set. The query reports the error at the LIMIT.
        assertMemoryLeak(() -> {
            createTable();
            final String sql = SQL + " LIMIT $1";
            bindVariableService.clear();
            bindVariableService.setLong(0, -2);
            try (RecordCursorFactory factory = select(sql)) {
                planSink.of(factory, sqlExecutionContext);
                TestUtils.assertContains(planSink.getSink(), "fan-out limit: 2");
                bindVariableService.clear();
                planSink.of(factory, sqlExecutionContext);
                TestUtils.assertNotContains(planSink.getSink(), "limit:");
                TestUtils.assertContains(planSink.getSink(), "Row forward scan");
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("the LIMIT has no value");
                } catch (SqlException e) {
                    Assert.assertEquals(SQL.length() + 7, e.getPosition());
                    TestUtils.assertEquals("undefined bind variable: 0", e.getFlyweightMessage());
                }
                bindVariableService.setLong(0, -2);
                assertBoundRows(factory, -2, rows(5, 7));
            }
        });
    }

    @Test
    public void testLimits() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertLimits(SQL);
        });
    }

    @Test
    public void testOrderByTimestampDescending() throws Exception {
        // the scan runs backward to follow ORDER BY, and a negative LIMIT scans it forward
        assertMemoryLeak(() -> {
            createTable();
            assertQuery(SQL + " ORDER BY t.ts DESC LIMIT -2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns(HEADER + "5\t5.75\n3\t3.0\n");
            assertQuery(SQL + " ORDER BY t.ts DESC LIMIT -4")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns(HEADER + "5\t5.0\n5\t5.5\n5\t5.75\n3\t3.0\n");
            assertQuery(SQL + " ORDER BY t.ts DESC LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .returns(HEADER + "8\t8.0\n7\t7.0\n");
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("ALTER TABLE ta CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-07'");
            assertLimits(SQL);
            execute("ALTER TABLE ta CONVERT PARTITION TO PARQUET WHERE ts >= '2024-01-07'");
            assertLimits(SQL);
        });
    }

    @Test
    public void testShapesWithoutFanOutLimit() throws Exception {
        // A filter after UNNEST, an INNER join or a second UNNEST drop rows that the scan cannot count, and a
        // random array may change between two reads of a row. The filter of t takes no LIMIT then.
        assertMemoryLeak(() -> {
            createTable();
            createZipTable();
            execute("CREATE TABLE w (k INT)");
            execute("INSERT INTO w VALUES (5)");
            assertNoScanLimit(
                    SQL + " AND u.val > 5.6 LIMIT -2",
                    HEADER + "7\t7.5\n8\t8.0\n"
            );
            // the last rows of ta unnest into the LIMIT rows, but only row 5 matches w
            assertNoScanLimit(
                    "SELECT t.id, u.val FROM ta t, UNNEST(t.arr) u(val) JOIN w ON w.k = t.id WHERE t.x > 1 LIMIT -2",
                    HEADER + "5\t5.5\n5\t5.75\n"
            );
            // a RIGHT join whose ON is false keeps the row of w alone, with a NULL array, so UNNEST returns no row
            assertNoScanLimit(
                    "SELECT t.id, u.val FROM ta t RIGHT JOIN w ON w.k = t.id AND 1 = 0 CROSS JOIN UNNEST(t.arr) u(val) WHERE w.k > 0 LIMIT -2",
                    HEADER
            );
            // only row 1 of tz holds elements in both arrays, and each array holds elements in a later row
            assertNoScanLimit(
                    "SELECT t.id, u.a, v.b FROM tz t, UNNEST(t.a) u(a), UNNEST(t.b) v(b) WHERE t.x > 1 LIMIT -1",
                    "id\ta\tb\n1\t1.0\t1.75\n"
            );
            assertQuery("SELECT t.id, u.val FROM ta t, UNNEST(rnd_double_array(1, 0, 0, 3)) u(val) WHERE t.x > 1 LIMIT -2")
                    .noLeakCheck()
                    .assertsPlanNotContaining("limit:");
        });
    }

    @Test
    public void testSqlContexts() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // CROSS JOIN UNNEST is the comma form
            assertFanOutLimit("SELECT t.id, u.val FROM ta t CROSS JOIN UNNEST(t.arr) u(val) WHERE t.x > 1 LIMIT -2", HEADER + rows(5, 7));
            // a computed column puts a model between the LIMIT and the level
            assertFanOutLimit(
                    "SELECT t.id, u.val * 2 v2 FROM ta t, UNNEST(t.arr) u(val) WHERE t.x > 1 LIMIT -2",
                    "id\tv2\n7\t15.0\n8\t16.0\n"
            );
            assertRows("SELECT * FROM (" + SQL + " LIMIT -2)", rows(5, 7));
            assertRows("WITH r AS (" + SQL + " LIMIT -3) SELECT * FROM r", rows(4, 7));
            assertRows("SELECT * FROM (" + SQL + ") LIMIT -3", rows(4, 7));
            assertRows("SELECT * FROM (" + SQL + " LIMIT -1) UNION ALL SELECT * FROM (" + SQL + " LIMIT -2)", rows(6, 7) + rows(5, 7));
            assertQuery("SELECT count() FROM (" + SQL + " LIMIT -4)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n4\n");
            // an ORDER BY that needs a sort takes no LIMIT advice
            assertQuery(SQL + " ORDER BY u.val DESC LIMIT -2")
                    .noLeakCheck()
                    .expectSize()
                    .returns(HEADER + "5\t5.0\n3\t3.0\n");
        });
    }

    @Test
    public void testUnnestOfJoinedColumns() throws Exception {
        // The scan counts the rows of UNNEST on its own rows, ahead of the joins between the two. So it takes no
        // fan-out limit when UNNEST reads a column that a join adds, or an expression whose columns the code
        // generator does not follow, such as an array slice. With no join in between, UNNEST reads the rows of
        // the scan, whatever its expression.
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE w (k INT, warr DOUBLE[])");
            execute("INSERT INTO w VALUES (7, ARRAY[70.0, 70.5]), (8, ARRAY[]::DOUBLE[])");
            // rows 7 and 8 match w, and only the array of row 7 holds elements
            assertNoScanLimit(
                    "SELECT t.id, u.val FROM ta t LEFT JOIN w ON w.k = t.id CROSS JOIN UNNEST(w.warr) u(val) WHERE t.x > 1 LIMIT -1",
                    HEADER + "7\t70.5\n"
            );
            // the slice keeps the first two elements of each array
            assertNoScanLimit(
                    "SELECT t.id, u.val FROM ta t LEFT JOIN w ON w.k = t.id CROSS JOIN UNNEST(t.arr[1:3]) u(val) WHERE t.x > 1 LIMIT -3",
                    HEADER + "7\t7.0\n7\t7.5\n8\t8.0\n"
            );
            assertFanOutLimit(
                    "SELECT t.id, u.val FROM ta t, UNNEST(t.arr[1:3]) u(val) WHERE t.x > 1 LIMIT -4",
                    HEADER + "5\t5.5\n7\t7.0\n7\t7.5\n8\t8.0\n"
            );
            // a column of the scan behind a join keeps the fan-out limit
            assertFanOutLimit(
                    "SELECT t.id, u.val, w.k FROM ta t LEFT JOIN w ON w.k = t.id CROSS JOIN UNNEST(t.arr) u(val) WHERE t.x > 1 LIMIT -3",
                    "id\tval\tk\n7\t7.0\t7\n7\t7.5\t7\n8\t8.0\t8\n"
            );
        });
    }

    @Test
    public void testUnnestExpressionsBehindJoin() throws Exception {
        // The code generator follows functions, operators, constants and bind variables to the columns that
        // an UNNEST expression behind a join reads. w holds one row per key, so the LEFT join keeps the rows
        // of ta one for one.
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE w (k INT, warr DOUBLE[])");
            execute("INSERT INTO w VALUES (7, ARRAY[70.0, 70.5]), (8, ARRAY[]::DOUBLE[])");
            final String from = "FROM ta t LEFT JOIN w ON w.k = t.id CROSS JOIN ";
            // the running sums of rows 7 and 8
            assertFanOutLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(array_cum_sum(t.arr)) u(val) WHERE t.x > 1 LIMIT -3",
                    HEADER + "7\t7.0\n7\t14.5\n8\t8.0\n"
            );
            assertFanOutLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(t.arr * 2.0) u(val) WHERE t.x > 1 LIMIT -2",
                    HEADER + "7\t15.0\n8\t16.0\n"
            );
            // a function of no argument
            assertFanOutLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(t.arr * pi()) u(val) WHERE t.x > 1 LIMIT -2",
                    HEADER + "7\t" + 7.5 * Math.PI + "\n8\t" + 8.0 * Math.PI + "\n"
            );
            bindVariableService.clear();
            bindVariableService.setDouble(0, 10.0);
            assertFanOutLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(t.arr * $1) u(val) WHERE t.x > 1 LIMIT -1",
                    HEADER + "8\t80.0\n"
            );
            // rows 7 and 8 pass x > 9 and keep their arrays
            assertFanOutLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(CASE WHEN t.x > 9 THEN t.arr ELSE t.arr * 10.0 END) u(val) WHERE t.x > 1 LIMIT -4",
                    HEADER + "5\t57.5\n7\t7.0\n7\t7.5\n8\t8.0\n"
            );
            assertNoScanLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(w.warr * 2.0) u(val) WHERE t.x > 1 LIMIT -1",
                    HEADER + "7\t141.0\n"
            );
            // the joined array of row 7 runs to 140.5, and that of row 8 is empty
            assertNoScanLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(array_cum_sum(w.warr)) u(val) WHERE t.x > 1 LIMIT -1",
                    HEADER + "7\t140.5\n"
            );
            assertNoScanLimit(
                    "SELECT t.id, u.val " + from + "UNNEST(CASE WHEN t.x > 9 THEN w.warr ELSE t.arr END) u(val) WHERE t.x > 1 LIMIT -2",
                    HEADER + "7\t70.0\n7\t70.5\n"
            );
        });
    }

    @Test
    public void testUnnestVariants() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // WITH ORDINALITY numbers the elements of each row
            assertFanOutLimit(
                    "SELECT t.id, u.val, u.ord FROM ta t, UNNEST(t.arr) WITH ORDINALITY u(val, ord) WHERE t.x > 1 LIMIT -2",
                    "id\tval\tord\n7\t7.5\t2\n8\t8.0\t1\n"
            );
            // two arrays run to the longer one; rows 4 and 5 unnest into no row
            createZipTable();
            final String zip = "SELECT t.id, u.a, u.b FROM tz t, UNNEST(t.a, t.b) u(a, b) WHERE t.x > 1";
            assertFanOutLimit(zip + " LIMIT -3", "id\ta\tb\n2\t2.0\tnull\n2\t2.5\tnull\n3\tnull\t3.5\n");
            assertFanOutLimit(zip + " LIMIT -4", "id\ta\tb\n1\tnull\t1.75\n2\t2.0\tnull\n2\t2.5\tnull\n3\tnull\t3.5\n");
            // a JSON array; an empty, a NULL and a malformed document unnest into no row
            execute("CREATE TABLE tj (id INT, x INT, j VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tj VALUES
                    (1, 2, '[1, 2]', '2024-01-05T01:00:00.000000Z'),
                    (2, 3, '[]', '2024-01-05T02:00:00.000000Z'),
                    (3, 4, '[4]', '2024-01-06T01:00:00.000000Z'),
                    (4, 5, NULL, '2024-01-06T02:00:00.000000Z'),
                    (5, 6, '[]', '2024-01-07T01:00:00.000000Z'),
                    (6, 7, 'not json', '2024-01-07T02:00:00.000000Z')
                    """);
            assertFanOutLimit(
                    "SELECT t.id, u.v FROM tj t, UNNEST(t.j COLUMNS(v INT)) u WHERE t.x > 1 LIMIT -2",
                    "id\tv\n1\t2\n3\t4\n"
            );
            // a two-dimensional array unnests into its rows
            execute("CREATE TABLE tm (id INT, x INT, m DOUBLE[][], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tm VALUES
                    (1, 2, ARRAY[[1.0, 2.0], [3.0, 4.0]], '2024-01-05T01:00:00.000000Z'),
                    (2, 3, ARRAY[[5.0, 6.0]], '2024-01-06T01:00:00.000000Z'),
                    (3, 4, NULL, '2024-01-07T01:00:00.000000Z')
                    """);
            assertFanOutLimit(
                    "SELECT t.id, u.r FROM tm t, UNNEST(t.m) u(r) WHERE t.x > 1 LIMIT -2",
                    "id\tr\n1\t[3.0,4.0]\n2\t[5.0,6.0]\n"
            );
        });
    }

    @Test
    public void testWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (id INT, x INT, arr DOUBLE[], s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertRows();
            drainWalQueue();
            assertLimits(SQL);
        });
    }

    private static String rows(int from, int to) {
        final StringBuilder sb = new StringBuilder();
        for (int i = from; i < to; i++) {
            sb.append(ROWS[i]);
        }
        return sb.toString();
    }

    private void assertBoundRows(RecordCursorFactory factory, long limit, String expectedRows) throws Exception {
        bindVariableService.setLong(0, limit);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursorTwoPass(HEADER + expectedRows, cursor, factory.getMetadata());
        }
    }

    // The filter of t runs as an async JIT filter, an async Java filter and a serial filter.
    private void assertExecutionModes() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final int callerJitMode = sqlExecutionContext.getJitMode();
            final boolean callerParallel = sqlExecutionContext.isParallelFilterEnabled();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    for (boolean isParallel : new boolean[]{false, true}) {
                        sqlExecutionContext.setJitMode(jitMode);
                        sqlExecutionContext.setParallelFilterEnabled(isParallel);
                        assertLimits(SQL);
                        // the regular expression runs in the Java filter whether JIT is on or off
                        assertLimits(SQL.replace("t.x > 1", "t.x > 1 AND t.s ~ '^n'"));
                        if (isParallel) {
                            final String filter = jitMode == SqlJitMode.JIT_MODE_ENABLED ? "Async JIT Filter" : "Async Filter";
                            assertQuery(SQL + " LIMIT -2")
                                    .noLeakCheck()
                                    .assertsPlanContaining(filter, "fan-out limit: 2", "Row backward scan");
                            // a positive LIMIT stops the scan from above, through UNNEST
                            assertQuery(SQL + " LIMIT 2")
                                    .noLeakCheck()
                                    .assertsPlanNotContaining("limit:");
                            // a parent that steals the filter of a table, as GROUP BY does, finds no fan-out limit
                            assertQuery("SELECT sum(x) FROM ta WHERE x > 1")
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .withPlanContaining("Group By", "filter: 1<x")
                                    .returns("sum\n81\n");
                            // without UNNEST, the filter keeps the LIMIT of its own rows, and so it does ahead of
                            // joins that keep its rows
                            assertQuery("SELECT id FROM ta WHERE x > 1 LIMIT -2")
                                    .noLeakCheck()
                                    .expectSize()
                                    .withPlanContaining(filter, "limit: 2")
                                    .withPlanNotContaining("fan-out limit")
                                    .returns("id\n9\n10\n");
                            assertQuery("SELECT t.id FROM ta t ASOF JOIN ta u WHERE t.x > 1 LIMIT -2")
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .sizeMayVary()
                                    .withPlanContaining(filter, "limit: 2")
                                    .withPlanNotContaining("fan-out limit")
                                    .returns("id\n9\n10\n");
                            assertQuery(SQL.replace("t.x > 1", "t.x > 1 AND t.s ~ '^n'") + " LIMIT -2")
                                    .noLeakCheck()
                                    .assertsPlanContaining("Async Filter", "fan-out limit: 2", "Row backward scan");
                        } else {
                            assertQuery(SQL + " LIMIT -2")
                                    .noLeakCheck()
                                    .assertsPlanNotContaining("limit:");
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
                sqlExecutionContext.setParallelFilterEnabled(callerParallel);
            }
        });
    }

    // the filter of t takes the fan-out limit and scans backward, and the query returns the expected rows
    private void assertFanOutLimit(String sql, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .sizeMayVary()
                .withPlanContaining("fan-out limit:", "Row backward scan")
                .returns(expected);
    }

    private void assertLimits(String sql) throws Exception {
        for (int n = 1; n <= ROWS.length + 1; n++) {
            assertRows(sql + " LIMIT -" + n, rows(Math.max(0, ROWS.length - n), ROWS.length));
            assertRows(sql + " LIMIT " + n, rows(0, Math.min(n, ROWS.length)));
        }
        assertRows(sql + " LIMIT -100", rows(0, ROWS.length));
        assertRows(sql + " LIMIT 0", "");
        assertRows(sql + " LIMIT 1, 3", rows(1, 3));
        // the last three rows but the last one
        assertRows(sql + " LIMIT -3, -1", rows(4, 6));
    }

    // no filter takes the LIMIT, and the query returns the expected rows
    private void assertNoScanLimit(String sql, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .sizeMayVary()
                .withPlanNotContaining("limit:")
                .returns(expected);
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

    private void createTable() throws Exception {
        execute("CREATE TABLE ta (id INT, x INT, arr DOUBLE[], s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        insertRows();
    }

    // tz holds two arrays, which hold elements in different rows
    private void createZipTable() throws Exception {
        execute("CREATE TABLE tz (id INT, x INT, a DOUBLE[], b DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO tz VALUES
                (1, 2, ARRAY[1.0], ARRAY[1.5, 1.75], '2024-01-05T01:00:00.000000Z'),
                (2, 3, ARRAY[2.0, 2.5], ARRAY[]::DOUBLE[], '2024-01-05T02:00:00.000000Z'),
                (3, 4, ARRAY[]::DOUBLE[], ARRAY[3.5], '2024-01-06T01:00:00.000000Z'),
                (4, 5, ARRAY[]::DOUBLE[], NULL, '2024-01-06T02:00:00.000000Z'),
                (5, 6, NULL, ARRAY[]::DOUBLE[], '2024-01-07T01:00:00.000000Z')
                """);
    }

    private String getError(String sql) {
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            return null;
        } catch (SqlException e) {
            return e.getFlyweightMessage().toString();
        } catch (Throwable e) {
            return e.getMessage();
        }
    }

    private void insertRows() throws Exception {
        execute("""
                INSERT INTO ta VALUES
                (1, 5, ARRAY[]::DOUBLE[], 'n1', '2024-01-05T01:00:00.000000Z'),
                (2, 6, NULL, 'n2', '2024-01-05T02:00:00.000000Z'),
                (3, 7, ARRAY[3.0], 'n3', '2024-01-05T03:00:00.000000Z'),
                (4, 0, ARRAY[4.0, 4.5], 'm4', '2024-01-05T04:00:00.000000Z'),
                (5, 8, ARRAY[5.0, 5.5, 5.75], 'n5', '2024-01-06T01:00:00.000000Z'),
                (6, 9, ARRAY[]::DOUBLE[], 'n6', '2024-01-06T02:00:00.000000Z'),
                (7, 10, ARRAY[7.0, 7.5], 'n7', '2024-01-06T03:00:00.000000Z'),
                (8, 11, ARRAY[8.0], 'n8', '2024-01-07T01:00:00.000000Z'),
                (9, 12, NULL, 'n9', '2024-01-07T02:00:00.000000Z'),
                (10, 13, ARRAY[]::DOUBLE[], 'n10', '2024-01-07T03:00:00.000000Z')
                """);
    }
}
