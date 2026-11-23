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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.TestTimestampType;
import io.questdb.test.griffin.model.TickExprEngineJobTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * A TICK range between date variables can start after it ends at some times: on 2026-03-15,
 * '$now-1M..$now-30d' runs from Feb 15 to Feb 13. A user's query fails then, as on any range that
 * starts after it ends. SQL that the engine runs on its own, such as a materialized view refresh,
 * has no caller to report the failure to, and there the range adds no interval: IN matches no row,
 * and NOT IN every row. Either way, no interval scan sees an inverted interval.
 */
@RunWith(Parameterized.class)
public class ReversedTickRangeTest extends AbstractCairoTest {
    private static final String RANGE_ORDER_ERROR = "Invalid date range: start is after end";
    private static final String REVERSED_NOW = "2026-03-15T12:00:00.000000Z";
    private static final String VALID_NOW = "2026-04-15T12:00:00.000000Z";
    private final byte partitionFormat;
    private final TestTimestampType timestampType;

    public ReversedTickRangeTest(byte partitionFormat, TestTimestampType timestampType) {
        this.partitionFormat = partitionFormat;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "format={0},ts={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {PartitionFormat.NATIVE, TestTimestampType.MICRO},
                {PartitionFormat.NATIVE, TestTimestampType.NANO},
                {PartitionFormat.PARQUET, TestTimestampType.MICRO},
                {PartitionFormat.PARQUET, TestTimestampType.NANO}
        });
    }

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Test
    public void testCachedRangeReversal() throws Exception {
        assertMemoryLeak(() -> {
            createTable("MONTH");
            final String predicate = " WHERE ts IN '$now-1M..$now-30d'";
            final String forwardSql = "SELECT v FROM x" + predicate;
            final String backwardSql = "SELECT v FROM x" + predicate + " ORDER BY ts DESC";
            final String countSql = "SELECT count() FROM x" + predicate;
            try (SqlExecutionContextImpl engineJob = TickExprEngineJobTest.newEngineJobContext()) {
                for (SqlExecutionContext context : new SqlExecutionContext[]{sqlExecutionContext, engineJob}) {
                    setNow(engineJob, VALID_NOW);
                    try (
                            RecordCursorFactory forward = select(forwardSql, context);
                            RecordCursorFactory backward = select(backwardSql, context);
                            RecordCursorFactory count = select(countSql, context)
                    ) {
                        QueryAssertion forwardAssertion = new QueryAssertion(engine, forward).withContext(context);
                        QueryAssertion backwardAssertion = new QueryAssertion(engine, backward).withContext(context);
                        QueryAssertion countAssertion = new QueryAssertion(engine, count).withContext(context).expectSize().noRandomAccess();

                        forwardAssertion.returns("v\n7\n8\n9\n");
                        backwardAssertion.returns("v\n9\n8\n7\n");
                        countAssertion.returns("count\n3\n");

                        // The same factories now evaluate [Feb 15, Feb 13], with rows between the bounds.
                        setNow(engineJob, REVERSED_NOW);
                        if (context == engineJob) {
                            forwardAssertion.returns("v\n");
                            backwardAssertion.returns("v\n");
                            countAssertion.returns("count\n0\n");
                        } else {
                            assertCursorFails(forward, forwardSql);
                            assertCursorFails(backward, backwardSql);
                            assertCursorFails(count, countSql);
                        }

                        setNow(engineJob, VALID_NOW);
                        forwardAssertion.returns("v\n7\n8\n9\n");
                        backwardAssertion.returns("v\n9\n8\n7\n");
                        countAssertion.returns("count\n3\n");
                    }
                }
            }
        });
    }

    @Test
    public void testEqualBounds() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            assertRange(sqlExecutionContext, "ts IN '$now..$now'", "v\n7\n", "v\n7\n", 1);
            assertRange(sqlExecutionContext, "ts IN '$now-1h..$now'", "v\n5\n6\n7\n", "v\n7\n6\n5\n", 3);
        });
    }

    @Test
    public void testNegatedReversedBounds() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            try (SqlExecutionContextImpl engineJob = TickExprEngineJobTest.newEngineJobContext()) {
                setNow(engineJob, REVERSED_NOW);
                assertRangeFails("ts NOT IN '$now..$now-1h'", "'$now..$now-1h'");
                assertRange(engineJob, "ts NOT IN '$now..$now-1h'", "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "v\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", 10);
                assertRangeFails("ts NOT IN '$now..$now-1h' AND ts IN '2026-03-15'", "'$now..$now-1h'");
                assertRange(engineJob, "ts NOT IN '$now..$now-1h' AND ts IN '2026-03-15'", "v\n4\n5\n6\n7\n8\n", "v\n8\n7\n6\n5\n4\n", 5);
                // An AND evaluates the interval of its right side first, so that range fails
                assertRangeFails("ts IN '$now..$now-1h' AND ts NOT IN '$now..$now-2h'", "'$now..$now-2h'");
                assertRange(engineJob, "ts IN '$now..$now-1h' AND ts NOT IN '$now..$now-2h'", "v\n", "v\n", 0);
                assertRangeFails("ts NOT IN '$now..$now-2h' AND ts IN '$now..$now-1h'", "'$now..$now-1h'");
                assertRange(engineJob, "ts NOT IN '$now..$now-2h' AND ts IN '$now..$now-1h'", "v\n", "v\n", 0);
            }
        });
    }

    @Test
    public void testReversedBounds() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            try (SqlExecutionContextImpl engineJob = TickExprEngineJobTest.newEngineJobContext()) {
                setNow(engineJob, REVERSED_NOW);
                assertRangeFails("ts IN '$now..$now-1h'", "'$now..$now-1h'");
                assertRange(engineJob, "ts IN '$now..$now-1h'", "v\n", "v\n", 0);
            }
        });
    }

    @Test
    public void testReversedSetOperations() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            try (SqlExecutionContextImpl engineJob = TickExprEngineJobTest.newEngineJobContext()) {
                setNow(engineJob, REVERSED_NOW);
                assertRangeFails("ts IN '[$now-2h, $now..$now-1h, $now+1h]'", "'[$now-2h, $now..$now-1h, $now+1h]'");
                assertRange(engineJob, "ts IN '[$now-2h, $now..$now-1h, $now+1h]'", "v\n4\n8\n", "v\n8\n4\n", 2);
                assertRangeFails("ts IN '$now..$now-1h' AND ts > '2026-03-01'", "'$now..$now-1h'");
                assertRange(engineJob, "ts IN '$now..$now-1h' AND ts > '2026-03-01'", "v\n", "v\n", 0);
            }
        });
    }

    @Test
    public void testStatementCompiledBeforeRangeStartRunsAfterIt() throws Exception {
        // A statement prepared at 03:00, when the range from 09:30 to "now" starts after it ends,
        // compiles: the JIT compiler opens the interval scan of the filter as the statement
        // compiles, and the range adds no interval there. The statement runs at 12:00, and fails
        // only when it runs before 09:30. Its end is "now" itself, and at 09:30 the range is that
        // one point in time.
        final int jitMode = sqlExecutionContext.getJitMode();
        assertMemoryLeak(() -> {
            final String predicate = " WHERE ts IN '$today + 570m..$now' AND v > 0";
            final String forwardSql = "SELECT v FROM x" + predicate;
            final String backwardSql = "SELECT v FROM x" + predicate + " ORDER BY ts DESC";
            final String countSql = "SELECT count() FROM x" + predicate;
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                // a Parquet partition converts from a partitioned table only
                for (String partitionBy : partitionFormat == PartitionFormat.PARQUET ? new String[]{"DAY"} : new String[]{"DAY", "NONE"}) {
                    execute("DROP TABLE IF EXISTS x");
                    createTable(partitionBy);
                    // the context keeps 03:00, and the clock says 12:00
                    setCurrentMicros(MicrosFormatUtils.parseTimestamp("2026-03-15T03:00:00.000000Z"));
                    currentMicros = MicrosFormatUtils.parseTimestamp(REVERSED_NOW);
                    try (
                            RecordCursorFactory forward = select(forwardSql);
                            RecordCursorFactory backward = select(backwardSql);
                            RecordCursorFactory count = select(countSql)
                    ) {
                        final QueryAssertion forwardAssertion = new QueryAssertion(engine, forward).withContext(sqlExecutionContext);
                        final QueryAssertion backwardAssertion = new QueryAssertion(engine, backward).withContext(sqlExecutionContext);
                        final QueryAssertion countAssertion = new QueryAssertion(engine, count).withContext(sqlExecutionContext).expectSize().noRandomAccess();
                        // the rows at 10:00, 11:00, 11:30 and 12:00
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp(REVERSED_NOW));
                        forwardAssertion.returns("v\n4\n5\n6\n7\n");
                        backwardAssertion.returns("v\n7\n6\n5\n4\n");
                        countAssertion.returns("count\n4\n");
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2026-03-15T10:00:00.000000Z"));
                        forwardAssertion.returns("v\n4\n");
                        backwardAssertion.returns("v\n4\n");
                        countAssertion.returns("count\n1\n");
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2026-03-15T09:30:00.000000Z"));
                        forwardAssertion.returns("v\n");
                        backwardAssertion.returns("v\n");
                        countAssertion.returns("count\n0\n");
                        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2026-03-15T09:29:59.999999Z"));
                        assertCursorFails(forward, forwardSql);
                        assertCursorFails(backward, backwardSql);
                        assertCursorFails(count, countSql);
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    /**
     * Asserts that the cursor of a user's query fails on its range, which starts after it ends.
     */
    private static void assertCursorFails(RecordCursorFactory factory, String sql) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            TestUtils.drainCursor(cursor);
            Assert.fail("the query should fail: " + sql);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), RANGE_ORDER_ERROR);
            Assert.assertEquals(sql.indexOf('\''), e.getPosition());
        }
    }

    /**
     * Asserts that a user's query fails on a range that starts after it ends, in each of the query
     * shapes of {@link #assertRange}, with the error at {@code literal}, the range that fails.
     */
    private static void assertRangeFails(String predicate, String literal) throws Exception {
        for (String sql : new String[]{
                "SELECT v FROM x WHERE " + predicate,
                "SELECT v FROM x WHERE " + predicate + " ORDER BY ts DESC",
                "SELECT count() FROM x WHERE " + predicate
        }) {
            assertExceptionNoLeakCheck(sql, sql.indexOf(literal), RANGE_ORDER_ERROR);
        }
    }

    /**
     * Sets "now" for a user's query, through the test clock, and for the engine job.
     */
    private static void setNow(SqlExecutionContextImpl engineJob, String now) {
        final long micros = MicrosFormatUtils.parseTimestamp(now);
        setCurrentMicros(micros);
        engineJob.setNowAndFixClock(micros, ColumnType.TIMESTAMP_MICRO);
    }

    private void assertRange(
            SqlExecutionContext context,
            String predicate,
            String expectedForward,
            String expectedBackward,
            long expectedCount
    ) throws Exception {
        assertQuery("SELECT v FROM x WHERE " + predicate)
                .noLeakCheck()
                .withContext(context)
                .withPlanContaining("Interval forward scan")
                .returns(expectedForward);
        assertQuery("SELECT v FROM x WHERE " + predicate + " ORDER BY ts DESC")
                .noLeakCheck()
                .withContext(context)
                .withPlanContaining("Interval backward scan")
                .returns(expectedBackward);
        assertQuery("SELECT count() FROM x WHERE " + predicate)
                .noLeakCheck()
                .withContext(context)
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + expectedCount + "\n");
    }

    private void createTable(String partitionBy) throws Exception {
        setCurrentMicros(MicrosFormatUtils.parseTimestamp(REVERSED_NOW));
        execute("CREATE TABLE x (v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY " + partitionBy + " BYPASS WAL");
        execute("""
                INSERT INTO x VALUES
                    (1, '2026-02-13T12:00:00.000000Z'),
                    (2, '2026-02-14T12:00:00.000000Z'),
                    (3, '2026-02-15T12:00:00.000000Z'),
                    (4, '2026-03-15T10:00:00.000000Z'),
                    (5, '2026-03-15T11:00:00.000000Z'),
                    (6, '2026-03-15T11:30:00.000000Z'),
                    (7, '2026-03-15T12:00:00.000000Z'),
                    (8, '2026-03-15T13:00:00.000000Z'),
                    (9, '2026-03-16T12:00:00.000000Z'),
                    (10, '2026-04-01T00:00:00.000000Z')
                """);
        if (partitionFormat == PartitionFormat.PARQUET) {
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2026-04-01'");
        }
    }
}
