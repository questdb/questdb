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

package io.questdb.test.griffin.engine.groupby.vect;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.std.Rosti;
import io.questdb.std.RostiAllocFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The vectorized keyed GROUP BY aggregates a page frame whose key column holds no data, a column top,
 * with the SIMD kernels of a non-keyed aggregation, and merges what they accumulated into the NULL key
 * of the map at wrap-up, through a map of one key and the functions' merge natives.
 */
public class GroupByVectorizedNullKeyMergeTest extends AbstractCairoTest {
    private static final String COLUMN_TOP_SQL = "SELECT k, sum(v) s FROM t ORDER BY k";
    // 2^62: four of them sum to 2^64, past the LONG range.
    private static final long TWO_TO_62 = 4_611_686_018_427_387_904L;

    @Test
    public void testAvgOfLongPastLongRange() throws Exception {
        // The keyed natives sum avg(LONG) into a 128-bit accumulator: the column top rows' sum, which the
        // SIMD kernel returns as a double, enters it whole, past 2^63 and below -2^63 too. Each case
        // puts column top rows and explicit NULL keys in the NULL key; the expected average is plain
        // arithmetic over the rows.
        assertMemoryLeak(() -> {
            // Column tops 2^62 x 4 (2^64), then 2^62 under an explicit NULL key: 5 x 2^62 / 5.
            assertNullKeyAvg(TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62, TWO_TO_62 + "", "4.611686018427388E18");
            // Column tops -2^62 x 4 (-2^64), then -2^62: -2^62.
            assertNullKeyAvg("-" + TWO_TO_62 + ", -" + TWO_TO_62 + ", -" + TWO_TO_62 + ", -" + TWO_TO_62, "-" + TWO_TO_62, "-4.611686018427388E18");
            // Column tops 2^62 x 2 (2^63, one past Long.MAX_VALUE), then 2^62 x 2 (2^63) under explicit
            // NULL keys: the two halves of 2^64 carry into the high half, 2^64 / 4.
            assertNullKeyAvg(TWO_TO_62 + ", " + TWO_TO_62, TWO_TO_62 + ", " + TWO_TO_62, "4.611686018427388E18");
            // Column tops 2^62 x 3 (3 x 2^62, whose low 64 bits read as a negative LONG), then 2^62:
            // 2^64 / 4.
            assertNullKeyAvg(TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62, TWO_TO_62 + "", "4.611686018427388E18");
            // Column tops 2^62 x 7 (2^64 + 3 x 2^62), then 2^62: 2^65 / 8.
            assertNullKeyAvg(
                    TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62,
                    TWO_TO_62 + "",
                    "4.611686018427388E18"
            );
            // Column tops -1 and -2, a negative sum within the LONG range, then 0: -3 / 3.
            assertNullKeyAvg("-1, -2", "0", "-1.0");
            // Column tops only: the NULL key enters the map at the merge, 2^64 / 4.
            assertNullKeyAvg(TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62 + ", " + TWO_TO_62, null, "4.611686018427388E18");
        });
    }

    @Test
    public void testColumnTopFramesTakeTheSimdKernels() throws Exception {
        // Each frame of column tops makes one non-keyed call per function, and no frame with keys makes
        // one; the factory counts them on every execution.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // v 1 to 1_000, d 0.5 to 500.0, all on 2024-01-01.
            execute("INSERT INTO t SELECT x, x / 2.0, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(1_000)");
            execute("ALTER TABLE t ADD COLUMN k INT");
            execute("INSERT INTO t (v, d, ts, k) VALUES (7, 3.5, '2024-01-02T00:00:00Z', 1), (9, 4.5, '2024-01-02T01:00:00Z', NULL)");
            // The NULL key: v 1 to 1_000 and 9, sum 500_500 + 9; max(d) 500.0; 1_001 rows.
            final String expected = """
                    k\ts\tm\tc
                    null\t500509\t500.0\t1001
                    1\t7\t3.5\t1
                    """;
            sqlExecutionContext.changePageFrameSizes(100, 100);
            try (RecordCursorFactory factory = select("SELECT k, sum(v) s, max(d) m, count() c FROM t ORDER BY k")) {
                final GroupByRecordCursorFactory groupBy = findGroupBy(factory);
                for (int i = 0; i < 2; i++) {
                    assertCursor(factory, expected);
                    // 10 frames of 100 column top rows, 3 functions.
                    Assert.assertEquals(30, groupBy.getNullKeyFrameCount());
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
            // The second partition alone has no column tops.
            final String secondPartitionSql = "SELECT k, sum(v) s FROM t WHERE ts >= '2024-01-02' ORDER BY k";
            final String secondPartitionExpected = """
                    k\ts
                    null\t9
                    1\t7
                    """;
            try (RecordCursorFactory factory = select(secondPartitionSql)) {
                assertCursor(factory, secondPartitionExpected);
                Assert.assertEquals(0, findGroupBy(factory).getNullKeyFrameCount());
            }
            // A context of four workers has four maps, which merge into one before the NULL key does.
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 4)
                    .with(securityContext, bindVariableService, null, -1, circuitBreaker)) {
                try (RecordCursorFactory factory = engine.select("SELECT k, sum(v) s, max(d) m, count() c FROM t ORDER BY k", context)) {
                    assertCursor(factory, context, expected);
                    // The first partition is one frame of 1_000 column top rows, 3 functions.
                    Assert.assertEquals(3, findGroupBy(factory).getNullKeyFrameCount());
                }
                try (RecordCursorFactory factory = engine.select(secondPartitionSql, context)) {
                    assertCursor(factory, context, secondPartitionExpected);
                    Assert.assertEquals(0, findGroupBy(factory).getNullKeyFrameCount());
                }
            }
        });
    }

    @Test
    public void testNullKeyMapAllocationFails() throws Exception {
        // The factory allocates the map of the NULL key before the per-worker maps; when its
        // allocation fails, the factory allocates no per-worker map, and the query compiles and runs
        // once the allocation succeeds.
        final int workerCount = 4;
        final AtomicInteger allocations = new AtomicInteger();
        configOverrideRostiAllocFacade(new RostiAllocFacadeImpl() {
            @Override
            public long alloc(ColumnTypes types, long capacity) {
                allocations.incrementAndGet();
                return super.alloc(types, capacity);
            }
        });
        assertMemoryLeak(() -> {
            createColumnTopTable();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, workerCount)
                    .with(securityContext, bindVariableService, null, -1, circuitBreaker)) {
                Rosti.enableOOMOnMalloc();
                try {
                    engine.select(COLUMN_TOP_SQL, context).close();
                    Assert.fail("expected the allocation of the NULL key's map to fail");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not allocate rosti hash table");
                } finally {
                    Rosti.disableOOMOnMalloc();
                }
                Assert.assertEquals(0, allocations.get());
                assertColumnTopResult(context);
            }
        });
    }

    @Test
    public void testPerWorkerMapAllocationFailsAfterNullKeyMap() throws Exception {
        // The allocation of the last per-worker map fails after the map of the NULL key and the other
        // per-worker maps succeeded: the factory frees them all, which assertMemoryLeak checks, and
        // the query compiles and runs once the allocation succeeds.
        final int workerCount = 4;
        final AtomicInteger allocations = new AtomicInteger();
        final AtomicBoolean isFailing = new AtomicBoolean(true);
        configOverrideRostiAllocFacade(new RostiAllocFacadeImpl() {
            @Override
            public long alloc(ColumnTypes types, long capacity) {
                if (isFailing.get() && allocations.incrementAndGet() == workerCount) {
                    // Rosti allocations fail from here on.
                    Rosti.enableOOMOnMalloc();
                }
                return super.alloc(types, capacity);
            }
        });
        assertMemoryLeak(() -> {
            createColumnTopTable();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, workerCount)
                    .with(securityContext, bindVariableService, null, -1, circuitBreaker)) {
                try {
                    engine.select(COLUMN_TOP_SQL, context).close();
                    Assert.fail("expected the allocation of the last per-worker map to fail");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not allocate rosti hash table");
                } finally {
                    Rosti.disableOOMOnMalloc();
                }
                Assert.assertEquals(workerCount, allocations.get());
                isFailing.set(false);
                assertColumnTopResult(context);
            }
        });
    }

    @Test
    public void testSumAndAvgOfIntPastIntRange() throws Exception {
        // The keyed natives sum INT values into a LONG. The column top rows' sum, 6_000_000_000, merges
        // into the NULL key whole, onto the explicit NULL key's 2_000_000_000 and without it.
        assertMemoryLeak(() -> {
            for (boolean hasExplicitNullKey : new boolean[]{true, false}) {
                execute("CREATE TABLE t (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("""
                        INSERT INTO t VALUES
                            (2_000_000_000, '2024-01-01T00:00:00Z'),
                            (2_000_000_000, '2024-01-01T01:00:00Z'),
                            (2_000_000_000, '2024-01-01T02:00:00Z')
                        """);
                execute("ALTER TABLE t ADD COLUMN k INT");
                execute("INSERT INTO t (i, ts, k) VALUES (5, '2024-01-02T00:00:00Z', 1)");
                if (hasExplicitNullKey) {
                    execute("INSERT INTO t (i, ts, k) VALUES (2_000_000_000, '2024-01-02T01:00:00Z', NULL)");
                }
                // The NULL key: 3 or 4 rows of 2_000_000_000, whose average is 2_000_000_000.
                final String nullKeySum = hasExplicitNullKey ? "8000000000" : "6000000000";
                final String nullKeyCount = hasExplicitNullKey ? "4" : "3";
                assertVectorized("SELECT k, sum(i) s, count(i) c FROM t ORDER BY k", "k\ts\tc\n"
                        + "null\t" + nullKeySum + '\t' + nullKeyCount + '\n'
                        + "1\t5\t1\n");
                assertVectorized("SELECT k, sum(i) s FROM t ORDER BY k", "k\ts\n"
                        + "null\t" + nullKeySum + '\n'
                        + "1\t5\n");
                assertVectorized("SELECT k, avg(i) a FROM t ORDER BY k", """
                        k\ta
                        null\t2.0E9
                        1\t5.0
                        """);
                execute("DROP TABLE t");
            }
        });
    }

    private static void createColumnTopTable() throws Exception {
        execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t SELECT x, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(10)");
        execute("ALTER TABLE t ADD COLUMN k INT");
        execute("INSERT INTO t (v, ts, k) VALUES (100, '2024-01-02T00:00:00Z', 1)");
    }

    private static GroupByRecordCursorFactory findGroupBy(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof GroupByRecordCursorFactory groupBy) {
                return groupBy;
            }
        }
        throw new AssertionError("no vectorized GROUP BY in the plan");
    }

    private void assertColumnTopResult(SqlExecutionContext context) throws Exception {
        // The NULL key: v 1 to 10, sum 55.
        try (RecordCursorFactory factory = engine.select(COLUMN_TOP_SQL, context)) {
            assertCursor(factory, context, """
                    k\ts
                    null\t55
                    1\t100
                    """);
        }
    }

    private void assertCursor(RecordCursorFactory factory, String expected) throws Exception {
        assertCursor(factory, sqlExecutionContext, expected);
    }

    private void assertCursor(RecordCursorFactory factory, SqlExecutionContext context, String expected) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
            TestUtils.assertEquals(expected, sink);
        }
    }

    private void assertNullKeyAvg(String columnTopValues, String explicitNullKeyValues, String expectedAvg) throws Exception {
        execute("CREATE TABLE t (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        final StringBuilder insert = new StringBuilder("INSERT INTO t VALUES ");
        for (String value : columnTopValues.split(", ")) {
            insert.append("(").append(value).append(", '2024-01-01T00:00:00Z'),");
        }
        execute(insert.substring(0, insert.length() - 1));
        execute("ALTER TABLE t ADD COLUMN k INT");
        insert.setLength(0);
        insert.append("INSERT INTO t (l, ts, k) VALUES (1, '2024-01-02T00:00:00Z', 1)");
        if (explicitNullKeyValues != null) {
            for (String value : explicitNullKeyValues.split(", ")) {
                insert.append(", (").append(value).append(", '2024-01-02T01:00:00Z', NULL)");
            }
        }
        execute(insert);
        assertVectorized("SELECT k, avg(l) a FROM t ORDER BY k", "k\ta\n"
                + "null\t" + expectedAvg + '\n'
                + "1\t1.0\n");
        execute("DROP TABLE t");
    }

    private void assertVectorized(String sql, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("GroupBy vectorized: true")
                .returns(expected);
    }
}
