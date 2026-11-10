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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * A parallel hash join under a LIMIT that the query states: the join takes the LIMIT, and an
 * execution under a positive one first dispatches only the leading probe frames whose rows reach
 * it, the last of them joining only the rows the LIMIT still needs; the consumer continues that
 * frame itself, in chunks that double, if it reads further, and only then opens the full window. The
 * rows always equal the ordinary plan's, in order: the LIMIT stays with its own factory. A NULL, zero
 * or negative LIMIT, or one with an upper bound, leaves the execution as it was.
 * <p>
 * The probe table t holds 64 rows in one partition, cut into frames of four rows: sixteen frames,
 * which a full window dispatches at once.
 */
public class AsyncHashJoinLimitTest extends AbstractCairoTest {
    private static final int FRAME_COUNT = 16;
    private static final int FRAME_ROWS = 4;
    // The joins of the stars: LEFT joins, deferred chains, residuals, and an INNER join that drops
    // the probe rows of key 0.
    private static final String[] STAR_JOINS = {
            " FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k",
            " FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 % 2 = 0 LEFT JOIN d3 ON t.k = d3.k",
            " FROM t JOIN d2 ON t.k = d2.k JOIN d1 ON t.k = d1.k",
            " FROM t JOIN d3 ON t.k = d3.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 > t.v",
    };
    // The stars, each projecting some of its columns.
    private static final String[] STARS = {
            "SELECT t.v, d1.w, d2.w2" + STAR_JOINS[0],
            "SELECT t.v, d1.w, d2.w2, d3.w3" + STAR_JOINS[1],
            "SELECT t.v, d2.w2, d1.w" + STAR_JOINS[2],
            "SELECT t.v, d3.w3, d2.w2" + STAR_JOINS[3],
    };

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
    public void testBindVariableLimitPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = STARS[0] + " LIMIT :lim";
            bindVariableService.setLong("lim", 1);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // One factory, executed under each value: the first window follows each execution's.
                final long[] limits = {2, 100, -2, Numbers.LONG_NULL, 5, 0};
                // The frames a first row needs: one for two rows, two for five; a LIMIT of more rows than
                // a full window holds, or none, dispatches a full window.
                final int[] firstWindows = {1, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, 2, FRAME_COUNT};
                for (int i = 0; i < limits.length; i++) {
                    bindVariableService.setLong("lim", limits[i]);
                    final String label = "LIMIT " + limits[i];
                    final String expected = ordinaryRows(sql);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                        cursor.toTop();
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                    }
                    // The join's own cursor, under the same LIMIT, returns its first row from the frames
                    // that the LIMIT needs, and continues the capped frame, if any, to read every row.
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(label, firstWindows[i], join.getDispatchedFrameCount());
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.assertEquals(label, firstWindows[i] < FRAME_COUNT, join.getAtom().getContinuationCount() > 0);
                    }
                }
            }
        });
    }

    @Test
    public void testCalculateSizeAndToTopInsideTheCappedFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                final String sql = star + " LIMIT 3";
                // The join's own rows, which the ordinary plan's outermost join returns.
                final String all = ordinaryJoinRows(sql);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    // The join's own cursor returns every row: the LIMIT stays above it, and narrows only
                    // the first window, whose capped frame a count after the first row counts to its end.
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(star, 1, join.getDispatchedFrameCount());
                        Assert.assertTrue(star, join.getAtom().getRing().getResumePosition(0) > 0);
                        Assert.assertEquals(star, countLines(all) - 1, size(cursor));
                        Assert.assertTrue(star, join.getCountedContinuationCount() > 0);
                        cursor.toTop();
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(star, 1, join.getDispatchedFrameCount());
                        cursor.toTop();
                        Assert.assertEquals(star, all, printRows(cursor, join.getMetadata()));
                    }
                }
            }
        });
    }

    @Test
    public void testCancelledInsideTheCappedFrameAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = STARS[0] + " LIMIT 3";
            final String all = ordinaryJoinRows(sql);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                // The join's own cursor reads on past the LIMIT's rows, into the capped frame's
                // continuation, which the cancellation keeps from running.
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < 3; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    breaker.cancel();
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                }
                Assert.assertEquals(0, join.getAtom().getContinuationCount());
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                breaker.reset();
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(all, printRows(cursor, join.getMetadata()));
                }
                Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testInvalidLimitFailsAsTheOrdinaryPlanDoes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String limit : new String[]{" LIMIT 'abc'", " LIMIT true", " LIMIT 1.5", " LIMIT now()", " LIMIT k"}) {
                final String sql = STARS[0] + limit;
                // The ordinary plan's error is the oracle: the join reads the same LIMIT before the
                // LIMIT's own factory does, and fails as that one would, leaking nothing.
                sqlExecutionContext.setParallelHashJoinEnabled(false);
                final SqlException expected = Assert.assertThrows(sql, SqlException.class, () -> select(sql).close());
                sqlExecutionContext.setParallelHashJoinEnabled(true);
                final SqlException actual = Assert.assertThrows(sql, SqlException.class, () -> select(sql).close());
                Assert.assertEquals(sql, expected.getPosition(), actual.getPosition());
                Assert.assertEquals(sql, expected.getFlyweightMessage().toString(), actual.getFlyweightMessage().toString());
            }
            // The tables still serve the join.
            assertParallelHashJoin(STARS[0] + " LIMIT 3", sqlExecutionContext);
        });
    }

    @Test
    public void testLimitDispatchesTheFirstWindowOnly() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                // Without a LIMIT the first row comes with a full window of frames in flight.
                try (RecordCursorFactory factory = select(star)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(star, FRAME_COUNT, join.getDispatchedFrameCount());
                        Assert.assertEquals(star, FRAME_COUNT * FRAME_ROWS, join.getAtom().getJoinedProbeRowCount());
                    }
                }
                // LIMIT 3 dispatches the first frame alone, whose task joins three of its rows.
                for (int limit : new int[]{1, 3, FRAME_ROWS}) {
                    final String sql = star + " LIMIT " + limit;
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                            Assert.assertEquals(sql, limit, join.getAtom().getJoinedProbeRowCount());
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            // The rows the LIMIT returns came from the first frame's capped task.
                            Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                            Assert.assertEquals(sql, limit, join.getAtom().getJoinedProbeRowCount());
                            Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLimitOverSeveralFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                // Ten rows take the first two frames whole and two rows of the third.
                final String sql = star + " LIMIT 10";
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(sql, 3, join.getDispatchedFrameCount());
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.assertEquals(sql, 3, join.getDispatchedFrameCount());
                        // The owner joins a frame when it reads it, so the frames it did not need to
                        // read joined none of their rows.
                        Assert.assertTrue(sql, join.getAtom().getJoinedProbeRowCount() <= 10);
                    }
                }
                // A first window wider than the ring's leaves the execution as it was: no task stops
                // short of its frame's end, which the join's own cursor, reading every row, shows.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(sql, 2, join.getDispatchedFrameCount());
                    }
                    Assert.assertEquals(sql, ordinaryRows(sql), printRows(factory));
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(sql, ordinaryJoinRows(sql), printRows(cursor, join.getMetadata()));
                    }
                    Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                } finally {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, null);
                }
            }
        });
    }

    @Test
    public void testLimitOverAProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE m AS (SELECT 3 k, 33 w FROM long_sequence(1))");
            // A filter of the left input in a sub-query, which the join steals, keeps two rows of each
            // frame; the LIMIT caps the rows that the filter keeps.
            final String even = "(SELECT * FROM t WHERE v % 2 = 0) t";
            final String odd = "(SELECT * FROM t WHERE v % 2 = 1) t";
            for (boolean isCopy : new boolean[]{true, false}) {
                AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(isCopy);
                try {
                    // One kept row of the first frame yields the two rows of its chain.
                    final String sql = "SELECT t.v, d1.w, d2.w2 FROM " + even + " LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k LIMIT 1";
                    TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "filter: v%2=0");
                    assertFirstRow(sql, 1, 1, 0, 1);
                    // The first kept row, of key 1, misses m; the chunk of one more, of key 3, meets it.
                    final String inner = "SELECT t.v, m.w FROM " + odd + " JOIN m ON t.k = m.k LIMIT 1";
                    assertParallelHashJoin(inner, sqlExecutionContext);
                    assertFirstRow(inner, 1, 2, 1, 1);
                    for (int limit : new int[]{1, 2, 3, 5, 9, 31, 32, 33}) {
                        assertParallelHashJoin("SELECT t.v, d1.w, d2.w2 FROM " + odd + " LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 > t.v LIMIT " + limit, sqlExecutionContext);
                        assertParallelHashJoin("SELECT t.v, m.w FROM " + even + " JOIN m ON t.k = m.k LIMIT " + limit, sqlExecutionContext);
                    }
                } finally {
                    AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
                }
            }
        });
    }

    @Test
    public void testLimitReadsOnThroughTheContinuation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Only t's rows of key 3, every fourth one from the third on, meet m's one row.
            execute("CREATE TABLE m AS (SELECT 3 k, 33 w FROM long_sequence(1))");
            execute("CREATE TABLE n AS (SELECT 3L v, 33 w FROM long_sequence(1))");
            final String inner = "SELECT t.v, m.w FROM t JOIN m ON t.k = m.k";
            // The optimiser gives the join the LIMIT under an ORDER BY of the designated timestamp that
            // names it as the table does.
            for (String order : new String[]{"", " ORDER BY ts DESC"}) {
                // LIMIT 1 caps the first frame's task at one row, which misses. The consumer goes on
                // with one row, then two: forward, rows 2 and 3, the third of which meets the row it
                // needs, four rows joined in all, by the task and two chunks; backward, the frame's
                // second row from its end does, two rows joined, by the task and one chunk.
                final String sql = inner + order + " LIMIT 1";
                assertParallelHashJoin(sql, sqlExecutionContext);
                if (order.isEmpty()) {
                    assertFirstRow(sql, 1, 4, 2, 1);
                } else {
                    assertFirstRow(sql, 1, 2, 1, 1);
                }
                // LIMIT 6 takes the first frame whole and caps the second at two rows; it reads on
                // through the second frame's continuation and then the full window.
                final String sql6 = inner + order + " LIMIT 6";
                assertParallelHashJoin(sql6, sqlExecutionContext);
                assertFirstRow(sql6, 2, FRAME_ROWS + 2, 0, 6);
                // Only t's third row meets n's. Forward, the capped task's two rows miss, and the
                // chunk of two more meets it; the LIMIT then reads on through the frames after the
                // first, none of which yields a row, to the end. Backward, the first frame yields
                // none, and the row comes from the last frame, of the full window.
                final String sparse = "SELECT t.v, n.w FROM t JOIN n ON t.v = n.v" + order + " LIMIT 2";
                assertParallelHashJoin(sparse, sqlExecutionContext);
                if (order.isEmpty()) {
                    assertFirstRow(sparse, 1, 4, 1, 1);
                } else {
                    assertFirstRow(sparse, FRAME_COUNT, FRAME_COUNT * FRAME_ROWS, 1, 1);
                }
            }
        });
    }

    @Test
    public void testNegativeZeroAndRangeLimitsUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                for (String limit : new String[]{" LIMIT -1", " LIMIT -3", " LIMIT -100", " LIMIT 0", " LIMIT 2, 5", " LIMIT -5, -2"}) {
                    final String sql = star + limit;
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        Assert.assertEquals(sql, ordinaryRows(sql), printRows(factory));
                        // No task stopped short of its frame's end, which a first window under a LIMIT of
                        // one or three rows would have made it do.
                        Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                        try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, FRAME_COUNT, join.getDispatchedFrameCount());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testParityUnderEachLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (boolean isCopy : new boolean[]{true, false}) {
                // The steppers' shared template, as on a JVM without hidden classes, too.
                AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(isCopy);
                try {
                    assertParityUnderEachLimit();
                } finally {
                    AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
                }
            }
        });
    }

    private static void assertFirstRow(String sql, int dispatched, long joinedRows, int continuations, int rows) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(sql, dispatched, join.getDispatchedFrameCount());
                Assert.assertEquals(sql, continuations, join.getAtom().getContinuationCount());
                if (rows == 1) {
                    Assert.assertEquals(sql, joinedRows, join.getAtom().getJoinedProbeRowCount());
                }
                int count = 1;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(sql, rows, count);
            }
        }
    }

    private static void assertParityUnderEachLimit() throws SqlException {
        for (String star : STARS) {
            for (String suffix : new String[]{
                    " LIMIT 1", " LIMIT 2", " LIMIT 5", " LIMIT 17", " LIMIT 63", " LIMIT 64", " LIMIT 65", " LIMIT 1000",
                    " ORDER BY t.ts DESC LIMIT 3", " ORDER BY ts DESC LIMIT 3", " ORDER BY ts DESC LIMIT 9", " ORDER BY ts LIMIT 7",
            }) {
                final String sql = star + suffix;
                assertParallelHashJoin(sql, sqlExecutionContext);
                // In a sub-query and in a CTE.
                assertParallelHashJoin("SELECT * FROM (" + sql + ")", sqlExecutionContext);
                assertParallelHashJoin("WITH q AS (" + sql + ") SELECT * FROM q", sqlExecutionContext);
            }
        }
    }

    private static int countLines(String rows) {
        int count = 0;
        for (int i = 0, n = rows.length(); i < n; i++) {
            if (rows.charAt(i) == '\n') {
                count++;
            }
        }
        return count;
    }

    private static void createTables() throws Exception {
        // 64 rows in one partition, cut into sixteen frames of four rows.
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(64)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        // d1 holds one row per key, d2 two and d3 three, but none for key 0.
        execute("CREATE TABLE d1 AS (SELECT (x % 4)::INT k, x * 10 w FROM long_sequence(4))");
        execute("CREATE TABLE d2 AS (SELECT (x % 4)::INT k, x * 100 w2 FROM long_sequence(8))");
        execute("CREATE TABLE d3 AS (SELECT (x % 3 + 1)::INT k, x * 1000 w3 FROM long_sequence(9))");
        sqlExecutionContext.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
    }

    // The rows of the ordinary plan's outermost join, which the LIMIT and the projections over it
    // leave out, as the parallel join's own cursor returns them.
    private static String ordinaryJoinRows(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql)) {
            RecordCursorFactory join = factory;
            while (join.getBaseFactory() != null) {
                join = join.getBaseFactory();
            }
            Assert.assertTrue(join.getClass().getName(), join.getClass().getSimpleName().contains("Join"));
            return printRows(join);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    private static String ordinaryRows(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql)) {
            return printRows(factory);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    private static String printRows(RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            return printRows(cursor, factory.getMetadata());
        }
    }

    private static String printRows(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        while (cursor.hasNext()) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
        return sink.toString();
    }

    private static long size(RecordCursor cursor) {
        final RecordCursor.Counter counter = new RecordCursor.Counter();
        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
        return counter.get();
    }
}
