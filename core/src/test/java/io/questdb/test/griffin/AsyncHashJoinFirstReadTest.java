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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * A parallel hash join whose rows are the query's own, under the query's projections alone, takes
 * the rows that the client reads first, see {@code SqlExecutionContext.getFirstReadRows()}, as it
 * takes a LIMIT: an execution first dispatches only the leading probe frames whose rows reach them,
 * the last of them joining only the rows still needed. A reader that has read those rows and asks
 * for more opens the full window at once, while the consumer continues the capped frame; a reader
 * that lacks some of them, which an INNER join dropped, continues the frame first. Any other query
 * over the join, under a sort, a window function or a LIMIT, never reads the hint, which leaves
 * its first window as the query's LIMIT, if any, makes it. The rows always equal the ordinary
 * plan's, in order.
 * <p>
 * The probe table t holds 64 rows in one partition, cut into frames of four rows: sixteen frames,
 * which a full window dispatches at once. Every row of t meets one row of d1.
 */
public class AsyncHashJoinFirstReadTest extends AbstractCairoTest {
    private static final int FRAME_COUNT = 16;
    private static final int FRAME_ROWS = 4;
    private static final int ROW_COUNT = FRAME_COUNT * FRAME_ROWS;
    // The join whose rows the query returns: under a projection of its columns, under one that
    // computes, and alone.
    private static final String[] SHAPES = {
            "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k",
            "SELECT t.v + 1 y, d1.w FROM t JOIN d1 ON t.k = d1.k",
            "SELECT * FROM t JOIN d1 ON t.k = d1.k",
    };
    // Stars: LEFT joins, chains of several matches, residuals, and INNER joins that drop rows.
    private static final String[] STARS = {
            "SELECT t.v, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k",
            "SELECT t.v, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 % 2 = 0 LEFT JOIN d3 ON t.k = d3.k",
            "SELECT t.v, d3.w3, d2.w2 FROM t JOIN d3 ON t.k = d3.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 > t.v",
            "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k WHERE d1.w > t.v % 30",
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownFirstRead() {
        sqlExecutionContext.setFirstReadRows(Long.MAX_VALUE);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCancelledAfterTheWindowOpened() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = SHAPES[0];
            final String expected = ordinaryRows(sql);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                // The reader reads past the three rows it reads first, which opens the window, and the
                // execution is then cancelled, inside the capped frame's continuation.
                try (RecordCursor cursor = openUnder(factory, 3)) {
                    for (int i = 0; i < 4; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    Assert.assertEquals(FRAME_COUNT, join.getDispatchedFrameCount());
                    breaker.cancel();
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                }
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                breaker.reset();
                // The factory serves the next execution, under the hint and without it.
                try (RecordCursor cursor = openUnder(factory, 3)) {
                    Assert.assertEquals(expected, printRows(cursor, factory.getMetadata()));
                }
                Assert.assertEquals(expected, printRows(factory));
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testChainRowsCountTowardTheFirstRead() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // m2 holds three rows of key 2 and one of key 3; the join has no residual, so the consumer
            // walks the chain of key 2's matches.
            execute("CREATE TABLE m2 AS (SELECT CASE WHEN x < 4 THEN 2 ELSE 3 END k, x * 10 w FROM long_sequence(4))");
            final String sql = "SELECT t.v, m2.w FROM t JOIN m2 ON t.k = m2.k";
            final String expected = ordinaryRows(sql);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertTrue(join.isClientRowSource());
                // Two rows read first cap the first frame's task at t's first two rows: v = 1, of key
                // 1, misses m2, and v = 2 meets the chain, whose three rows come from the first window.
                try (RecordCursor cursor = openUnder(factory, 2)) {
                    final Record record = cursor.getRecord();
                    final RecordMetadata metadata = factory.getMetadata();
                    final StringSink rows = new StringSink();
                    for (int i = 0; i < 3; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(2, record.getLong(0));
                        CursorPrinter.println(record, metadata, rows, true);
                        Assert.assertEquals(1, join.getDispatchedFrameCount());
                    }
                    Assert.assertEquals(0, join.getAtom().getContinuationCount());
                    // The chain's rows count as read, so the reader has read more than the two rows it
                    // reads first, and the next row, v = 3, of the capped frame's continuation, opens
                    // the full window.
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(3, record.getLong(0));
                    CursorPrinter.println(record, metadata, rows, true);
                    Assert.assertEquals(FRAME_COUNT, join.getDispatchedFrameCount());
                    Assert.assertEquals(1, join.getAtom().getContinuationCount());
                    while (cursor.hasNext()) {
                        CursorPrinter.println(record, metadata, rows, true);
                    }
                    TestUtils.assertEquals(expected, rows);
                }
            }
        });
    }

    @Test
    public void testContextThatServesNoClientHasNoFirstRead() {
        // A context that serves no client, here one that implements only what SqlExecutionContext
        // requires, reports no rows read first and ignores a protocol's hint.
        final SqlExecutionContext context = new SqlExecutionContextStub(engine);
        Assert.assertEquals(Long.MAX_VALUE, context.getFirstReadRows());
        context.setFirstReadRows(3);
        Assert.assertEquals(Long.MAX_VALUE, context.getFirstReadRows());
    }

    @Test
    public void testFirstReadNarrowsTheFirstWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The rows read first, and the frames of four rows that they need: a capped first frame,
            // the first frame whole, and the first frames whole and a capped last one.
            final long[] firstReads = {1, 3, FRAME_ROWS, 5, 10};
            final int[] firstWindows = {1, 1, 1, 2, 3};
            for (String sql : SHAPES) {
                final String expected = ordinaryRows(sql);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    Assert.assertTrue(sql, join.isClientRowSource());
                    for (int i = 0; i < firstReads.length; i++) {
                        final long firstRead = firstReads[i];
                        final String label = sql + ", first read " + firstRead;
                        try (RecordCursor cursor = openUnder(factory, firstRead)) {
                            Assert.assertEquals(label, firstRead, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                            final StringSink rows = new StringSink();
                            final RecordMetadata metadata = factory.getMetadata();
                            // The rows read first come from the first window alone, which joined them
                            // and no more.
                            for (int r = 0; r < firstRead; r++) {
                                Assert.assertTrue(label, cursor.hasNext());
                                CursorPrinter.println(cursor.getRecord(), metadata, rows, true);
                                Assert.assertEquals(label, firstWindows[i], join.getDispatchedFrameCount());
                            }
                            Assert.assertEquals(label, firstRead, join.getAtom().getJoinedProbeRowCount());
                            Assert.assertEquals(label, 0, join.getAtom().getContinuationCount());
                            // The next row opens the full window: the consumer continues the capped
                            // frame, if any, while the workers join the frames after it.
                            Assert.assertTrue(label, cursor.hasNext());
                            CursorPrinter.println(cursor.getRecord(), metadata, rows, true);
                            Assert.assertEquals(label, FRAME_COUNT, join.getDispatchedFrameCount());
                            Assert.assertEquals(label, firstRead % FRAME_ROWS != 0, join.getAtom().getContinuationCount() > 0);
                            while (cursor.hasNext()) {
                                CursorPrinter.println(cursor.getRecord(), metadata, rows, true);
                            }
                            TestUtils.assertEquals(label, expected, rows);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFirstReadOfTheStars() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (boolean isCopy : new boolean[]{true, false}) {
                // The stages' hidden copies from the first task, and the shared template, as on a JVM
                // without hidden classes.
                useSteppers(isCopy);
                try {
                    for (String star : STARS) {
                        final String expected = ordinaryRows(star);
                        try (RecordCursorFactory factory = select(star)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(star, join);
                            Assert.assertTrue(star, join.isClientRowSource());
                            for (long firstRead : new long[]{1, 2, 5, 17, ROW_COUNT - 1, ROW_COUNT, ROW_COUNT + 1}) {
                                try (RecordCursor cursor = openUnder(factory, firstRead)) {
                                    Assert.assertTrue(star, cursor.hasNext());
                                    // The first row needs the frames that the rows read first span.
                                    Assert.assertEquals(star, (int) Math.min(FRAME_COUNT, (firstRead + FRAME_ROWS - 1) / FRAME_ROWS), join.getDispatchedFrameCount());
                                    cursor.toTop();
                                    Assert.assertEquals(star + ", first read " + firstRead, expected, printRows(cursor, factory.getMetadata()));
                                    assertStepperRuns(join, isCopy, star);
                                }
                            }
                        }
                    }
                } finally {
                    restoreSteppers();
                }
            }
        });
    }

    @Test
    public void testFirstReadOnlyReachesTheJoinOfTheQueryRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String join = " FROM t JOIN d1 ON t.k = d1.k";
            // The join's rows are the query's own through sub-queries and CTEs of projections, and
            // in the designated timestamp's order, which the probe scan follows.
            final String[] marked = {
                    "SELECT * FROM (SELECT t.v, d1.w" + join + ")",
                    "WITH q AS (SELECT t.v, d1.w" + join + ") SELECT v + 1 y, w FROM q",
                    "SELECT t.ts, d1.w" + join + " ORDER BY t.ts",
                    "SELECT t.ts, d1.w" + join + " ORDER BY t.ts DESC",
            };
            for (String sql : marked) {
                final String expected = ordinaryRows(sql);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory asyncJoin = find(factory);
                    Assert.assertNotNull(sql, asyncJoin);
                    Assert.assertTrue(sql, asyncJoin.isClientRowSource());
                    try (RecordCursor cursor = openUnder(factory, 3)) {
                        Assert.assertEquals(sql, 3, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 1, asyncJoin.getDispatchedFrameCount());
                        cursor.toTop();
                        Assert.assertEquals(sql, expected, printRows(cursor, factory.getMetadata()));
                    }
                }
            }
            // A sort, a window function, a UNION ALL and a CROSS JOIN over the join read other
            // rows than the client reads, and a LIMIT bounds them itself: the join never reads the
            // hint, and its first window is the LIMIT's, if any.
            final String[] unmarked = {
                    "SELECT t.v, d1.w" + join + " ORDER BY d1.w, t.v",
                    "SELECT t.v, row_number() OVER () rn" + join,
                    "SELECT t.v, d1.w" + join + " UNION ALL SELECT t.v, d1.w" + join,
                    "SELECT q.v, c.x FROM (SELECT t.v" + join + ") q CROSS JOIN long_sequence(2) c",
                    "SELECT t.v, d1.w" + join + " LIMIT 5",
                    "SELECT * FROM (SELECT t.v, d1.w" + join + " LIMIT 9)",
                    "SELECT t.v, d1.w" + join + " LIMIT 100",
            };
            final long[] firstWindowRows = {Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 5, 9, 100};
            // A join in a scalar sub-query of the WHERE clause, whose cursor the filter's function
            // opens with the query's: the client reads the rows of the outer query, not the join's.
            final String subQuery = "SELECT v FROM t WHERE v = (SELECT t2.v FROM t t2 JOIN d1 ON t2.k = d1.k WHERE d1.w > t2.v + 35)";
            final String expectedSubQuery = ordinaryRows(subQuery);
            try (RecordCursorFactory factory = select(subQuery)) {
                Assert.assertNull(find(factory));
                try (RecordCursor cursor = openUnder(factory, 1)) {
                    Assert.assertEquals(subQuery, Long.MAX_VALUE, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                    Assert.assertEquals(subQuery, expectedSubQuery, printRows(cursor, factory.getMetadata()));
                }
            }
            for (int i = 0; i < unmarked.length; i++) {
                final String sql = unmarked[i];
                final String expected = ordinaryRows(sql);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory asyncJoin = find(factory);
                    if (asyncJoin != null) {
                        Assert.assertFalse(sql, asyncJoin.isClientRowSource());
                    }
                    for (long firstRead : new long[]{1, 1_000}) {
                        try (RecordCursor cursor = openUnder(factory, firstRead)) {
                            Assert.assertEquals(sql, firstWindowRows[i], AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                            Assert.assertEquals(sql, expected, printRows(cursor, factory.getMetadata()));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFirstReadPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = SHAPES[0];
            final String expected = ordinaryRows(sql);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // One factory, executed under each first read: none, zero and negative ones leave the
                // execution as it is, as do rows past those of every frame; all of them need every
                // frame.
                final long[] firstReads = {3, Long.MAX_VALUE, 10, 0, -5, Long.MIN_VALUE, ROW_COUNT, ROW_COUNT + 1, 1_000, 3};
                final long[] firstWindowRows = {3, Long.MAX_VALUE, 10, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, ROW_COUNT, ROW_COUNT + 1, 1_000, 3};
                final int[] firstWindows = {1, FRAME_COUNT, 3, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, 1};
                for (int i = 0; i < firstReads.length; i++) {
                    final String label = "first read " + firstReads[i];
                    try (RecordCursor cursor = openUnder(factory, firstReads[i])) {
                        Assert.assertEquals(label, firstWindowRows[i], AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                        Assert.assertTrue(label, cursor.hasNext());
                        Assert.assertEquals(label, firstWindows[i], join.getDispatchedFrameCount());
                        cursor.toTop();
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                    }
                    // An execution that the hint does not reach, as the protocol cleared it.
                    Assert.assertEquals(label, expected, printRows(factory));
                    Assert.assertEquals(label, Long.MAX_VALUE, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                }
            }
        });
    }

    @Test
    public void testFirstReadWithTasksCappedAtOneTuple() throws Exception {
        assertMemoryLeak(() -> {
            // A task stops once its tuples outnumber its frame's rows: each of t's rows of keys 1 to 3
            // meets three rows of d3 and two of d2, whose residual reads t, so every task leaves the
            // rest of its frame to the consumer's continuation, in the first window and past it.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            createTables();
            for (String sql : new String[]{STARS[2], STARS[2] + " WHERE d3.w3 > t.v"}) {
                final String expected = ordinaryRows(sql);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    for (long firstRead : new long[]{1, 3, 6}) {
                        final String label = sql + ", first read " + firstRead;
                        try (RecordCursor cursor = openUnder(factory, firstRead)) {
                            Assert.assertTrue(label, cursor.hasNext());
                            Assert.assertEquals(label, (int) ((firstRead + FRAME_ROWS - 1) / FRAME_ROWS), join.getDispatchedFrameCount());
                            cursor.toTop();
                            Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                            Assert.assertEquals(label, FRAME_COUNT, join.getDispatchedFrameCount());
                            Assert.assertTrue(label, join.getAtom().getContinuationCount() >= FRAME_COUNT);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLimitOpensTheWindowOnceReadPast() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The join's own cursor under LIMIT 3, as a test reads it, past the LIMIT's rows: the
            // fourth row opens the full window, as it does past the rows read first.
            final String sql = SHAPES[0] + " LIMIT 3";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertFalse(join.isClientRowSource());
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < 3; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1, join.getDispatchedFrameCount());
                    }
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(FRAME_COUNT, join.getDispatchedFrameCount());
                    int rows = 4;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(ROW_COUNT, rows);
                }
            }
        });
    }

    @Test
    public void testRowsThatTheJoinDropsContinueTheFrameFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Only t's rows of key 3, the third of each frame, meet m's one row.
            execute("CREATE TABLE m AS (SELECT 3 k, 33 w FROM long_sequence(1))");
            final String sql = "SELECT t.v, m.w FROM t JOIN m ON t.k = m.k";
            final String expected = ordinaryRows(sql);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertTrue(join.isClientRowSource());
                // [first read, probe rows joined for the first row, continuations]: one row read first
                // caps the task at one row, which misses, and the consumer goes on with one row, then
                // two, the third row meeting m's; two rows read first cap it at two, which miss, and
                // one chunk of two more meets it. The reader still lacks rows that the join dropped,
                // so the window stays narrow.
                final long[][] cases = {{1, 4, 2}, {2, 4, 1}};
                for (long[] c : cases) {
                    final String label = "first read " + c[0];
                    try (RecordCursor cursor = openUnder(factory, c[0])) {
                        Assert.assertTrue(label, cursor.hasNext());
                        Assert.assertEquals(label, 1, join.getDispatchedFrameCount());
                        Assert.assertEquals(label, c[1], join.getAtom().getJoinedProbeRowCount());
                        Assert.assertEquals(label, c[2], join.getAtom().getContinuationCount());
                        cursor.toTop();
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                    }
                }
            }
        });
    }

    @Test
    public void testToTopAndCountUnderTheFirstRead() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = SHAPES[0];
            final String expected = ordinaryRows(sql);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = openUnder(factory, 3)) {
                    // Two rows, then toTop(): the cursor reads the three rows first again, from the
                    // first frame alone, and the count after its first row counts the rest.
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.hasNext());
                    cursor.toTop();
                    for (int i = 0; i < 3; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1, join.getDispatchedFrameCount());
                    }
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(ROW_COUNT - 3, counter.get());
                    cursor.toTop();
                    Assert.assertEquals(expected, printRows(cursor, factory.getMetadata()));
                }
            }
        });
    }

    private static void createTables() throws Exception {
        // 64 rows in one partition, cut into sixteen frames of four rows.
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(64)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        // d1 holds one row of each of t's keys, d2 two, and d3 three of keys 1 to 3.
        execute("CREATE TABLE d1 AS (SELECT (x % 4)::INT k, x * 10 w FROM long_sequence(4))");
        execute("CREATE TABLE d2 AS (SELECT (x % 4)::INT k, x * 100 w2 FROM long_sequence(8))");
        execute("CREATE TABLE d3 AS (SELECT (x % 3 + 1)::INT k, x * 1000 w3 FROM long_sequence(9))");
        sqlExecutionContext.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
    }

    // Opens the factory's cursor as a protocol does for a client that reads the given rows first:
    // the hint reaches the getCursor() alone.
    private static RecordCursor openUnder(RecordCursorFactory factory, long firstReadRows) throws SqlException {
        sqlExecutionContext.setFirstReadRows(firstReadRows);
        try {
            return factory.getCursor(sqlExecutionContext);
        } finally {
            sqlExecutionContext.setFirstReadRows(Long.MAX_VALUE);
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
}
