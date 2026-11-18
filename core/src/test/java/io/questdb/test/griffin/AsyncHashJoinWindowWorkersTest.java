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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.IntList;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The probe frames and the dispatch window of a parallel hash join on shared query workers that run
 * on threads and on fibers: the first window under a LIMIT, the fan-out-aware frames and the
 * dynamic window of the tuple ring, whose entries frames in flight never share, which the ring
 * asserts. Once the consumer takes a frame, the join dispatches the frames after it up to the
 * window, so the workers join them while the consumer reads, rather than once the consumer drained
 * the window. Every query compares with the ordinary plan, in order.
 * <p>
 * Most tests wait, before the consumer reads the first row of a frame, until the workers joined
 * every frame dispatched so far: the consumer then finds each frame ready, as when the workers keep
 * up with it, and the frames dispatched at each frame follow from the window alone.
 */
public class AsyncHashJoinWindowWorkersTest extends AbstractCairoTest {
    // t8's frames of eight probe rows, each of which meets d3's three rows of its key, every one of
    // which the residual keeps.
    private static final int FRAME_JOINED_ROWS = 24;
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinWindowWorkersTest.class);
    // A walking join over t8's eight frames.
    private static final String WALK = "SELECT t8.v, d3.w FROM t8 LEFT JOIN d3 ON t8.k = d3.k AND d3.w > t8.v";
    // A walking join over t's frames of 64 rows, whose probe rows hold v from 64 * f + 1 in the first
    // partition's frame f.
    private static final String WALK_T = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v";
    private static final int WORKER_COUNT = 4;

    @Test
    public void testCancelledWithFramesAheadOnFiberWorkers() throws Exception {
        assertFrameOneHeldWithFramesAhead(WorkerPoolMode.FIBER_HOST, HeldFrameAction.CANCEL);
    }

    @Test
    public void testCancelledWithFramesAheadOnThreadWorkers() throws Exception {
        assertFrameOneHeldWithFramesAhead(WorkerPoolMode.LEGACY, HeldFrameAction.CANCEL);
    }

    @Test
    public void testClosedWithFramesAheadOnFiberWorkers() throws Exception {
        assertFrameOneHeldWithFramesAhead(WorkerPoolMode.FIBER_HOST, HeldFrameAction.CLOSE);
    }

    @Test
    public void testClosedWithFramesAheadOnThreadWorkers() throws Exception {
        assertFrameOneHeldWithFramesAhead(WorkerPoolMode.LEGACY, HeldFrameAction.CLOSE);
    }

    @Test
    public void testCountAfterRowsMeetsTheFramesAheadOnFiberWorkers() throws Exception {
        assertCountAfterRowsMeetsTheFramesAhead(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCountAfterRowsMeetsTheFramesAheadOnThreadWorkers() throws Exception {
        assertCountAfterRowsMeetsTheFramesAhead(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFramesAheadInSqlContextsOnFiberWorkers() throws Exception {
        assertFramesAheadInSqlContexts(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFramesAheadInSqlContextsOnThreadWorkers() throws Exception {
        assertFramesAheadInSqlContexts(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testHotKeyOverrunsTheCapWhileFramesRunAheadOnFiberWorkers() throws Exception {
        assertHotKeyOverrunsTheCap(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testHotKeyOverrunsTheCapWhileFramesRunAheadOnThreadWorkers() throws Exception {
        assertHotKeyOverrunsTheCap(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testMemoryLimitsWithFramesAheadOnFiberWorkers() throws Exception {
        assertMemoryLimitsWithFramesAhead(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testMemoryLimitsWithFramesAheadOnThreadWorkers() throws Exception {
        assertMemoryLimitsWithFramesAhead(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testWindowStaysFullAsTheConsumerReadsOnFiberWorkers() throws Exception {
        assertWindowStaysFullAsTheConsumerReads(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testWindowStaysFullAsTheConsumerReadsOnThreadWorkers() throws Exception {
        assertWindowStaysFullAsTheConsumerReads(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testWindowStaysFullUnderTheLimitsFirstWindowOnFiberWorkers() throws Exception {
        assertWindowStaysFullUnderTheLimitsFirstWindow(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testWindowStaysFullUnderTheLimitsFirstWindowOnThreadWorkers() throws Exception {
        assertWindowStaysFullUnderTheLimitsFirstWindow(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testWindowsOnFiberWorkers() throws Exception {
        assertWindows(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testWindowsOnThreadWorkers() throws Exception {
        assertWindows(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testWorkersJoinAheadWhileTheConsumerReadsOnFiberWorkers() throws Exception {
        assertFrameOneHeldWithFramesAhead(WorkerPoolMode.FIBER_HOST, HeldFrameAction.READ_ON);
    }

    @Test
    public void testWorkersJoinAheadWhileTheConsumerReadsOnThreadWorkers() throws Exception {
        assertFrameOneHeldWithFramesAhead(WorkerPoolMode.LEGACY, HeldFrameAction.READ_ON);
    }

    private static void assertCountAfterRowsMeetsTheFramesAhead(WorkerPoolMode mode) throws Exception {
        withEightFrames(mode, (db, context) -> {
            final String expected = ordinaryRows(db, WALK, context);
            final long rows = countLines(expected);
            try (RecordCursorFactory factory = db.select(WALK, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final IntList dispatched = new IntList();
                final StringSink sink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(context)) {
                    // The consumer holds frame 2, and frames 3 and 4 went out with tuples, ahead of it.
                    readFramesWhenReady(cursor, factory.getMetadata(), join, 2, dispatched, sink);
                    TestUtils.assertEquals("[3,4,5]", dispatched.toString());
                    final long readRows = 2 * FRAME_JOINED_ROWS + 1;
                    Assert.assertEquals(readRows, countLines(sink));
                    // The count reads the tuples of frame 2 and of the two frames ahead of it, and
                    // dispatches count-only tasks for the three frames left.
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(context.getCircuitBreaker(), counter);
                    Assert.assertEquals(rows - readRows, counter.get());
                    Assert.assertEquals(3, join.getCountedTupleFrameCount());
                    Assert.assertEquals(3, join.getCountOnlyFrameCount());
                    // toTop() reads every row again.
                    cursor.toTop();
                    TestUtils.assertEquals(expected, printRows(cursor, factory.getMetadata()));
                }
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
            }
        });
    }

    // Runs the query on the operator and compares it with the ordinary plan, and checks the probe
    // frames of an execution and the frames dispatched once it read its first row, or found none.
    private static void assertDispatchedAtFirstRow(
            CairoEngine db,
            SqlExecutionContextImpl context,
            String sql,
            boolean hasRows,
            int probeFrames,
            int dispatchedFrames
    ) throws SqlException {
        assertParallelHashJoin(sql, context);
        try (RecordCursorFactory factory = db.select(sql, context)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(context)) {
                Assert.assertEquals(sql, hasRows, cursor.hasNext());
                Assert.assertEquals(sql, probeFrames, join.getProbeFrameCount());
                Assert.assertEquals(sql, dispatchedFrames, join.getDispatchedFrameCount());
            }
            Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        }
    }

    // Holds the consumer on frame 1 of t's frames of 64 rows once the frame after the window that
    // the first row dispatched went out too, and the workers joined it, while the consumer holds
    // frame 1; then reads on, cancels the query and reads on, or closes the cursor. Each leaves the
    // workers' slots and the readers free, and the factory runs again.
    private static void assertFrameOneHeldWithFramesAhead(WorkerPoolMode mode, HeldFrameAction action) throws Exception {
        // A window of three frames, which the dispatch limit sets and the ring's default budget keeps.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            createTables(db, context);
            context.changePageFrameSizes(64, 64);
            final String expected = ordinaryRows(db, WALK_T, context);
            context.setParallelHashJoinEnabled(true);
            final SqlExecutionCircuitBreaker previous = context.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(db, 0);
            context.with(breaker);
            try (RecordCursorFactory factory = db.select(WALK_T, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final RecordMetadata metadata = factory.getMetadata();
                final StringSink sink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(context)) {
                    final Record record = cursor.getRecord();
                    // The first row dispatched the first window: frames 0, 1 and 2.
                    Assert.assertTrue(cursor.hasNext());
                    CursorPrinter.println(record, metadata, sink);
                    Assert.assertEquals(3, join.getDispatchedFrameCount());
                    awaitReducedFrames(join);
                    // Frame 1's first row: the consumer took frame 1, which was ready, and frame 3
                    // went out, ...
                    while (cursor.hasNext()) {
                        CursorPrinter.println(record, metadata, sink);
                        if (record.getLong(0) == 65) {
                            break;
                        }
                    }
                    Assert.assertEquals(65, record.getLong(0));
                    Assert.assertEquals(4, join.getDispatchedFrameCount());
                    // ... which the workers join while the consumer holds frame 1.
                    awaitReducedFrames(join);
                    Assert.assertEquals(4, join.getReducedFrameCount());
                    switch (action) {
                        case READ_ON -> {
                            while (cursor.hasNext()) {
                                CursorPrinter.println(record, metadata, sink);
                            }
                            TestUtils.assertEquals(expected, sink);
                        }
                        case CANCEL -> {
                            breaker.cancel();
                            final CairoException e = Assert.assertThrows(CairoException.class, () -> {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                }
                            });
                            Assert.assertTrue(e.isCancellation());
                            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                        }
                        case CLOSE -> {
                            // The cursor closes with frames 2 and 3 joined and unread.
                        }
                    }
                }
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertEquals(0, db.getBusyReaderCount());
                breaker.reset();
                TestUtils.assertEquals(expected, printRows(factory, context));
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
            } finally {
                context.with(previous);
            }
        }));
    }

    private static void assertFramesAheadInSqlContexts(WorkerPoolMode mode) throws Exception {
        withEightFrames(mode, (db, context) -> {
            db.execute("CREATE TABLE e8 (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
            db.execute("CREATE TABLE t1 AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(5)) TIMESTAMP(ts) PARTITION BY DAY", context);
            db.execute("CREATE TABLE t2 AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(16)) TIMESTAMP(ts) PARTITION BY DAY", context);
            db.execute("CREATE TABLE ed (k INT, w LONG)", context);
            // dm's keys match no probe row.
            db.execute("CREATE TABLE dm AS (SELECT (1_000 + x % 4)::INT k, x * 100 w FROM long_sequence(12))", context);
            final String walk5 = "SELECT t8.v, d3.w FROM t8 JOIN d3 ON t8.k = d3.k WHERE d3.w > t8.v LIMIT 5";
            for (String sql : new String[]{
                    // A negative LIMIT reads every frame.
                    WALK + " LIMIT -5",
                    // A CROSS JOIN reads its slave from the top for each master row, and each read
                    // dispatches the LIMIT's first window again, as a sub-query and as a CTE.
                    "SELECT a.x, j.v, j.w FROM long_sequence(3) a CROSS JOIN (" + walk5 + ") j",
                    "WITH j AS (" + walk5 + ") SELECT a.x, j.v, j.w FROM long_sequence(3) a CROSS JOIN j",
                    // Two walking joins, one after the other.
                    WALK + " UNION ALL SELECT t8.v, d3.w FROM t8 JOIN d3 ON t8.k = d3.k WHERE d3.w + t8.v > 0",
            }) {
                assertParallelHashJoin(sql, context);
            }
            // An empty probe has no frame to dispatch, and an INNER join over an empty build needs no
            // probe at all.
            assertDispatchedAtFirstRow(db, context, "SELECT e8.v, d3.w FROM e8 LEFT JOIN d3 ON e8.k = d3.k AND d3.w > e8.v", false, 0, 0);
            assertDispatchedAtFirstRow(db, context, "SELECT t8.v, ed.w FROM t8 JOIN ed ON t8.k = ed.k WHERE ed.w > t8.v", false, 0, 0);
            // One frame, and two, fewer than the window: the first row's dispatch takes every frame,
            // and none is left to go out ahead.
            assertDispatchedAtFirstRow(db, context, "SELECT t1.v, d3.w FROM t1 LEFT JOIN d3 ON t1.k = d3.k AND d3.w > t1.v", true, 1, 1);
            assertDispatchedAtFirstRow(db, context, "SELECT t2.v, d3.w FROM t2 LEFT JOIN d3 ON t2.k = d3.k AND d3.w > t2.v", true, 2, 2);
            // Every probe row misses: the consumer collects each frame without a tuple, and reads
            // through all eight frames to find none.
            assertDispatchedAtFirstRow(db, context, "SELECT t8.v, dm.w FROM t8 JOIN dm ON t8.k = dm.k WHERE dm.w > t8.v", false, 8, 8);
        });
    }

    private static void assertHotKeyOverrunsTheCap(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            // The first probe row's key matches fifty build rows, which the residual keeps: the
            // first frame's task writes them all, past the cap of the frame's four rows, and stops
            // after that row, and the consumer joins the rest of the frame on the owner's slot in
            // two chunks of the same cap, rows 2 and 3, then row 4, while the workers join the
            // frames ahead. Every other probe row meets two rows of d: each later frame's task
            // stops after two rows, and one chunk joins the other two.
            db.execute("CREATE TABLE t AS (SELECT CASE WHEN x = 1 THEN 7 ELSE (x % 3)::INT END k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(48)) TIMESTAMP(ts) PARTITION BY HOUR", context);
            db.execute("CREATE TABLE d AS (SELECT CASE WHEN x <= 50 THEN 7 ELSE (x % 3)::INT END k, x w FROM long_sequence(56))", context);
            context.changePageFrameSizes(4, 4);
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 1_000";
            final String expected = ordinary(sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int execution = 0; execution < 2; execution++) {
                    Assert.assertEquals(expected, result(factory, context));
                    Assert.assertEquals(2 + 11, join.getAtom().getContinuationCount());
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                }
                try (RecordCursor cursor = factory.getCursor(context)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.assertEquals(12, join.getProbeFrameCount());
                }
            }
        }));
    }

    private static void assertMemoryLimitsWithFramesAhead(WorkerPoolMode mode) throws Exception {
        withEightFrames(mode, (db, context) -> {
            final String expected = ordinaryRows(db, WALK, context);
            try (RecordCursorFactory factory = db.select(WALK, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Limits from a few bytes up fail the query at each allocation in turn, the workers'
                // ones ahead of the consumer among them; each failure leaves nothing, and the first
                // limit that admits the query reads the rows.
                final int failures = assertMemoryLimitSweep(
                        expected,
                        limit -> setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit),
                        () -> printRows(factory, context),
                        null,
                        () -> {
                            Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
                            Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(0, db.getBusyReaderCount());
                        }
                );
                Assert.assertTrue(failures > 0);
            }
        });
    }

    private static void assertWindowStaysFullAsTheConsumerReads(WorkerPoolMode mode) throws Exception {
        withEightFrames(mode, (db, context) -> {
            final String expected = ordinaryRows(db, WALK, context);
            try (RecordCursorFactory factory = db.select(WALK, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final IntList dispatched = new IntList();
                final StringSink sink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(context)) {
                    // Each frame the consumer takes sends the frame three past it out, so that three
                    // frames stay in flight, the one the consumer reads included.
                    readFramesWhenReady(cursor, factory.getMetadata(), join, 2, dispatched, sink);
                    TestUtils.assertEquals("[3,4,5]", dispatched.toString());
                    // toTop() in the middle of frame 2 starts over from the first window.
                    cursor.toTop();
                    dispatched.clear();
                    sink.clear();
                    readFramesWhenReady(cursor, factory.getMetadata(), join, Integer.MAX_VALUE, dispatched, sink);
                    TestUtils.assertEquals("[3,4,5,6,7,8,8,8]", dispatched.toString());
                    TestUtils.assertEquals(expected, sink);
                }
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                // The cached factory runs again.
                TestUtils.assertEquals(expected, printRows(factory, context));
            }
        });
    }

    private static void assertWindowStaysFullUnderTheLimitsFirstWindow(WorkerPoolMode mode) throws Exception {
        withEightFrames(mode, (db, context) -> {
            // LIMIT 20 needs the first two frames' 16 probe rows and four of frame 2's: the first
            // window holds frames 0 to 2, and frame 2's task joins four rows. Up to frame 2 the
            // frames go out within the first window, and from frame 3 on the window holds three
            // frames again.
            final String sql = WALK + " LIMIT 20";
            final String all = ordinaryJoinRows(db, sql, context);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertEquals(ordinaryRows(db, sql, context), printRows(factory, context));
                final IntList dispatched = new IntList();
                final StringSink sink = new StringSink();
                // The join's own cursor returns every row: the LIMIT stays above it.
                try (RecordCursor cursor = join.getCursor(context)) {
                    readFramesWhenReady(cursor, join.getMetadata(), join, Integer.MAX_VALUE, dispatched, sink);
                    TestUtils.assertEquals("[3,3,3,6,7,8,8,8]", dispatched.toString());
                    TestUtils.assertEquals(all, sink);
                    // The consumer joined the rest of frame 2 itself.
                    Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
                }
                Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
            }
        });
    }

    private static void assertWindows(WorkerPoolMode mode) throws Exception {
        // A ring budget of a frame of every row of the two-stage stars: the window starts at one
        // frame, and widens once the selective frames, the first half, show their few tuples.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 256 * 3 * Long.BYTES);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 512);
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            createTables(db, context);
            context.changePageFrameSizes(1, 256);
            for (String sql : new String[]{
                    // Sparse frames, then dense ones.
                    "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k WHERE t.v % 64 = 0 OR t.v > 5_000",
                    "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v WHERE t.v % 64 = 0 OR t.v > 5_000",
                    // Fan-out under a residual and a post-join filter: frames of 512 / 4 rows.
                    "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v",
                    "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k JOIN d ON t.k = d.k WHERE d.w + u.w > t.v",
                    // LIMITs: the first window, and LIMITs that it leaves as they are.
                    "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v LIMIT 3",
                    "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k LIMIT 700",
                    "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k ORDER BY ts DESC LIMIT 5",
                    "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v LIMIT -7",
            }) {
                final String expected = ordinary(sql, context);
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    for (int execution = 0; execution < 2; execution++) {
                        Assert.assertEquals(sql, expected, result(factory, context));
                        Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                    if (sql.endsWith("LIMIT 3")) {
                        // The first frame's task alone went out before the first row, and joined
                        // three rows, the workers' tasks included.
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                            Assert.assertEquals(sql, 3, join.getAtom().getJoinedProbeRowCount());
                        }
                    } else if (!sql.contains("LIMIT")) {
                        // The fan-out-aware frames need no continuation.
                        Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                    }
                } finally {
                    context.setParallelHashJoinEnabled(false);
                }
            }
        }));
    }

    // Waits until the workers joined every probe frame dispatched so far.
    private static void awaitReducedFrames(AsyncHashJoinRecordCursorFactory join) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (join.getReducedFrameCount() < join.getDispatchedFrameCount()) {
            Assert.assertTrue("the workers did not join the frames dispatched", System.nanoTime() < deadline);
            Os.pause();
        }
    }

    private static long countLines(CharSequence text) {
        long lines = 0;
        for (int i = 0, n = text.length(); i < n; i++) {
            if (text.charAt(i) == '\n') {
                lines++;
            }
        }
        return lines;
    }

    private static void createTables(CairoEngine db, SqlExecutionContextImpl context) throws Exception {
        db.execute("""
                CREATE TABLE t AS (
                    SELECT (x % 50)::INT k, x v, timestamp_sequence('2024-01-01', 10_000_000) ts
                    FROM long_sequence(10_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """, context);
        db.execute("CREATE TABLE u AS (SELECT (x % 50)::INT k, x * 7 w FROM long_sequence(50))", context);
        // Four rows of d per key.
        db.execute("CREATE TABLE d AS (SELECT (x % 50)::INT k, x * 100 w FROM long_sequence(200))", context);
    }

    // The rows of the ordinary plan's join under the query, which the operator's own cursor returns
    // whatever the query's LIMIT.
    private static String ordinaryJoinRows(CairoEngine db, String sql, SqlExecutionContextImpl context) throws SqlException {
        final boolean isEnabledBefore = context.isParallelHashJoinEnabled();
        context.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = db.select(sql, context)) {
            RecordCursorFactory join = factory;
            while (join.getBaseFactory() != null) {
                join = join.getBaseFactory();
            }
            Assert.assertTrue(join.getClass().getName(), join.getClass().getSimpleName().contains("Join"));
            return printRows(join, context);
        } finally {
            context.setParallelHashJoinEnabled(isEnabledBefore);
        }
    }

    private static String ordinaryRows(CairoEngine db, String sql, SqlExecutionContextImpl context) throws SqlException {
        final boolean isEnabledBefore = context.isParallelHashJoinEnabled();
        context.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = db.select(sql, context)) {
            Assert.assertNull(sql, find(factory));
            return printRows(factory, context);
        } finally {
            context.setParallelHashJoinEnabled(isEnabledBefore);
        }
    }

    private static String printRows(RecordCursorFactory factory, SqlExecutionContextImpl context) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            return printRows(cursor, factory.getMetadata());
        }
    }

    private static String printRows(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            CursorPrinter.println(record, metadata, sink);
        }
        return sink.toString();
    }

    // Reads WALK's rows into the sink, through the first row of the given frame or to the end, and
    // before the first row of each frame after the first waits until the workers joined every frame
    // dispatched so far, so that the consumer finds the frame ready. Adds the frames dispatched once
    // the consumer read each frame's first row.
    private static void readFramesWhenReady(
            RecordCursor cursor,
            RecordMetadata metadata,
            AsyncHashJoinRecordCursorFactory join,
            int lastFrame,
            IntList dispatchedFrames,
            StringSink sink
    ) {
        final Record record = cursor.getRecord();
        for (int row = 0; ; row++) {
            final boolean isFrameStart = row % FRAME_JOINED_ROWS == 0;
            if (isFrameStart && row > 0) {
                awaitReducedFrames(join);
            }
            if (!cursor.hasNext()) {
                return;
            }
            CursorPrinter.println(record, metadata, sink);
            if (isFrameStart) {
                dispatchedFrames.add(join.getDispatchedFrameCount());
                if (row / FRAME_JOINED_ROWS == lastFrame) {
                    return;
                }
            }
        }
    }

    // Runs the code on a pool of workers in the given mode, with a window of three frames, over t8,
    // 64 probe rows of four keys in one partition, cut into eight frames of eight rows, and d3, three
    // rows of each key, whose w, 100 and up, passes every probe row's v, up to 64.
    private static void withEightFrames(WorkerPoolMode mode, WorkersCode code) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            db.execute("""
                    CREATE TABLE t8 AS (
                        SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                        FROM long_sequence(64)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """, context);
            db.execute("CREATE TABLE d3 AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))", context);
            context.changePageFrameSizes(8, 8);
            context.setParallelHashJoinEnabled(true);
            code.run(db, context);
        }));
    }

    private enum HeldFrameAction {
        CANCEL, CLOSE, READ_ON
    }
}
