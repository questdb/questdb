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
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
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
 * the window. The window ramps up: the first frame goes out alone, and each frame that the
 * consumer takes doubles the frames in flight, up to the ring's window. Two joins that one query
 * reads in lock-step, the master and the slave of an ASOF or LT join, keep frames in flight on one
 * reduce queue at once. Every query compares with the ordinary plan, in order.
 * <p>
 * Most tests wait, before the consumer reads the first row of a frame, until the workers joined
 * every frame dispatched so far: the consumer then finds each frame ready, as when the workers keep
 * up with it, and the frames dispatched at each frame follow from the window alone.
 */
public class AsyncHashJoinWindowWorkersTest extends AbstractCairoTest {
    // The entry of a task that wrote its cap of 512 tuples of the one-stage join, two narrow slots of
    // 4 bytes each.
    private static final long CAP_ENTRY_BYTES = 512 * 2 * Integer.BYTES;
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
    public void testFanOutAtServerDefaultsFitsAQueryMemoryLimitOnThreadWorkers() throws Exception {
        // The server's defaults on eight workers: a dispatch limit of eight, a reduce queue of 32,
        // page frames of 100_000 to 1_000_000 rows, probe frames of at most 100_000 rows, and a task
        // cap of a probe frame's 100_000 rows. b holds 1_000 rows of each of a's eight keys, every
        // pair of which the residual keeps: the cap holds 100_000 / 1_000 = 100 rows of a, fewer
        // than the floor of 1_000 rows, so a's 32_000 rows make 32 frames of 1_000 rows. A frame's
        // task stops at the cap, after 100 rows, and the consumer joins the rest of the frame in
        // nine chunks of 100 rows: 288 continuations. The cap grows a frame's entry of narrow
        // tuples, two slots of 4 bytes each, doubling from the frame's 1_000 tuples, to 128_000
        // tuples, 1_024_000 bytes. A query memory limit of 64 MiB leaves the ring a quarter, 16 MiB,
        // which holds twenty tasks' caps of tuples, 800_000 bytes each: the ring's first window holds
        // the dispatch limit's eight frames, of which the ramp sends two out by the first row,
        // frame 0 and the frame after it. A limit of 4 MiB leaves 1 MiB, one task's tuples: the
        // first window holds one frame, whose entry, with the build, fits the limit.
        final int workerCount = 8;
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, workerCount);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 32);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        assertMemoryLeak(() -> executeOnWorkers(WorkerPoolMode.LEGACY, workerCount, null, configuration, LOG, (db, context) -> {
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinPageFrameMaxRows());
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinTaskOutputMaxRows());
            db.execute("CREATE TABLE b AS (SELECT (x % 8)::INT k, 1_000_000_000L + x w FROM long_sequence(8_000))", context);
            db.execute("""
                    CREATE TABLE a AS (
                        SELECT (x % 8)::INT k, x v, timestamp_sequence('2024-01-01', 1_000_000) ts
                        FROM long_sequence(32_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """, context);
            context.setParallelHashJoinEnabled(true);
            final String sql = "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k AND b.w > a.v";
            try {
                for (long[] limitAndWindow : new long[][]{{64 * 1024 * 1024, 8, 2}, {4 * 1024 * 1024, 1, 1}}) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limitAndWindow[0]);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        long rows = 0;
                        long vSum = 0;
                        long wSum = 0;
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            final Record record = cursor.getRecord();
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(32, join.getProbeFrameCount());
                            Assert.assertEquals(1_000, join.getProbeFrameRowCount(0));
                            Assert.assertEquals(limitAndWindow[1], join.getAtom().getRing().getDispatchLimit());
                            Assert.assertEquals(limitAndWindow[2], join.getDispatchedFrameCount());
                            do {
                                rows++;
                                vSum += record.getLong(0);
                                wSum += record.getLong(1);
                            } while (cursor.hasNext());
                            // Every frame's task stopped at its cap, and nine chunks joined the rest.
                            Assert.assertEquals(32 * 9, join.getAtom().getContinuationCount());
                        }
                        // Each of a's rows, v from 1 to 32_000, meets b's 1_000 rows of its key, so the
                        // 4_000 rows of a of each key meet b's rows of that key, and between them all
                        // of b's rows, w from 1_000_000_001 to 1_000_008_000, each 4_000 times.
                        Assert.assertEquals(32_000_000L, rows);
                        Assert.assertEquals(1_000L * (32_000L * 32_001 / 2), vSum);
                        Assert.assertEquals(4_000L * (8_000L * 1_000_000_000L + 8_000L * 8_001 / 2), wSum);
                        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                }
                // With probe frames and a task cap of a page frame's 1_000_000 rows, the frames hold
                // 1_000_000 / 1_000 = 1_000 rows, whose tasks each write 1_000_000 tuples, an entry
                // of 8_192_000 bytes, which the 4 MiB limit does not hold.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 1_000_000);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        try {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected the query memory limit to fail the query");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        }
                    }
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                }
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
        }));
    }

    @Test
    public void testFanOutFirstWindowWithinTheBudgetOnFiberWorkers() throws Exception {
        assertFanOutFirstWindowWithinTheBudget(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFanOutFirstWindowWithinTheBudgetOnThreadWorkers() throws Exception {
        assertFanOutFirstWindowWithinTheBudget(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFirstWindowUnderAnOuterLimitOnFiberWorkers() throws Exception {
        assertFirstWindowUnderAnOuterLimit(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFirstWindowUnderAnOuterLimitOnThreadWorkers() throws Exception {
        assertFirstWindowUnderAnOuterLimit(WorkerPoolMode.LEGACY);
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
    public void testTwoJoinsReadInLockStepOnFiberWorkers() throws Exception {
        assertTwoJoinsReadInLockStep(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testTwoJoinsReadInLockStepOnThreadWorkers() throws Exception {
        assertTwoJoinsReadInLockStep(WorkerPoolMode.LEGACY);
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
                    TestUtils.assertEquals("[2,4,5]", dispatched.toString());
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

    private static void assertFanOutFirstWindowWithinTheBudget(WorkerPoolMode mode) throws Exception {
        // Each probe row of t meets d's four rows of its key, every pair of which the residual keeps,
        // so a cap of 512 tuples cuts t's frames to 512 / 4 = 128 rows, whose tasks each write the
        // cap, which grows their entries, doubling from the frame's room, to 4 KiB. A ring budget of
        // two such entries holds a first window of two frames, where the frames' rows, a quarter of
        // the tuples, would let the dispatch limit's eight in.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 512);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 2 * CAP_ENTRY_BYTES);
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            createTables(db, context);
            context.changePageFrameSizes(1, 256);
            final String sql = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w + t.v > 0";
            final String expected = ordinaryRows(db, sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinTupleRing ring = join.getAtom().getRing();
                final RecordMetadata metadata = factory.getMetadata();
                try (RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(ring.isNarrow());
                    Assert.assertEquals(2, ring.getDispatchLimit());
                    Assert.assertEquals(2, join.getDispatchedFrameCount());
                    // Once the workers joined the first window, its two entries hold the budget.
                    awaitReducedFrames(join);
                    Assert.assertEquals(2 * CAP_ENTRY_BYTES, ring.getAllocatedBytes());
                    TestUtils.assertEquals(expected, printFromCurrentRow(cursor, metadata));
                    Assert.assertEquals(0, join.getAtom().getContinuationCount());
                }
                // A query memory limit of eight such entries, whose quarter is the budget: the two
                // entries of the first window, which the workers join while the consumer holds its
                // first frame, fit it, where the eight of a first window of the frames' rows would not.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * CAP_ENTRY_BYTES);
                try (RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(2, join.getDispatchedFrameCount());
                    awaitReducedFrames(join);
                    TestUtils.assertEquals(expected, printFromCurrentRow(cursor, metadata));
                }
                // Limits from a few bytes up fail the query at each allocation in turn, the workers'
                // ones among them; each failure leaves nothing, and the first limit that admits the
                // query reads the rows.
                final int failures = assertMemoryLimitSweep(
                        expected,
                        limit -> setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit),
                        () -> printRows(factory, context),
                        e -> TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded"),
                        () -> {
                            Assert.assertEquals(0, ring.getAllocatedBytes());
                            Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(0, db.getBusyReaderCount());
                        }
                );
                Assert.assertTrue(failures > 0);
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
        }));
    }

    private static void assertFirstWindowUnderAnOuterLimit(WorkerPoolMode mode) throws Exception {
        withEightFrames(mode, (db, context) -> {
            db.execute("CREATE TABLE q8 AS (SELECT (x % 4)::INT k, x * 7 p, timestamp_sequence('2024-01-01', 30_000_000) ts FROM long_sequence(128)) TIMESTAMP(ts) PARTITION BY DAY", context);
            // LIMIT 3 needs three probe rows of frame 0, each of which meets three rows of d3: the
            // LIMIT reaches the join through a sub-query, as the master of an ASOF join and in each
            // branch of a UNION ALL, whose first branch holds the rows. The workers join frame 0
            // alone, as the first window holds it alone, and the second branch never builds.
            for (String sql : new String[]{
                    "SELECT * FROM (" + WALK + ") LIMIT 3",
                    "SELECT t8.v, d3.w, q8.p FROM t8 LEFT JOIN d3 ON t8.k = d3.k AND d3.w > t8.v ASOF JOIN q8 ON (k) LIMIT 3",
                    WALK + " UNION ALL " + WALK + " LIMIT 3",
            }) {
                assertParallelHashJoin(sql, context);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final ObjList<AsyncHashJoinRecordCursorFactory> joins = findAll(factory, context);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 1, joins.getQuick(0).getDispatchedFrameCount());
                        int rows = 1;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(sql, 3, rows);
                        for (int i = 0, n = joins.size(); i < n; i++) {
                            Assert.assertEquals(sql, i == 0 ? 1 : 0, joins.getQuick(i).getDispatchedFrameCount());
                        }
                    }
                    for (int i = 0, n = joins.size(); i < n; i++) {
                        Assert.assertEquals(sql, 0, joins.getQuick(i).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                }
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
                    // The first row comes with frame 0, which went out alone, and frame 1, which the
                    // ramp sent out once the consumer took frame 0.
                    Assert.assertTrue(cursor.hasNext());
                    CursorPrinter.println(record, metadata, sink);
                    Assert.assertEquals(2, join.getDispatchedFrameCount());
                    awaitReducedFrames(join);
                    // Frame 1's first row: the consumer took frame 1, which was ready, which stepped
                    // the ramp past the window, and frames 2 and 3 went out, ...
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

    // An ASOF or LT join whose master and slave are both parallel hash joins reads the two in
    // lock-step: for each row of the master, it reads the slave on to the master's timestamp. Both
    // joins then have frames in flight at once, on the one shard of four slots that the test leaves
    // them, fewer than the two windows of three frames ask for, so that each join's dispatch meets
    // a queue that the other's frames fill, and the owner, as it waits for a frame of one join,
    // meets the tasks of the other.
    private static void assertTwoJoinsReadInLockStep(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        withEightFrames(mode, (db, context) -> {
            // u8's rows come half a minute after t8's, so the slave's frame f ends where the master's
            // frame f + 1 begins.
            db.execute("""
                    CREATE TABLE u8 AS (
                        SELECT (x % 4)::INT k, x * 3 uv, timestamp_sequence('2024-01-01T00:00:30', 60_000_000) ts
                        FROM long_sequence(64)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """, context);
            final String master = "(SELECT t8.ts, t8.k, t8.v, d3.w FROM t8 LEFT JOIN d3 ON t8.k = d3.k) a";
            final String slave = "((SELECT u8.ts, u8.k, u8.uv, d3.w uw FROM u8 LEFT JOIN d3 ON u8.k = d3.k) TIMESTAMP(ts)) b";
            final String asOf = "SELECT a.ts, a.v, a.w, b.uv, b.uw FROM " + master + " ASOF JOIN " + slave;
            for (String sql : new String[]{
                    asOf,
                    asOf + " ON (k)",
                    "SELECT a.ts, a.v, a.w, b.uv, b.uw FROM " + master + " LT JOIN " + slave + " ON (k)",
                    // The LIMIT reaches the master, whose first window it narrows, and closes both
                    // joins with frames in flight; a negative one counts both first.
                    asOf + " LIMIT 5",
                    asOf + " LIMIT -5",
            }) {
                // The plan runs both joins on the operator.
                final String plan = assertParallelHashJoin(sql, context);
                final String operator = "Async Hash Join workers:";
                Assert.assertTrue(plan, plan.indexOf(operator, plan.indexOf(operator) + 1) > -1);
            }
            final String expected = ordinaryRows(db, asOf, context);
            try (RecordCursorFactory factory = db.select(asOf, context)) {
                final ObjList<AsyncHashJoinRecordCursorFactory> joins = findAll(factory, context);
                Assert.assertEquals(2, joins.size());
                try (RecordCursor cursor = factory.getCursor(context)) {
                    final Record record = cursor.getRecord();
                    // The first row of the master's frame 4, half way through both joins.
                    boolean isHalfWay = false;
                    while (!isHalfWay && cursor.hasNext()) {
                        isHalfWay = record.getLong(1) == 33;
                    }
                    Assert.assertTrue(isHalfWay);
                    // Each join read some of its frames, with a window of three, and has others to
                    // go: the cursor closes here, with frames of both in flight or not yet out.
                    for (int i = 0, n = joins.size(); i < n; i++) {
                        final int dispatched = joins.getQuick(i).getDispatchedFrameCount();
                        Assert.assertTrue("join " + i + " dispatched " + dispatched, dispatched > 3 && dispatched < 8);
                    }
                }
                for (int i = 0, n = joins.size(); i < n; i++) {
                    final AsyncHashJoinRecordCursorFactory join = joins.getQuick(i);
                    Assert.assertFalse(join.isExecutionActive());
                    Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                }
                Assert.assertEquals(0, db.getBusyReaderCount());
                // The factory reads every row again, after the close that left both joins half way.
                Assert.assertEquals(expected, printRows(factory, context));
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
                    // Frame 0 goes out alone, and taking it sends frame 1 out; from frame 1 on, which
                    // steps the ramp past the window, each frame the consumer takes sends the frame
                    // three past it out, so that three frames stay in flight, the one the consumer
                    // reads included.
                    readFramesWhenReady(cursor, factory.getMetadata(), join, 2, dispatched, sink);
                    TestUtils.assertEquals("[2,4,5]", dispatched.toString());
                    // toTop() in the middle of frame 2 starts the ramp over.
                    cursor.toTop();
                    dispatched.clear();
                    sink.clear();
                    readFramesWhenReady(cursor, factory.getMetadata(), join, Integer.MAX_VALUE, dispatched, sink);
                    TestUtils.assertEquals("[2,4,5,6,7,8,8,8]", dispatched.toString());
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
        // A ring budget of a frame of every row of the two-stage stars, in narrow slots: the window
        // starts at one frame, and widens once the selective frames, the first half, show their few
        // tuples.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 256 * 3 * Integer.BYTES);
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

    // The cursor's current row and every row after it, as printRows() prints them.
    private static String printFromCurrentRow(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        final Record record = cursor.getRecord();
        do {
            CursorPrinter.println(record, metadata, sink);
        } while (cursor.hasNext());
        return sink.toString();
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
