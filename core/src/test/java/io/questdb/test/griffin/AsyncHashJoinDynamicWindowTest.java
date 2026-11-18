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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.jit.JitUtil;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The dynamic window of a parallel hash join's tuple ring: the ring keeps an entry for every frame
 * that the dispatch limit and the reduce queue let fly, which take memory only once a task writes
 * them, and its window, which starts at the worst case of a frame's task, every probe row of the
 * largest frame surviving into one tuple, or, where the workers walk duplicate matches, the task's
 * cap of tuples, follows the largest entry the consumer has read: frames that a selective probe
 * filter thins out widen it, and frames that duplicate matches fill narrow it. An entry gives its
 * memory back once read when the entries hold more than the ring budget. The rows always equal the
 * ordinary plan's, in order.
 * <p>
 * The probe table t holds 64 rows in one partition, cut into eight frames of eight rows; a tuple of
 * the one-stage join takes two longs, so a frame of tuples, one per row, takes 128 bytes, and a
 * task's cap of 24 tuples, the three matches in d of each of a frame's eight rows, 384 bytes.
 */
public class AsyncHashJoinDynamicWindowTest extends AbstractCairoTest {
    // A task's cap of tuples, cairo.sql.parallel.hash.join.task.output.max.rows, that a frame of
    // eight rows fills when each row meets d's three rows of its key.
    private static final int CAP = 24;
    private static final long CAP_BYTES = CAP * 2 * Long.BYTES;
    private static final long FRAME_BYTES = 8 * 2 * Long.BYTES;
    // A walking join: d's three rows of each key, whose w, 100 and up, the residual keeps for every
    // probe row's v, up to 64.
    private static final String WALK = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testComputeEntryCount() {
        Assert.assertEquals(8, HashJoinTupleRing.computeEntryCount(8, 32));
        Assert.assertEquals(32, HashJoinTupleRing.computeEntryCount(100, 32));
        Assert.assertEquals(32, HashJoinTupleRing.computeEntryCount(Integer.MAX_VALUE, 32));
        Assert.assertEquals(1, HashJoinTupleRing.computeEntryCount(1, 32));
        // A dispatch limit of 0, or less, still leaves the ring an entry.
        Assert.assertEquals(1, HashJoinTupleRing.computeEntryCount(0, 32));
        Assert.assertEquals(1, HashJoinTupleRing.computeEntryCount(-1, 32));
        Assert.assertEquals(1, HashJoinTupleRing.computeEntryCount(8, 0));
    }

    @Test
    public void testDenseFramesKeepTheWindowWithinTheBudget() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A budget of one frame: the window stays at one, and the frames cycle through all eight
            // entries, each of which gives a frame's memory back, past its share of an eighth of the
            // budget, once read: the frame in flight alone holds memory.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, FRAME_BYTES);
            final String sql = "SELECT t.v, u.w FROM t LEFT JOIN u ON t.k = u.k";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinTupleRing ring = join.getAtom().getRing();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < 9; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    // The ninth row is the second frame's first: the first frame alone was in flight.
                    Assert.assertEquals(2, join.getDispatchedFrameCount());
                    Assert.assertEquals(8, ring.getEntryCount());
                    Assert.assertEquals(1, ring.getDispatchLimit());
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.assertEquals(1, ring.getDispatchLimit());
                    Assert.assertEquals(0, ring.getAllocatedBytes());
                }
                Assert.assertEquals(0, ring.getAllocatedBytes());
                Assert.assertEquals(0, ring.getEntryCount());
            }
        });
    }

    @Test
    public void testCountReleasesTheEntriesItReads() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A budget of four tasks' caps of tuples: a first window of four frames, whose entries the
            // duplicate matches of d grow, doubling from a frame's room, to room for 32 tuples, 512
            // bytes each. The first row leaves three more tuple-carrying frames in flight, whose
            // entries a count reads and gives back: the entries keep the budget, and the window
            // follows the entries read to three frames.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, CAP);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 4 * CAP_BYTES);
            try (RecordCursorFactory factory = select(WALK)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinTupleRing ring = join.getAtom().getRing();
                // The ordinary plan counts the rows.
                final long rows = ordinaryRowCount(WALK, sqlExecutionContext);
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(4, join.getDispatchedFrameCount());
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(rows - 1, counter.get());
                    // The frame the cursor held and the three in flight.
                    Assert.assertEquals(4, join.getCountedTupleFrameCount());
                    Assert.assertTrue(ring.getAllocatedBytes() <= 4 * CAP_BYTES);
                    Assert.assertEquals(3, ring.getDispatchLimit());
                    cursor.toTop();
                    Assert.assertEquals(rows, countRows(cursor));
                }
            }
        });
    }

    @Test
    public void testEntriesWithinTheirShareKeepTheirMemory() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A budget of one frame of every row, which the first frame takes: the window stays at
            // one. That frame's entry, past its share of an eighth of the budget, gives its memory
            // back once read; the later frames keep one row each, a tuple of 16 bytes, within their
            // shares, and their entries keep their memory for the frames that map to them next.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, FRAME_BYTES);
            final String sql = "SELECT t.v, u.w FROM t LEFT JOIN u ON t.k = u.k WHERE t.v <= 8 OR t.v % 8 = 0";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinTupleRing ring = join.getAtom().getRing();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.assertEquals(1, ring.getDispatchLimit());
                    Assert.assertEquals(7 * 2 * Long.BYTES, ring.getAllocatedBytes());
                }
            }
        });
    }

    @Test
    public void testFanOutFirstWindowHoldsTheTaskCap() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // e holds no row, and t1 one.
            execute("CREATE TABLE e (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 AS (SELECT 1 k, 1L v, '2024-01-01'::TIMESTAMP ts FROM long_sequence(1)) TIMESTAMP(ts) PARTITION BY DAY");
            // A budget of two tasks' caps of tuples, 768 bytes, or six frames of one tuple per row. A
            // join whose workers walk d's three matches of each key fills a task's cap, three times
            // as many tuples as the frame's rows, so its first window holds two frames; a join whose
            // probe rows yield one tuple each keeps a first window of six.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, CAP);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 2 * CAP_BYTES);
            for (String sql : new String[]{
                    WALK,
                    "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE d.w > t.v",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                final String expected = ordinaryRows(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    final HashJoinTupleRing ring = join.getAtom().getRing();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 2, ring.getDispatchLimit());
                        Assert.assertEquals(sql, 2, join.getDispatchedFrameCount());
                        Assert.assertEquals(sql, 8, ring.getEntryCount());
                        Assert.assertEquals(sql, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                        // The entries grew, doubling from a frame's room, to room for 32 tuples, 512
                        // bytes, which leaves a window of one frame.
                        Assert.assertEquals(sql, 1, ring.getDispatchLimit());
                        Assert.assertTrue(sql, ring.getAllocatedBytes() <= 2 * CAP_BYTES);
                        // toTop() keeps the window that the frames read set, and reads the same rows.
                        cursor.toTop();
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                        Assert.assertEquals(sql, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                    }
                }
            }
            // A probe filter, compiled or not, keeps every row of a frame, whose task fills its cap.
            final String filtered = "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.v > 0 AND d.w > t.v";
            final int jitModeBefore = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    final String plan = assertParallelHashJoin(filtered, sqlExecutionContext);
                    Assert.assertEquals(plan, jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported(), plan.contains("Async JIT Hash Join"));
                    assertFirstWindow(filtered, 2, 2);
                }
            } finally {
                sqlExecutionContext.setJitMode(jitModeBefore);
            }
            // A single probe row is one frame, whose task's cap bounds the window too.
            final String single = "SELECT t1.v, d.w FROM t1 LEFT JOIN d ON t1.k = d.k AND d.w > t1.v";
            assertParallelHashJoin(single, sqlExecutionContext);
            assertFirstWindow(single, 2, 1);
            // Without probe frames the budget bounds nothing: the window is the dispatch limit.
            final String empty = "SELECT e.v, d.w FROM e LEFT JOIN d ON e.k = d.k AND d.w > e.v";
            assertParallelHashJoin(empty, sqlExecutionContext);
            try (RecordCursorFactory factory = select(empty)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertEquals(8, join.getAtom().getRing().getDispatchLimit());
                    Assert.assertEquals(0, join.getDispatchedFrameCount());
                }
            }
            // Joins whose probe rows yield one tuple at most: a build of one row per key, under a
            // residual or not, a stage that hands d's chains to the consumer, and a LEFT join's stage
            // whose build ran its whole residual over d's rows, which hands them on too.
            for (String sql : new String[]{
                    "SELECT t.v, u.w FROM t LEFT JOIN u ON t.k = u.k",
                    "SELECT t.v, u.w FROM t LEFT JOIN u ON t.k = u.k AND u.w > t.v",
                    "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k",
                    "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                assertFirstWindow(sql, 6, 6);
            }
        });
    }

    @Test
    public void testFanOutJoinFitsEveryMemoryLimitAboveOneThatAdmitsIt() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The default ring size is far above a quarter of each limit below, so the quarter is the
            // ring budget. The first window holds as many tasks of the walking join, each of which
            // fills its cap of 24 tuples, as the budget holds, so once a limit admits the query, every
            // larger one does. A first window counted in the frames' rows would hold three times as
            // many tasks as the budget holds, which the owner alone joins at once when it asks for
            // the second frame: a larger limit's wider first window would then fail where a smaller
            // limit's passed.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, CAP);
            final String expected = ordinary(WALK, sqlExecutionContext);
            try (RecordCursorFactory factory = select(WALK)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                long admittingLimit = 0;
                int failures = 0;
                for (long limit = 64; limit <= 64 * 1024; limit *= 2) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals("limit " + limit, expected, result(factory, sqlExecutionContext));
                        if (admittingLimit == 0) {
                            admittingLimit = limit;
                        }
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        Assert.assertEquals("limit " + limit + " failed though a lower one admitted the query", 0, admittingLimit);
                        failures++;
                        // The failure leaves nothing behind.
                        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
                        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    }
                }
                Assert.assertTrue(failures > 0);
                Assert.assertTrue(admittingLimit > 0);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
        });
    }

    @Test
    public void testFanOutNarrowsTheWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A budget of four tasks' caps of tuples, a window of four frames, until a frame's entry
            // grows: each probe row meets three rows of d, which a residual walks, and the frame's
            // twenty-four tuples, the cap, grow the entry, doubling from one frame's room, to room
            // for 32 tuples, 512 bytes. The window then holds three frames.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, CAP);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 4 * CAP_BYTES);
            assertParallelHashJoin(WALK, sqlExecutionContext);
            try (RecordCursorFactory factory = select(WALK)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinTupleRing ring = join.getAtom().getRing();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(4, ring.getDispatchLimit());
                    Assert.assertEquals(8, ring.getEntryCount());
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.assertEquals(3, ring.getDispatchLimit());
                    Assert.assertTrue(ring.getAllocatedBytes() <= 4 * CAP_BYTES);
                }
            }
        });
    }

    @Test
    public void testFirstWindowPerExecutionOfCachedFactory() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // g holds one row of each key, then three, then one again: the workers walk g's matches,
            // one tuple per probe row, then three, so the executions start at a window of six frames,
            // two, and six again.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, CAP);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 2 * CAP_BYTES);
            execute("CREATE TABLE g AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(4))");
            final String sql = "SELECT t.v, g.w FROM t LEFT JOIN g ON t.k = g.k AND g.w > t.v";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertEquals(6, readFirstWindow(factory, join, sql));
                execute("INSERT INTO g SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(8)");
                Assert.assertEquals(2, readFirstWindow(factory, join, sql));
                execute("TRUNCATE TABLE g");
                execute("INSERT INTO g SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(4)");
                Assert.assertEquals(6, readFirstWindow(factory, join, sql));
            }
        });
    }

    @Test
    public void testMemoryLimitsFailAndLeaveNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v WHERE t.v % 8 = 0 OR t.v > 40";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Limits from a few bytes up fail the query at each allocation in turn, the ring's
                // entries growing among them; each failure leaves nothing, and the first limit that
                // admits the query reads the rows.
                final int failures = assertMemoryLimitSweep(
                        expected,
                        limit -> setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit),
                        () -> result(factory, sqlExecutionContext),
                        null,
                        () -> {
                            Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
                            Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(0, engine.getBusyReaderCount());
                        }
                );
                Assert.assertTrue(failures > 0);
            }
        });
    }

    @Test
    public void testSelectiveProbeFilterWidensTheWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A budget of one frame of every row of the one-stage join: the window starts at one. The
            // filter keeps one row of each frame, whose entry takes one tuple, so once the consumer has
            // read the first frame the window holds as many frames as the budget holds tuples, up to
            // the eight entries: all of them for a tuple of two longs, five for one of three.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, FRAME_BYTES);
            final String[] queries = {
                    "SELECT t.v, u.w FROM t LEFT JOIN u ON t.k = u.k WHERE t.v % 8 = 0",
                    "SELECT t.v, u.w FROM t JOIN u ON t.k = u.k WHERE t.v % 8 = 0 ORDER BY ts DESC",
                    "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k WHERE t.v % 8 = 0",
            };
            final int[] windows = {8, 8, (int) (FRAME_BYTES / (3 * Long.BYTES))};
            for (int q = 0; q < queries.length; q++) {
                final String sql = queries[q];
                final int window = windows[q];
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    final HashJoinTupleRing ring = join.getAtom().getRing();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                        Assert.assertEquals(sql, 1, ring.getDispatchLimit());
                        Assert.assertEquals(sql, 8, ring.getEntryCount());
                        // The first frame's rows end with its one kept row's matches, three of d's rows
                        // at most; the next row is the second frame's, which goes out with as many
                        // frames as the window holds.
                        int rows = 1;
                        while (join.getDispatchedFrameCount() == 1 && cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertTrue(sql, rows <= 4);
                        Assert.assertEquals(sql, Math.min(8, 1 + window), join.getDispatchedFrameCount());
                        Assert.assertEquals(sql, window, ring.getDispatchLimit());
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.assertTrue(sql, ring.getAllocatedBytes() <= FRAME_BYTES);
                        // toTop() keeps the window, which the frames the consumer read set.
                        cursor.toTop();
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(sql, window, join.getDispatchedFrameCount());
                    }
                }
            }
        });
    }

    // An execution of the query starts at the given window of the ring, and its first row comes with
    // the given frames dispatched.
    private static void assertFirstWindow(String sql, int window, int dispatched) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
                Assert.assertEquals(sql, window, join.getAtom().getRing().getDispatchLimit());
                Assert.assertEquals(sql, dispatched, join.getDispatchedFrameCount());
            }
        }
    }

    private static long countRows(RecordCursor cursor) {
        long rows = 0;
        while (cursor.hasNext()) {
            rows++;
        }
        return rows;
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(64)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        // u holds one row per key, d three.
        execute("CREATE TABLE u AS (SELECT (x % 4)::INT k, x * 7 w FROM long_sequence(4))");
        execute("CREATE TABLE d AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
        sqlExecutionContext.changePageFrameSizes(8, 8);
    }

    // The cursor's current row and every row after it, as ordinaryRows() prints them.
    private static String printFromCurrentRow(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        do {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        } while (cursor.hasNext());
        return sink.toString();
    }

    // Reads an execution of the factory, whose rows equal the ordinary plan's, and returns the window
    // that it started at, which its first row came with.
    private static int readFirstWindow(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, String sql) throws SqlException {
        final String expected = ordinaryRows(sql, sqlExecutionContext);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(sql, cursor.hasNext());
            final int window = join.getAtom().getRing().getDispatchLimit();
            Assert.assertEquals(sql, window, join.getDispatchedFrameCount());
            Assert.assertEquals(sql, expected, printFromCurrentRow(cursor, factory.getMetadata()));
            return window;
        }
    }
}
