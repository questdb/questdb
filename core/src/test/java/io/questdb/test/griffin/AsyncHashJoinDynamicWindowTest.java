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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The dynamic window of a parallel hash join's tuple ring: the ring keeps an entry for every frame
 * that the dispatch limit and the reduce queue let fly, which take memory only once a task writes
 * them, and its window, which starts at the worst case of every probe row of the largest frame
 * surviving, follows the largest entry the consumer has read: frames that a selective probe filter
 * thins out widen it, and frames that duplicate matches fill narrow it. An entry gives its memory
 * back once read when the entries hold more than the ring budget. The rows always equal the ordinary
 * plan's, in order.
 * <p>
 * The probe table t holds 64 rows in one partition, cut into eight frames of eight rows; a tuple of
 * the one-stage join takes two longs, so a frame of tuples, one per row, takes 128 bytes.
 */
public class AsyncHashJoinDynamicWindowTest extends AbstractCairoTest {
    private static final long FRAME_BYTES = 8 * 2 * Long.BYTES;

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
            // A window of four frames, whose entries the duplicate matches of d grow to four frames'
            // room each. The first row leaves three more tuple-carrying frames in flight, whose
            // entries a count reads and gives back: the entries keep the budget.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 4 * FRAME_BYTES);
            final String sql = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinTupleRing ring = join.getAtom().getRing();
                // The ordinary plan counts the rows.
                final long rows;
                sqlExecutionContext.setParallelHashJoinEnabled(false);
                try (
                        RecordCursorFactory ordinary = select(sql);
                        RecordCursor cursor = ordinary.getCursor(sqlExecutionContext)
                ) {
                    rows = countRows(cursor);
                } finally {
                    sqlExecutionContext.setParallelHashJoinEnabled(true);
                }
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(4, join.getDispatchedFrameCount());
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(rows - 1, counter.get());
                    // The frame the cursor held and the three in flight.
                    Assert.assertEquals(4, join.getCountedTupleFrameCount());
                    Assert.assertTrue(ring.getAllocatedBytes() <= 4 * FRAME_BYTES);
                    Assert.assertEquals(1, ring.getDispatchLimit());
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
    public void testFanOutNarrowsTheWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A budget of four frames, a window of four, until a frame's entry grows: each probe row
            // meets three rows of d, which a residual walks, and the frame's twenty-four tuples grow
            // the entry, doubling from one frame's room, to four frames' room. The window then
            // holds one frame.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 4 * FRAME_BYTES);
            final String sql = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
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
                    Assert.assertEquals(1, ring.getDispatchLimit());
                    Assert.assertTrue(ring.getAllocatedBytes() <= 4 * FRAME_BYTES);
                }
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
}
