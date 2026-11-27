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
import io.questdb.cairo.SqlJitMode;
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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The ramp of a parallel hash join's dispatch window. An execution starts with one probe frame in
 * flight, and each frame that the consumer takes doubles the frames that it may keep in flight, the
 * one it reads included, up to the ring's window: a reader that stops within the first frame has
 * joined that frame alone, the frames after it go out as the reader goes on, and a full read
 * reaches the full window after a few frames. The first frame's task joins only the frame's first
 * 4_096 rows, if it has more, and each chunk of the frame that the consumer continues past them
 * steps the ramp as a frame taken does. The ramp steps on frames and chunks taken, empty ones
 * included, not on rows returned. A LIMIT, or the rows that the client reads first, start the ramp
 * at the first window that they pick, or at the full window when the window's frames do not hold
 * their rows; toTop() starts the ramp over. The rows always equal the ordinary plan's, in order.
 * <p>
 * The probe table t holds 128 rows in one partition, cut into 32 frames of four rows, and the
 * dispatch limit of eight sets the ring's window, and its entries, at eight frames. The probe table
 * tb holds 400_000 rows in one partition, cut into 20 frames of 20_000 rows, whose first frame's
 * task joins its first 4_096 rows, and whose continuation joins chunks of 4_096, 8_192 and 3_616
 * rows. The tests without workers run each frame's task on the owner, when the owner asks for the
 * frame, so the frames dispatched follow from the ramp alone: once the consumer takes frame
 * {@code f}, frames up to {@code f + window} have gone out.
 */
public class AsyncHashJoinRampTest extends AbstractCairoTest {
    private static final int BIG_FRAME_COUNT = 20;
    private static final int BIG_FRAME_ROWS = 20_000;
    private static final int BIG_ROW_COUNT = BIG_FRAME_COUNT * BIG_FRAME_ROWS;
    private static final int FRAME_COUNT = 32;
    private static final int FRAME_ROWS = 4;
    // The rows that the first frame's task joins without a LIMIT.
    private static final int HEAD_ROWS = 4_096;
    // A join of tb whose every probe row meets one row of d1, so that its row n is tb's row of v = n.
    private static final String JOIN_TB = "SELECT tb.v, d1.w FROM tb JOIN d1 ON tb.k = d1.k";
    // A join of t whose every probe row meets one row of d1, so that its rows of frame f are rows
    // 4 * f to 4 * f + 3; and the same join with t's designated timestamp.
    private static final String JOIN_D1 = "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k";
    private static final String JOIN_D1_TS = "SELECT t.v, d1.w, t.ts FROM t JOIN d1 ON t.k = d1.k";
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinRampTest.class);
    private static final int ROW_COUNT = FRAME_COUNT * FRAME_ROWS;
    // The frames that the dispatch limit lets the ring's window hold.
    private static final int WINDOW = 8;
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, WINDOW);
    }

    @After
    public void tearDownRamp() {
        sqlExecutionContext.setFirstReadRows(Long.MAX_VALUE);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCancelledAtEachRampStepOnFiberWorkers() throws Exception {
        assertStoppedAtEachRampStep(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testCancelledInsideTheFirstFrameOnFiberWorkers() throws Exception {
        assertStoppedInsideTheFirstFrame(WorkerPoolMode.FIBER_HOST, true);
    }

    @Test
    public void testCancelledInsideTheFirstFrameOnThreadWorkers() throws Exception {
        assertStoppedInsideTheFirstFrame(WorkerPoolMode.LEGACY, true);
    }

    @Test
    public void testClosedInsideTheFirstFrameOnFiberWorkers() throws Exception {
        assertStoppedInsideTheFirstFrame(WorkerPoolMode.FIBER_HOST, false);
    }

    @Test
    public void testClosedInsideTheFirstFrameOnThreadWorkers() throws Exception {
        assertStoppedInsideTheFirstFrame(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testCancelledAtEachRampStepOnThreadWorkers() throws Exception {
        assertStoppedAtEachRampStep(WorkerPoolMode.LEGACY, true);
    }

    @Test
    public void testClosedAtEachRampStepOnFiberWorkers() throws Exception {
        assertStoppedAtEachRampStep(WorkerPoolMode.FIBER_HOST, false);
    }

    @Test
    public void testClosedAtEachRampStepOnThreadWorkers() throws Exception {
        assertStoppedAtEachRampStep(WorkerPoolMode.LEGACY, false);
    }

    @Test
    public void testCountAfterEachRampStep() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = ordinaryRows(JOIN_D1, sqlExecutionContext);
            try (RecordCursorFactory factory = select(JOIN_D1)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // [rows read, frames dispatched once read, tuple-carrying frames that the count
                // reads, count-only frames]. No row read: the count builds and dispatches every frame
                // count-only. Frame 0's first row: the consumer holds frame 0, and frame 1 went out
                // with it. Frame 1's: frames 1 to 4. Frame 3's: frames 3 to 10, the full window.
                final int[][] cases = {
                        {0, 0, 0, FRAME_COUNT},
                        {1, 2, 2, FRAME_COUNT - 2},
                        {FRAME_ROWS + 1, 5, 4, FRAME_COUNT - 5},
                        {3 * FRAME_ROWS + 1, 11, 8, FRAME_COUNT - 11},
                };
                for (int[] c : cases) {
                    final String label = "rows read " + c[0];
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        for (int i = 0; i < c[0]; i++) {
                            Assert.assertTrue(label, cursor.hasNext());
                        }
                        Assert.assertEquals(label, c[1], join.getDispatchedFrameCount());
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(label, ROW_COUNT - c[0], counter.get());
                        Assert.assertEquals(label, c[2], join.getCountedTupleFrameCount());
                        Assert.assertEquals(label, c[3], join.getCountOnlyFrameCount());
                        Assert.assertFalse(label, cursor.hasNext());
                        // toTop() reads every row again, from the ramp's start.
                        cursor.toTop();
                        Assert.assertTrue(label, cursor.hasNext());
                        Assert.assertEquals(label, 2, join.getDispatchedFrameCount());
                        Assert.assertEquals(label, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                    }
                }
            }
        });
    }

    @Test
    public void testCountAndToTopInsideTheFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            createBigTables();
            final String expected = ordinaryRows(JOIN_TB, sqlExecutionContext);
            try (RecordCursorFactory factory = select(JOIN_TB)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // [rows read, frames dispatched once read, continuations that the count counted
                // rather than joined, tuple-carrying frames that it read, count-only frames]. No row
                // read: the count builds and counts every frame whole, frame 0 included. The first
                // row: the consumer holds frame 0, whose task joined its head, and the count counts
                // the rest of the frame. Row 4_097, of the first chunk: frames 1 to 3 went out with
                // tuples, and the count counts the rest of frame 0 past the chunk.
                final int[][] cases = {
                        {0, 0, 0, 0, BIG_FRAME_COUNT},
                        {1, 1, 1, 1, BIG_FRAME_COUNT - 1},
                        {HEAD_ROWS + 1, 4, 1, 4, BIG_FRAME_COUNT - 4},
                };
                for (int[] c : cases) {
                    final String label = "rows read " + c[0];
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        for (int i = 0; i < c[0]; i++) {
                            Assert.assertTrue(label, cursor.hasNext());
                        }
                        Assert.assertEquals(label, c[1], join.getDispatchedFrameCount());
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(label, BIG_ROW_COUNT - c[0], counter.get());
                        Assert.assertEquals(label, c[2], join.getCountedContinuationCount());
                        Assert.assertEquals(label, c[3], join.getCountedTupleFrameCount());
                        Assert.assertEquals(label, c[4], join.getCountOnlyFrameCount());
                        Assert.assertFalse(label, cursor.hasNext());
                        // toTop() reads every row again: the first frame's head first, and nothing
                        // after it is out.
                        cursor.toTop();
                        Assert.assertTrue(label, cursor.hasNext());
                        Assert.assertEquals(label, 1, join.getDispatchedFrameCount());
                        Assert.assertEquals(label, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                    }
                }
            }
        });
    }

    @Test
    public void testEmptyProbeDispatchesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE e (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // A LEFT join builds d1 and probes e, which has no frame: no first window, and no frame
            // to dispatch, without a LIMIT and under one.
            for (String sql : new String[]{
                    "SELECT e.v, d1.w FROM e LEFT JOIN d1 ON e.k = d1.k",
                    "SELECT e.v, d1.w FROM e LEFT JOIN d1 ON e.k = d1.k LIMIT 3",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertFalse(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 0, join.getProbeFrameCount());
                        Assert.assertEquals(sql, 0, join.getDispatchedFrameCount());
                        cursor.toTop();
                        Assert.assertFalse(sql, cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testEmptyFramesStepTheRamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The first row comes from frame 1, of v = 5 to 8: a probe filter that keeps no row of
            // frame 0, an INNER join whose ON residual drops its rows, and a post-join filter that
            // does. d1.w is 10 times t.k, 40 for key 0, so no row of v up to 4 passes
            // d1.w + t.v > 46, and v = 8, of key 0, the first that does. The consumer takes frame 0
            // without a row, which steps the ramp to two frames, and taking frame 1 steps it to four:
            // frames 1 to 4 went out. A ramp that stepped on rows returned would hold one frame, and
            // a full window from the first frame on eight.
            for (String sql : new String[]{
                    JOIN_D1 + " WHERE t.v = 6 OR t.v > 100",
                    "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k AND d1.w + t.v > 46",
                    JOIN_D1 + " WHERE d1.w + t.v > 46",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                final String expected = ordinaryRows(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 1 + 4, join.getDispatchedFrameCount());
                        Assert.assertEquals(sql, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                    }
                }
            }
        });
    }

    @Test
    public void testFirstFrameJoinsItsHeadFirst() throws Exception {
        assertMemoryLeak(() -> {
            createBigTables();
            final String expected = ordinaryRows(JOIN_TB, sqlExecutionContext);
            try (RecordCursorFactory factory = select(JOIN_TB)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int execution = 0; execution < 2; execution++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        // [row, frames dispatched once the query returned it, probe rows joined,
                        // continuations]. The first row: frame 0 alone, whose task joined its head. The
                        // first row of each chunk of frame 0, 4_097, 8_193 and 16_385: the ramp, at two
                        // frames once the consumer took frame 0, steps to four, eight, and eight. The
                        // first rows of frames 1 and 2: each frame taken sends out one more, at the
                        // ring's window of eight; the owner has run no frame after the first yet.
                        final long[][] checkpoints = {
                                {1, 1, HEAD_ROWS, 0},
                                {HEAD_ROWS, 1, HEAD_ROWS, 0},
                                {HEAD_ROWS + 1, 4, 2 * HEAD_ROWS, 1},
                                {2 * HEAD_ROWS + 1, 8, 4 * HEAD_ROWS, 2},
                                {4 * HEAD_ROWS + 1, 8, BIG_FRAME_ROWS, 3},
                                {BIG_FRAME_ROWS + 1, 9, -1, 3},
                                {2 * BIG_FRAME_ROWS + 1, 10, -1, 3},
                        };
                        final StringSink rows = new StringSink();
                        int checkpoint = 0;
                        for (long row = 1; cursor.hasNext(); row++) {
                            Assert.assertEquals(row, record.getLong(0));
                            CursorPrinter.println(record, factory.getMetadata(), rows, true);
                            if (checkpoint < checkpoints.length && checkpoints[checkpoint][0] == row) {
                                final long[] c = checkpoints[checkpoint++];
                                final String label = "row " + row;
                                Assert.assertEquals(label, c[1], join.getDispatchedFrameCount());
                                if (c[2] > -1) {
                                    Assert.assertEquals(label, c[2], join.getAtom().getJoinedProbeRowCount());
                                }
                                Assert.assertEquals(label, c[3], join.getAtom().getContinuationCount());
                            }
                        }
                        Assert.assertEquals(checkpoints.length, checkpoint);
                        Assert.assertEquals(WINDOW, join.getAtom().getRing().getDispatchLimit());
                        TestUtils.assertEquals(expected, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testFirstFrameHeadOfOneRow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = ordinaryRows(JOIN_D1, sqlExecutionContext);
            // A first task of one row, as the query fuzzer compiles some joins for its small
            // tables: frame 0's task joins row 1, the first chunk row 2, which steps the ramp from
            // the two frames that taking frame 0 left it at to four, and the second chunk rows 3
            // and 4, which steps it to the window's eight. Frame 1's first row sends frame 8 out.
            AsyncHashJoinRecordCursorFactory.setFirstTaskRowsForTesting(1);
            try (RecordCursorFactory factory = select(JOIN_D1)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // [row, frames dispatched once read, probe rows joined, continuations]
                    final long[][] checkpoints = {
                            {1, 1, 1, 0},
                            {2, 4, 2, 1},
                            {3, 8, 4, 2},
                            {4, 8, 4, 2},
                            {5, 9, -1, 2},
                    };
                    final Record record = cursor.getRecord();
                    final StringSink rows = new StringSink();
                    for (long[] c : checkpoints) {
                        final String label = "row " + c[0];
                        Assert.assertTrue(label, cursor.hasNext());
                        Assert.assertEquals(label, c[0], record.getLong(0));
                        CursorPrinter.println(record, factory.getMetadata(), rows, true);
                        Assert.assertEquals(label, c[1], join.getDispatchedFrameCount());
                        if (c[2] > -1) {
                            Assert.assertEquals(label, c[2], join.getAtom().getJoinedProbeRowCount());
                        }
                        Assert.assertEquals(label, c[3], join.getAtom().getContinuationCount());
                    }
                    rows.put(printFromNextRow(cursor, factory.getMetadata()));
                    TestUtils.assertEquals(expected, rows);
                }
            } finally {
                AsyncHashJoinRecordCursorFactory.setFirstTaskRowsForTesting(AsyncHashJoinRecordCursorFactory.FIRST_TASK_ROWS);
            }
            // A factory that compiled once the hook was restored joins frame 0 whole: its four rows
            // are fewer than the default head.
            try (RecordCursorFactory factory = select(JOIN_D1)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(2, join.getDispatchedFrameCount());
                    Assert.assertEquals(FRAME_ROWS, join.getAtom().getJoinedProbeRowCount());
                    Assert.assertEquals(expected, printFromCurrentRow(cursor, factory.getMetadata()));
                    Assert.assertEquals(0, join.getAtom().getContinuationCount());
                }
            }
        });
    }

    @Test
    public void testFirstFrameOfEveryShapeJoinsItsHeadFirst() throws Exception {
        assertMemoryLeak(() -> {
            createBigTables();
            final int jitModeBefore = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    // [query, probe rows joined before the first row, continuations once read]. The
                    // first row of each comes from frame 0, of tb or, swapped, of tb as the probe of
                    // d1's build, and a reader that stops there leaves the frames after it unsent: a
                    // window function over the join, a LEFT join, an INNER join whose ON residual drops
                    // most rows, a probe filter that keeps every other row, whose head is 4_096 kept
                    // rows, a fan-out of d3's three rows per key, and a stack. A probe filter that keeps
                    // twenty rows of frame 0, fewer than its head, joins them whole, and continues
                    // nothing.
                    final Object[][] cases = {
                            {"SELECT tb.v, d1.w, row_number() OVER () rn FROM tb JOIN d1 ON tb.k = d1.k", HEAD_ROWS, 3},
                            {"SELECT tb.v, d1.w FROM tb LEFT JOIN d1 ON tb.k = d1.k", HEAD_ROWS, 3},
                            {"SELECT tb.v, d1.w FROM tb JOIN d1 ON tb.k = d1.k AND d1.w + tb.v > 1_000", HEAD_ROWS, 3},
                            {JOIN_TB + " WHERE tb.v % 2 = 0", HEAD_ROWS, 2},
                            {"SELECT tb.v, d3.w FROM tb LEFT JOIN d3 ON tb.k = d3.k AND d3.w > tb.v", HEAD_ROWS, 3},
                            {"SELECT tb.v, d1.w, d3.w w3 FROM tb JOIN d1 ON tb.k = d1.k LEFT JOIN d3 ON tb.k = d3.k", HEAD_ROWS, 3},
                            {"SELECT tb.v, d1.w FROM d1 JOIN tb ON d1.k = tb.k", HEAD_ROWS, 3},
                            {JOIN_TB + " WHERE tb.v % 1_000 = 0", BIG_FRAME_ROWS / 1_000, 0},
                    };
                    for (Object[] c : cases) {
                        final String sql = (String) c[0];
                        final String label = sql + ", jit " + jitMode;
                        final String expected = ordinaryRows(sql, sqlExecutionContext);
                        try (RecordCursorFactory factory = select(sql)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(label, join);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                Assert.assertTrue(label, cursor.hasNext());
                                Assert.assertEquals(label, BIG_FRAME_COUNT, join.getProbeFrameCount());
                                Assert.assertEquals(label, 1, join.getDispatchedFrameCount());
                                Assert.assertEquals(label, (int) c[1], join.getAtom().getJoinedProbeRowCount());
                                TestUtils.assertEquals(label, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                                Assert.assertEquals(label, (int) c[2], join.getAtom().getContinuationCount());
                            }
                            Assert.assertEquals(label, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(jitModeBefore);
            }
        });
    }

    @Test
    public void testFullReadDoublesTheWindowWithEachFrameTaken() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = ordinaryRows(JOIN_D1, sqlExecutionContext);
            try (RecordCursorFactory factory = select(JOIN_D1)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int execution = 0; execution < 2; execution++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final IntList dispatched = new IntList();
                        final StringSink rows = new StringSink();
                        readFrames(cursor, factory.getMetadata(), join, Integer.MAX_VALUE, dispatched, rows);
                        // Frame f went out with frames up to f + window: a window of 2, 4 and 8 frames
                        // once the consumer took frames 0, 1 and 2, then the ring's eight.
                        TestUtils.assertEquals(
                                "[2,5,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,32,32,32,32,32,32,32]",
                                dispatched.toString()
                        );
                        Assert.assertEquals(WINDOW, join.getAtom().getRing().getDispatchLimit());
                        TestUtils.assertEquals(expected, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testLimitStartsTheRampAtItsFirstWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // LIMIT 3 needs three rows of frame 0, the first window. Taking frame 0 stepped the ramp
            // to two frames, and the reader that reads past the LIMIT's rows, through the join's own
            // cursor, steps it to four with the capped frame's first chunk: frames 1 to 3 go out.
            try (RecordCursorFactory factory = select(JOIN_D1 + " LIMIT 3")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < 3; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1, join.getDispatchedFrameCount());
                    }
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(4, join.getDispatchedFrameCount());
                    int rows = 4;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(ROW_COUNT, rows);
                }
            }
            // LIMIT 6 needs frame 0 and two rows of frame 1, the first window, which goes out at once
            // and which holds every frame in flight until the reader has read the LIMIT's rows. The
            // consumer took frames 0 and 1, which stepped the ramp to eight frames, so the reader
            // that reads past the LIMIT's rows, through the join's own cursor, sends frames 2 to 8
            // out while the consumer continues frame 1.
            final String sql = JOIN_D1 + " LIMIT 6";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // The join's own cursor returns the keys too, and every row of t: v in order, each
                // with the w of d1's row of key v % 4.
                final int vIndex = join.getMetadata().getColumnIndex("v");
                final int wIndex = join.getMetadata().getColumnIndex("w");
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final IntList dispatched = new IntList();
                    for (int v = 1; v <= ROW_COUNT; v++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(v, record.getLong(vIndex));
                        Assert.assertEquals(10L * (v % 4 == 0 ? 4 : v % 4), record.getLong(wIndex));
                        if (v <= 6) {
                            dispatched.add(join.getDispatchedFrameCount());
                        } else if (v == 7) {
                            TestUtils.assertEquals("[2,2,2,2,2,2]", dispatched.toString());
                            Assert.assertEquals(1 + WINDOW, join.getDispatchedFrameCount());
                            Assert.assertEquals(1, join.getAtom().getContinuationCount());
                        }
                        if (v == 6) {
                            Assert.assertEquals(6, join.getAtom().getJoinedProbeRowCount());
                        }
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            // A LIMIT that the window's frames do not hold, 40 rows of eight frames' 32, or of more
            // rows than t's 128, opens the full window at once; one that the window's last frame
            // meets, 32 rows, starts the ramp at that first window, all eight frames.
            for (String limited : new String[]{JOIN_D1 + " LIMIT 40", JOIN_D1 + " LIMIT 1000", JOIN_D1 + " LIMIT 32"}) {
                final String expected = ordinaryRows(limited, sqlExecutionContext);
                try (RecordCursorFactory factory = select(limited)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(limited, join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(limited, cursor.hasNext());
                        Assert.assertEquals(limited, WINDOW, join.getDispatchedFrameCount());
                        Assert.assertEquals(limited, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                    }
                }
            }
        });
    }

    @Test
    public void testRingWindowBoundsTheRamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A ring budget of three frames of every probe row of the join, a tuple of two narrow
            // slots per row: the ring's window holds three frames, which the ramp reaches once the
            // consumer took frame 1, and never passes.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 3 * FRAME_ROWS * 2 * Integer.BYTES);
            final String expected = ordinaryRows(JOIN_D1, sqlExecutionContext);
            try (RecordCursorFactory factory = select(JOIN_D1)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final IntList dispatched = new IntList();
                    final StringSink rows = new StringSink();
                    readFrames(cursor, factory.getMetadata(), join, 4, dispatched, rows);
                    TestUtils.assertEquals("[2,4,5,6,7]", dispatched.toString());
                    Assert.assertTrue(join.getAtom().getRing().isNarrow());
                    Assert.assertEquals(3, join.getAtom().getRing().getDispatchLimit());
                    rows.put(printFromNextRow(cursor, factory.getMetadata()));
                    TestUtils.assertEquals(expected, rows);
                }
            }
        });
    }

    @Test
    public void testShapesMatchTheOrdinaryPlanOnFiberWorkers() throws Exception {
        assertShapesOnWorkers(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testShapesMatchTheOrdinaryPlanOnThreadWorkers() throws Exception {
        assertShapesOnWorkers(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testShortReadsUnderOperatorsThatNoLimitReaches() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createShapeTables();
            // An operator that may read more rows of the join than it returns keeps the LIMIT, and
            // the rows that the client reads first, from the join: a window function, a CROSS JOIN, a
            // serial join over the join, a filter that the join cannot run, UNION, EXCEPT and
            // INTERSECT, and the join as the slave of an ASOF join. Its first row comes from the
            // join's first frame, which the consumer took alone, and which sent frame 1 out: the
            // consumer joined frame 0's four probe rows, where a full window would have dispatched
            // eight frames. EXCEPT and INTERSECT read their second join whole before the first one's
            // first row; UNION never opens its second join's frames.
            final String[] operators = {
                    "SELECT t.v, row_number() OVER () rn FROM t JOIN d1 ON t.k = d1.k",
                    "SELECT * FROM (" + JOIN_D1 + ") j CROSS JOIN long_sequence(2) c",
                    "SELECT t.v, d1.w, d4.z FROM t JOIN d1 ON t.k = d1.k LEFT JOIN d4 ON d1.w = d4.w",
                    "SELECT * FROM (" + JOIN_D1 + ") WHERE rnd_int(0, 1, 0) >= 0",
                    JOIN_D1 + " UNION " + JOIN_D1,
                    JOIN_D1 + " EXCEPT " + JOIN_D1 + " WHERE t.v > 100",
                    JOIN_D1 + " INTERSECT " + JOIN_D1 + " WHERE t.v < 50",
                    "SELECT q.p, j.w FROM q ASOF JOIN ((" + JOIN_D1_TS + ") TIMESTAMP(ts)) j",
            };
            // The probe rows that the second join joined once the query returned its first row, the
            // rows that its probe filter keeps for EXCEPT and INTERSECT, and none for UNION; -1 for a
            // query of one join.
            final int[] secondJoinRows = {-1, -1, -1, -1, 0, ROW_COUNT - 100, 49, -1};
            for (int i = 0; i < operators.length; i++) {
                for (String sql : new String[]{operators[i], operators[i] + " LIMIT 3"}) {
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    final String expected = ordinaryRows(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final ObjList<AsyncHashJoinRecordCursorFactory> joins = findAll(factory, sqlExecutionContext);
                        Assert.assertEquals(sql, secondJoinRows[i] < 0 ? 1 : 2, joins.size());
                        final AsyncHashJoinRecordCursorFactory join = joins.getQuick(0);
                        Assert.assertFalse(sql, join.isClientRowSource());
                        // Opened as the protocols open the query for a client that reads one row
                        // first and a thousand, and without the hint.
                        for (long firstRead : new long[]{Long.MAX_VALUE, 1, 1_000}) {
                            final String label = sql + ", first read " + firstRead;
                            try (RecordCursor cursor = openUnder(factory, firstRead)) {
                                Assert.assertTrue(label, cursor.hasNext());
                                Assert.assertEquals(label, 2, join.getDispatchedFrameCount());
                                Assert.assertEquals(label, FRAME_ROWS, join.getAtom().getJoinedProbeRowCount());
                                if (secondJoinRows[i] > -1) {
                                    Assert.assertEquals(label, secondJoinRows[i], joins.getQuick(1).getAtom().getJoinedProbeRowCount());
                                }
                                TestUtils.assertEquals(label, expected, printFromCurrentRow(cursor, factory.getMetadata()));
                            }
                            for (int j = 0, n = joins.size(); j < n; j++) {
                                Assert.assertEquals(label, 0, joins.getQuick(j).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            }
                        }
                    }
                }
            }
            Assert.assertEquals(0, engine.getBusyReaderCount());
        });
    }

    @Test
    public void testToTopRestartsTheRamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = ordinaryRows(JOIN_D1, sqlExecutionContext);
            try (RecordCursorFactory factory = select(JOIN_D1)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // toTop() before any row, inside frame 0, inside frame 1 and once the ramp holds
                    // the full window: each read from the top steps through the same windows.
                    for (int lastFrame : new int[]{-1, 0, 1, 5}) {
                        final IntList dispatched = new IntList();
                        final StringSink rows = new StringSink();
                        if (lastFrame > -1) {
                            readFrames(cursor, factory.getMetadata(), join, lastFrame, dispatched, rows);
                            Assert.assertTrue(cursor.hasNext());
                        }
                        cursor.toTop();
                        dispatched.clear();
                        rows.clear();
                        readFrames(cursor, factory.getMetadata(), join, 3, dispatched, rows);
                        TestUtils.assertEquals("toTop() after frame " + lastFrame, "[2,5,10,11]", dispatched.toString());
                        rows.put(printFromNextRow(cursor, factory.getMetadata()));
                        TestUtils.assertEquals(expected, rows);
                        cursor.toTop();
                    }
                }
            }
        });
    }

    // Reads every row of each shape on the workers, as a full read and as a read of one row that a
    // LIMIT does not reach, whose rows equal the ordinary plan's, in order: a selective probe filter,
    // an INNER join that drops most rows, fan-out under a residual, a stack of joins, and a join
    // that swaps its inputs.
    private static void assertShapesOnWorkers(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            createTables(db, context);
            final String[] shapes = {
                    JOIN_D1 + " WHERE t.v % 16 = 0",
                    "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k AND d1.w + t.v > 100",
                    "SELECT t.v, d3.w FROM t LEFT JOIN d3 ON t.k = d3.k AND d3.w > t.v",
                    "SELECT t.v, d1.w, d3.w w3 FROM t JOIN d1 ON t.k = d1.k LEFT JOIN d3 ON t.k = d3.k AND d3.w > t.v",
                    "SELECT t.v, d1.w FROM d1 JOIN t ON d1.k = t.k",
            };
            for (String shape : shapes) {
                for (String sql : new String[]{shape, "SELECT *, row_number() OVER () rn FROM (" + shape + ") LIMIT 1"}) {
                    final String expected = ordinaryRows(sql, context);
                    context.setParallelHashJoinEnabled(true);
                    try {
                        assertParallelHashJoin(sql, context);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            for (int execution = 0; execution < 2; execution++) {
                                Assert.assertEquals(sql, expected, printRows(factory, context));
                            }
                            if (join != null) {
                                Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            }
                        }
                    } finally {
                        context.setParallelHashJoinEnabled(false);
                    }
                }
            }
            Assert.assertEquals(0, db.getBusyReaderCount());
        }));
    }

    // Stops a read of the join on the workers at the first row of frames 0 to 3, once the ramp
    // held two, four, eight and eight frames: cancels the execution and reads on, which fails, or
    // closes the cursor. Each leaves the workers' slots and the readers free, and the factory runs
    // again.
    private static void assertStoppedAtEachRampStep(WorkerPoolMode mode, boolean isCancelled) throws Exception {
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            createTables(db, context);
            final String sql = JOIN_D1;
            final String expected = ordinaryRows(sql, context);
            // The frames dispatched once the consumer took frame f: frames up to f + window.
            final int[] dispatched = {2, 5, 10, 11};
            context.setParallelHashJoinEnabled(true);
            final SqlExecutionCircuitBreaker previous = context.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(db, 0);
            context.with(breaker);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                for (int frame = 0; frame < 4; frame++) {
                    final String label = "frame " + frame;
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        final Record record = cursor.getRecord();
                        final long firstV = (long) frame * FRAME_ROWS + 1;
                        do {
                            Assert.assertTrue(label, cursor.hasNext());
                        } while (record.getLong(0) != firstV);
                        Assert.assertEquals(label, dispatched[frame], join.getDispatchedFrameCount());
                        if (isCancelled) {
                            breaker.cancel();
                            final CairoException e = Assert.assertThrows(CairoException.class, () -> {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                }
                            });
                            Assert.assertTrue(label, e.isCancellation());
                            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                        }
                    }
                    Assert.assertEquals(label, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertEquals(label, 0, db.getBusyReaderCount());
                    breaker.reset();
                    TestUtils.assertEquals(label, expected, printRows(factory, context));
                }
            } finally {
                context.with(previous);
            }
        }));
    }

    // Stops a read of tb's join on the workers at the head's last row, at the first row of each
    // chunk of frame 0, and at frame 1's first row: cancels the execution and reads on, which
    // fails, or closes the cursor. Each leaves the workers' slots and the readers free, and the
    // factory runs again.
    private static void assertStoppedInsideTheFirstFrame(WorkerPoolMode mode, boolean isCancelled) throws Exception {
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            createBigTables(db, context);
            final String expected = ordinaryRows(JOIN_TB, context);
            context.setParallelHashJoinEnabled(true);
            final SqlExecutionCircuitBreaker previous = context.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(db, 0);
            context.with(breaker);
            try (RecordCursorFactory factory = db.select(JOIN_TB, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // [row, frames dispatched once read]: frame 0 alone through its head, then the
                // chunks' ramp, then frame 1 taken.
                final int[][] stops = {
                        {HEAD_ROWS, 1},
                        {HEAD_ROWS + 1, 4},
                        {2 * HEAD_ROWS + 1, 8},
                        {4 * HEAD_ROWS + 1, 8},
                        {BIG_FRAME_ROWS + 1, 9},
                };
                for (int[] stop : stops) {
                    final String label = "row " + stop[0];
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        for (int i = 0; i < stop[0]; i++) {
                            Assert.assertTrue(label, cursor.hasNext());
                        }
                        Assert.assertEquals(label, stop[0], cursor.getRecord().getLong(0));
                        Assert.assertEquals(label, stop[1], join.getDispatchedFrameCount());
                        if (isCancelled) {
                            breaker.cancel();
                            final CairoException e = Assert.assertThrows(CairoException.class, () -> {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                }
                            });
                            Assert.assertTrue(label, e.isCancellation());
                            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                        }
                    }
                    Assert.assertEquals(label, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertEquals(label, 0, db.getBusyReaderCount());
                    breaker.reset();
                    TestUtils.assertEquals(label, expected, printRows(factory, context));
                }
            } finally {
                context.with(previous);
            }
        }));
    }

    private static void createBigTables() throws SqlException {
        createBigTables(engine, sqlExecutionContext);
    }

    // tb holds 400_000 rows of t's four keys in one partition, cut into 20 frames of 20_000 rows, and
    // d1 and d3 the builds of createTables().
    private static void createBigTables(CairoEngine db, SqlExecutionContext context) throws SqlException {
        db.execute("""
                CREATE TABLE tb AS (
                    SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 100_000) ts
                    FROM long_sequence(400_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """, context);
        db.execute("CREATE TABLE d1 AS (SELECT (x % 4)::INT k, x * 10 w FROM long_sequence(4))", context);
        db.execute("CREATE TABLE d3 AS (SELECT (x % 4)::INT k, x * 1_000_000 w FROM long_sequence(12))", context);
        context.changePageFrameSizes(BIG_FRAME_ROWS, BIG_FRAME_ROWS);
    }

    private static void createShapeTables() throws SqlException {
        // q holds 128 rows, one a minute from t's first one, of t's four keys; d4 one row per value
        // of d1.w, which a serial join over the join looks up.
        execute("""
                CREATE TABLE q AS (
                    SELECT (x % 4)::INT k, x * 7 p, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(128)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE d4 AS (SELECT x * 10 w, x * 1000 z FROM long_sequence(4))");
    }

    private static void createTables() throws SqlException {
        createTables(engine, sqlExecutionContext);
    }

    private static void createTables(CairoEngine db, SqlExecutionContext context) throws SqlException {
        // 128 rows in one partition, cut into 32 frames of four rows.
        db.execute("""
                CREATE TABLE t AS (
                    SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(128)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """, context);
        // d1 holds one row of each of t's keys, w 10 times the key, from 10 to 40, and d3 three rows
        // of each, whose w, 1,000 and up, passes every probe row's v.
        db.execute("CREATE TABLE d1 AS (SELECT (x % 4)::INT k, x * 10 w FROM long_sequence(4))", context);
        db.execute("CREATE TABLE d3 AS (SELECT (x % 4)::INT k, x * 1000 w FROM long_sequence(12))", context);
        context.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
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

    private static String printFromCurrentRow(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        do {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        } while (cursor.hasNext());
        return sink.toString();
    }

    private static String printFromNextRow(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        while (cursor.hasNext()) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
        return sink.toString();
    }

    private static String printRows(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            return printFromNextRow(cursor, factory.getMetadata());
        }
    }

    // Reads JOIN_D1's rows into the sink, through the first row of the given frame or to the end,
    // and adds the frames dispatched once the consumer read each frame's first row.
    private static void readFrames(
            RecordCursor cursor,
            RecordMetadata metadata,
            AsyncHashJoinRecordCursorFactory join,
            int lastFrame,
            IntList dispatchedFrames,
            StringSink sink
    ) {
        for (int row = 0; cursor.hasNext(); row++) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
            if (row % FRAME_ROWS == 0) {
                dispatchedFrames.add(join.getDispatchedFrameCount());
                if (row / FRAME_ROWS == lastFrame) {
                    return;
                }
            }
        }
    }
}
