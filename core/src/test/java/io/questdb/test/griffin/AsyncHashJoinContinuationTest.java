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
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinSpec;
import io.questdb.griffin.engine.table.HashJoinInput;
import io.questdb.griffin.engine.table.HashJoinStageSpec;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The cap on the tuples that a parallel hash join's probe task writes: a task stops at a probe row's
 * boundary once its frame's tuples reach {@code max(cairo.sql.parallel.hash.join.task.output.max.rows,
 * the frame's rows)}, and the consumer joins the rest of the frame in chunks of the same cap on the
 * owner's slot. Only a stage that walks its matches on the workers, one whose pairs run an ON
 * residual or a post-join filter, can reach the cap: a stage without them writes at most one tuple
 * per probe row, and so does a LEFT join's stage whose build ran its whole ON residual over the
 * build's rows. The rows and their order match the ordinary plan across chunks, scan directions
 * and probe filters, and a cancellation before a chunk or inside one ends the query with the
 * cancellation, leaks nothing, and leaves the factory to run again.
 */
public class AsyncHashJoinContinuationTest extends AbstractCairoTest {
    // LEFT joins whose ON residual reads d alone, which the build runs over d's rows: the stage hands
    // d's chains to the consumer, one tuple per probe row, which never outgrows the cap.
    private static final String[] PUSHED = {
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.w % 2 = 0",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.sym = d.s AND d.w % 2 = 1",
    };
    // Each probe row of t meets up to six rows of d, under a residual or a post-join filter that the
    // pairs run.
    private static final String[] QUERIES = {
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.w % 2 = t.v % 2",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k WHERE d.w IS NULL OR d.w % 3 <> 0",
            "SELECT t.k, t.v, d.w, d.s FROM t JOIN d ON t.k = d.k WHERE t.v > 1 AND d.w + t.v > 20",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.sym = d.s AND d.w % 2 <> t.v % 2",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND t.sym = d.s WHERE d.w IS NULL OR d.w < t.v",
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
    public void testBackwardScanContinues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            for (int frameRows : new int[]{3, 8}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (String sql : QUERIES) {
                    // The left input scans backward, and so does each continuation of a frame.
                    final String backward = sql + " ORDER BY t.ts DESC";
                    TestUtils.assertContains(assertParallelHashJoin(backward, sqlExecutionContext), "Frame backward scan on: t");
                    Assert.assertTrue(backward, continuations(backward) > 0);
                }
                for (String sql : PUSHED) {
                    final String backward = sql + " ORDER BY t.ts DESC";
                    TestUtils.assertContains(assertParallelHashJoin(backward, sqlExecutionContext), "Frame backward scan on: t");
                    Assert.assertEquals(backward, 0, continuations(backward));
                }
            }
        });
    }

    @Test
    public void testCancelledBeforeContinuationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = QUERIES[0];
            final String expected = ordinary(sql, sqlExecutionContext);
            sqlExecutionContext.changePageFrameSizes(4, 4);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            // One frame in flight at a time, so that no frame after the first is joined before the
            // cancellation.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    // The first frame's task stopped at its cap, so its frame goes on in a chunk that
                    // the cancellation keeps from running.
                    Assert.assertTrue(join.getAtom().getRing().getResumePosition(0) > 0);
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
                Assert.assertEquals(0, engine.getBusyReaderCount());
                breaker.reset();
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testCancelledInsideChunkAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // Every probe row of t matches d's three rows of key 1. With frames of four rows, which
            // the breaker is checked every four pairs of, and a cap of four tuples, the first frame's
            // task joins two rows, six pairs, and the first chunk of its continuation starts at the
            // seventh pair.
            execute("CREATE TABLE t AS (SELECT 1 k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT 1 k, x w FROM long_sequence(3))");
            sqlExecutionContext.changePageFrameSizes(4, 4);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            final String expected = ordinaryRows("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                // The filter trips the breaker at the chunk's first pair, and the chunk observes it as
                // it counts its fourth pair down, before it runs the filter over that pair.
                assertCancelledAt(breaker, "t", "d", false, 7, 9, 1, expected);
                // Inside the first frame's task, at its second pair: the task observes it at its fourth.
                assertCancelledAt(breaker, "t", "d", false, 2, 3, 0, expected);
                // The same over the rows that a probe filter kept, every row here.
                assertCancelledAt(breaker, "t", "d", true, 7, 9, 1, expected);
                assertCancelledAt(breaker, "t", "d", true, 2, 3, 0, expected);
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testCancelledInsideUniqueBuildTaskAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            // A build of one row per key positions its payload on the lookup itself: every probe row
            // has one pair, so the task's fourth pair is its fourth row.
            execute("CREATE TABLE t AS (SELECT (x % 3)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE u AS (SELECT (x % 3)::INT k, x w FROM long_sequence(3))");
            sqlExecutionContext.changePageFrameSizes(4, 4);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            final String expected = ordinaryRows("SELECT * FROM t LEFT JOIN u ON t.k = u.k", sqlExecutionContext);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                assertCancelledAt(breaker, "t", "u", false, 2, 3, 0, expected);
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testContinuations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int cap : new int[]{1, 7, 20}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, cap);
                for (int frameRows : new int[]{1, 3, 8}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    for (String sql : QUERIES) {
                        assertParallelHashJoin(sql, sqlExecutionContext);
                        // A frame of one row never continues, since a probe row's pairs always finish
                        // in one chunk; with the cap at its floor of a frame's rows, the frames' tuples,
                        // up to six per probe row, outgrow it.
                        final int continuations = continuations(sql);
                        final String label = sql + ", cap " + cap + ", frame rows " + frameRows;
                        if (frameRows == 1) {
                            Assert.assertEquals(label, 0, continuations);
                        } else if (cap == 1) {
                            Assert.assertTrue(label, continuations > 0);
                        }
                    }
                    for (String sql : PUSHED) {
                        assertParallelHashJoin(sql, sqlExecutionContext);
                        Assert.assertEquals(sql + ", cap " + cap + ", frame rows " + frameRows, 0, continuations(sql));
                    }
                }
            }
        });
    }

    @Test
    public void testFrameRowsLiftTheCap() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cap below the frame's rows takes the frame's rows instead, so a join of at most one
            // match per probe row never needs a continuation: a unique build under a residual and a
            // post-join filter, and a stage without either, which leaves its chains to the consumer.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            sqlExecutionContext.changePageFrameSizes(8, 8);
            for (String sql : new String[]{
                    "SELECT t.k, t.v, u.w FROM t LEFT JOIN u ON t.k = u.k AND u.w > t.v",
                    "SELECT t.k, t.v, u.w FROM t LEFT JOIN u ON t.k = u.k WHERE u.w IS NULL OR u.w > t.v",
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                Assert.assertEquals(sql, 0, continuations(sql));
            }
            // At its default, the cap is a probe frame's most rows, the smaller of the page frame
            // maximum, 1,000,000, and the join's own, 100,000, which no frame of these tables reaches
            // with its tuples.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, null);
            sqlExecutionContext.restoreToDefaultPageFrameSizes();
            Assert.assertEquals(1_000_000, configuration.getSqlPageFrameMaxRows());
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinPageFrameMaxRows());
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinTaskOutputMaxRows());
            for (String sql : QUERIES) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                Assert.assertEquals(sql, 0, continuations(sql));
            }
        });
    }

    @Test
    public void testHotKeyOverrunsTheCap() throws Exception {
        assertMemoryLeak(() -> {
            // The first probe row's key matches fifty build rows, which a residual keeps, so the first
            // frame's task writes them all, past the cap of the frame's four rows, and stops after that
            // row: one probe row's pairs always finish in one chunk.
            execute("CREATE TABLE t AS (SELECT CASE WHEN x = 1 THEN 7 ELSE (x % 3)::INT END k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT CASE WHEN x <= 50 THEN 7 ELSE (x % 3)::INT END k, x w FROM long_sequence(56))");
            sqlExecutionContext.changePageFrameSizes(4, 4);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 1_000";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final HashJoinTupleRing ring = find(factory).getAtom().getRing();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(50, ring.getTuples(0).size());
                    Assert.assertEquals(1, ring.getResumePosition(0));
                    // The entry's memory, which the hot key grew, is the execution's.
                    Assert.assertTrue(ring.getAllocatedBytes() >= 50L * ring.getTupleBytes());
                }
                Assert.assertEquals(0, ring.getAllocatedBytes());
            }
            // A residual of d alone, which the build runs over d's rows, leaves the pairs no filter:
            // the stage hands the hot key's chain to the consumer, so the first frame's task writes
            // one tuple for each of the frame's four rows and joins the whole frame.
            final String pushed = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0";
            assertParallelHashJoin(pushed, sqlExecutionContext);
            try (RecordCursorFactory factory = select(pushed)) {
                final HashJoinTupleRing ring = find(factory).getAtom().getRing();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(4, ring.getTuples(0).size());
                    Assert.assertEquals(-1, ring.getResumePosition(0));
                }
            }
        });
    }

    @Test
    public void testLimitAndToTopMidContinuation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            for (String sql : QUERIES) {
                final String expected = ordinaryRows(sql, sqlExecutionContext);
                // A LIMIT closes the cursor inside a frame whose continuation is pending. The optimiser
                // gives a LIMIT to the left input's filter, which the join then cannot steal.
                if (!sql.contains("t.v > 1")) {
                    assertParallelHashJoin(sql + " LIMIT 5", sqlExecutionContext);
                    assertParallelHashJoin(sql + " LIMIT -5", sqlExecutionContext);
                }
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // toTop() inside the first frame's first chunk and inside a later chunk each
                        // read the whole result again.
                        for (int stop : new int[]{2, 9, Integer.MAX_VALUE}) {
                            final String rows = printRows(cursor, factory.getMetadata(), stop);
                            Assert.assertTrue(sql + '\n' + rows, expected.startsWith(rows));
                            cursor.toTop();
                        }
                        Assert.assertEquals(expected, printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE));
                    }
                    Assert.assertTrue(sql, join.getAtom().getContinuationCount() > 0);
                }
            }
        });
    }

    @Test
    public void testOwnerFiltersRewindAndClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 3)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT (x % 3)::INT k, x w FROM long_sequence(6))");
            final String expected = ordinaryRows("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            // The owner's residual and post-join filter go back to the top with the cursor, as the
            // serial join's filter and the filter over it do, and learn that the cursor closed, also
            // when one of them fails as it does: the cursor goes on releasing the rest, the build and
            // the scans' readers included, reports the failure, and opens again.
            for (boolean isThrowing : new boolean[]{false, true}) {
                final TrippingFilter onFilter = new TrippingFilter(breaker, -1);
                final TrippingFilter postFilter = new TrippingFilter(breaker, -1);
                postFilter.isThrowingOnClose = isThrowing;
                try (
                        RecordCursorFactory probe = select("SELECT * FROM t");
                        RecordCursorFactory build = select("SELECT * FROM d");
                        AsyncHashJoinRecordCursorFactory factory = newLeftJoin(probe, build, null, onFilter, postFilter)
                ) {
                    final String plan = plan(factory, sqlExecutionContext);
                    TestUtils.assertContains(plan, "onFilter: TrippingFilter");
                    TestUtils.assertContains(plan, "postJoinFilter: TrippingFilter");
                    for (int execution = 1; execution < 3; execution++) {
                        final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                        Assert.assertTrue(expected.startsWith(printRows(cursor, factory.getMetadata(), 5)));
                        Assert.assertEquals(execution - 1, onFilter.toTops);
                        cursor.toTop();
                        Assert.assertEquals(execution, onFilter.toTops);
                        Assert.assertEquals(execution, postFilter.toTops);
                        Assert.assertEquals(expected, printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE));
                        Assert.assertEquals(execution - 1, postFilter.closedCursors);
                        try {
                            cursor.close();
                            Assert.assertFalse("expected the post-join filter to fail as the cursor closes", isThrowing);
                        } catch (CairoException e) {
                            Assert.assertTrue(isThrowing);
                            TestUtils.assertContains(e.getFlyweightMessage(), "injected cursorClosed failure");
                        }
                        Assert.assertEquals(execution, onFilter.closedCursors);
                        Assert.assertEquals(execution, postFilter.closedCursors);
                        Assert.assertNull(factory.getAtom().getStage(0).getFrozenBuild());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    }
                }
            }
        });
    }

    @Test
    public void testProbeFilterContinues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            sqlExecutionContext.changePageFrameSizes(8, 8);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (String sql : new String[]{
                        // A continuation walks the rows that the probe filter kept, in both scan
                        // directions.
                        "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 2 = t.v % 2 WHERE t.v % 3 <> 0",
                        "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 2 = t.v % 2 WHERE t.v % 3 <> 0 ORDER BY t.ts DESC",
                        "SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.str ~ 'str[12]' AND d.w > t.v",
                        "SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.str ~ 'str[12]' AND d.w > t.v ORDER BY t.ts DESC",
                }) {
                    TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "filter: ");
                    Assert.assertTrue(sql, continuations(sql) > 0);
                }
                // A stage whose build ran its whole residual writes one tuple per kept row.
                for (String sql : new String[]{
                        "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 2 = 0 WHERE t.v % 3 <> 0",
                        "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 2 = 0 WHERE t.v % 3 <> 0 ORDER BY t.ts DESC",
                }) {
                    TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "filter: ");
                    Assert.assertEquals(sql, 0, continuations(sql));
                }
            }
        });
    }

    @Test
    public void testUninterruptibleExecutionIgnoresTheBreaker() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 1 k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT 1 k, x w FROM long_sequence(3))");
            sqlExecutionContext.changePageFrameSizes(4, 4);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            final String expected = ordinaryRows("SELECT * FROM t LEFT JOIN d ON t.k = d.k", sqlExecutionContext);
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            // An execution that must not be interrupted, as a WAL apply's, reads every row although the
            // breaker trips inside the first frame's task and before its continuation.
            try (
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
                        @Override
                        public boolean isUninterruptible() {
                            return true;
                        }
                    }.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, breaker);
                    RecordCursorFactory probe = select("SELECT * FROM t");
                    RecordCursorFactory build = select("SELECT * FROM d");
                    AsyncHashJoinRecordCursorFactory factory = newLeftJoin(probe, build, null, new TrippingFilter(breaker, 2))
            ) {
                context.changePageFrameSizes(4, 4);
                Assert.assertEquals(expected, printRows(factory, context));
                Assert.assertTrue(factory.getAtom().getContinuationCount() > 0);
            }
        });
    }

    // Runs a LEFT join of the two tables on their first columns with a post-join filter that trips
    // the breaker at the given pair and passes every pair: the query fails with the cancellation once
    // the filter ran the given number of pairs, after the given number of continuation chunks; then
    // the same factory, with the breaker reset, reads every row. Each stage's stepper is a hidden
    // copy of the template, and then the shared template, as a JVM without hidden classes runs it.
    private static void assertCancelledAt(
            AtomicBooleanCircuitBreaker breaker,
            String probeTable,
            String buildTable,
            boolean isProbeFiltered,
            int tripAt,
            int pairsRun,
            int continuations,
            String expected
    ) throws SqlException {
        for (boolean isCopy : new boolean[]{true, false}) {
            // The copies run from the first task.
            useSteppers(isCopy);
            final TrippingFilter filter = new TrippingFilter(breaker, tripAt);
            try (
                    RecordCursorFactory probe = select("SELECT * FROM " + probeTable);
                    RecordCursorFactory build = select("SELECT * FROM " + buildTable);
                    AsyncHashJoinRecordCursorFactory factory = newLeftJoin(probe, build, isProbeFiltered ? new PassingFilter() : null, filter)
            ) {
                Assert.assertEquals(isCopy, factory.getStepperClass(0).isHidden());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                }
                Assert.assertEquals(pairsRun, filter.calls);
                Assert.assertEquals(continuations, factory.getAtom().getContinuationCount());
                Assert.assertEquals(0, factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                // The cancelled tasks and chunks ran the stepper under test.
                assertStepperRuns(factory, isCopy, probeTable);
                breaker.reset();
                filter.tripAt = -1;
                Assert.assertEquals(expected, printRows(factory, sqlExecutionContext));
            } finally {
                restoreSteppers();
            }
        }
    }

    // The continuation chunks that one execution of the query ran.
    private static int continuations(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            result(factory, sqlExecutionContext);
            return find(factory).getAtom().getContinuationCount();
        }
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT
                        CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 5)::INT END k,
                        x v,
                        ('s' || (x % 3))::SYMBOL sym,
                        CASE WHEN x % 7 = 0 THEN NULL ELSE 'str' || (x % 4) END str,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(40)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        // Six rows of d share each key, and its symbols have other keys than t's.
        execute("""
                CREATE TABLE d AS (
                    SELECT
                        (x % 5)::INT k,
                        x w,
                        ('s' || (2 - x % 3))::SYMBOL s
                    FROM long_sequence(30)
                )
                """);
        execute("CREATE TABLE u AS (SELECT (x % 5)::INT k, x * 9 w FROM long_sequence(5))");
    }

    // A LEFT join of two compiled scans on their first columns, planned by hand, with a post-join
    // filter of the test's own. The factory takes the scans that the two QueryProgress factories wrap,
    // and the wrappers close them again, which is a no-op.
    private static AsyncHashJoinRecordCursorFactory newLeftJoin(
            RecordCursorFactory probeQuery,
            RecordCursorFactory buildQuery,
            @Nullable BooleanFunction probeFilter,
            BooleanFunction postFilter
    ) throws SqlException {
        return newLeftJoin(probeQuery, buildQuery, probeFilter, null, postFilter);
    }

    // As above, with an ON residual of the test's own too.
    private static AsyncHashJoinRecordCursorFactory newLeftJoin(
            RecordCursorFactory probeQuery,
            RecordCursorFactory buildQuery,
            @Nullable BooleanFunction probeFilter,
            @Nullable BooleanFunction onFilter,
            BooleanFunction postFilter
    ) throws SqlException {
        final RecordCursorFactory probe = probeQuery.getBaseFactory();
        final RecordCursorFactory build = buildQuery.getBaseFactory();
        final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(keys.add(0, probe.getMetadata().getColumnType(0), 0, build.getMetadata().getColumnType(0)));
        final IntList probeColumns = identity(probe.getMetadata().getColumnCount());
        final IntList buildColumns = identity(build.getMetadata().getColumnCount());
        final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, new BytecodeAssembler(), keys,
                "t", probe.getMetadata(), probeColumns, "b", build.getMetadata(), buildColumns);
        final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, probeColumns.size() + buildColumns.size());
        metadata.copyColumnMetadataFrom("t", probe.getMetadata());
        metadata.copyColumnMetadataFrom("b", build.getMetadata());
        metadata.setTimestampIndex(probe.getMetadata().getTimestampIndex());
        final HashJoinStageSpec stage = new HashJoinStageSpec(
                new HashJoinInput(build, buildColumns, new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0)),
                keyLayout,
                true,
                "b.k=t.k",
                metadata,
                onFilter,
                null
        );
        stage.ofPostFilter(postFilter, null);
        final AsyncHashJoinSpec spec = new AsyncHashJoinSpec(
                new HashJoinInput(probe, probeColumns, new AsyncFilterContext(configuration, null, null, null, probeFilter, null, null, 1, 0, 0, 0)),
                stage,
                metadata,
                probe.followedOrderByAdvice(),
                probe.getScanDirection()
        );
        return new AsyncHashJoinRecordCursorFactory(
                engine,
                configuration,
                engine.getMessageBus(),
                spec,
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                1
        );
    }

    private static String printRows(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            return printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE);
        }
    }

    // Up to the given number of the cursor's next rows, as CursorPrinter prints them.
    private static String printRows(RecordCursor cursor, RecordMetadata metadata, int maxRows) {
        final StringSink sink = new StringSink();
        for (int rows = 0; rows < maxRows && cursor.hasNext(); rows++) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
        return sink.toString();
    }

    // A probe filter that keeps every row.
    private static final class PassingFilter extends BooleanFunction {

        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    }

    // A filter that passes every pair, trips the breaker at the given pair, and counts the times a
    // cursor that runs it goes back to the top and closes, which it may fail.
    private static final class TrippingFilter extends BooleanFunction {
        private final AtomicBooleanCircuitBreaker breaker;
        int calls;
        int closedCursors;
        boolean isThrowingOnClose;
        int toTops;
        int tripAt;

        TrippingFilter(AtomicBooleanCircuitBreaker breaker, int tripAt) {
            this.breaker = breaker;
            this.tripAt = tripAt;
        }

        @Override
        public void cursorClosed() {
            closedCursors++;
            if (isThrowingOnClose) {
                throw CairoException.nonCritical().put("injected cursorClosed failure");
            }
        }

        @Override
        public boolean getBool(Record rec) {
            if (++calls == tripAt) {
                breaker.cancel();
            }
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("TrippingFilter");
        }

        @Override
        public void toTop() {
            toTops++;
        }
    }
}
