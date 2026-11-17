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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static io.questdb.griffin.engine.table.AsyncHashJoinAtom.*;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's stepper warm-up: a factory's probe tasks, count-only tasks and
 * continuation chunks run the shared stepper template until the consumer has collected
 * {@link AsyncHashJoinAtom#STEPPER_WARM_UP_ROWS} probe rows over the factory's executions, and the
 * stages' hidden copies from then on, each picking one when it starts. The tests lower the
 * threshold. Both write the same rows, which equal the ordinary plan's, in order.
 * <p>
 * The probe table t holds 32 rows in one partition, cut into eight frames of four rows, and the
 * dispatch limit is one, so that each frame's task starts once the consumer collected the frame
 * before it: frame f's task sees the 4 * f rows of the frames before it, plus the rows the
 * factory's earlier executions collected.
 */
public class AsyncHashJoinWarmUpTest extends AbstractCairoTest {
    private static final String CONTINUED = "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE d.w > t.v % 3";
    private static final int FRAME_COUNT = 8;
    private static final int FRAME_ROWS = 4;
    private static final String SINGLE = "SELECT t.v, u.w FROM t LEFT JOIN u ON t.k = u.k";
    // s is smaller than t, so the first join hashes s and probes t's frames; the second join reads its
    // key from the first join's build rows.
    private static final String SWAPPED_STACK = "SELECT s.k, s.sv, t.v, u.w FROM s JOIN t ON s.k = t.k JOIN u ON s.lk = u.k";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
    }

    @After
    public void tearDownWarmUp() {
        AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(STEPPER_WARM_UP_ROWS);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCachedFactoryGoesOnAcrossExecutions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // An execution collects 32 rows: the first runs the template alone, the second starts
            // on it and switches once 40 rows were collected, after its first two frames, and the
            // third runs the copies alone.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(40);
            final String expected = ordinary(SINGLE, sqlExecutionContext);
            try (RecordCursorFactory factory = select(SINGLE)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final AsyncHashJoinAtom atom = join.getAtom();
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, FRAME_COUNT, 0);
                Assert.assertEquals(32, atom.getProbedRowCount());
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, 2, FRAME_COUNT - 2);
                Assert.assertEquals(64, atom.getProbedRowCount());
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, 0, FRAME_COUNT);
                Assert.assertEquals(96, atom.getProbedRowCount());
            }
            // A factory compiled again starts over.
            try (RecordCursorFactory factory = select(SINGLE)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, FRAME_COUNT, 0);
            }
        });
    }

    @Test
    public void testContinuations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A task writes four tuples, its frame's rows, and the frame's up to twelve pairs, a key
            // of d's three rows per probe row, leave the rest to continuation chunks of four. The
            // threshold is an execution's rows: the first execution runs the template alone, every
            // chunk of its last frame included, which runs before the consumer collects the frame,
            // and the second the copies alone.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(FRAME_COUNT * FRAME_ROWS);
            final String expected = ordinary(CONTINUED, sqlExecutionContext);
            try (RecordCursorFactory factory = select(CONTINUED)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, FRAME_COUNT, 0);
                final long chunks = atom.getStepperRunCount(STEPPER_RUN_CONTINUATION, false);
                Assert.assertTrue(chunks >= FRAME_COUNT);
                assertRuns(atom, STEPPER_RUN_CONTINUATION, chunks, 0);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, 0, FRAME_COUNT);
                assertRuns(atom, STEPPER_RUN_CONTINUATION, 0, chunks);
            }
            // Within one execution, a lower threshold switches after the third frame: each frame's
            // chunks run what its task ran, since the consumer collects the frame after them.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(10);
            try (RecordCursorFactory factory = select(CONTINUED)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, 3, FRAME_COUNT - 3);
                final long templateChunks = atom.getStepperRunCount(STEPPER_RUN_CONTINUATION, false);
                final long copyChunks = atom.getStepperRunCount(STEPPER_RUN_CONTINUATION, true);
                Assert.assertTrue(templateChunks >= 3);
                Assert.assertTrue(copyChunks >= FRAME_COUNT - 3);
            }
        });
    }

    @Test
    public void testCopiesDefinedOnWarmUp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A factory defines no stepper copy as it compiles or while it runs the template: the
            // execution that collects the warm-up's 40 rows, the second, defines them, one per stage
            // and one more for a first join that may swap its inputs, each a class of its own.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(40);
            for (String sql : new String[]{SINGLE, SWAPPED_STACK}) {
                final int stageCount = sql.equals(SINGLE) ? 1 : 2;
                final int copyCount = sql.equals(SINGLE) ? 1 : 3;
                final String expected = ordinary(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    final AsyncHashJoinAtom atom = join.getAtom();
                    Assert.assertEquals(sql, 0, atom.getStepperCopyCount());
                    Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                    Assert.assertEquals(sql, 0, atom.getStepperCopyCount());
                    for (int k = 0; k < stageCount; k++) {
                        Assert.assertFalse(sql, join.getStepperClass(k).isHidden());
                    }
                    Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                    Assert.assertEquals(sql, copyCount, atom.getStepperCopyCount());
                    final Set<Class<?>> classes = new HashSet<>();
                    for (int k = 0; k < stageCount; k++) {
                        Assert.assertTrue(sql, join.getStepperClass(k).isHidden());
                        classes.add(join.getStepperClass(k));
                    }
                    Assert.assertEquals(sql, stageCount, classes.size());
                    // Later executions keep the copies.
                    Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                    Assert.assertEquals(sql, copyCount, atom.getStepperCopyCount());
                    for (int k = 0; k < stageCount; k++) {
                        Assert.assertTrue(sql, classes.contains(join.getStepperClass(k)));
                    }
                    assertRuns(atom, STEPPER_RUN_TASK, 0, FRAME_COUNT);
                }
            }
            // Under the default threshold, executions define none.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(STEPPER_WARM_UP_ROWS);
            try (RecordCursorFactory factory = select(SWAPPED_STACK)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(0, atom.getStepperCopyCount());
                result(factory, sqlExecutionContext);
                Assert.assertEquals(0, atom.getStepperCopyCount());
            }
        });
    }

    @Test
    public void testCopiesRunInTheOtherOrientation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A factory defines its copies within an execution, after its third frame, as each copy's
            // stage is bound then: in the orientation the execution picked, enumerated or not. The
            // copies then run an execution that picks the other orientation, whose first stage walks
            // its matches only when swapped, and whose later stage reads its key from the probe rows
            // rather than from the first stage's build rows, or the other way round. See
            // AsyncHashJoinStackSwapTest#testHandPlannedStackOverRemappedLeftInput for keys that sit
            // at other columns in the two orientations.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(10);
            final String sql = SWAPPED_STACK;
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                // s holds 4 rows and t 32: the execution hashes s and probes t's eight frames.
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertTrue(atom.isSwapped());
                assertRuns(atom, STEPPER_RUN_TASK, 3, FRAME_COUNT - 3);
                // s grows to 40 rows, past t: the execution hashes t and probes s's ten frames.
                execute("INSERT INTO s SELECT (x % 5)::INT k, (x % 4)::INT lk, x * 13 sv FROM long_sequence(36)");
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertFalse(atom.isSwapped());
                assertRuns(atom, STEPPER_RUN_TASK, 0, 10);
            }
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertFalse(atom.isSwapped());
                assertRuns(atom, STEPPER_RUN_TASK, 3, 10 - 3);
                // t grows to 48 rows, past s: the execution swaps the inputs again, and probes t's
                // twelve frames.
                execute("INSERT INTO t SELECT (x % 5)::INT k, (x % 3)::INT lk, x + 1_000 v, timestamp_sequence('2024-01-02', 60_000_000) ts FROM long_sequence(16)");
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertTrue(atom.isSwapped());
                assertRuns(atom, STEPPER_RUN_TASK, 0, 12);
            }
        });
    }

    @Test
    public void testCopiesWalkAsEachExecutionDecides() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // g's regex conjunct reads g alone, so an execution runs it over g's rows when g holds no
            // more rows than t, and over the pairs otherwise, which makes the workers walk both
            // stages' matches. The copies, defined while the first execution walks none, walk them
            // in the second.
            execute("CREATE TABLE g AS (SELECT (x % 3)::INT k, x * 1_000 w, ('a' || (x % 4))::VARCHAR name FROM long_sequence(24))");
            final String sql = "SELECT t.v, d.w, g.w gw FROM t JOIN d ON t.k = d.k LEFT JOIN g ON t.lk = g.k AND g.name ~ 'a[12]'";
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(10);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final AsyncHashJoinAtom atom = join.getAtom();
                for (int run = 0; run < 2; run++) {
                    Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                    assertRuns(atom, STEPPER_RUN_TASK, run == 0 ? 3 : 0, run == 0 ? FRAME_COUNT - 3 : FRAME_COUNT);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertFalse(atom.isSwapped());
                        Assert.assertEquals(run == 0, atom.getStage(1).isBuildOnFilterApplied());
                    }
                    if (run == 0) {
                        // g grows to 40 rows, past t's 32.
                        execute("INSERT INTO g SELECT (x % 3)::INT k, x * 1_000 + 7 w, ('a' || (x % 4))::VARCHAR name FROM long_sequence(16)");
                    }
                }
            }
        });
    }

    @Test
    public void testCountOnlyTasksAndCountedRest() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(FRAME_COUNT * FRAME_ROWS);
            final long rows = countOrdinary(CONTINUED);
            try (RecordCursorFactory factory = select(CONTINUED)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                // A count from the start dispatches a count-only task per frame: the first count runs
                // the template, and collects the rows that warm the factory up for the second.
                for (int i = 0; i < 2; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(rows, counter.get());
                    }
                    assertRuns(atom, STEPPER_RUN_COUNT_TASK, i == 0 ? FRAME_COUNT : 0, i == 0 ? 0 : FRAME_COUNT);
                    assertRuns(atom, STEPPER_RUN_TASK, 0, 0);
                }
            }
            // A count after the first row counts the rest of the first frame, which its task left to a
            // continuation, on the owner's slot, and the other frames on count-only tasks.
            try (RecordCursorFactory factory = select(CONTINUED)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                for (int i = 0; i < 2; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(rows - 1, counter.get());
                    }
                    final boolean isWarm = i > 0;
                    assertRuns(atom, STEPPER_RUN_TASK, isWarm ? 0 : 1, isWarm ? 1 : 0);
                    assertRuns(atom, STEPPER_RUN_COUNTED_REST, isWarm ? 0 : 1, isWarm ? 1 : 0);
                    assertRuns(atom, STEPPER_RUN_COUNT_TASK, isWarm ? 0 : FRAME_COUNT - 1, isWarm ? FRAME_COUNT - 1 : 0);
                }
            }
        });
    }

    @Test
    public void testDefaultThreshold() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            Assert.assertEquals(50_000_000, STEPPER_WARM_UP_ROWS);
            final String expected = ordinary(SINGLE, sqlExecutionContext);
            try (RecordCursorFactory factory = select(SINGLE)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                for (int i = 1; i <= 3; i++) {
                    Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                    assertRuns(atom, STEPPER_RUN_TASK, FRAME_COUNT, 0);
                    Assert.assertEquals(i * FRAME_COUNT * FRAME_ROWS, atom.getProbedRowCount());
                }
            }
        });
    }

    @Test
    public void testEveryShapeOnEitherStepper() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Stacks whose stages walk their matches on the workers and hand their chains to the
            // consumer, residuals and post-join filters, a swapped first join under later stages
            // and a count, on the template alone, the copies alone and both within an execution.
            final String[] shapes = {
                    SINGLE,
                    CONTINUED,
                    "SELECT t.v, u.w, d.w dw, e.w ew FROM t LEFT JOIN u ON t.k = u.k JOIN d ON t.k = d.k LEFT JOIN d e ON t.lk = e.k",
                    "SELECT t.v, u.w, d.w dw FROM t LEFT JOIN u ON t.k = u.k AND u.w > t.v LEFT JOIN d ON t.k = d.k WHERE d.w <> t.v",
                    "SELECT t.v, d.w, u.w uw FROM t JOIN d ON t.k = d.k LEFT JOIN u ON t.lk = u.k AND u.w < d.w",
                    SWAPPED_STACK,
                    "SELECT count(*) FROM t JOIN d ON t.k = d.k LEFT JOIN u ON t.lk = u.k",
            };
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            for (long threshold : new long[]{0, 10, Long.MAX_VALUE}) {
                AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(threshold);
                for (String sql : shapes) {
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinAtom atom = getAtom(factory);
                        Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                        final long template = countRuns(atom, false);
                        final long copies = countRuns(atom, true);
                        Assert.assertTrue(sql, template + copies >= FRAME_COUNT);
                        if (threshold == 0) {
                            Assert.assertEquals(sql, 0, template);
                        } else if (threshold == Long.MAX_VALUE) {
                            Assert.assertEquals(sql, 0, copies);
                        } else {
                            // The first task starts before any row was collected. A count dispatches
                            // its tasks past the dispatch limit, so they may all start before then.
                            Assert.assertTrue(sql, template > 0);
                            Assert.assertTrue(sql, copies > 0 || sql.startsWith("SELECT count(*)"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testTasksSwitchWithinAnExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Frames 0, 1 and 2 start before 10 rows were collected, frames 3 to 7 after.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(10);
            final String expected = ordinary(SINGLE, sqlExecutionContext);
            try (RecordCursorFactory factory = select(SINGLE)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, 3, FRAME_COUNT - 3);
                Assert.assertEquals(32, atom.getProbedRowCount());
            }
            // The threshold of an execution's rows less one switches for the last frame alone.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(FRAME_COUNT * FRAME_ROWS - FRAME_ROWS);
            try (RecordCursorFactory factory = select(SINGLE)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, FRAME_COUNT - 1, 1);
            }
            // A threshold of zero runs the copies from the first task.
            AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(0);
            try (RecordCursorFactory factory = select(SINGLE)) {
                final AsyncHashJoinAtom atom = getAtom(factory);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                assertRuns(atom, STEPPER_RUN_TASK, 0, FRAME_COUNT);
            }
        });
    }

    private static void assertRuns(AsyncHashJoinAtom atom, int kind, long template, long copies) {
        Assert.assertEquals("template runs of kind " + kind, template, atom.getStepperRunCount(kind, false));
        Assert.assertEquals("copy runs of kind " + kind, copies, atom.getStepperRunCount(kind, true));
    }

    private static long countOrdinary(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            long rows = 0;
            while (cursor.hasNext()) {
                rows++;
            }
            return rows;
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    // The frames and chunks of the last execution that ran either stepper, every kind together.
    private static long countRuns(AsyncHashJoinAtom atom, boolean isWarm) {
        return atom.getStepperRunCount(STEPPER_RUN_TASK, isWarm)
                + atom.getStepperRunCount(STEPPER_RUN_COUNT_TASK, isWarm)
                + atom.getStepperRunCount(STEPPER_RUN_CONTINUATION, isWarm)
                + atom.getStepperRunCount(STEPPER_RUN_COUNTED_REST, isWarm);
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 5)::INT k, (x % 3)::INT lk, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(32)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        // u holds one row per key but key 4, d three rows per key.
        execute("CREATE TABLE u AS (SELECT (x % 4)::INT k, x * 7 w FROM long_sequence(4))");
        execute("CREATE TABLE d AS (SELECT (x % 5)::INT k, x * 100 w FROM long_sequence(15))");
        // s, smaller than t, which a first join over both hashes.
        execute("CREATE TABLE s AS (SELECT (x % 5)::INT k, (x % 4)::INT lk, x * 11 sv FROM long_sequence(4))");
        sqlExecutionContext.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
    }

    private static AsyncHashJoinAtom getAtom(RecordCursorFactory factory) {
        final AsyncHashJoinRecordCursorFactory join = find(factory);
        Assert.assertNotNull(join);
        return join.getAtom();
    }
}
