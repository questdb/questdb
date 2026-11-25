/*******************************************************************************
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

package io.questdb.test.cairo.covering;

import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the multi-chunk (key, partition) resume branch of the per-key
 * (unordered) covering scan.
 * <p>
 * {@code cairo.sql.page.frame.max.rows} defaults to 1,000,000, and every
 * (key, partition) measured elsewhere in this feature fit inside a single
 * frame -- so the resume branch, where a key's drain spans several frames, has
 * never run. That branch is where the original per-key loop bug lived:
 * {@code fillFrameForKeyCheap} clears its resume state when the frame it
 * returns was the key's LAST chunk, so a caller that advances to the next key
 * only when a frame comes back {@code null} re-opens any key that fits in a
 * SINGLE frame and emits it forever. The contract is documented on
 * {@code CoveringIndexRecordCursorFactory.MultiKeyCoveringPageFrameCursor#isKeyMidDrain()}:
 * a non-null return does not mean the key has more rows -- only
 * {@code isKeyMidDrain()} answers that.
 * <p>
 * This suite shrinks the frame cap to 100 rows so each of the four keys spans
 * roughly 100 chunks per partition, forcing {@code resumeKeyDrain()} to run
 * many times per key instead of zero.
 * <p>
 * The cap is forced through
 * {@link CoveringIndexRecordCursorFactory#setMaxRowsPerFrameForTesting(int)}, NOT
 * through {@code cairo.sql.page.frame.max.rows}. That property cannot reach this
 * scan from an instance {@code @Before}: the covering cursor reads its cap from
 * {@code SqlExecutionContext.getPageFrameMaxRows()}, which
 * {@code SqlExecutionContextImpl} captures into a FINAL field in its constructor,
 * and that constructor runs once in {@code AbstractCairoTest.setUpStatic()} -- a
 * {@code @BeforeClass}, before any {@code @Before} can override the property.
 * (The sibling {@code CAIRO_SQL_PARALLEL_GROUPBY_ENABLED} override works only
 * because {@code AbstractCairoTest.setUp()} explicitly re-pushes the parallel
 * flags into the live context; there is no such push for page-frame sizes.) An
 * earlier revision of this suite set the property and silently ran the whole test
 * at the 1,000,000-row default, where every key fits in ONE frame and the resume
 * branch never executes -- see {@code assertDrainSpannedFrames} for the guard
 * that now makes that failure loud.
 * {@code sqlExecutionContext.changePageFrameSizes(1, 100)} would also work, but
 * the {@code @TestOnly} setter is the convention already used by the sibling
 * resume tests in {@code CoveringIndexTest} and
 * {@code CoveringIndexMultiKeyOrderingTest}, and it cannot be undone mid-query by
 * {@code restoreToDefaultPageFrameSizes()}.
 */
public class CoveringIndexPerKeyResumeTest extends AbstractCoveringIndexQueryTest {

    // 40 000 rows over 4 keys in ONE partition = 10 000 rows per key. At
    // MAX_ROWS_PER_FRAME = 100 a key needs 100 chunks, so it is resumed 99 times;
    // four keys per execution => 396 resumes. Assert a lower bound rather than the
    // exact figure so partition/chunk arithmetic changes do not make the guard
    // brittle -- any value at or above this is unreachable at the default cap,
    // where the correct answer is exactly 0.
    private static final long MIN_EXPECTED_RESUMES = 300;
    private static final int MAX_ROWS_PER_FRAME = 100;

    @After
    public void resetFrameCap() {
        // Static override: MUST be cleared or it leaks into every later test class
        // in the same JVM fork.
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(MAX_ROWS_PER_FRAME);
    }

    // Bounded so the regression this pins fails FAST. Per isKeyMidDrain()'s
    // contract a per-key loop that advances on a null frame instead of on
    // !isKeyMidDrain() re-opens a single-frame key and emits it forever: the
    // cursor never terminates, so without a timeout the failure mode is a stalled
    // fork that only the surefire timeout ends, minutes later and with no useful
    // message.
    @Test(timeout = 120_000)
    public void testPerKeySpansManyFramesPerPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 10 000 rows per key in one partition against a 100-row frame cap: ~100
            // chunks per key, so every key's drain spans many frames and resumeKeyDrain()
            // runs repeatedly instead of never.
            createTelemetryLarge();
            final String where = " WHERE param_id IN ('SFID','HOTMIC','KCAS','CALT') ORDER BY param_id";
            // count() is the arm that catches the original bug: if the per-key loop
            // re-opens a drained key instead of advancing past it, the key emits its rows
            // forever and the cursor never terminates -- the @Test timeout above, not a
            // row mismatch, is what reports that. max()/first()/last() catch the weaker
            // failure where a chunk boundary drops or duplicates a bounded number of rows.
            CoveringIndexRecordCursorFactory.resetKeyDrainResumesForTesting();
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(
                    "SELECT param_id, max(value), count() FROM telemetry" + where,
                    "SELECT /*+ no_index */ param_id, max(value), count() FROM telemetry" + where
            );
            // Vacuity guard 1 (the execution really ran per-key) and 2 (the drain really
            // spanned frames).
            assertRanPerKey();
            assertDrainSpannedFrames();

            CoveringIndexRecordCursorFactory.resetKeyDrainResumesForTesting();
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(
                    "SELECT param_id, first(value), last(value) FROM telemetry" + where,
                    "SELECT /*+ no_index */ param_id, first(value), last(value) FROM telemetry" + where
            );
            assertRanPerKey();
            assertDrainSpannedFrames();
        });
    }

    /**
     * Non-vacuity guard: prove the per-key drain actually crossed frame boundaries.
     * <p>
     * Nothing user-visible distinguishes "this key fit in one frame" from "this key
     * spanned 100" -- not the plan text, not the result rows, not the row count. The
     * only observable is {@code resumeKeyDrain()}'s own call count, which is why this
     * branch carries a {@code @TestOnly} counter
     * ({@link CoveringIndexRecordCursorFactory#getKeyDrainResumesForTesting()}).
     * Without this assertion a frame cap that silently failed to land -- exactly what
     * happened here once already -- leaves every assertion in this class still
     * passing while the branch under test never executes.
     */
    /**
     * Non-vacuity guard: prove the execution really took the per-key mode.
     * <p>
     * This used to assert the PLAN -- {@code frames: per-key (unordered)} -- which is blind to
     * it. The plan prints the plan-stable PERMISSION, granted at code generation; whether a
     * given open exercises the permission is decided per execution by the density and
     * frame-count gates, and merged returns the same rows. The plan assertion therefore passed
     * under three separate mutations that broke per-key mode outright. The mode-selection
     * counters are the per-execution answer, and the log record the factory writes at INFO is
     * the same fact for a user who is not running a test.
     */
    private static void assertRanPerKey() {
        Assert.assertTrue(
                "the execution fell back to the timestamp-ordered merge, so nothing below this"
                        + " point exercised the per-key resume branch. Merged returns the same rows,"
                        + " and the plan prints the plan-stable permission either way, so only this"
                        + " assertion can tell the difference. Check the density and frame-count"
                        + " gates against this fixture: "
                        + CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() + " per-key open(s), "
                        + CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting() + " merged.",
                CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0
        );
        Assert.assertEquals(
                "some open of this query fell back to the merge while another took per-key",
                0,
                CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting()
        );
    }

    private static void assertDrainSpannedFrames() {
        final long resumes = CoveringIndexRecordCursorFactory.getKeyDrainResumesForTesting();
        Assert.assertTrue(
                "per-key drain never resumed mid-key: the " + MAX_ROWS_PER_FRAME + "-row frame cap did not"
                        + " reach the covering scan, so this test proved nothing about the resume branch."
                        + " Expected at least " + MIN_EXPECTED_RESUMES + " resumeKeyDrain() calls, got " + resumes,
                resumes >= MIN_EXPECTED_RESUMES
        );
    }

    /**
     * Local to this class: the shared fixture's 10 000 rows over the DEFAULT
     * 1,000,000-row frame cap never span a frame boundary, which defeats the whole
     * point of this suite. Deliberately NOT added to the shared
     * {@link AbstractCoveringIndexQueryTest}: committed tests over the shared
     * generators pin exact plan text and exact result rows derived from their
     * specific spacing and row counts.
     */
    private void createTelemetryLarge() throws Exception {
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        // 40k rows over 4 keys, one partition, 100-row frames -> ~100 chunks per key.
        execute("INSERT INTO telemetry SELECT (x * 1000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " x::double" +
                " FROM long_sequence(40000)");
    }
}
