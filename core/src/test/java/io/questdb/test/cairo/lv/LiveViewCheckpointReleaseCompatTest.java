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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Cross-version restore: a checkpoint tree written by the released 10.0.1 build, read back by
 * this one.
 * <p>
 * Everything else in the suite that reaches the legacy anchor-root shape reaches it through
 * this branch's own writers - {@code LiveViewFusionDisabledTest} flips
 * {@code cairo.sql.window.map.fusion.enabled} off, seals, and turns it back on. That covers the
 * upgrade adapter but not the premise underneath it: that the bytes on a real 10.0.x instance
 * are the bytes this branch's decoder expects. A writer and a decoder from the same tree agree
 * with each other by construction, so a shared misreading of the released layout would pass
 * every one of those cases.
 * <p>
 * The fixture in {@code /lv/lv_checkpoint_10_0_1.zip} closes that gap. It is a whole database
 * root - base table, live-view table and state, {@code _checkpoints} tree - emitted by an
 * unmodified 10.0.1 checkout and never touched by a writer from this branch. Its live view
 * carries the anchored cumulative shape this branch fuses into one
 * {@link LiveViewCheckpointWindowRoot} ({@code PAGE_KIND = 0x1d}) but 10.0.1 wrote as an anchor
 * root ({@code 0x1b}) plus a function root per window call ({@code 0x18}), so the restore has to
 * take the legacy tagged-union arm and hoist each function's own root into the fused runtime.
 * <p>
 * The load-bearing assertion is {@link LiveViewInstance#isCheckpointRestoreSucceeded()}. Without
 * it the cases prove nothing: a view whose restore threw would retire the timeline, replay from
 * the applied base and land on exactly the same rows, so a row-content oracle passes either way.
 * The rows are still compared - against a from-base recompute rather than against the runtime's
 * own arithmetic - because a restore that succeeded on a misread page would be worse than one
 * that failed.
 * <p>
 * To regenerate the fixture, copy {@code /lv/LiveViewReleaseFixtureGenerator.java.txt} into a
 * clean {@code 10.0.1} checkout's {@code io.questdb.test.cairo.lv} package and run it; the
 * constants below are the values it prints.
 */
public class LiveViewCheckpointReleaseCompatTest extends AbstractLiveViewCheckpointCompatTest {

    private static final String FIXTURE_RESOURCE = "/lv/lv_checkpoint_10_0_1.zip";
    // The simulated clock the fixture's own run left behind. This one starts above it, so the
    // flush cadence reads a forward-moving clock rather than one that jumped backwards.
    private static final long FIXTURE_END_MICROS = 2_500_000L;
    // Sealed boundaries the fixture carries. The generator drove one commit per boundary, so a
    // restart that retired the timeline and opened a fresh generation would answer 0 or 1 here.
    private static final int FIXTURE_SEALED_BOUNDARIES = 5;
    // The head the fixture's own last seal published: the live-view and base sequencer txns it
    // stands on.
    private static final long FIXTURE_HEAD_LV_SEQ_TXN = 5;
    private static final long FIXTURE_HEAD_BASE_SEQ_TXN = 5;
    private static final String DAILY_ANCHOR = "2026-01-01T";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // Matches the cadence the fixture was sealed under, so a commit made after the upgrade
        // seals a boundary of its own rather than waiting for a row budget to fill.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(2 * FIXTURE_END_MICROS);
    }

    @Test
    public void testAReleasedCheckpointConvertsToTheFusedShapeAndRestartsOffIt() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertTrue(
                        "the upgrade must restore off the released roots",
                        instance("lv").isCheckpointRestoreSucceeded()
                );

                // The first commit after the upgrade seals through this branch's writers, which
                // fuse the shape the released build kept in separate roots.
                insertAccount(job, timestamp(50), "acct-1", 100.0);
                Assert.assertTrue("the seal after the upgrade must publish a fused window root", isFusedHead("lv"));
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }

            // A second restart, now reading back the converted root rather than the released one.
            restartCycle();
            Assert.assertFalse("the converted view must stay valid across a restart", instance("lv").isInvalid());
            Assert.assertTrue(
                    "the restart must restore off the converted fused root",
                    instance("lv").isCheckpointRestoreSucceeded()
            );
            Assert.assertTrue("the restored head must still be the fused root", isFusedHead("lv"));
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");

            // The converted state keeps accumulating: a partition that came back empty would
            // answer this row's own amount rather than the running total.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                insertAccount(resumed, timestamp(60), "acct-2", 200.0);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }

            assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv")
                    .timestamp("created_at")
                    .expectSize()
                    .returns("created_at\taccount_id\tcumulative_sum\tcumulative_count\n" +
                            "2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1\n" +
                            "2026-01-01T09:00:10.000000Z\tacct-2\t11.0\t1\n" +
                            "2026-01-01T09:00:20.000000Z\tacct-1\t22.0\t2\n" +
                            "2026-01-01T09:00:30.000000Z\tacct-2\t42.0\t2\n" +
                            "2026-01-01T09:00:40.000000Z\tacct-1\t63.0\t3\n" +
                            "2026-01-01T09:00:50.000000Z\tacct-1\t163.0\t4\n" +
                            "2026-01-01T09:01:00.000000Z\tacct-2\t242.0\t3\n");
        });
    }

    @Test
    public void testAReleasedCheckpointWithASymbolKeyRebuildsFromTheBaseAfterTheSchemaChange() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();

            // The fixture is genuinely legacy-shaped: 10.0.1 has no fused root to write, so a
            // probe that reported one here would mean the case is testing this branch's own bytes.
            Assert.assertFalse(
                    "the fixture's head must be a legacy anchor root, not a fused window root",
                    isFusedHead("lv")
            );
            assertReleasedLineage("the fixture must arrive with the lineage the released build sealed");

            // The upgrade's first refresh cycle. The fixture's view partitions by a SYMBOL column,
            // and the released build sealed that key as a resolved STRING - this branch's compiled
            // runtime now keys the same column as a translated SYMBOL (LiveViewSymbolIdRegistry),
            // so the restore correctly refuses the schema-mismatched root ("live view checkpoint
            // anchor key schema does not match the compiled runtime") and rebuilds from the applied
            // base instead, the same recovery a torn or unreadable head takes.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse("a schema-mismatched checkpoint must not invalidate the view", instance.isInvalid());
            Assert.assertTrue("the restore must have run", instance.isCheckpointRestoreAttempted());
            // The rebuild-from-base fallback is itself a successful recovery - it is what
            // isCheckpointRestoreSucceeded distinguishes from a restore that threw and left the
            // runtime in a state a later cycle still has to repair - so this stays true even though
            // the schema mismatch means the restore did not reuse a single one of the released roots.
            Assert.assertTrue(
                    "the schema-mismatch fallback must still complete as a successful recovery",
                    instance.isCheckpointRestoreSucceeded()
            );
            assertNoRefreshFaults("lv");

            // The rebuild retires the released timeline and opens a fresh one: the lineage does not
            // carry forward, because the very thing this seals under is the schema the released
            // build's roots cannot describe.
            Assert.assertEquals(
                    "the rebuild opens a fresh generation rather than extending the released one",
                    1,
                    countSealedBoundaries("lv")
            );

            // A rebuild is only as good as the rows it lands on, so they are compared against a
            // from-base recompute.
            assertViewMatchesRecompute();
        });
    }

    private static String timestamp(int secondOfDay) {
        return DAILY_ANCHOR + String.format("09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    /**
     * Asserts the head the released build sealed is still the head in force: the same number of
     * logical boundaries, standing on the same live-view and base sequencer txns.
     * <p>
     * The boundary timestamp is deliberately not among these. It is runtime state the seal and
     * the restore publish, not something the live-view state file carries, so before the first
     * refresh it reads {@code LONG_NULL} whatever the timeline holds. The cases assert it
     * separately, after the restore that fills it in.
     */
    private void assertReleasedLineage(String message) {
        final LiveViewInstance instance = instance("lv");
        Assert.assertEquals(message + " [sealedBoundaries]", FIXTURE_SEALED_BOUNDARIES, countSealedBoundaries("lv"));
        Assert.assertEquals(message + " [headLvSeqTxn]", FIXTURE_HEAD_LV_SEQ_TXN, instance.getHeadCheckpointLvSeqTxn());
        Assert.assertEquals(message + " [headBaseSeqTxn]", FIXTURE_HEAD_BASE_SEQ_TXN, instance.getHeadCheckpointBaseSeqTxn());
    }

    /**
     * Compares the view against a from-base recompute of the same window. ANCHOR is live-view
     * syntax, so the daily bucket is written out as an ordinary partition term.
     */
    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, account_id, "
                        + "sum(amount) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_sum, "
                        + "count(account_id) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_count "
                        + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, double amount)
            throws Exception {
        execute("INSERT INTO tx VALUES ('" + timestamp + "', '" + account + "', " + amount + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Unpacks the fixture and registers its live view, without refreshing it yet, so a case may
     * inspect the released tree before this branch's runtime has touched it.
     */
    private void openFixture() throws IOException {
        replaceDbContent(FIXTURE_RESOURCE);
        engine.buildViewGraphs();
        Assert.assertFalse("the fixture must not carry an invalid view", instance("lv").isInvalid());
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }
}
