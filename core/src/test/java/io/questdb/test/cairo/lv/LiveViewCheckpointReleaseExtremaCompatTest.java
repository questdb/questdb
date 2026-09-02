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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Cross-version restore for the one released shape this branch deliberately <b>cannot</b>
 * restore, and what that costs.
 * <p>
 * {@link LiveViewCheckpointReleaseCompatTest} and
 * {@link LiveViewCheckpointReleaseShapesCompatTest} both read released trees whose state
 * layout did not move, so both prove a ladder carries forward. This case is their inverse.
 * An anchored cumulative {@code max}/{@code min} compiles to
 * {@code MaxDoubleWindowFunctionFactory.MaxMinOverUnboundedPartitionRowsFrameFunction} and
 * its LONG twin, and this branch took the redundant "initialized" byte out of both images -
 * a nine-byte freeze became eight - and bumped {@code checkpointStateFormatVersion()}
 * 1 -&gt; 2 to say so. The version rides inside the codec identity, which is the function
 * directory's lookup key, so every root {@code 10.0.1} wrote for these functions stops
 * resolving. That is the bump working, not failing.
 * <p>
 * What the case pins is the consequence, because nothing else in the suite does and nothing
 * in a running instance reports it. The view stays valid, counts no refresh fault, and
 * serves correct rows the whole way through - and silently throws its checkpoint ladder
 * away and recomputes the window from the base table. The three witnesses are therefore
 * {@link LiveViewInstance#isCheckpointRestoreSucceeded()} reading <b>false</b>, the boundary
 * count collapsing, and the rebuild's own log lines. Rows alone would prove nothing: a
 * from-base replay lands on exactly the same numbers, which is the whole reason this is
 * invisible.
 * <p>
 * The cost is also pinned as being paid <b>once</b>. The second restart restores off what
 * this branch sealed, so an upgraded instance does not recompute on every start.
 * <p>
 * This fixture's view partitions by {@code account_id}, a SYMBOL column, which this branch
 * keys as a translated LV-private id rather than the resolved STRING the released build
 * sealed. That schema mismatch is checked ahead of the function directory on every restore,
 * so it forces the same rebuild the state-format bump alone would have - and pre-empts the
 * bump's own log line in the process, since every root in the released ladder shares the
 * one legacy STRING schema. The rebuilds-from-the-base case below documents that ordering
 * rather than the bump in isolation; the once-paid-then-restores case still exercises the
 * bump on its own once this branch has resealed the ladder with its own schema.
 * <p>
 * To regenerate the fixture, copy {@code /lv/LiveViewReleaseExtremaFixtureGenerator.java.txt}
 * into a clean {@code 10.0.1} checkout's {@code io.questdb.test.cairo.lv} package and run it;
 * the constants below are the values it prints.
 */
public class LiveViewCheckpointReleaseExtremaCompatTest extends AbstractLiveViewCheckpointCompatTest {

    /**
     * The daily anchor the released view was declared with, written out as an ordinary
     * partition term for the recompute oracle. ANCHOR is live-view syntax.
     */
    private static final String CUMULATIVE_FRAME =
            "PARTITION BY account_id, bucket ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    // The simulated clock the fixture's own run left behind. This one starts above it, so the
    // flush cadence reads a forward-moving clock rather than one that jumped backwards.
    private static final long FIXTURE_END_MICROS = 2_500_000L;
    private static final String FIXTURE_RESOURCE = "/lv/lv_checkpoint_10_0_1_extrema.zip";
    /**
     * Boundaries the released build sealed. The upgrade is expected to end below this, which
     * is the measurement the case exists for.
     */
    private static final int RELEASED_BOUNDARIES = 5;
    private static final long RELEASED_HEAD_BASE_SEQ_TXN = 5;
    private static final long RELEASED_HEAD_LV_SEQ_TXN = 5;
    private static final String VIEW_NAME = "lv_extrema";
    // A valid view holding correct rows is the ending of both a restore and a rebuild, so the
    // state a case can read afterwards does not say which one ran. The log does.
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // Matches the cadence the fixture was sealed under, so a commit made after the upgrade
        // seals a boundary of its own rather than waiting for a row budget to fill.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(2 * FIXTURE_END_MICROS);
        capture.start();
    }

    @Test
    public void testABumpedFunctionRetiresTheReleasedLadderOnceAndThenRestoresNormally() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();

            // The upgrade's first refresh cycle, which is where the retirement happens.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                // A commit through this branch's writers, so the next restart has one of its
                // own seals to come back to.
                insertExtrema(job, 60);
            }
            assertViewMatchesRecompute("after a commit through this branch's writers");

            // The second restart is the one that shows the cost is paid once: the roots are this
            // branch's now, so the restore resolves and the ladder survives. start() clears
            // everything the upgrade logged, so what follows is this restart's own record.
            capture.start();
            restartCycle();
            final LiveViewInstance instance = instance(VIEW_NAME);
            Assert.assertFalse(VIEW_NAME + ": must stay valid across a restart", instance.isInvalid());
            assertNoRefreshFaults(VIEW_NAME);
            // The absence of the rebuild, not isCheckpointRestoreSucceeded(): the from-base
            // rebuild sets that flag too, so it cannot tell the two endings apart.
            capture.drain();
            capture.assertNotLogged("could not restore live view from checkpoint timeline, rebuilding derived state");
            capture.assertNotLogged("live view restart rebuilding from applied base");
            Assert.assertTrue(
                    VIEW_NAME + ": a restart off this branch's own roots must keep its ladder",
                    countSealedBoundaries(VIEW_NAME) > 1
            );
            assertViewMatchesRecompute("after a restart off this branch's own seal");
        });
    }

    @Test
    public void testAReleasedCheckpointWithASymbolKeyRebuildsFromTheBaseBeforeTheFunctionBumpIsEverChecked() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();

            // The fixture is genuinely legacy-shaped: 10.0.1 has no fused root to write, so a
            // probe reporting one here would mean the case is reading this branch's own bytes.
            Assert.assertFalse(
                    VIEW_NAME + ": the fixture's head must be a legacy root, not a fused one",
                    isFusedHead(VIEW_NAME)
            );
            Assert.assertEquals(
                    VIEW_NAME + ": the fixture must arrive with the lineage the released build sealed",
                    RELEASED_BOUNDARIES,
                    countSealedBoundaries(VIEW_NAME)
            );
            Assert.assertEquals(
                    VIEW_NAME + ": the fixture must arrive on the released head [headLvSeqTxn]",
                    RELEASED_HEAD_LV_SEQ_TXN,
                    instance(VIEW_NAME).getHeadCheckpointLvSeqTxn()
            );
            Assert.assertEquals(
                    VIEW_NAME + ": the fixture must arrive on the released head [headBaseSeqTxn]",
                    RELEASED_HEAD_BASE_SEQ_TXN,
                    instance(VIEW_NAME).getHeadCheckpointBaseSeqTxn()
            );

            // The upgrade's first refresh cycle.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            // This fixture partitions by account_id, a SYMBOL column, and the released build
            // sealed that key as a resolved STRING - this branch's compiled runtime keys it as
            // a translated SYMBOL instead. validateState runs before validateFunctions inside
            // the restore, so the schema check refuses the newest root - and every older one
            // behind it, since a released build wrote every root in the ladder with the same
            // STRING key - before the walk ever reaches the function directory the state-format
            // bump (v1 -> v2 for max/min) would otherwise turn away on its own. The bump is still
            // real and would force the same rebuild on its own merits, but this fixture cannot
            // isolate that anymore: the schema mismatch pre-empts it, so the "root is missing a
            // compiled function" line this case used to name never gets a chance to log.
            capture.drain();
            capture.assertLogged("could not restore live view from checkpoint timeline, rebuilding derived state");
            capture.assertLogged("anchor key schema does not match the compiled runtime");
            capture.assertNotLogged("root is missing a compiled function");
            capture.assertLogged("live view restart rebuilding from applied base");

            final LiveViewInstance instance = instance(VIEW_NAME);
            Assert.assertFalse(
                    VIEW_NAME + ": a schema-mismatched checkpoint must not invalidate the view",
                    instance.isInvalid()
            );
            Assert.assertTrue(
                    VIEW_NAME + ": the restore must have been attempted",
                    instance.isCheckpointRestoreAttempted()
            );
            // isCheckpointRestoreSucceeded() is deliberately not a witness here: the from-base
            // rebuild sets it as well, so it reads true on both endings. The log lines above and
            // the ladder below are what separate them.
            //
            // Nothing counts this as a fault either, so the retirement leaves no trace in any
            // counter a running instance exposes.
            assertNoRefreshFaults(VIEW_NAME);
            Assert.assertTrue(
                    VIEW_NAME + ": the released ladder must be retired rather than carried forward, "
                            + "which is what the schema mismatch costs",
                    countSealedBoundaries(VIEW_NAME) < RELEASED_BOUNDARIES
            );

            // And the rows are right anyway, which is exactly why this is worth a test: a
            // from-base replay lands on the same numbers a restore would have.
            assertViewMatchesRecompute("after the rebuild the schema mismatch forced");
        });
    }

    private void assertViewMatchesRecompute(String at) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT created_at, account_id, "
                        + "max(amount) OVER (" + CUMULATIVE_FRAME + ") AS running_max, "
                        + "min(amount) OVER (" + CUMULATIVE_FRAME + ") AS running_min, "
                        + "max(qty) OVER (" + CUMULATIVE_FRAME + ") AS running_qty_max, "
                        + "min(qty) OVER (" + CUMULATIVE_FRAME + ") AS running_qty_min "
                        + "FROM (SELECT created_at, account_id, amount, qty, "
                        + "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket "
                        + "FROM ext)) ORDER BY 2, 1",
                '(' + VIEW_NAME + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(VIEW_NAME);
        LOG.info().$("released extrema view matches its from-base recompute [at=").$(at).$(']').$();
    }

    private void insertExtrema(LiveViewRefreshJob job, int second) throws Exception {
        execute("INSERT INTO ext VALUES ('" + timestamp(second) + "', 'acct-1', "
                + (second + 1.5) + ", " + (second * 1_000L) + ')');
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
        Assert.assertFalse(
                VIEW_NAME + ": the fixture must not carry an invalid view",
                instance(VIEW_NAME).isInvalid()
        );
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }

    private static String timestamp(int secondOfDay) {
        return String.format("2026-01-01T09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }
}
