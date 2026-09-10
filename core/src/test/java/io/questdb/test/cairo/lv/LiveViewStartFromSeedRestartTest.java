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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Restart and failure recovery of the initial seed sweep, under a START FROM boundary that
 * <b>cuts</b> the base.
 * <p>
 * The seed's siblings in {@link LiveViewSmokeTest} - {@code testSeedRestartResumesFromCheckpoint},
 * {@code testSeedRestartWithoutCheckpointReSweeps} and the rest of that family - all seed from
 * BEGINNING, whose boundary is {@code LONG_NULL}. That hides the thing most likely to break a
 * resume, because BEGINNING makes two different coordinate spaces coincide:
 * <ul>
 *     <li>the sweep's {@code dataOffset} (and so the seed cursor the timeline's newest generation
 *     carries, which IS that offset) counts rows of the <b>bounded</b> cursor - the one
 *     {@code getCursorFromTimestamp} opens AT {@code viewLowerBoundTimestamp}, having culled the
 *     partitions below it and binary-searched into the first one;</li>
 *     <li>the skip-write floor counts <b>LV output</b> rows already on disk.</li>
 * </ul>
 * Under BEGINNING both equal "base rows swept", so a resume that skipped {@code dataOffset} rows of
 * a differently-based cursor, or that confused output rows with base rows, still lands on the right
 * row and still passes. Under a finite boundary they part ways: four base rows sit below the bound
 * and are never row zero of anything. A resume that re-derives the offset against a full-base cursor
 * rewinds into rows an earlier turn already fed and double-advances the accumulators.
 * <p>
 * Restoring the seed's pre-bounded-cursor design - a full base scan with the sub-boundary rows
 * dropped inside the feed loop - leaves all eight of those BEGINNING seed-restart tests green while
 * turning four of the five below red. (The fifth, {@code testAlterBaseMidSeed...}, survives it: its
 * O3 commit re-derives the view through {@code o3HeadMissReplay}'s own bounded scan, which heals the
 * corrupted seed before the assertions run.) That split is the gap this class exists to close.
 * <p>
 * So every test here cuts the base in half with an explicit boundary, and asserts the view's rows,
 * their {@code rn} row number and a running sum wide enough to span the whole admitted set - not a
 * row count, which cannot tell a duplicated row from a dropped one.
 */
public class LiveViewStartFromSeedRestartTest extends AbstractLiveViewTest {

    // Four rows below the boundary, four at or above it. The running sum's frame (3 preceding)
    // spans all four admitted rows, so a leaked sub-boundary row or a re-fed row shows up in s as
    // well as in rn.
    private static final String EXPECTED_SEEDED = """
            ts\tx\trn\ts
            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
            2026-04-01T00:00:06.000000Z\t6\t2\t11.0
            2026-04-01T00:00:07.000000Z\t7\t3\t18.0
            2026-04-01T00:00:08.000000Z\t8\t4\t26.0
            """;
    private static final String START_FROM = "'2026-04-01T00:00:05.000000Z'";
    private static final String VIEW_SQL = """
            SELECT ts, x, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn,
                   sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s
            FROM base""";

    @After
    public void unpinClock() {
        // currentMicros is a static that outlives the class; hand the next one a clean slate.
        setCurrentMicros(-1);
    }

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAlterBaseMidSeedConvergesUnderFiniteBoundary() throws Exception {
        // An ALTER lands on the base while the sweep is mid-flight, holding a pinned snapshot at
        // the pre-ALTER metadata. Adding an unreferenced column keeps the view valid by design, so
        // the sweep must finish and the view must still hold exactly the admitted rows.
        //
        // The sweep itself rides it out: it reads through its pinned snapshot with the factory that
        // was compiled against the same metadata version, so the two agree and no drift is detected
        // while SEEDING, and the seed completes on the pre-ALTER snapshot.
        //
        // The drift only surfaces once the view scans the applied BASE TABLE through its cached
        // factory and meets a reader at the new metadata version. Neither the ALTER itself nor a
        // later in-order row does that: a structural commit opens no base data reader, and the
        // forward drain reads the base WAL, not the base table. An OUT-OF-ORDER commit does - it
        // routes to the replay, which re-derives from the applied base - so that is what arms the
        // drift here. The replay recompiles against the new metadata and rebuilds the view through
        // the same boundary floor, so the four seeded rows must come back unchanged rather than
        // being recomputed from row zero of the base. The fault-count assertion pins that the drift
        // really fired; without it this test would still pass if the drift path stopped running.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance instance = driveSeedTurnsToOffset(job, 2);

                execute("ALTER TABLE base ALTER COLUMN y TYPE LONG");
                drainWalQueue();
                Assert.assertFalse(
                        "retyping an unreferenced column must not invalidate the view",
                        instance.isInvalid()
                );

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the seed must complete on its pre-ALTER snapshot",
                        LiveViewState.SEED_STATE_ACTIVE,
                        instance.getStateReader().getSeedState()
                );

                // An out-of-order row (above the boundary, below the base's max ts): its replay
                // scans the applied base through the cached factory and meets the new metadata.
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-04-01T00:00:07.500000Z', 'a', 75)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertFalse("the view must survive the recompile", instance.isInvalid());
                // handleRefreshFailure records a fault on every cycle that throws, including the
                // drift cycle it then recovers from without charging the retry budget.
                Assert.assertTrue(
                        "the metadata drift must actually have been detected and recovered from",
                        instance.getRefreshFaultCount() > 0
                );
                Assert.assertEquals(
                        "a recovered drift must not charge the flush retry budget",
                        0,
                        instance.getFlushRetryCount()
                );
            }

            // The recompiled replay rebuilt the view from the applied base at the boundary: the four
            // seeded rows are still there, the O3 row slots into its event-time place, and the four
            // sub-boundary base rows stayed out of a re-derive that scanned the whole base.
            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
                            2026-04-01T00:00:06.000000Z\t6\t2\t11.0
                            2026-04-01T00:00:07.000000Z\t7\t3\t18.0
                            2026-04-01T00:00:07.500000Z\t75\t4\t93.0
                            2026-04-01T00:00:08.000000Z\t8\t5\t96.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMidSeedRefreshFailureDoesNotDoubleAdvanceWindowState() throws Exception {
        // A seed turn that feeds rows through the incremental window cursor - advancing the
        // accumulators - and then throws before the LV commit must not leave those accumulators
        // advanced. The uncommitted WAL rows roll back when the WalWriter closes; the accumulator
        // advance does not, and neither dataOffset nor lvRowsTotal are updated (both assignments sit
        // after the throwing block). So the retry would re-feed the same rows from the unchanged
        // offset into already-advanced state.
        //
        // handleRefreshFailure catches it via windowStateDirty and routes to
        // rebuildWindowStateAfterMidDrainFailure, whose SEEDING branch re-arms the sweep's resume
        // (clearing the window state and re-deriving the offset + skip-write floor from disk) while
        // deliberately KEEPING the pinned base snapshot, so the positional resume stays sound.
        // Revert either half - the windowStateDirty flag in the seed loop, or the
        // resetSeedResumeAttempted in that branch - and the sums below come out inflated.
        //
        // The base spans two partitions and the fault is a one-shot read failure on the second
        // one's x.d, which the sweep only reaches after the first partition's four admitted rows.
        // The fault self-clears, so the re-armed sweep reads cleanly.
        final String[] baseDir = new String[1];
        final AtomicBoolean armSecondPartitionRead = new AtomicBoolean();
        final AtomicBoolean readFailed = new AtomicBoolean();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armSecondPartitionRead.get()
                        && baseDir[0] != null
                        && Utf8s.endsWithAscii(name, "x.d")
                        && Utf8s.containsAscii(name, baseDir[0])
                        && Utf8s.containsAscii(name, "2026-04-02")
                        && !Utf8s.containsAscii(name, "wal")) {
                    armSecondPartitionRead.set(false);
                    readFailed.set(true);
                    return -1;
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(0L);
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Day one is cut by the boundary (rows 1-4 below it, 5-8 at or above); day two sits
            // wholly above it, so the sweep must cross the partition seam to reach rows 9 and 10.
            execute("""
                    INSERT INTO base (ts, sym, x) VALUES
                    ('2026-04-01T00:00:01.000000Z', 'a', 1),
                    ('2026-04-01T00:00:02.000000Z', 'a', 2),
                    ('2026-04-01T00:00:03.000000Z', 'a', 3),
                    ('2026-04-01T00:00:04.000000Z', 'a', 4),
                    ('2026-04-01T00:00:05.000000Z', 'a', 5),
                    ('2026-04-01T00:00:06.000000Z', 'a', 6),
                    ('2026-04-01T00:00:07.000000Z', 'a', 7),
                    ('2026-04-01T00:00:08.000000Z', 'a', 8),
                    ('2026-04-02T00:00:01.000000Z', 'a', 9),
                    ('2026-04-02T00:00:02.000000Z', 'a', 10)""");
            drainWalQueue();
            baseDir[0] = engine.verifyTableName("base").getDirName();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM " + START_FROM + " AS " + VIEW_SQL);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                armSecondPartitionRead.set(true);
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                Assert.assertTrue("the mid-seed read must actually have been failed", readFailed.get());
                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertFalse("a mid-seed fault must not invalidate the view", instance.isInvalid());
                Assert.assertEquals(
                        "the re-armed sweep must complete",
                        LiveViewState.SEED_STATE_ACTIVE,
                        instance.getStateReader().getSeedState()
                );
                Assert.assertEquals(
                        "the mid-seed rebuild recovers without charging the retry budget",
                        0,
                        instance.getFlushRetryCount()
                );
            }

            // rn is gapless and the running sum matches a clean single pass. A re-feed into
            // un-cleared accumulators inflates s (the day-one rows counted twice) and pushes rn
            // past 6.
            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
                            2026-04-01T00:00:06.000000Z\t6\t2\t11.0
                            2026-04-01T00:00:07.000000Z\t7\t3\t18.0
                            2026-04-01T00:00:08.000000Z\t8\t4\t26.0
                            2026-04-02T00:00:01.000000Z\t9\t5\t30.0
                            2026-04-02T00:00:02.000000Z\t10\t6\t34.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartBeforeSeedCompletionFlipsActiveWithoutDuplicating() throws Exception {
        // The narrowest crash window in the sweep: every admitted row has been fed, committed and
        // applied, and a seed boundary records it - but the cursor's exhaustion has not been
        // observed yet, so
        // the SEEDING -> ACTIVE flip has not run and the view is still SEEDING on disk.
        //
        // The resumed turn must skip all four rows, find the cursor empty, append nothing, and flip
        // ACTIVE. Two things can go wrong and neither shows up in a row count: a resume that re-fed
        // the rows would duplicate them, and a resume that dropped the restored root's maxTimestamp
        // would flip ACTIVE with latestSeenTs still LONG_NULL, which leaves the view with no head
        // boundary at all - so the next O3 commit routes to the head-MISS replay instead of the
        // head-hit one.
        // The head assertion below is what pins that.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            final long lastAdmittedTs = ts("2026-04-01T00:00:08.000000Z");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Sweep all four admitted rows. The turn budget makes the sweep yield after the last
                // one, so exhaustion is only discovered on the turn AFTER this.
                driveSeedTurnsToOffset(job, 4);

                restart();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);
                Assert.assertEquals(
                        "the view must come back SEEDING",
                        LiveViewState.SEED_STATE_SEEDING,
                        reloaded.getStateReader().getSeedState()
                );

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        LiveViewState.SEED_STATE_ACTIVE,
                        reloaded.getStateReader().getSeedState()
                );
                Assert.assertEquals(
                        "the completing turn must anchor the head at the restored latestSeenTs, not at LONG_NULL",
                        lastAdmittedTs,
                        reloaded.getHeadCheckpointMaxTs()
                );

                // The head is real, so an out-of-order commit above the boundary takes the head-hit
                // replay off it. Its row must slot in without renumbering the head's.
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-04-01T00:00:09.000000Z', 'a', 9)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
                            2026-04-01T00:00:06.000000Z\t6\t2\t11.0
                            2026-04-01T00:00:07.000000Z\t7\t3\t18.0
                            2026-04-01T00:00:08.000000Z\t8\t4\t26.0
                            2026-04-01T00:00:09.000000Z\t9\t5\t30.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedResumesFromCheckpointUnderFiniteBoundary() throws Exception {
        // A restart mid-sweep resumes from the surviving timeline. The generation's seed cursor IS
        // the sweep's data offset, and that offset counts rows of the cursor opened AT the boundary
        // - so after two admitted rows it must be 2, not 6 (the two admitted rows plus the four the
        // boundary culled). Asserting the cursor is what pins the coordinate space; asserting the
        // rows is what pins that the resume then skipped into the right place.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance instance = driveSeedTurnsToOffset(job, 2);
                Assert.assertEquals(
                        "the seed cursor is the bounded cursor's row offset, not the base's",
                        2,
                        instance.getSeedCheckpointDataOffset()
                );

                restart();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);
                Assert.assertEquals(
                        "the durable generation must carry the seed cursor the sweep resumes from",
                        2,
                        durableSeedCursorOffset(reloaded)
                );

                // One turn: the resume restores offset 2 and feeds exactly one more row. A from-zero
                // re-sweep would land on 1 instead, so this separates the two paths.
                job.run();
                drainWalQueue();
                Assert.assertEquals(
                        "the turn after the restart must resume from the durable seed cursor, not from zero",
                        3,
                        reloaded.getSeedDataOffset()
                );

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            assertSeededRows();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedAcrossABaseStructuralChangeReSweeps() throws Exception {
        // A structural sequencer entry (walId <= 0) between the root's base snapshot and the
        // one the restart pins refuses the resume, even though a metadata change on its own
        // preserves physical row order.
        //
        // The reason is what a structural entry does NOT say: one of them can be the
        // ALTER TABLE ... SET TTL that turned the base's TTL back to 0 after an eviction had
        // already run, and the entry carries no timestamps to test that against. The TTL guard
        // reads the base's CURRENT ttl, so without this refusal that sequence - evict, clear,
        // restart - would resume over a prefix rows had left.
        //
        // So this is a pin on the rule, not a bug repro: adding a column changes no row, and the
        // rows below come out the same either way. What separates the two branches is the log
        // line and the zeroed skip-write floor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedTurnsToOffset(job, 2);

                restart();

                execute("ALTER TABLE base ADD COLUMN z INT");
                drainWalQueue();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);
                Assert.assertFalse("an unreferenced added column must not invalidate the view", reloaded.isInvalid());

                capture.start();
                try {
                    driveSeedToCompletion(job, "lv");
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view discarding seed checkpoint across a base structural change [view=lv");
                } finally {
                    capture.stop();
                }

                Assert.assertEquals(
                        "a refused resume skip-writes nothing; the replacement carries the discard",
                        0,
                        reloaded.getSeedSkipWriteFloor()
                );
            }

            assertSeededRows();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedUnderBaseTtlReSweepsWhenEvictionCouldReachTheView() throws Exception {
        // TTL evicts inside the committing writer's housekeeping. There is no transaction of its
        // own to walk, so the WAL-E scan cannot see it and the resume needs a separate witness
        // that the swept prefix still holds every row it counted.
        //
        // That witness is the base's own minimum timestamp against the view's lower bound: TTL
        // evicts a PREFIX of the partitions, so a surviving base row at or below the boundary
        // proves the eviction never reached the rows the view's cursor counts. Here the boundary
        // sits BELOW every base row, so no row can stand at or below it and the resume is
        // refused - conservatively, since nothing was in fact evicted (the pinned clock keeps the
        // threshold far below the data).
        //
        // A pin on the rule rather than a bug repro: the forward row the restart adds shifts no
        // position, so the rows below come out the same with or without the guard.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createTtlBaseAndUncutView();

            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedTurnsToOffset(job, 2);

                restart();

                // Base progress the resume has to account for; without it the root's snapshot and
                // the re-pinned one are the same seqTxn and the proof short-circuits.
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-04-01T00:00:09.000000Z', 'a', 9)");
                drainWalQueue();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);

                capture.start();
                try {
                    driveSeedToCompletion(job, "lv");
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view discarding seed checkpoint over a base whose TTL may have evicted [view=lv");
                } finally {
                    capture.stop();
                }

                Assert.assertEquals(
                        "a refused resume skip-writes nothing; the replacement carries the discard",
                        0,
                        reloaded.getSeedSkipWriteFloor()
                );
            }

            // Every base row exactly once, the re-swept prefix having replaced the partial output
            // rather than landing beside it.
            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:01.000000Z\t1\t1\t1.0
                            2026-04-01T00:00:02.000000Z\t2\t2\t3.0
                            2026-04-01T00:00:03.000000Z\t3\t3\t6.0
                            2026-04-01T00:00:04.000000Z\t4\t4\t10.0
                            2026-04-01T00:00:05.000000Z\t5\t5\t14.0
                            2026-04-01T00:00:06.000000Z\t6\t6\t18.0
                            2026-04-01T00:00:07.000000Z\t7\t7\t22.0
                            2026-04-01T00:00:08.000000Z\t8\t8\t26.0
                            2026-04-01T00:00:09.000000Z\t9\t9\t30.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedUnderBaseTtlResumesWhenTheBaseStillReachesTheBoundary() throws Exception {
        // The other side of the TTL witness. The base carries the same TTL, but the view's
        // boundary CUTS it: four base rows sit below the boundary, so the base's minimum
        // timestamp is at or below it and the eviction prefix demonstrably never reached the
        // view's range. The resume goes ahead.
        //
        // Without this case the TTL guard could degrade into "any TTL on the base re-seeds", and
        // TTL on a base table is the common configuration, not the exotic one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createTtlBaseAndCutView();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedTurnsToOffset(job, 2);

                restart();

                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-04-01T00:00:09.000000Z', 'a', 9)");
                drainWalQueue();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);

                // One turn: a resume restores offset 2 and feeds exactly one more row, a re-sweep
                // would land on 1.
                job.run();
                drainWalQueue();
                Assert.assertEquals(
                        "a base whose oldest row still sits at or below the boundary must resume",
                        3,
                        reloaded.getSeedDataOffset()
                );

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
                            2026-04-01T00:00:06.000000Z\t6\t2\t11.0
                            2026-04-01T00:00:07.000000Z\t7\t3\t18.0
                            2026-04-01T00:00:08.000000Z\t8\t4\t26.0
                            2026-04-01T00:00:09.000000Z\t9\t5\t30.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedWithBaseCommitAboveTheSweptPrefixResumes() throws Exception {
        // The base moved while the process was down, but every row it gained sorts ABOVE the
        // swept prefix. Nothing inside [0, seedCursorOffset) moved, so the positional resume is
        // still sound and the sweep must take it rather than re-scanning the whole history.
        //
        // This is the case the cheap alternative - refusing any resume whose root names a
        // different base seqTxn than the one the restart pinned - would throw away, and a base
        // that keeps ingesting during a restart is the normal case, not the exotic one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedTurnsToOffset(job, 2);

                restart();

                // Above the base's own maximum, and so above the two rows already swept.
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-04-01T00:00:09.000000Z', 'a', 9)");
                drainWalQueue();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);
                Assert.assertEquals(
                        "the durable generation must carry the seed cursor the sweep resumes from",
                        2,
                        durableSeedCursorOffset(reloaded)
                );

                // One turn: the resume restores offset 2 and feeds exactly one more row. A from-zero
                // re-sweep would land on 1 instead, so this separates the two paths.
                job.run();
                drainWalQueue();
                Assert.assertEquals(
                        "a forward-only base commit must not cost the sweep its resume point",
                        3,
                        reloaded.getSeedDataOffset()
                );

                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
                            2026-04-01T00:00:06.000000Z\t6\t2\t11.0
                            2026-04-01T00:00:07.000000Z\t7\t3\t18.0
                            2026-04-01T00:00:08.000000Z\t8\t4\t26.0
                            2026-04-01T00:00:09.000000Z\t9\t5\t30.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedWithBaseCommitBetweenTheRootAndTheDurableFrontierReSweeps() throws Exception {
        // The trusted prefix is not the root's prefix. A resume rewinds the CURSOR to the root but
        // keeps skip-writing up to the table's row count, so it trusts the base rows behind the
        // durable output too - and in production that stretch is the long one: a turn ends on the
        // 50ms budget while a root is sealed every 1M rows or 5 minutes, so the root sits far
        // behind the disk.
        //
        // This test reproduces that shape - a per-turn budget of one row with no row or duration
        // cadence to seal on, so only the first root is written - and lands the base commit
        // BETWEEN the root's maxTimestamp and the output's. A bound taken from the root alone
        // admits it, and then the re-feed re-numbers the rows the skip-write floor is counting: the
        // last durable row is appended a second time and the back-dated one is skip-written into
        // nothing.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_TURN_MAX_DURATION_MICROS, 0); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance instance = driveSeedTurnsToOffset(job, 3);
                Assert.assertEquals(
                        "only the first turn seals; the root must stay behind the durable output",
                        ts("2026-04-01T00:00:05.000000Z"),
                        instance.getSeedCheckpointMaxTs()
                );
                Assert.assertEquals(
                        "three admitted rows must be durable behind that one root",
                        3,
                        instance.getLvRowsTotal()
                );

                restart();

                // Above the root's 00:00:05 and at or below the output's 00:00:07.
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-04-01T00:00:06.500000Z', 'a', 65)");
                drainWalQueue();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);
                Assert.assertEquals(
                        "the durable generation must carry the first turn's cursor",
                        1,
                        durableSeedCursorOffset(reloaded)
                );

                capture.start();
                try {
                    driveSeedToCompletion(job, "lv");
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view discarding seed checkpoint behind a base commit inside its trusted prefix [view=lv");
                } finally {
                    capture.stop();
                }

                Assert.assertEquals(
                        "a refused resume skip-writes nothing; the replacement carries the discard",
                        0,
                        reloaded.getSeedSkipWriteFloor()
                );
            }

            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
                            2026-04-01T00:00:06.000000Z\t6\t2\t11.0
                            2026-04-01T00:00:06.500000Z\t65\t3\t76.0
                            2026-04-01T00:00:07.000000Z\t7\t4\t83.0
                            2026-04-01T00:00:08.000000Z\t8\t5\t86.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedWithBaseCommitInsideTheSweptPrefixReSweeps() throws Exception {
        // The hazard the base-snapshot proof exists for. The sweep counts its resume point as a
        // ROW POSITION in a bounded base cursor, and a restart cannot re-open the snapshot that
        // position was counted against - a historical MVCC snapshot dies with the process that
        // pinned it. So the restart pins the latest applied base instead, and an out-of-order
        // commit that landed below the swept prefix while the process was down has inserted a row
        // INSIDE [0, seedCursorOffset).
        //
        // Without the proof: skipRows(2) skips 05 and the back-dated 05.5, so the sweep resumes on
        // 06 - a row it has already emitted and whose accumulator state the restored root already
        // carries. The skip-write floor lets it through (it sits at the floor, not below it), so
        // the view ends up holding 06 twice, never holding 05.5 at all, and carrying window values
        // that counted 06 twice. A row count cannot tell that apart from the truth, which is why
        // rn and the running sum are asserted here.
        //
        // With it, the root is discarded, the sweep re-runs from offset zero against the snapshot
        // it actually pinned, and one full-range replacement swaps the unproven prefix for the
        // recomputed one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance instance = driveSeedTurnsToOffset(job, 2);
                Assert.assertEquals(
                        // A one-row seal cadence keeps the root level with the durable output, so
                        // the proof's bound - the higher of the two - is this timestamp either way.
                        "the two swept rows must end at the root's maxTimestamp",
                        ts("2026-04-01T00:00:06.000000Z"),
                        instance.getSeedCheckpointMaxTs()
                );

                restart();

                // Out of order, and at or below the bound: it sorts INSIDE the swept prefix.
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-04-01T00:00:05.500000Z', 'a', 55)");
                drainWalQueue();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);
                Assert.assertEquals(
                        "the durable generation must still carry the seed cursor the resume would take",
                        2,
                        durableSeedCursorOffset(reloaded)
                );

                capture.start();
                try {
                    driveSeedToCompletion(job, "lv");
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view discarding seed checkpoint behind a base commit inside its trusted prefix [view=lv");
                } finally {
                    capture.stop();
                }

                Assert.assertEquals(
                        "a refused resume skip-writes nothing; the replacement carries the discard",
                        0,
                        reloaded.getSeedSkipWriteFloor()
                );
                Assert.assertFalse(
                        "the replacement must be discharged by the time the sweep completes",
                        reloaded.isSeedReplacePending()
                );
            }

            // The back-dated row is in its event-time place, 06 appears once, and the running sum
            // saw the four rows before each one exactly once.
            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\trn\ts
                            2026-04-01T00:00:05.000000Z\t5\t1\t5.0
                            2026-04-01T00:00:05.500000Z\t55\t2\t60.0
                            2026-04-01T00:00:06.000000Z\t6\t3\t66.0
                            2026-04-01T00:00:07.000000Z\t7\t4\t73.0
                            2026-04-01T00:00:08.000000Z\t8\t5\t76.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartMidSeedWithoutCheckpointReSweepsUnderFiniteBoundary() throws Exception {
        // No timeline survives (a crash before the first cadence write, or a view whose window
        // functions cannot snapshot). Nothing then proves what the two rows already on disk are:
        // a root's stored position is the only durable statement of the emitted ordinal those
        // rows stand at, and a row count is not one - a partition removal can leave the count
        // equal by coincidence while the rows behind it are not the prefix the re-sweep computes.
        // So the resumed sweep re-runs from offset 0 with no skip-write floor and discards the
        // partial output with a full-range replacement on its first commit, rather than keeping
        // it and appending over the top.
        //
        // Under a cutting boundary the coordinate spaces are what make that visible: the sweep's
        // cursor offset counts BASE rows of the bounded cursor (6 scanned) and the output ordinal
        // counts LV OUTPUT rows (2 on disk). The proven-resume sibling above is where a resume has
        // to keep them apart; here the replacement makes the question moot, and the rows below are
        // what say the re-swept output landed exactly once.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            createCutBaseAndView();

            final LogCapture capture = new LogCapture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance instance = driveSeedTurnsToOffset(job, 2);
                Assert.assertEquals(
                        "two admitted rows must be durable before the timeline is dropped",
                        2,
                        instance.getLvRowsTotal()
                );
                Assert.assertNotEquals(
                        "a seed boundary must exist before it can be dropped",
                        Numbers.LONG_NULL,
                        instance.getSeedCheckpointDataOffset()
                );
                retireSeedCheckpointTimeline(instance);

                restart();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(reloaded);
                Assert.assertEquals(
                        "no timeline survives, so there is no durable seed cursor to resume from",
                        Numbers.LONG_NULL,
                        durableSeedCursorOffset(reloaded)
                );

                capture.start();
                try {
                    driveSeedToCompletion(job, "lv");
                    driveRefreshToQuiescence(job);
                    capture.waitFor("live view seed sweep replacing unproven durable output [view=lv");
                } finally {
                    capture.stop();
                }

                Assert.assertEquals(
                        "an unproven re-sweep skip-writes nothing; the replacement carries the discard",
                        0,
                        reloaded.getSeedSkipWriteFloor()
                );
                Assert.assertFalse(
                        "the replacement must be discharged by the time the sweep completes",
                        reloaded.isSeedReplacePending()
                );
            }

            // The four admitted rows are there exactly once - the two the first sweep committed
            // came back through the replacement, not beside it - and their running sum saw all four.
            assertSeededRows();
            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertSeededRows() throws Exception {
        assertQuery("SELECT ts, x, rn, s FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(EXPECTED_SEEDED);
    }

    // y is never referenced by the view; it exists so a test can retype it and drift the base
    // metadata without invalidating the view.
    private void createCutBaseAndView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertEightBaseRows();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM " + START_FROM + " AS " + VIEW_SQL);
    }

    /**
     * The cut base of {@link #createCutBaseAndView()} with a TTL on it, under the same cutting
     * boundary. The base's oldest row therefore sits BELOW the boundary, which is what lets the
     * resume prove a TTL eviction could not have reached the view's range.
     * <p>
     * The TTL never fires here: the pinned clock keeps the eviction threshold far below the data.
     * What the tests exercise is the proof, which reads the base's declared TTL rather than any
     * eviction it may or may not have taken.
     */
    private void createTtlBaseAndCutView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY TTL 3 DAYS WAL");
        insertEightBaseRows();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM " + START_FROM + " AS " + VIEW_SQL);
    }

    /**
     * A TTL'd base under a boundary that admits every row - it sits below the base's oldest one -
     * so no surviving base row can stand at or below it and the resume has no witness that a TTL
     * eviction stayed clear of the view's range.
     */
    private void createTtlBaseAndUncutView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY TTL 3 DAYS WAL");
        insertEightBaseRows();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-03-31T00:00:00.000000Z' AS " + VIEW_SQL);
    }

    private void insertEightBaseRows() throws Exception {
        execute("""
                INSERT INTO base (ts, sym, x) VALUES
                ('2026-04-01T00:00:01.000000Z', 'a', 1),
                ('2026-04-01T00:00:02.000000Z', 'a', 2),
                ('2026-04-01T00:00:03.000000Z', 'a', 3),
                ('2026-04-01T00:00:04.000000Z', 'a', 4),
                ('2026-04-01T00:00:05.000000Z', 'a', 5),
                ('2026-04-01T00:00:06.000000Z', 'a', 6),
                ('2026-04-01T00:00:07.000000Z', 'a', 7),
                ('2026-04-01T00:00:08.000000Z', 'a', 8)""");
        drainWalQueue();
    }

    /**
     * Runs seed turns until the sweep's data offset reaches {@code targetOffset}, leaving the view
     * SEEDING. With a one-row turn budget one turn advances the offset by exactly one admitted row.
     */
    private LiveViewInstance driveSeedTurnsToOffset(LiveViewRefreshJob job, long targetOffset) {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        for (int i = 0; i < 100 && instance.getSeedDataOffset() < targetOffset; i++) {
            if (instance.getStateReader().getSeedState() != LiveViewState.SEED_STATE_SEEDING) {
                break;
            }
            job.run();
            drainWalQueue();
        }
        Assert.assertEquals(
                "the sweep must stop on exactly the requested prefix of admitted rows",
                targetOffset,
                instance.getSeedDataOffset()
        );
        Assert.assertEquals(
                "the sweep must still be SEEDING at this point",
                LiveViewState.SEED_STATE_SEEDING,
                instance.getStateReader().getSeedState()
        );
        return instance;
    }

    /**
     * Reads the seed cursor the view's durable timeline generation carries, or
     * {@link Numbers#LONG_NULL} when it has no valid generation. This is the coordinate a restart
     * resumes the sweep from, so a test can pin it without driving a turn.
     */
    private long durableSeedCursorOffset(LiveViewInstance instance) {
        try (
                Path checkpointsDir = new Path();
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(engine.getConfiguration())
        ) {
            checkpointsDir.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            metaStore.of(checkpointsDir);
            return metaStore.isValid() ? metaStore.getSuperblock().seedCursorOffset : Numbers.LONG_NULL;
        }
    }

    /**
     * Simulated restart: drop the in-memory registry and rebuild it from the on-disk {@code _lv} /
     * {@code _lv.s}, which is the path startup takes.
     */
    private void restart() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }
}
