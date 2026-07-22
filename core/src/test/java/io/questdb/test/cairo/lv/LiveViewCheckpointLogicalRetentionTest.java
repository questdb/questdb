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
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Acceptance coverage for the retention rule the versioned timeline replaced the
 * retained checkpoint ring with: inside one history epoch a logical checkpoint
 * entry is never removed. The ring bounded retention by count and bytes, so an
 * out-of-order row older than the surviving horizon fell back to a replay from
 * {@code START FROM}; the timeline instead keeps every boundary it ever sealed
 * and versions - rather than deletes - the roots a repair corrects.
 * <p>
 * The oracle throughout is the epoch's complete logical entry set rather than
 * its size alone. Every case reads the published generation's checkpoint ids and
 * asserts they are exactly {@code [0, nextCheckpointId)}: the epoch allocates ids
 * from zero and monotonically, so a contiguous run ending one below the next id
 * to allocate proves no entry went missing anywhere in the timeline, not merely
 * that the total happened to hold. A count comparison alone would pass a
 * publication that dropped an old boundary and appended a new one in the same
 * generation.
 * <p>
 * Each case then pins one way the count could plausibly drop: ordinary cadence
 * past the ring's former count bound, a localized repair re-versioning a
 * historical interval, and the physical lifecycle - purge and restart - running
 * over a live timeline. The last case is the counterfactual that keeps the other
 * three from being vacuous: a repair whose influence reaches the runtime frontier
 * has no converged suffix to keep, so it retires the timeline and a post-replay
 * seal starts a fresh history. The entry set restarts at zero there, which is a
 * new epoch rather than a pruned tail of the old one.
 */
public class LiveViewCheckpointLogicalRetentionTest extends AbstractLiveViewTest {

    // Deep historical corrections one case drives, each three seconds above the group
    // its ordinal names, so it lands between two in-order groups without colliding.
    private static final int CORRECTIONS = 6;
    // In-order commits every case builds its history from. At one logical root per
    // commit this is also the epoch's logical entry count - eight times the retained
    // ring's former default count bound, which is the retention the timeline replaced.
    private static final int SEALS = 64;
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // The fastest cadence the view can seal: one logical root per commit, so the
        // history accumulates the most boundaries a repair or a purge can drop.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Pin the clock below the (2026) data so a START FROM NOW view resolves its
        // lower bound under every row it will ever see, corrections included.
        setCurrentMicros(0);
    }

    @Test
    public void testCadenceRetainsEveryLogicalEntryPastTheRetiredRingBound() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                long entries = 0;
                for (int commit = 1; commit <= SEALS; commit++) {
                    appendAndRefresh(job, commit * 10, commit);
                    final LiveViewInstance instance = viewInstance();
                    entries = assertEpochRetainsEveryEntry(instance, entries, "after commit " + commit);
                    Assert.assertEquals("one cadence seal appends one logical entry", commit, entries);
                }

                // The oldest boundary stays addressable at its original coordinate after
                // every later event, which is what makes an old O3 row's predecessor
                // lookup a search rather than a fallback to START FROM.
                final LiveViewInstance instance = viewInstance();
                final LiveViewCheckpointTimelineEntry oldest = new LiveViewCheckpointTimelineEntry();
                assertPredecessorIs(instance, ts(timestamp(20)), 0, oldest);
                Assert.assertEquals(
                        "the oldest boundary keeps the coordinate it was sealed at",
                        ts(timestamp(10)),
                        oldest.maxTimestamp
                );
                Assert.assertTrue(findsEntry(instance, ts(timestamp(10)), 0, oldest));
            }
        });
    }

    @Test
    public void testLifecycleCyclesRetainEveryLogicalEntry() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance instance = buildHistory(job);
                // One correction first, so the purge below has superseded physical
                // versions to reclaim rather than an untouched steady-state timeline.
                correct(job, instance, historicalSecond(1), 900);
                long entries = assertEpochRetainsEveryEntry(instance, SEALS, "after the correction");

                purgeCycle(instance);
                entries = assertEpochRetainsEveryEntry(instance, entries, "after the purge");

                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                instance = viewInstance();
                entries = assertEpochRetainsEveryEntry(instance, entries, "after the restart");
                Assert.assertEquals("the physical lifecycle owns no logical entry", SEALS, entries);

                // The restarted view keeps sealing into the same epoch rather than
                // starting a new one under the reconciled timeline.
                try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                    appendAndRefresh(resumed, (SEALS + 1) * 10, SEALS + 1);
                    driveRefreshToQuiescence(resumed);
                }
                entries = assertEpochRetainsEveryEntry(viewInstance(), entries, "after resuming the cadence");
                Assert.assertEquals(SEALS + 1, entries);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLocalizedRepairsReVersionRootsWithoutDroppingOne() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                long entries = SEALS;
                for (int correction = 1; correction <= CORRECTIONS; correction++) {
                    final long generationBefore = generation(instance);
                    correct(job, instance, historicalSecond(correction), 900 + correction);

                    final String when = "after correction " + correction;
                    Assert.assertTrue(
                            "a localized repair publishes a new generation " + when,
                            generation(instance) > generationBefore
                    );
                    entries = assertEpochRetainsEveryEntry(instance, entries, when);
                    Assert.assertEquals(
                            "a converged repair re-versions roots in [C, H) and creates no boundary " + when,
                            SEALS,
                            entries
                    );
                    assertViewMatchesRecompute();
                }
            }
        });
    }

    @Test
    public void testOnlyATimelineRetirementResetsTheLogicalEntrySet() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = buildHistory(job);
                assertEpochRetainsEveryEntry(instance, SEALS, "before the retirement");

                // Five seconds under the head, so the correction's influence reaches the
                // runtime frontier instead of converging under it. There is no proven
                // converged suffix to keep, so the repair retires the whole timeline and
                // the post-replay seal starts a fresh history.
                correct(job, instance, SEALS * 10 - 5, 800);

                Assert.assertEquals(
                        "a retired timeline restarts the generation counter",
                        1,
                        generation(instance)
                );
                final long entries = assertEpochRetainsEveryEntry(instance, 0, "after the retirement");
                Assert.assertEquals(
                        "the post-replay seal is the new epoch's first and only boundary",
                        1,
                        entries
                );
                // The surviving id 0 sits at the current head rather than at the coordinate
                // the retired epoch's first boundary held, so this is a new history epoch
                // and not the old one pruned back to its newest entry.
                final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue(findsEntry(instance, ts(timestamp(SEALS * 10)), 0, entry));
                Assert.assertFalse(
                        "no boundary of the retired epoch survives into the new history",
                        findsEntry(instance, ts(timestamp(10)), 0, entry)
                );
                assertViewMatchesRecompute();

                // Monotonic retention resumes immediately inside the new epoch.
                appendAndRefresh(job, (SEALS + 1) * 10, SEALS + 1);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(2, assertEpochRetainsEveryEntry(instance, entries, "after the next commit"));
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Second-of-day of the n-th historical correction: three seconds above the group
    // its ordinal names, so it collides with no in-order group (a multiple of ten) and
    // stays deep enough below the head for its influence to converge under the frontier.
    private static int historicalSecond(int correction) {
        return 10 * correction + 3;
    }

    // Base rows a repair replayed over this instance's lifetime, through either
    // disposition: the resume from a boundary below the change, or the localized
    // rebuild over the change's own dependency interval. In-order appends leave both
    // at zero.
    private static long repairedRows(LiveViewInstance instance) {
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    private static String timestamp(int secondOfDay) {
        return String.format("2026-01-01T00:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    private void appendAndRefresh(LiveViewRefreshJob job, int second, long value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base VALUES ('" + timestamp(second) + "', 'a', " + value + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    /**
     * Asserts the published generation still holds every logical entry the epoch ever
     * allocated - the checkpoint ids {@code [0, nextCheckpointId)}, in order - and that
     * the count has not fallen below {@code previousEntries}. Returns the current count
     * so a caller can thread it through the next step.
     */
    private long assertEpochRetainsEveryEntry(LiveViewInstance instance, long previousEntries, String when) {
        final LongList ids = logicalCheckpointIds(instance);
        final long nextCheckpointId = nextCheckpointId(instance);
        Assert.assertEquals(
                "the epoch must hold every checkpoint id it allocated " + when,
                nextCheckpointId,
                ids.size()
        );
        for (int i = 0, n = ids.size(); i < n; i++) {
            Assert.assertEquals("logical entry at index " + i + " " + when, i, ids.getQuick(i));
        }
        Assert.assertTrue(
                "the logical entry count must never drop " + when
                        + " [before=" + previousEntries + ", after=" + ids.size() + ']',
                ids.size() >= previousEntries
        );
        return ids.size();
    }

    private void assertPredecessorIs(
            LiveViewInstance instance,
            long correctionTimestamp,
            long checkpointId,
            LiveViewCheckpointTimelineEntry out
    ) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            Assert.assertTrue(
                    "a correction at " + correctionTimestamp + " must find a predecessor boundary",
                    reader.predecessor(pin.getTimelineRootRef(), correctionTimestamp, out)
            );
        }
        Assert.assertEquals(checkpointId, out.checkpointId);
    }

    // The live view must equal the same window recomputed directly over the base table.
    // A refresh fault self-heals into exactly that recompute, so the fault count guards
    // that the view converged through the incremental and repair paths rather than
    // through a recovery rebuild that would also have thrown the timeline away.
    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + VIEW_SQL + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private LiveViewInstance buildHistory(LiveViewRefreshJob job) throws Exception {
        for (int commit = 1; commit <= SEALS; commit++) {
            appendAndRefresh(job, commit * 10, commit);
        }
        driveRefreshToQuiescence(job);
        final LiveViewInstance instance = viewInstance();
        Assert.assertEquals(SEALS, assertEpochRetainsEveryEntry(instance, 0, "after building the history"));
        return instance;
    }

    // Commits one out-of-order row and drives the refresh job to quiescence over it,
    // asserting the change was actually repaired: a case whose corrections quietly
    // degenerated into appends would leave every retention assertion beside them
    // testing nothing but ordinary cadence.
    private void correct(LiveViewRefreshJob job, LiveViewInstance instance, int second, long value) throws Exception {
        final long repairedBefore = repairedRows(instance);
        appendAndRefresh(job, second, value);
        driveRefreshToQuiescence(job);
        Assert.assertTrue(
                "the row at second " + second + " must be repaired rather than appended",
                repairedRows(instance) > repairedBefore
        );
    }

    private void createView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + VIEW_SQL);
    }

    /** Point lookup of one logical boundary by its full {@code (maxTimestamp, checkpointId)} key. */
    private boolean findsEntry(
            LiveViewInstance instance,
            long maxTimestamp,
            long checkpointId,
            LiveViewCheckpointTimelineEntry out
    ) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            return reader.findExact(pin.getTimelineRootRef(), maxTimestamp, checkpointId, out);
        }
    }

    private long generation(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().generation;
        }
    }

    private LongList logicalCheckpointIds(LiveViewInstance instance) {
        final LongList ids = new LongList();
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader reader = openTimelineReader(instance)
        ) {
            reader.iterateAll(pin.getTimelineRootRef(), entry -> ids.add(entry.checkpointId));
        }
        return ids;
    }

    private long nextCheckpointId(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().nextCheckpointId;
        }
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = checkpointsDir(instance)) {
            store.of(dir);
        }
        return store;
    }

    private LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
        try (Path dir = checkpointsDir(instance)) {
            reader.of(dir);
        }
        return reader;
    }

    // The primary-owned lifecycle reconciliation a publication or a startup would run,
    // driven here at a quiescent point so the correction's obsolete segments are
    // reclaimed against a timeline nothing else is touching. The definition txn and
    // history epoch are the ones the engine passes, so this must never look like an
    // epoch change and retire the timeline.
    private void purgeCycle(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            final LiveViewCheckpointLifecycle.ReconcileResult result = LiveViewCheckpointLifecycle.reconcile(
                    configuration,
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    true
            );
            Assert.assertFalse("the definition and epoch are fixed for the whole case", result.isEpochReplaced());
            Assert.assertFalse("this build wrote the directory it is reconciling", result.isFormatReset());
            Assert.assertEquals("no obsolete segment may fail to unlink", 0, result.getFailedPurgeCount());
            Assert.assertEquals("no orphan may fail removal", 0, result.getFailedOrphanCount());
        }
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
