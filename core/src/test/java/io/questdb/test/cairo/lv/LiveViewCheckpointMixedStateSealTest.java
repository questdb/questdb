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
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * One live view carrying both checkpoint state shapes at once: a RANGE-framed
 * {@code count} - a valueless ring whose chunk is a single timestamp page - beside a
 * ROWS-framed {@code avg}, which keeps a whole-state image in a data page the leaf
 * names by reference.
 * <p>
 * The seal freezes both functions into one scratch owner, and the frozen partition
 * shells and state page references it hands out are pooled across seals. The two
 * arms therefore have to keep their references apart: the whole-state arm names a
 * page it drew from the shared reference pool, and the ring arm writes its chunk
 * reference into whatever object the pooled shell it reuses already holds. A shell
 * the whole-state arm filled at one seal and the ring arm reuses at the next puts
 * both functions on one reference object, and the seal publishes a root whose ring
 * entry names the other function's whole-state page - which the next seal's
 * predecessor probe, and any restore, rejects outright.
 * <p>
 * The commits below grow the key set by one per seal, which is what moves the
 * boundary between the two functions' shells along the shared pool.
 */
public class LiveViewCheckpointMixedStateSealTest extends AbstractLiveViewTest {

    // Bounded ROWS: a positional frame, so avg keeps a whole-state image rather than
    // sharing the timestamp-keyed ring, and the image is too wide to inline.
    private static final String AVG_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";
    private static final int COMMITS = 6;
    // Bounded RANGE: count shares the checkpoint ring under the valueless kind, so a
    // one-chunk partition spends exactly one state page reference - the same count the
    // whole-state arm always writes.
    private static final String COUNT_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW";
    private static final int ROWS_PER_COMMIT = 8;
    private static final String VIEW_NAME = "lv_mixed";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal, so every
        // commit's key growth crosses a seal boundary.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testRingAndWholeStateFunctionsKeepDistinctStatePages() throws Exception {
        assertMemoryLeak(() -> {
            long newestBoundary;
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW " + VIEW_NAME + " FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, sym,"
                    + " count(x) OVER (" + COUNT_FRAME + ") AS c,"
                    + " avg(x) OVER (" + AVG_FRAME + ") AS a"
                    + " FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitGrowingKeySet(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewCarriesBothStateShapes();
                assertViewSealsCleanly();
                assertViewMatchesRecompute();
                newestBoundary = assertViewPublishedCheckpoints("before the restart", Long.MIN_VALUE);
            }

            // A restart reads the published roots back through the ring reader and the
            // whole-state page reader, so a root whose ring entry names the other
            // function's page cannot survive it.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 2; commit++) {
                    commitGrowingKeySet(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertRestartRestoredFromPublishedRoots(newestBoundary);
                assertViewSealsCleanly();
                assertViewMatchesRecompute();
                assertViewPublishedCheckpoints("after the restart", newestBoundary);
            }
        });
    }

    private void assertViewCarriesBothStateShapes() {
        final ObjList<WindowFunction> functions = unwrapWindowFunctions(viewInstance(VIEW_NAME));
        Assert.assertEquals("the view must carry exactly the two functions this case is about", 2, functions.size());
        final WindowFunction ring = functions.getQuick(0);
        final WindowFunction wholeState = functions.getQuick(1);
        Assert.assertTrue("count over a bounded RANGE frame must share the checkpoint ring",
                ring.supportsCheckpointRingState());
        Assert.assertFalse("avg over a bounded ROWS frame must not share the checkpoint ring",
                wholeState.supportsCheckpointRingState());
        Assert.assertTrue("avg over a bounded ROWS frame must checkpoint its state",
                wholeState.supportsCheckpointState());
        Assert.assertFalse(
                "avg over a bounded ROWS frame must keep its image in a data page it names, not inline in the leaf;"
                        + " an inlined image never draws a pooled state page reference and the case stops covering"
                        + " the shape",
                LiveViewCheckpointContracts.isInlineableStateLength(wholeState.checkpointStateFixedLength())
        );
    }

    /**
     * The positive half of the oracle. The two counters only say that nothing went
     * wrong, and a view that sealed no checkpoint at all scores 0 on both - so on their
     * own they would keep passing if this case ever stopped sealing. The published
     * ladder is what says a seal happened: one {@code (maxTimestamp, position)} pair per
     * logical root, read back from the timeline on disk rather than from a counter.
     * <p>
     * A view that published nothing at all fails here before the assertions, when the
     * ladder read finds no generation to pin - which is the same red, and names its own
     * cause. Retiring the published timeline just before this call is what confirmed
     * that: both counters above still read 0 over it, and only this fails.
     * <p>
     * Counting the boundaries this phase added, rather than the ladder's total length,
     * is what makes the count match the mechanism. The aliasing needs a shell the
     * whole-state arm filled at one seal and the ring arm reuses at the next, so it
     * takes two seals inside ONE writer's lifetime with the key set growing between
     * them; a phase that sealed once would leave a ladder two slots long, and a
     * ladder-length check would call that covered. The phase's own boundaries are the
     * ones above the newest boundary the previous phase left, which is exactly what
     * {@code newerThan} carries in.
     *
     * @param newerThan the newest boundary the view had published before this phase, so
     *                  the phase has to add its own above it. {@link Long#MIN_VALUE} for
     *                  the first phase, whose every boundary is its own
     * @return the newest boundary timestamp the view has published
     */
    private long assertViewPublishedCheckpoints(String phase, long newerThan) {
        final LongList ladder = snapshotCheckpointLadder(viewInstance(VIEW_NAME));
        int sealsThisPhase = 0;
        for (int i = 0, n = ladder.size(); i < n; i += 2) {
            if (ladder.getQuick(i) > newerThan) {
                sealsThisPhase++;
            }
        }
        Assert.assertTrue(
                "live view '" + VIEW_NAME + "' must publish at least two checkpoints " + phase
                        + ", within the one writer lifetime that phase spans: one seal never hands the ring"
                        + " arm a shell the whole-state arm filled, so a single-seal phase covers nothing"
                        + " [sealsThisPhase=" + sealsThisPhase + ", ladderEntries=" + (ladder.size() / 2)
                        + ", previous=" + newerThan + ']',
                sealsThisPhase >= 2
        );
        // Ascending pairs, so the newest boundary's timestamp is the second-to-last slot.
        final long newestBoundary = ladder.getQuick(ladder.size() - 2);
        Assert.assertTrue(
                "live view '" + VIEW_NAME + "' published no checkpoint above the one it already held "
                        + phase + " [newestBoundary=" + newestBoundary + ", previous=" + newerThan + ']',
                newestBoundary > newerThan
        );
        return newestBoundary;
    }

    /**
     * What makes the restart phase a restart. The restore is silent about its own
     * outcome from the caller's side: {@code tryRestoreFromTimeline} catches every
     * {@link Throwable} and falls through to {@code rebuildTimelineRecoveryFromAppliedBase},
     * which recomputes the whole window from the applied base. That rebuild produces
     * correct rows, faults no refresh cycle and fails no seal, so the recompute oracle,
     * the fault count and the seal-failure count all stay green over it - and the phase
     * would have stopped reading the published roots back, which is the only place a
     * root whose ring entry names the other function's page surfaces.
     * <p>
     * {@link LiveViewInstance#isCheckpointRestoreSucceeded()} cannot tell those apart on
     * its own: the rebuild path sets it too, so it reports that the restart resolved its
     * derived state rather than which way. It is asserted here as the necessary half -
     * it goes false only when the rebuild failed as well - and the lineage below is the
     * half that discriminates. A rebuild retires the timeline before it replays, so the
     * boundaries the pre-restart writer published are gone from the ladder afterwards;
     * a restore resumes on them and leaves them in place.
     *
     * @param newestBoundaryBeforeRestart the newest boundary the pre-restart writer
     *                                    published, which a resumed ladder still carries
     */
    private void assertRestartRestoredFromPublishedRoots(long newestBoundaryBeforeRestart) {
        final LiveViewInstance instance = viewInstance(VIEW_NAME);
        Assert.assertTrue(
                "live view '" + VIEW_NAME + "' did not resolve its derived state after the restart;"
                        + " both the timeline restore and the applied-base rebuild failed",
                instance.isCheckpointRestoreSucceeded()
        );
        final LongList ladder = snapshotCheckpointLadder(instance);
        boolean carriesPreRestartLineage = false;
        for (int i = 0, n = ladder.size(); i < n; i += 2) {
            if (ladder.getQuick(i) == newestBoundaryBeforeRestart) {
                carriesPreRestartLineage = true;
                break;
            }
        }
        Assert.assertTrue(
                "live view '" + VIEW_NAME + "' rebuilt from the applied base instead of restoring off the"
                        + " roots it had published: the rebuild retires the timeline, so the pre-restart"
                        + " boundary is no longer on the ladder [newestBoundaryBeforeRestart="
                        + newestBoundaryBeforeRestart + ", ladderEntries=" + (ladder.size() / 2) + ']',
                carriesPreRestartLineage
        );
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, count(x) OVER (" + COUNT_FRAME + ") AS c,"
                        + " avg(x) OVER (" + AVG_FRAME + ") AS a FROM base) ORDER BY 2, 1",
                "(SELECT ts, sym, c, a FROM " + VIEW_NAME + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(VIEW_NAME);
    }

    /**
     * The decisive assertion. A root whose ring entry names the whole-state arm's page
     * is rejected by the very next seal's predecessor probe, which fails the head
     * checkpoint write; the refresh cycle itself still succeeds, so only the seal
     * failure counter tells a working checkpoint apart from one that stopped advancing.
     */
    private void assertViewSealsCleanly() {
        Assert.assertEquals(
                "live view '" + VIEW_NAME + "' must not fail a head checkpoint seal",
                0L,
                viewInstance(VIEW_NAME).getCheckpointSealFailures()
        );
    }

    // Commit c carries c distinct keys, so the ring function's frozen partition count
    // grows by one per seal and its shells walk into the ones the whole-state function
    // filled at the seal before.
    private void commitGrowingKeySet(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, x) VALUES ");
        final int firstSecond = (commit - 1) * ROWS_PER_COMMIT;
        boolean isFirstRow = true;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            final int second = firstSecond + i;
            for (int k = 0; k < commit; k++) {
                if (!isFirstRow) {
                    sql.append(", ");
                }
                isFirstRow = false;
                sql.append("('").append(timestamp(second)).append("', 'k").append(k).append("', ")
                        .append((second * 31 + 17 + k * 29) % 101).append(')');
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        return instance;
    }

    private static String timestamp(int second) {
        return String.format(
                "2026-01-%02dT%02d:%02d:%02d.000000Z",
                1 + second / 86_400,
                (second % 86_400) / 3600,
                (second % 3600) / 60,
                second % 60
        );
    }
}
