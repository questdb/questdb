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
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Coverage for {@code cairo.sql.window.map.fusion.enabled} over a live view.
 * <p>
 * The switch is the operational escape hatch for a shape whose Map implementation or key
 * distribution regresses in the field, and a live view fuses harder than generic SQL does:
 * the group's accumulators move into the anchor map's own value, and the seal writes one
 * fused window root where it would otherwise write a legacy root per function. So the
 * hatch has to reach the refresh runtime, the durable shape, the restart that reads it
 * back and the out-of-order repair that rewrites it - not just the generic path.
 * <p>
 * What the switch must NOT change is the compile. The group stays worked out either way,
 * exactly as {@code WindowMapState.createGroups} leaves a generic plan compiled and
 * unbound, so the cases below assert the plan is still on the factory while the window
 * declines it. That is also what makes the hatch cheap: declining is the path
 * {@code LiveViewWindow.bindCheckpointWindowStatePlan(null)} already migrates state
 * through, not a second implementation of the runtime.
 */
public class LiveViewFusionDisabledTest extends AbstractLiveViewTest {

    private static final String DAILY_ANCHOR = "2026-01-01T";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpFusionDisabled() {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        // One logical boundary per commit, so a case has a sealed timeline to restart off.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testAKillSwitchedViewSurvivesARestart() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 40; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                assertViewMatchesRecompute();
                Assert.assertFalse("the seal must write a legacy anchor root", isFusedHead());
            }

            // A real restart: the registry is dropped, the view's SQL recompiles under the
            // same switch and the runtime comes back off the roots the seal above wrote.
            restartCycle();

            Assert.assertNull(
                    "the recompiled window must decline the plan too",
                    window().getCheckpointWindowStatePlan()
            );
            // Without this the case cannot tell a restore from a rebuild: a view that
            // failed to restore and re-derived itself from the applied base would match
            // the recompute just as well, and prove nothing about the roots.
            Assert.assertTrue(
                    "the restart must restore off the timeline rather than rebuild from the base",
                    instance().isCheckpointRestoreSucceeded()
            );
            Assert.assertFalse("the restored head must still be a legacy anchor root", isFusedHead());
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");

            // ...and the restored state must keep accumulating rather than restart at zero,
            // which a restore that silently dropped the functions' private maps would.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                insertAccount(resumed, timestamp(50), "acct-1", 100.0);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAKillSwitchedViewSurvivesAnOutOfOrderCorrection() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 10; second <= 60; second += 10) {
                    insertAccount(job, timestamp(second), "acct-1", second / 10.0);
                }
                insertAccount(job, timestamp(70), "acct-2", 9.0);
                final long repairedBefore = repairedRows(instance());

                // Below the frontier, so the refresh cannot append: it has to replay the
                // dependency interval over the legacy roots the kill switch left behind.
                insertAccount(job, timestamp(35), "acct-1", 100.0);
                Assert.assertTrue(
                        "the row below the frontier must be repaired rather than appended",
                        repairedRows(instance()) > repairedBefore
                );

                Assert.assertNull(
                        "the repair must not fuse the window",
                        window().getCheckpointWindowStatePlan()
                );
                assertViewMatchesRecompute();
                Assert.assertFalse("the repair must republish a legacy anchor root", isFusedHead());
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testASealWithoutFusionRestartsIntoTheFusedShape() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 40; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                Assert.assertNull(
                        "the switch must leave this seal unfused",
                        window().getCheckpointWindowStatePlan()
                );
                Assert.assertFalse("the seal must write a legacy anchor root", isFusedHead());
            }

            // The operator puts the hatch back. The restart recompiles under the switch, so
            // the window adopts the plan this time and the restore has to carry each
            // function's own legacy root up into the fused value. That upgrade adapter was
            // reachable only across a version change before the gate landed; turning the
            // switch back on is now a supported way to reach it, so the direction is
            // covered here rather than left to the release it first ships in.
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "true");
            restartCycle();

            Assert.assertNotNull(
                    "the recompiled window must adopt the plan once the switch is back on",
                    window().getCheckpointWindowStatePlan()
            );
            Assert.assertTrue(
                    "the restart must restore off the legacy roots rather than rebuild from the base",
                    instance().isCheckpointRestoreSucceeded()
            );
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");

            // Only a later row tells the upgrade apart from a reset: a partition that came
            // back empty would answer this row's own amount rather than the running total.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                insertAccount(resumed, timestamp(50), "acct-1", 100.0);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTheKillSwitchLeavesEveryGroupedFunctionOnItsPrivateMap() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, timestamp(0), "acct-1", 5.0);
                insertAccount(job, timestamp(10), "acct-2", 7.0);
                insertAccount(job, timestamp(20), "acct-1", 11.0);

                // The compile is untouched: the group is still worked out and still visible
                // on the factory, and only the runtime binding is withheld.
                Assert.assertNotNull(
                        "the compiler must still work the group out",
                        windowFactory(instance()).getCheckpointWindowStatePlan()
                );
                final LiveViewWindow window = window();
                Assert.assertNull(
                        "the kill switch must leave the window unfused",
                        window.getCheckpointWindowStatePlan()
                );
                Assert.assertEquals(2, window.getAnchorMapSize());

                // Both SELECT-list calls are back on the map each owns outside a group, and
                // each holds every partition - which is what the fused shape would empty.
                final ObjList<WindowFunction> functions = windowFactory(instance()).getWindowFunctions();
                Assert.assertEquals("the target shape declares two window calls", 2, functions.size());
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final WindowFunction function = functions.getQuick(i);
                    Assert.assertFalse("function " + i + " must own its state", function.isWindowStateOwned());
                    Assert.assertEquals(
                            "function " + i + " must hold every partition",
                            2,
                            function.getPartitionMap().size()
                    );
                }

                assertViewMatchesRecompute();
                Assert.assertFalse("the seal must write a legacy anchor root", isFusedHead());
                assertNoRefreshFaults("lv");
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * Base rows a repair replayed over this instance's lifetime, through either
     * disposition: the resume from a boundary below the change, or the localized rebuild
     * over the change's own dependency interval.
     */
    private static long repairedRows(LiveViewInstance instance) {
        return instance.getO3BoundaryReplayRows() + instance.getO3ResumeReplayRows();
    }

    private static String timestamp(int secondOfDay) {
        return DAILY_ANCHOR + String.format("09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    private static WindowRecordCursorFactory windowFactory(LiveViewInstance instance) {
        RecordCursorFactory factory = instance.getCompiledFactory();
        while (factory != null) {
            if (factory instanceof WindowRecordCursorFactory windowFactory) {
                return windowFactory;
            }
            if (factory instanceof QueryProgress) {
                factory = factory.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    /**
     * Compares the view against a from-base recompute of the same window. ANCHOR is
     * live-view syntax, so the daily bucket is written out as an ordinary partition term.
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

    private void createTargetView() throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol, amount double) "
                + "timestamp(created_at) partition by hour wal");
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, sum(amount) over w as cumulative_sum, "
                + "count(account_id) over w as cumulative_count "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, double amount)
            throws Exception {
        execute("insert into tx values ('" + timestamp + "', '" + account + "', " + amount + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    /**
     * Whether the newest sealed boundary carries a fused window root rather than the
     * legacy anchor root plus function directory.
     */
    private boolean isFusedHead() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration)
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), newest));
            root.of(checkpointsDir, newest.rootRef);
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            root.getStateRootRef(stateRootRef);
            return !stateRootRef.isNull() && windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef);
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

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }

    private LiveViewWindow window() {
        final LiveViewInstance instance = instance();
        final LiveViewWindow window = instance.getAnchorWindow();
        Assert.assertNotNull("the anchored view must have built its window", window);
        return window;
    }
}
