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
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coverage for the route a live view's one restart recovery attempt records.
 * <p>
 * A restart has two ways to end up with correct rows and only one of them is the
 * checkpoint timeline. {@code tryRestoreFromTimeline} catches every {@link Throwable}
 * and falls through to the applied-base rebuild, which throws the published roots away
 * and recomputes the entire window from the base table. The rebuild produces the same
 * rows, faults no refresh cycle, fails no seal and reports the same
 * {@code isCheckpointRestoreSucceeded()} - so every oracle a restart test would
 * otherwise reach for is green over it, and a restore that silently stopped working
 * looks exactly like one that works.
 * <p>
 * The cases below pin that pairing directly: the fallback case asserts the success flag
 * is {@code true} <b>while</b> the route is {@code fallback_rebuild}, which is the
 * demonstration that the flag cannot carry a restore assertion on its own. The witness
 * is what carries it.
 */
public class LiveViewCheckpointRestoreRouteTest extends AbstractLiveViewTest {

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so a handful of rows leaves a real ladder to
        // restart off rather than a single root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testAFailedRebuildRecordsABlockedRoute() throws Exception {
        // The timeline is gone, so the restart has nothing to restore, and the rebuild
        // that covers for that cannot read the base either. The view then has no derived
        // state at all - the disposition no other case can reach, because a rebuild that
        // works covers for every restore that does not.
        final AtomicBoolean failBaseColumnReads = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Partition column reads only. The WAL segment copies carry the same
                // file name and the apply that reads them must keep working, or the
                // fixture never gets its rows into the base table.
                if (failBaseColumnReads.get()
                        && Utf8s.endsWithAscii(name, "amount.d")
                        && !Utf8s.containsAscii(name, "wal")) {
                    return -1;
                }
                return super.openRO(name);
            }
        }, () -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), "acct-1", second + 1.0);
                }
            }

            removeTimeline();
            failBaseColumnReads.set(true);
            try {
                restartCycle();
            } finally {
                failBaseColumnReads.set(false);
            }

            final LiveViewInstance instance = instance();
            assertRoute(LiveViewCheckpointRestoreRoute.BLOCKED, instance);
            Assert.assertFalse(
                    "a blocked attempt resolved no derived state, so it must not report success",
                    instance.isCheckpointRestoreSucceeded()
            );
            Assert.assertEquals(
                    "the attempt must have started exactly one rebuild before blocking",
                    1L,
                    instance.getCheckpointRebuildAttempts()
            );
            Assert.assertEquals(
                    "a blocked attempt names no root",
                    Numbers.LONG_NULL,
                    instance.getCheckpointRestoreCheckpointId()
            );
        });
    }

    @Test
    public void testAnAbsentTimelineRecordsAFallbackRebuildThatStillReportsSuccess() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                assertViewMatchesRecompute();
            }

            // The roots are still on disk; only the ladder that addresses them is gone,
            // which is what sends the restart down the applied-base rebuild.
            removeTimeline();
            restartCycle();

            final LiveViewInstance instance = instance();
            assertRebuiltFromAppliedBase("lv");
            Assert.assertTrue(
                    "the rebuild resolves the derived state too, which is exactly why the success flag"
                            + " cannot carry a restore assertion on its own",
                    instance.isCheckpointRestoreSucceeded()
            );
            Assert.assertTrue(
                    "the rebuild retires the timeline before it replays",
                    instance.getCheckpointTimelineResets() > 0
            );
            Assert.assertEquals(
                    "a rebuild names no root: it restored from none",
                    Numbers.LONG_NULL,
                    instance.getCheckpointRestoreGeneration()
            );
            // The rows a naive restart oracle would have stopped at. They are correct
            // either way, which is the point.
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testARestartOffPublishedRootsRecordsTheRootItRestoredFrom() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            final long publishedGeneration;
            final long newestCheckpointId;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                assertViewMatchesRecompute();
                publishedGeneration = newestGeneration();
                newestCheckpointId = newestCheckpointId();
            }

            restartCycle();

            // Captured before the restart, because the first post-restart flush reseals
            // over the lineage: the witness is stamped at restore time and survives it,
            // the ladder does not.
            assertRestoredFromTimeline("lv");
            final LiveViewInstance instance = instance();
            Assert.assertEquals(
                    "the restart must name the root the pre-restart writer left as the newest boundary",
                    newestCheckpointId,
                    instance.getCheckpointRestoreCheckpointId()
            );
            Assert.assertEquals(
                    "the restart must restore under the generation the previous process published",
                    publishedGeneration,
                    instance.getCheckpointRestoreGeneration()
            );
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");

            // A restore that dropped the accumulators would answer this row's own amount
            // rather than the running total, and the witness above would still be green.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                insertAccount(resumed, timestamp(40), "acct-1", 100.0);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    private static void assertRoute(int expected, LiveViewInstance instance) {
        Assert.assertEquals(
                "live view 'lv' took the wrong restart recovery route",
                LiveViewCheckpointRestoreRoute.name(expected),
                LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute())
        );
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static String timestamp(int secondOfDay) {
        return "2026-01-01T09:" + String.format("%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT created_at, account_id, "
                        + "sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum "
                        + "FROM (SELECT created_at, account_id, amount, " + bucket + " AS bucket FROM tx)"
                        + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
    }

    private void createTargetView() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, double amount)
            throws Exception {
        execute("INSERT INTO tx VALUES ('" + timestamp + "', '" + account + "', " + amount + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private long newestCheckpointId() {
        final LiveViewInstance instance = instance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            store.of(dir);
            timeline.of(dir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue(
                        "the view must have sealed a boundary to restart off",
                        timeline.last(pin.getTimelineRootRef(), newest)
                );
                return newest.checkpointId;
            }
        }
    }

    private long newestGeneration() {
        final LiveViewInstance instance = instance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)
        ) {
            store.of(dir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                return pin.getGeneration();
            }
        }
    }

    private void removeTimeline() {
        try (
                Path dir = checkpointsDir(instance());
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertTrue(
                    "the fixture must have published a timeline to remove",
                    configuration.getFilesFacade().removeQuiet(timelinePath.$())
            );
        }
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }
}
