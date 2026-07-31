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
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Coverage for which keys a re-versioned logical boundary describes.
 * <p>
 * A localized repair replays {@code [L, H)} over runtime state the scratch overlay
 * has taken out of the way, and freezes the state it stands on as the new root
 * version of every boundary it crosses. That state is the whole truth about the
 * boundary - a key the replay never carried is removed from the root - so a repair
 * may only re-version a boundary when its replay reconstructs every key, not only
 * the keys its bounds were derived for.
 * <p>
 * A time-expiring dependency does reconstruct every key. Nothing a RANGE frame or an
 * anchor segment holds at a row at or above {@code R} came from below {@code L}, so a
 * key the replay never saw holds nothing there either - which is exactly what an
 * absent key restores as. A ROWS frame does not: it holds a key's last {@code Nmax}
 * rows however old they are, and the discovery walks back only far enough to warm up
 * the output key domain {@code Q}, the keys with a row in {@code [R, H)}. So a ROWS
 * repair used to publish roots naming a fraction of the view's keys, and a later
 * resume off one of them replayed forward from a state missing the rest. It now
 * truncates the timeline at {@code R} instead and re-seals a head from the restored
 * runtime, which is the same disposition a localized repair with no converged suffix
 * already took.
 */
public class LiveViewCheckpointRepairKeyCoverageTest extends AbstractLiveViewTest {

    // The one key the trickle feeds. Every other key is written once, far below the
    // corrections, and left cold - which is what puts its whole history under L.
    private static final String HOT_KEY = "k00";
    private static final int KEYS = 8;
    private static final String RANGE_WINDOW = "sum(x) OVER (PARTITION BY sym ORDER BY ts "
            + "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW)";
    private static final String ROWS_WINDOW = "sum(x) OVER (PARTITION BY sym ORDER BY ts "
            + "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so the boundaries a repair re-versions are
        // exactly the commits its interval covers.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testARangeRepairStillReVersionsItsBoundaries() throws Exception {
        // The control, and the reason the gate is shaped as a dependency question
        // rather than as a blanket refusal. The same workload, the same two
        // corrections and the same cold keys, over a frame that expires by time: the
        // repair splices, the roots it re-versions do come back naming one key, and
        // the resume that then restores one of them still lands on the recompute -
        // because a row above R reads no further back than R - 30s, which is L.
        assertMemoryLeak(() -> {
            createView(RANGE_WINDOW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildResumeHistory(job);
                final LiveViewInstance instance = viewInstance();

                commitHotKey(job, 365, 9_000);
                driveRefreshToQuiescence(job);
                assertRepairOutcome("range", "localized rebuild", null);
                Assert.assertTrue(
                        "a RANGE repair must re-version boundaries rather than truncate",
                        narrowedBoundaryCount(instance) > 0
                );

                commitHotKey(job, 395, 9_100);
                driveRefreshToQuiescence(job);
                assertRepairOutcome("range", "resume from anchor", "resume cheaper");
                assertViewMatchesRecompute(RANGE_WINDOW);
            }
        });
    }

    @Test
    public void testARowsRepairLeavesEveryBoundaryNamingEveryKey() throws Exception {
        assertMemoryLeak(() -> {
            createView(ROWS_WINDOW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Every key once at the bottom, then a trickle into one of them. The
                // cold keys' whole history is then far below any floor a correction
                // near the top of the trickle can derive.
                commitEveryKey(job, 10);
                for (int second = 20; second <= 400; second += 10) {
                    commitHotKey(job, second, second);
                }
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(ROWS_WINDOW);
                final LiveViewInstance instance = viewInstance();

                // Deep enough that the localized rebuild beats the resume on price,
                // and high enough that L sits above the cold keys' only row.
                commitHotKey(job, 55, 9_000);
                driveRefreshToQuiescence(job);
                assertRepairOutcome("rows", "localized rebuild", null);
                assertViewMatchesRecompute(ROWS_WINDOW);

                // Stated as the whole timeline, because both halves matter and the
                // first is what fails before the change: the roots at or below R
                // survive untouched, everything above them goes rather than being
                // re-versioned from a partial replay, and the post-replay seal puts a
                // head back at the frontier. Before the change the boundaries at 60,
                // 70 and 80 survived naming one key out of eight.
                Assert.assertEquals(
                        "[10=8, 20=8, 30=8, 40=8, 50=8, 400=8]",
                        boundaries(instance).toString()
                );
            }
        });
    }

    @Test
    public void testAResumeAfterARowsRepairKeepsEveryKeysHistory() throws Exception {
        assertMemoryLeak(() -> {
            createView(ROWS_WINDOW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildResumeHistory(job);

                // The repair that used to narrow the boundaries just under the second
                // correction.
                commitHotKey(job, 365, 9_000);
                driveRefreshToQuiescence(job);
                assertRepairOutcome("rows", "localized rebuild", null);
                assertViewMatchesRecompute(ROWS_WINDOW);

                // A correction one boundary further up, close enough to the head that
                // the resume reads fewer rows than a rebuild would. Its anchor is the
                // newest boundary below it - one of the three the repair above covers -
                // so the state the resume restores is the state that repair published.
                // Before the change that state named one key, and every cold key's row
                // above the anchor came back summing its own value alone.
                commitHotKey(job, 395, 9_100);
                driveRefreshToQuiescence(job);
                assertRepairOutcome("rows", "resume from anchor", "resume cheaper");
                assertViewMatchesRecompute(ROWS_WINDOW);
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Encoded partition keys are codec bytes rather than text, so they are compared
    // as hex: two distinct keys must never collapse into one set entry.
    private static String hex(byte[] key) {
        final StringBuilder sb = new StringBuilder(key.length * 2);
        for (byte b : key) {
            sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    private static String key(int index) {
        return String.format("k%02d", index);
    }

    private static String timestamp(int secondOfDay) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                secondOfDay / 3600,
                (secondOfDay % 3600) / 60,
                secondOfDay % 60
        );
    }

    private void assertRepairOutcome(String plan, String disposition, String denial) throws Exception {
        assertQuery("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition, " +
                "checkpoint_repair_last_denial FROM live_views()")
                .noLeakCheck().noRandomAccess()
                .returns("checkpoint_repair_plan\tcheckpoint_repair_last_disposition\t" +
                        "checkpoint_repair_last_denial\n" +
                        plan + "\t" + disposition + "\t" + (denial == null ? "" : denial) + "\n");
    }

    private void assertViewMatchesRecompute(String window) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + window + " AS s FROM base) ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * One entry per logical boundary, ascending, rendered as
     * {@code <second of day>=<keys the boundary's function root names>}.
     */
    private List<String> boundaries(LiveViewInstance instance) {
        final List<String> out = new ArrayList<>();
        final long epoch = ts("2026-01-01T00:00:00.000000Z");
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    final Set<String> keys = new HashSet<>();
                    root.of(dir, entry.rootRef);
                    root.getFunctionDirectoryRef(functionDirectoryRef);
                    functions.of(dir, functionDirectoryRef);
                    Assert.assertEquals("the view declares exactly one window function", 1, functions.size());
                    functions.getRootRef(0, functionRootRef);
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getPartitionMapRootRef(partitionMapRoot);
                    partitions.iterateAll(partitionMapRoot, partition -> keys.add(hex(partition.getKey())));
                    out.add((entry.maxTimestamp - epoch) / 1_000_000L + "=" + keys.size());
                });
            }
        }
        return out;
    }

    /**
     * The history both resume cases share: every key once at the bottom, a long
     * trickle into one of them, a second row for every key part-way up, and a short
     * trickle above that. The second row is what makes a narrowed boundary
     * observable - it gives the cold keys output above the anchor a resume restores
     * from - and the short tail is what makes that resume cheaper than a rebuild.
     */
    private void buildResumeHistory(LiveViewRefreshJob job) throws Exception {
        commitEveryKey(job, 10);
        for (int second = 20; second <= 400; second += 10) {
            commitHotKey(job, second, second);
        }
        commitEveryKey(job, 405);
        for (int second = 410; second <= 450; second += 10) {
            commitHotKey(job, second, second);
        }
        driveRefreshToQuiescence(job);
    }

    // One row for every key, at one designated timestamp, plus a refresh turn.
    private void commitEveryKey(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, x) VALUES ");
        final String rowTs = timestamp(second);
        for (int k = 0; k < KEYS; k++) {
            if (k > 0) {
                sql.append(", ");
            }
            sql.append("('").append(rowTs).append("', '").append(key(k)).append("', ").append(k + 1).append(')');
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // One row for the single hot key, plus a refresh turn.
    private void commitHotKey(LiveViewRefreshJob job, int second, long x) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(second) + "', '" + HOT_KEY + "', " + x + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createView(String window) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + window + " AS s FROM base");
    }

    private int narrowedBoundaryCount(LiveViewInstance instance) {
        int narrowed = 0;
        for (String boundary : boundaries(instance)) {
            if (!boundary.endsWith("=" + KEYS)) {
                narrowed++;
            }
        }
        return narrowed;
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
