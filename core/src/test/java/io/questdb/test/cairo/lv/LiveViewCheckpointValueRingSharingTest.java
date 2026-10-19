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
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The partitioned DOUBLE value functions - {@code first_value}, {@code last_value},
 * {@code nth_value} over a bounded RANGE frame - reuse the same persistent
 * {@code (ts, double)} checkpoint ring as {@code avg}/{@code sum}, so adjacent
 * roots share chunk pages instead of each writing a complete frame image.
 * <p>
 * Two things separate these functions from avg/sum and are what this test pins:
 * their ring carries NULLs (a first/last/nth value over a NULL row is NaN), which
 * the shared reader now admits; and they carry no running aggregate, so the ring
 * scalar slot is unused and restore rebuilds the emitted value from the ring.
 */
public class LiveViewCheckpointValueRingSharingTest extends AbstractLiveViewTest {

    // Rows per key one commit adds, at one-second spacing. Comfortably above
    // LiveViewCheckpointRingSeal.MIN_SHARED_CHUNK_ROWS so a seal's chunk carries
    // enough rows to be worth referencing from a later root.
    private static final int ROWS_PER_COMMIT = 120;
    private static final int COMMITS = 8;

    // first_value/nth_value read the oldest / k-th row of the frame, so CURRENT ROW
    // is fine. last_value over CURRENT ROW is a stateless per-row projection and
    // does not reach the ring class, so its frame trails the current row. Its ring
    // width is the high-bound offset (it keeps only the rows inside minDiff of the
    // current row plus one carried entry), so that offset must stay well above
    // LiveViewCheckpointRingSeal.MIN_SHARED_CHUNK_ROWS for the ring to be worth
    // sharing at all.
    private static final String FIRST_FN = "first_value(x)";
    private static final String FIRST_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW";
    private static final String LAST_FN = "last_value(x)";
    private static final String LAST_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND '200' SECOND PRECEDING";
    private static final String NTH_FN = "nth_value(x, 2)";
    private static final String NTH_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW";
    // IGNORE NULLS routes first_value to a subclass with its own ring overrides and
    // last_value to one that inherits the base's; both drop nulls from the ring, so
    // they exercise the all-finite ring beside the base functions' NaN-carrying one.
    private static final String FIRST_IG_FN = "first_value(x) ignore nulls";
    private static final String LAST_IG_FN = "last_value(x) ignore nulls";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal, so
        // roots pile up and every seal has earlier chunks to reference.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testValueRingRepairLocalizesThroughNulls() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);

                // An out-of-order row deep in history forces the repair path, which
                // restores a predecessor root from the timeline through the ring
                // reader before replaying the corrected interval forward.
                commitOutOfOrder(job, 137, 4242.0);
                driveRefreshToQuiescence(job);

                assertViewsMatchRecompute();
            }
        });
    }

    @Test
    public void testValueRingsShareChunksAndSurviveRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);

                // Sharing is the whole point: a value ring seal that reused nothing
                // would leave every segment referenced by exactly one root.
                assertSomeSegmentShared("lv_first");
                assertSomeSegmentShared("lv_last");
                assertSomeSegmentShared("lv_nth");
                assertSomeSegmentShared("lv_first_ig");
            }

            // A restart rebuilds the runtime state from the timeline through
            // restoreCheckpointRingState, so a following refresh that stays fault
            // free and matches a fresh recompute proves the ring round-tripped -
            // NaNs and all.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 3; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
            }
        });
    }

    private void assertSomeSegmentShared(String viewName) {
        final LiveViewInstance instance = viewInstance(viewName);
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointSegmentDirectoryReader directory =
                        new LiveViewCheckpointSegmentDirectoryReader(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            final int[] sharedSegments = {0};
            directory.iterateAll(entry -> {
                if (entry.referenceCount > 1) {
                    sharedSegments[0]++;
                }
            });
            Assert.assertTrue(
                    viewName + ": no data segment is referenced by more than one root",
                    sharedSegments[0] > 0
            );
        }
    }

    private void assertViewMatchesRecompute(String viewName, String fn, String frame) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + fn + " OVER (" + frame + ") AS v FROM base) ORDER BY 2, 1",
                "(SELECT ts, sym, v FROM " + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    private void assertViewsMatchRecompute() throws Exception {
        assertViewMatchesRecompute("lv_first", FIRST_FN, FIRST_FRAME);
        assertViewMatchesRecompute("lv_last", LAST_FN, LAST_FRAME);
        assertViewMatchesRecompute("lv_nth", NTH_FN, NTH_FRAME);
        assertViewMatchesRecompute("lv_first_ig", FIRST_IG_FN, FIRST_FRAME);
        assertViewMatchesRecompute("lv_last_ig", LAST_IG_FN, LAST_FRAME);
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Commits ROWS_PER_COMMIT ascending rows per key at one-second spacing, giving
    // the refresh job a turn on them. Every tenth row's value is NULL, so the frame
    // functions' rings carry NaN and their emitted value is NaN whenever the row
    // they read is one of them.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, x) VALUES ");
        final int firstSecond = (commit - 1) * ROWS_PER_COMMIT;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            final String value = second % 10 == 0 ? "null" : Double.toString(second + 0.5);
            final String valueB = second % 7 == 0 ? "null" : Double.toString(second + 1000.5);
            sql.append("('").append(timestamp(second)).append("', 'a', ").append(value).append("), ")
                    .append("('").append(timestamp(second)).append("', 'b', ").append(valueB).append(')');
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row deep in history and gives the refresh job a turn.
    private void commitOutOfOrder(LiveViewRefreshJob job, int second, double value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + timestamp(second) + "', 'a', " + value + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndViews() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv_first FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + FIRST_FN + " OVER (" + FIRST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_last FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + LAST_FN + " OVER (" + LAST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_nth FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + NTH_FN + " OVER (" + NTH_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_first_ig FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + FIRST_IG_FN + " OVER (" + FIRST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_last_ig FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + LAST_IG_FN + " OVER (" + LAST_FRAME + ") AS v FROM base");
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            store.of(checkpointsDir);
        }
        return store;
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
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

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }
}
