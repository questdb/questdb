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
 * {@code count} over a partitioned bounded RANGE frame buffers one designated timestamp
 * per in-window row and the frame's running count, and nothing else. It therefore shares
 * the persistent checkpoint ring like the value and deque families, under a valueless
 * ring kind whose chunk is the timestamp page alone - there is no value column to write.
 * <p>
 * This drives the one production class every {@code count} variant over such a frame
 * resolves to ({@code CountFunctionFactoryHelper.CountOverPartitionRangeFrameFunction},
 * reached from the {@code count(*)} factory as well as the per-type ones), proving the
 * rings share chunks, survive a restart through the ring reader, localize an
 * out-of-order repair, and match a fresh recompute for a current-row and a trailing
 * frame, with NULL arguments the ring never buffers.
 */
public class LiveViewCheckpointCountRingSharingTest extends AbstractLiveViewTest {

    private static final int COMMITS = 8;
    // A current-row frame: every buffered row is in-frame, so the restored count covers
    // the whole ring.
    private static final String CUR_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW";
    private static final int ROWS_PER_COMMIT = 120;
    // A trailing frame: the ring's newest rows sit outside the frame, so the stored
    // frame size is below the live row count and restore must keep the two apart.
    private static final String TRAIL_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND '200' SECOND PRECEDING";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testCountRingRepairLocalizes() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);

                // An out-of-order row deep in history forces the localized repair path,
                // which restores a predecessor root from the timeline through the ring
                // reader before replaying the corrected interval forward. It carries a
                // non-null argument, so every frame it joins counts one more row.
                commitOutOfOrderHalfSecond(job, 137);
                driveRefreshToQuiescence(job);

                assertViewsMatchRecompute();
            }
        });
    }

    @Test
    public void testCountRingsShareChunksAndSurviveRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);

                assertSomeSegmentShared("lv_count_star");
                assertSomeSegmentShared("lv_count_x");
                assertSomeSegmentShared("lv_count_trail");
                assertSomeSegmentShared("lv_count_sym");
            }

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
        assertViewMatchesRecompute("lv_count_star", "count(*)", CUR_FRAME);
        assertViewMatchesRecompute("lv_count_x", "count(xd)", CUR_FRAME);
        assertViewMatchesRecompute("lv_count_trail", "count(xd)", TRAIL_FRAME);
        assertViewMatchesRecompute("lv_count_sym", "count(tag)", CUR_FRAME);
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Commits ROWS_PER_COMMIT ascending rows per key at one-second spacing and gives the
    // refresh job a turn. Every tenth/eleventh value is NULL, which count(arg) excludes
    // from both the frame count and the ring while count(*) keeps, so the two views
    // diverge and the ring's row count is not simply the row number.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, xd, tag) VALUES ");
        final int firstSecond = (commit - 1) * ROWS_PER_COMMIT;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            for (int k = 0; k < 2; k++) {
                if (k > 0) {
                    sql.append(", ");
                }
                sql.append("('").append(timestamp(second)).append("', '").append(k == 0 ? 'a' : 'b').append("', ")
                        .append(doubleValue(second, k)).append(", ")
                        .append(tagValue(second, k)).append(')');
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row half a second past an in-order timestamp - a fresh
    // designated timestamp, so it ties with no existing row and the recompute ordering
    // stays deterministic - and gives the refresh job a turn.
    private void commitOutOfOrderHalfSecond(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String ts = timestamp(second).replace(".000000Z", ".500000Z");
        execute("INSERT INTO base (ts, sym, xd, tag) VALUES ('" + ts + "', 'a', 4242.5, 'late')");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndViews() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, xd DOUBLE, tag SYMBOL) " +
                "TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv_count_star FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, count(*) OVER (" + CUR_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_count_x FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, count(xd) OVER (" + CUR_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_count_trail FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, count(xd) OVER (" + TRAIL_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_count_sym FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, count(tag) OVER (" + CUR_FRAME + ") AS v FROM base");
    }

    private static String doubleValue(int second, int key) {
        if (second % 10 == key) {
            return "null";
        }
        return Double.toString(((second * 31 + 17 + key * 29) % 101) + 0.25);
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

    private static String tagValue(int second, int key) {
        if (second % 11 == key) {
            return "null";
        }
        return "'t" + ((second * 17 + key) % 7) + "'";
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
