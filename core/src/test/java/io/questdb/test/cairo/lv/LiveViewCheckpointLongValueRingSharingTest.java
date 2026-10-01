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
 * The partitioned LONG/DATE/TIMESTAMP value functions - {@code first_value},
 * {@code last_value}, {@code nth_value} over a bounded RANGE frame - reuse the same
 * persistent checkpoint ring as the DOUBLE ones, but their value column stores a
 * raw 64-bit payload instead of exact-double bits. A LONG value has no floating
 * point structure to compress and, crucially, must never be reinterpreted as a
 * double: a bit pattern that happens to be a NaN would be canonicalized and the
 * value corrupted. This test drives the two production paths end to end - the LONG
 * column exercises the dedicated {@code *LongWindowFunctionFactory} classes, the
 * TIMESTAMP column exercises the shared {@code *WindowFunctionFactoryHelper} bases
 * that DATE and TIMESTAMP both use - proving they share chunks, survive a restart
 * through the ring reader, localize an out-of-order repair, and round-trip NULLs.
 */
public class LiveViewCheckpointLongValueRingSharingTest extends AbstractLiveViewTest {

    private static final int ROWS_PER_COMMIT = 120;
    private static final int COMMITS = 8;

    private static final String FIRST_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW";
    private static final String LAST_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND '200' SECOND PRECEDING";
    private static final String NTH_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW";

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
                // reader before replaying the corrected interval forward. The value
                // is a large long whose low bits alias a double NaN, so a repair that
                // ever routed it through a double would drop it.
                commitOutOfOrder(job, 137);
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

                assertSomeSegmentShared("lv_first_l");
                assertSomeSegmentShared("lv_last_l");
                assertSomeSegmentShared("lv_nth_l");
                assertSomeSegmentShared("lv_first_t");
                assertSomeSegmentShared("lv_last_t");
                assertSomeSegmentShared("lv_nth_t");
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
        assertViewMatchesRecompute("lv_first_l", "first_value(xl)", FIRST_FRAME);
        assertViewMatchesRecompute("lv_last_l", "last_value(xl)", LAST_FRAME);
        assertViewMatchesRecompute("lv_nth_l", "nth_value(xl, 2)", NTH_FRAME);
        assertViewMatchesRecompute("lv_first_ig_l", "first_value(xl) ignore nulls", FIRST_FRAME);
        assertViewMatchesRecompute("lv_last_ig_l", "last_value(xl) ignore nulls", LAST_FRAME);
        assertViewMatchesRecompute("lv_first_t", "first_value(xt)", FIRST_FRAME);
        assertViewMatchesRecompute("lv_last_t", "last_value(xt)", LAST_FRAME);
        assertViewMatchesRecompute("lv_nth_t", "nth_value(xt, 2)", NTH_FRAME);
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Commits ROWS_PER_COMMIT ascending rows per key at one-second spacing. Every
    // tenth long is NULL and every seventh timestamp is NULL, so the frame
    // functions' rings carry the LONG_NULL sentinel and their emitted value is NULL
    // whenever the row they read is one of them. Long values deliberately include a
    // large value whose low 32 bits form a double NaN payload.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, xl, xt) VALUES ");
        final int firstSecond = (commit - 1) * ROWS_PER_COMMIT;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            final String longA = second % 10 == 0 ? "null" : Long.toString(0x7ff0_0000_0000_0000L + second);
            final String longB = second % 5 == 0 ? "null" : Long.toString(-3L * second - 11);
            final String tsA = second % 7 == 0 ? "null" : (second * 1_000L + 500) + "::timestamp";
            final String tsB = second % 3 == 0 ? "null" : (second * 2_000L + 250) + "::timestamp";
            sql.append("('").append(timestamp(second)).append("', 'a', ").append(longA).append(", ").append(tsA).append("), ")
                    .append("('").append(timestamp(second)).append("', 'b', ").append(longB).append(", ").append(tsB).append(')');
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row deep in history and gives the refresh job a turn.
    private void commitOutOfOrder(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, xl, xt) VALUES ('" + timestamp(second)
                + "', 'a', " + (0x7ff8_0000_0000_00ffL) + ", " + (second * 9_000L + 3) + "::timestamp)");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndViews() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, xl LONG, xt TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv_first_l FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, first_value(xl) OVER (" + FIRST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_last_l FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, last_value(xl) OVER (" + LAST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_nth_l FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, nth_value(xl, 2) OVER (" + NTH_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_first_ig_l FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, first_value(xl) ignore nulls OVER (" + FIRST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_last_ig_l FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, last_value(xl) ignore nulls OVER (" + LAST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_first_t FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, first_value(xt) OVER (" + FIRST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_last_t FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, last_value(xt) OVER (" + LAST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_nth_t FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, nth_value(xt, 2) OVER (" + NTH_FRAME + ") AS v FROM base");
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
