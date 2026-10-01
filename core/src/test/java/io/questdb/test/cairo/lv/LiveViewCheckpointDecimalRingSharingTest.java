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
 * The partitioned DECIMAL window functions over a bounded RANGE frame now share the
 * persistent checkpoint ring the DOUBLE and LONG families already share, at every
 * physical width. A width of 8, 16, 32 or 64 bits rides the existing one-word raw
 * payload; 128 and 256 bits ride the wide value pages this test's widest columns
 * exercise. The scalar continuation state widens independently of the value: a narrow
 * {@code avg} carries a 64-bit accumulator over a 16-bit value, while a 128-bit
 * {@code sum} carries a 256-bit one.
 * <p>
 * Every admitted family is driven at every width - {@code avg} (plain and rescaled),
 * {@code sum}, {@code max}, {@code min}, {@code first_value} (base and IGNORE NULLS),
 * {@code last_value} and {@code nth_value} - proving they share chunks, survive a
 * restart through the ring reader, localize an out-of-order repair, and match a fresh
 * recompute including the NULL rows the ring must carry verbatim.
 */
public class LiveViewCheckpointDecimalRingSharingTest extends AbstractLiveViewTest {

    private static final int COMMITS = 6;
    // A current-row frame (minDiff == 0): every ring row is in-frame.
    private static final String CUR_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '400' SECOND PRECEDING AND CURRENT ROW";
    private static final int ROWS_PER_COMMIT = 90;
    // A trailing frame (minDiff > 0): the ring's newest rows sit in the waiting room
    // outside the frame, so frameSize < size.
    private static final String TRAIL_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '400' SECOND PRECEDING AND '5' SECOND PRECEDING";
    // One column per physical decimal width: 8, 16, 32, 64, 128 and 256 bits.
    private static final String[] WIDTHS = {"d8", "d16", "d32", "d64", "d128", "d256"};

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
    public void testDecimalRingRepairLocalizes() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);

                // An out-of-order row inside history forces the localized repair path,
                // which restores a predecessor root from the timeline through the ring
                // reader before replaying the corrected interval forward. Its value is
                // an extreme high at every width, so max/first/last/nth all re-emit.
                commitOutOfOrder(job, 51);
                driveRefreshToQuiescence(job);

                assertViewsMatchRecompute();
            }
        });
    }

    @Test
    public void testDecimalRingsShareChunksAndSurviveRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                for (String width : WIDTHS) {
                    assertSomeSegmentShared("lv_avg_" + width);
                    assertSomeSegmentShared("lv_sum_" + width);
                    assertSomeSegmentShared("lv_max_" + width);
                    assertSomeSegmentShared("lv_first_" + width);
                }
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 2; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                // A failed restore self-heals into a rebuild from the applied base, which a
                // recompute oracle cannot tell from a working one. Only this flag proves the
                // rows actually came back through the ring reader.
                assertRestoredFromCheckpoint();
                assertViewsMatchRecompute();
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * @return one row's literal per width. Every 7th row is NULL so the value ring
     * carries the NULL sentinel verbatim and IGNORE NULLS diverges from the base
     * first_value. The wide columns carry a magnitude no 64-bit word holds, so the
     * 128-bit and 256-bit rings actually exercise their high words
     */
    private static String decimalValues(int second, int key) {
        if ((second + key) % 7 == 0) {
            return "null, null, null, null, null, null";
        }
        final int varied = (second * 37 + 11 + key * 17) % 89;
        return varied % 10 + "." + varied % 10 + "m, "
                + varied + ".5m, "
                + varied + "123.456m, "
                + varied + "1234567890.12m, "
                + varied + "12345678901234567890.123456m, "
                + varied + "1234567890123456789012345678901234567890m";
    }

    private static String timestamp(int second) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                second / 3600,
                (second % 3600) / 60,
                second % 60
        );
    }

    private void assertRestoredFromCheckpoint() {
        for (String width : WIDTHS) {
            for (String prefix : new String[]{
                    "lv_avg_", "lv_avgr_", "lv_sum_", "lv_max_", "lv_min_",
                    "lv_first_", "lv_firstn_", "lv_last_", "lv_nth_"
            }) {
                final String viewName = prefix + width;
                Assert.assertTrue(
                        viewName + ": window state did not come back through the checkpoint ring",
                        viewInstance(viewName).isCheckpointRestoreSucceeded()
                );
            }
        }
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
        for (String width : WIDTHS) {
            assertViewMatchesRecompute("lv_avg_" + width, "avg(" + width + ")", CUR_FRAME);
            assertViewMatchesRecompute("lv_avgr_" + width, "avg(" + width + ", 8)", CUR_FRAME);
            assertViewMatchesRecompute("lv_sum_" + width, "sum(" + width + ")", TRAIL_FRAME);
            assertViewMatchesRecompute("lv_max_" + width, "max(" + width + ")", CUR_FRAME);
            assertViewMatchesRecompute("lv_min_" + width, "min(" + width + ")", TRAIL_FRAME);
            assertViewMatchesRecompute("lv_first_" + width, "first_value(" + width + ")", CUR_FRAME);
            assertViewMatchesRecompute(
                    "lv_firstn_" + width, "first_value(" + width + ") IGNORE NULLS", CUR_FRAME);
            assertViewMatchesRecompute("lv_last_" + width, "last_value(" + width + ")", TRAIL_FRAME);
            assertViewMatchesRecompute("lv_nth_" + width, "nth_value(" + width + ", 2)", CUR_FRAME);
        }
    }

    // Commits ROWS_PER_COMMIT ascending rows per key at one-second spacing.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder(
                "INSERT INTO base (ts, sym, d8, d16, d32, d64, d128, d256) VALUES ");
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
                        .append(decimalValues(second, k)).append(")");
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row inside history with extreme-high values at every
    // width, so every frame it enters re-emits, and gives the refresh job a turn.
    private void commitOutOfOrder(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, d8, d16, d32, d64, d128, d256) VALUES ('" + timestamp(second)
                + "', 'a', 9.9m, 999.9m, 999999.999m, 9999999999999.99m,"
                + " 999999999999999999999999999999.123456m,"
                + " 99999999999999999999999999999999999999999999999999m)");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndViews() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, "
                + "d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 3), "
                + "d64 DECIMAL(18, 2), d128 DECIMAL(38, 6), d256 DECIMAL(60, 0)) "
                + "TIMESTAMP(ts) PARTITION BY DAY WAL");
        for (String width : WIDTHS) {
            createView("lv_avg_" + width, "avg(" + width + ")", CUR_FRAME);
            createView("lv_avgr_" + width, "avg(" + width + ", 8)", CUR_FRAME);
            createView("lv_sum_" + width, "sum(" + width + ")", TRAIL_FRAME);
            createView("lv_max_" + width, "max(" + width + ")", CUR_FRAME);
            createView("lv_min_" + width, "min(" + width + ")", TRAIL_FRAME);
            createView("lv_first_" + width, "first_value(" + width + ")", CUR_FRAME);
            createView("lv_firstn_" + width, "first_value(" + width + ") IGNORE NULLS", CUR_FRAME);
            createView("lv_last_" + width, "last_value(" + width + ")", TRAIL_FRAME);
            createView("lv_nth_" + width, "nth_value(" + width + ", 2)", CUR_FRAME);
        }
    }

    private void createView(String viewName, String fn, String frame) throws Exception {
        execute("CREATE LIVE VIEW " + viewName + " FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + fn + " OVER (" + frame + ") AS v FROM base");
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

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }
}
