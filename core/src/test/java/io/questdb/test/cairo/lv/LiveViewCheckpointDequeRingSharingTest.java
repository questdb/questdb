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
 * The partitioned {@code max}/{@code min} window functions over a bounded RANGE frame
 * keep a monotonic deque beside a frame ring, and they now share the same persistent
 * checkpoint ring as the value functions: the shared ring carries the {@code (ts, value)}
 * frame ring, and the runtime deque is replayed from it at restore rather than persisted.
 * The value pages are tagged with the deque page kinds so a {@code max}/{@code min} root's
 * pages stay distinct from a value-ring root's.
 * <p>
 * This drives all three production code paths end to end - the DOUBLE column exercises the
 * dedicated {@code MaxDoubleWindowFunctionFactory} class, the LONG column the
 * {@code MaxLongWindowFunctionFactory} class, and the DATE/TIMESTAMP columns the shared
 * {@code MaxMinWindowFunctionFactoryHelper} base - proving they share chunks, survive a
 * restart through the ring reader (which rebuilds the deque), localize an out-of-order
 * repair, and match a fresh recompute for both {@code max} and {@code min} and both a
 * current-row and a trailing frame.
 */
public class LiveViewCheckpointDequeRingSharingTest extends AbstractLiveViewTest {

    private static final int COMMITS = 8;
    // A current-row frame (minDiff == 0): every ring row is in-frame, so the deque
    // reconstruction replays the whole ring.
    private static final String CUR_FRAME =
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '1000' SECOND PRECEDING AND CURRENT ROW";
    private static final int ROWS_PER_COMMIT = 120;
    // A trailing frame (minDiff > 0): the ring's newest rows sit in the waiting room
    // outside the frame, so frameSize < size and the deque covers only the in-frame prefix.
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
    public void testDequeRingRepairLocalizes() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);

                // An out-of-order row deep in history forces the localized repair path,
                // which restores a predecessor root from the timeline through the ring
                // reader - rebuilding the deque from the frame ring - before replaying the
                // corrected interval forward. Its value is an extreme high that becomes the
                // new max of every frame it enters, so the max views actually re-emit.
                commitOutOfOrder(job, 137);
                driveRefreshToQuiescence(job);

                assertViewsMatchRecompute();
            }
        });
    }

    @Test
    public void testDequeRingsShareChunksAndSurviveRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);

                assertSomeSegmentShared("lv_max_d");
                assertSomeSegmentShared("lv_min_d");
                assertSomeSegmentShared("lv_max_l");
                assertSomeSegmentShared("lv_min_l");
                assertSomeSegmentShared("lv_max_date");
                assertSomeSegmentShared("lv_max_t");
                assertSomeSegmentShared("lv_min_t");
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
        assertViewMatchesRecompute("lv_max_d", "max(xd)", CUR_FRAME);
        assertViewMatchesRecompute("lv_min_d", "min(xd)", TRAIL_FRAME);
        assertViewMatchesRecompute("lv_max_l", "max(xl)", CUR_FRAME);
        assertViewMatchesRecompute("lv_min_l", "min(xl)", TRAIL_FRAME);
        assertViewMatchesRecompute("lv_max_date", "max(xdate)", CUR_FRAME);
        assertViewMatchesRecompute("lv_min_date", "min(xdate)", TRAIL_FRAME);
        assertViewMatchesRecompute("lv_max_t", "max(xt)", CUR_FRAME);
        assertViewMatchesRecompute("lv_min_t", "min(xt)", TRAIL_FRAME);
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Commits ROWS_PER_COMMIT ascending rows per key at one-second spacing. Each column
    // carries a non-monotonic value sequence so the deque actually pops candidates, plus
    // a scattering of NULLs the max/min drop from the ring, and the long column plants a
    // value whose bits alias a double NaN to prove the raw-bit ring never routes it
    // through a double.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, xd, xl, xdate, xt) VALUES ");
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
                        .append(longValue(second, k)).append(", ")
                        .append(dateValue(second, k)).append(", ")
                        .append(timestampValue(second, k)).append(")");
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row deep in history with extreme-high values so every
    // frame it enters re-emits a new max, and gives the refresh job a turn.
    private void commitOutOfOrder(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, xd, xl, xdate, xt) VALUES ('" + timestamp(second)
                + "', 'a', 987654.5, " + 0x7ff8_0000_0000_00ffL
                + ", '2099-12-31T23:59:00.000Z'::date, " + (9_000_000_000L) + "::timestamp)");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndViews() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, xd DOUBLE, xl LONG, xdate DATE, xt TIMESTAMP) " +
                "TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv_max_d FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, max(xd) OVER (" + CUR_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_min_d FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, min(xd) OVER (" + TRAIL_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_max_l FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, max(xl) OVER (" + CUR_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_min_l FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, min(xl) OVER (" + TRAIL_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_max_date FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, max(xdate) OVER (" + CUR_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_min_date FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, min(xdate) OVER (" + TRAIL_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_max_t FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, max(xt) OVER (" + CUR_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_min_t FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, min(xt) OVER (" + TRAIL_FRAME + ") AS v FROM base");
    }

    private static String dateValue(int second, int key) {
        if (second % 11 == key) {
            return "null";
        }
        final int varied = (second * 41 + 7 + key * 53) % 400;
        final int day = 1 + varied % 27;
        final int hour = varied % 24;
        final int minute = (varied * 7) % 60;
        return "'" + String.format("2027-06-%02dT%02d:%02d:00.000Z", day, hour, minute) + "'::date";
    }

    private static String doubleValue(int second, int key) {
        if (second % 10 == key) {
            return "null";
        }
        return Double.toString(((second * 31 + 17 + key * 29) % 101) + 0.25);
    }

    private static String longValue(int second, int key) {
        if (second % 7 == key) {
            return "null";
        }
        if (second % 53 == 0) {
            // A large value whose low bits form a double NaN payload.
            return Long.toString(0x7ff0_0000_0000_0000L + second + key);
        }
        return Long.toString(((second * 37L + 13 + key * 41) % 211) - 100);
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

    private static String timestampValue(int second, int key) {
        if (second % 13 == key) {
            return "null";
        }
        return (((second * 43L + 5 + key * 47) % 197) * 1_000 + 250) + "::timestamp";
    }

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }
}
