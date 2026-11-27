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
import io.questdb.cairo.lv.LiveViewCheckpointCaptureLedger;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * The whole-imaged predicate - what the resume's chain price and the adaptive cadence read -
 * against what the seal actually images, for every class the ANCHOR window dispatches to, per
 * argument type family, fusion on and off. Per view, over 8 keys and roots after the seed and
 * at 11:00:20, :30 and :40, one row each:
 * <ol>
 *     <li>the third seal's ledger: roots imaged whole (captures minus incremental captures)
 *     equal the expected count, and the keys imaged are 8 per whole root and 1 per incremental
 *     one;</li>
 *     <li>a late row at 11:00:25, two roots deep, with scan.max.keys = 1: the estimate is
 *     (long) (12 rows x 20 s / (39 s + 1 us)) = 6, so the price declines the chain iff a whole
 *     root exists (2 x 8 > 6);</li>
 *     <li>the same correction, 15 s deep, tightens the cadence to the one-minute floor iff no
 *     root is whole, and leaves the configured 5 minutes otherwise;</li>
 *     <li>every dispatched, stateful function the window does not own claims a dirty set
 *     exactly when it holds one, and every such claim has its full scan cleared;</li>
 *     <li>the rows match the recompute.</li>
 * </ol>
 * The expected counts come from the class table: durable and tracked families 0; lag, rank,
 * dense_rank, corr/covar*, every EXCLUDE CURRENT ROW call, row_number past the leaf budget and a
 * bounded ROWS call 1. CI runs one view per family; every view runs with
 * {@code -Dquestdb.lv.chain.price.full=true}.
 */
@RunWith(Parameterized.class)
public class LiveViewCheckpointImagedWholeLedgerTest extends AbstractLiveViewTest {
    private static final String ANCHOR = "PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00'";
    private static final String DAY = "2026-01-01T11:00:";
    private static final String EXCLUDE = "PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW ANCHOR DAILY '00:00'";
    // projection | expected whole roots, under the default ANCHOR window
    private static final String[] DEFAULT_FRAME_VIEWS = {
            "sum(x) OVER w v|0", "sum(l) OVER w v|0", "sum(i) OVER w v|0", "avg(x) OVER w v|0", "avg(l) OVER w v|0",
            "count() OVER w v|0", "count(x) OVER w v|0", "count(l) OVER w v|0", "count(sym) OVER w v|0", "count(v) OVER w v|0",
            "ksum(x) OVER w v|0", "stddev_samp(x) OVER w v|0", "var_pop(x) OVER w v|0", "max(x) OVER w v|0", "min(l) OVER w v|0",
            "max(t) OVER w v|0", "min(dt) OVER w v|0", "first_value(x) OVER w v|0", "first_value(l) OVER w v|0", "nth_value(x, 2) OVER w v|0",
            "sum(d8) OVER w v|0", "sum(d16) OVER w v|0", "sum(d32) OVER w v|0", "sum(d64) OVER w v|0", "sum(d128) OVER w v|0",
            "sum(d256) OVER w v|0", "avg(d64) OVER w v|0", "max(d128) OVER w v|0", "min(d256) OVER w v|0", "first_value(d32) OVER w v|0",
            "nth_value(d64, 2) OVER w v|0", "avg(x, 'period', 5) OVER w v|0", "avg(x, 'period', 5, y) OVER w v|0",
            "row_number() OVER w v|0", "last_value(x) OVER w v|0",
            "lag(x) OVER w v|1", "lag(l) OVER w v|1", "lag(dt) OVER w v|1", "lag(t) OVER w v|1", "lag(d8) OVER w v|1",
            "lag(d16) OVER w v|1", "lag(d32) OVER w v|1", "lag(d64) OVER w v|1", "lag(x, 2) OVER w v|1",
            "rank() OVER w v|1", "dense_rank() OVER w v|1", "corr(x, y) OVER w v|1", "covar_pop(x, y) OVER w v|1",
            "covar_samp(x, y) OVER w v|1",
            // A runtime-only member past the 16-sum leaf budget: tracked classes stay incremental.
            sums16() + ", avg(x) OVER w v|0", sums16() + ", count() OVER w v|0", sums16() + ", max(x) OVER w v|0",
            sums16() + ", ksum(x) OVER w v|0", sums16() + ", stddev_samp(x) OVER w v|0", sums16() + ", row_number() OVER w v|1",
            "sum(x) OVER w s, rank() OVER w v|1", "count() OVER w c, row_number() OVER w v|0", "sum(x) OVER w s, lag(x) OVER w v|1",
            "sum(x) OVER w s, sum(d64) OVER w v|0", "sum(x) OVER w s, last_value(x) OVER w v|0",
            "sum(x) OVER w s, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) v|1"
    };
    // One view of DEFAULT_FRAME_VIEWS per branch of the predicate, with its expected count.
    private static final String[] DEFAULT_FRAME_FAMILY_VIEWS = {
            // A durable projection, and a DECIMAL sum, a residual the window dispatches to and tracks.
            "sum(x) OVER w v|0", "sum(d64) OVER w v|0",
            // lag keeps no dirty set; rank and the bivariate calls keep no baseline.
            "lag(x) OVER w v|1", "rank() OVER w v|1", "covar_pop(x, y) OVER w v|1",
            // Runtime-only members past the 16-sum leaf budget, a tracked and an untracked one.
            sums16() + ", avg(x) OVER w v|0", sums16() + ", row_number() OVER w v|1",
            // A bounded ROWS call the ANCHOR window does not dispatch to.
            "sum(x) OVER w s, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) v|1"
    };
    // Every call EXCLUDE CURRENT ROW accepts is a ring, which no seal freezes incrementally: each
    // ring class per argument type, IGNORE NULLS where the family takes it (nth_value does not),
    // and avg(d256, scale) aside, which CREATE refuses for its rescaled precision.
    private static final String[] EXCLUDE_FRAME_VIEWS = {
            "sum(x) OVER w v|1", "sum(l) OVER w v|1", "avg(x) OVER w v|1", "avg(l) OVER w v|1",
            "sum(d8) OVER w v|1", "sum(d16) OVER w v|1", "sum(d32) OVER w v|1", "sum(d64) OVER w v|1",
            "sum(d128) OVER w v|1", "sum(d256) OVER w v|1", "avg(d8) OVER w v|1", "avg(d16) OVER w v|1",
            "avg(d32) OVER w v|1", "avg(d64) OVER w v|1", "avg(d128) OVER w v|1", "avg(d256) OVER w v|1",
            "avg(d8, 2) OVER w v|1", "avg(d16, 2) OVER w v|1", "avg(d32, 2) OVER w v|1", "avg(d64, 4) OVER w v|1",
            "avg(d128, 4) OVER w v|1",
            "count() OVER w v|1", "count(*) OVER w v|1", "count(x) OVER w v|1", "count(l) OVER w v|1",
            "count(sym) OVER w v|1", "count(v) OVER w v|1", "count(d64) OVER w v|1",
            "first_value(x) OVER w v|1", "first_value(x) IGNORE NULLS OVER w v|1",
            "first_value(l) OVER w v|1", "first_value(l) IGNORE NULLS OVER w v|1",
            "first_value(t) OVER w v|1", "first_value(t) IGNORE NULLS OVER w v|1",
            "first_value(dt) OVER w v|1", "first_value(dt) IGNORE NULLS OVER w v|1",
            "first_value(d8) OVER w v|1", "first_value(d8) IGNORE NULLS OVER w v|1",
            "first_value(d16) OVER w v|1", "first_value(d16) IGNORE NULLS OVER w v|1",
            "first_value(d32) OVER w v|1", "first_value(d32) IGNORE NULLS OVER w v|1",
            "first_value(d64) OVER w v|1", "first_value(d64) IGNORE NULLS OVER w v|1",
            "first_value(d128) OVER w v|1", "first_value(d128) IGNORE NULLS OVER w v|1",
            "first_value(d256) OVER w v|1", "first_value(d256) IGNORE NULLS OVER w v|1",
            "last_value(x) OVER w v|1", "last_value(x) IGNORE NULLS OVER w v|1",
            "last_value(l) OVER w v|1", "last_value(l) IGNORE NULLS OVER w v|1",
            "last_value(t) OVER w v|1", "last_value(t) IGNORE NULLS OVER w v|1",
            "last_value(dt) OVER w v|1", "last_value(dt) IGNORE NULLS OVER w v|1",
            "last_value(d8) OVER w v|1", "last_value(d8) IGNORE NULLS OVER w v|1",
            "last_value(d16) OVER w v|1", "last_value(d16) IGNORE NULLS OVER w v|1",
            "last_value(d32) OVER w v|1", "last_value(d32) IGNORE NULLS OVER w v|1",
            "last_value(d64) OVER w v|1", "last_value(d64) IGNORE NULLS OVER w v|1",
            "last_value(d128) OVER w v|1", "last_value(d128) IGNORE NULLS OVER w v|1",
            "last_value(d256) OVER w v|1", "last_value(d256) IGNORE NULLS OVER w v|1",
            "nth_value(x, 2) OVER w v|1", "nth_value(l, 2) OVER w v|1", "nth_value(t, 2) OVER w v|1",
            "nth_value(dt, 2) OVER w v|1", "nth_value(d8, 2) OVER w v|1", "nth_value(d16, 2) OVER w v|1",
            "nth_value(d32, 2) OVER w v|1", "nth_value(d64, 2) OVER w v|1", "nth_value(d128, 2) OVER w v|1",
            "nth_value(d256, 2) OVER w v|1"
    };
    // An EXCLUDE CURRENT ROW avg, AvgDouble's bounded RANGE class, beside a count over the
    // base-class ring and a DECIMAL first_value: three ring families, each claiming no dirty set.
    private static final String[] EXCLUDE_FRAME_FAMILY_VIEWS = {
            "avg(x) OVER w v|1", "count(x) OVER w v|1", "first_value(d64) OVER w v|1"
    };
    private final boolean isFused;
    private int viewSeq;

    public LiveViewCheckpointImagedWholeLedgerTest(boolean isFused) {
        this.isFused = isFused;
    }

    @Parameterized.Parameters(name = "fused={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{true}, {false}});
    }

    @Test
    public void testDefaultFrameFamilies() throws Exception {
        assertViews(DEFAULT_FRAME_FAMILY_VIEWS, ANCHOR);
    }

    @Test
    public void testEveryDefaultFrameView() throws Exception {
        assumeFullMatrix();
        assertViews(DEFAULT_FRAME_VIEWS, ANCHOR);
    }

    @Test
    public void testEveryExcludeCurrentRowView() throws Exception {
        assumeFullMatrix();
        assertViews(EXCLUDE_FRAME_VIEWS, EXCLUDE);
    }

    @Test
    public void testExcludeCurrentRowFamilies() throws Exception {
        assertViews(EXCLUDE_FRAME_FAMILY_VIEWS, EXCLUDE);
    }

    // 140 views take minutes, past CI's time budget.
    private static void assumeFullMatrix() {
        Assume.assumeTrue(
                "every view runs with -D" + LiveViewCheckpointChainPriceTest.FULL_MATRIX_PROPERTY + "=true",
                LiveViewCheckpointChainPriceTest.IS_FULL_MATRIX
        );
    }

    private static boolean isDispatched(ObjList<WindowFunction> anchorable, WindowFunction function) {
        for (int i = 0, n = anchorable.size(); i < n; i++) {
            if (anchorable.getQuick(i) == function) {
                return true;
            }
        }
        return false;
    }

    private static long[] ledger(LiveViewInstance instance) {
        final LiveViewCheckpointCaptureLedger ledger = instance.getCheckpointCaptureLedgerForTest();
        return new long[]{
                ledger.getWindowCaptures() + ledger.getFunctionCaptures(),
                ledger.getWindowIncrementalCaptures() + ledger.getFunctionIncrementalCaptures(),
                ledger.getWindowKeysImaged() + ledger.getFunctionKeysImaged()
        };
    }

    private static String sums16() {
        final StringBuilder sb = new StringBuilder();
        for (int q = 1; q <= 16; q++) {
            if (q > 1) {
                sb.append(", ");
            }
            sb.append("sum(q").append(q).append(") OVER w s").append(q);
        }
        return sb.toString();
    }

    private void assertView(String projection, int expectedWhole, String window) throws Exception {
        final int seq = ++viewSeq;
        final String base = "base" + seq;
        final String lv = "lv" + seq;
        final StringBuilder q = new StringBuilder();
        for (int c = 1; c <= 16; c++) {
            q.append(", q").append(c).append(" DOUBLE");
        }
        execute("CREATE TABLE " + base + " (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y DOUBLE, l LONG, i INT, t TIMESTAMP, dt DATE, "
                + "d8 DECIMAL(2,1), d16 DECIMAL(4,1), d32 DECIMAL(9,2), d64 DECIMAL(18,2), d128 DECIMAL(38,2), d256 DECIMAL(76,2), "
                + "v VARCHAR" + q + ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        final String select = "SELECT ts, sym, " + projection + " FROM " + base + " WINDOW w AS (" + window + ")";
        execute("CREATE LIVE VIEW " + lv + " FLUSH EVERY 100ms START FROM BEGINNING AS " + select);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, lv);
            final StringBuilder seed = new StringBuilder();
            for (int k = 1; k <= 8; k++) {
                if (k > 1) {
                    seed.append(", ");
                }
                seed.append(row("0" + k + ".000000Z", "k" + k, k));
            }
            commitInOrder(job, base, seed.toString());
            commitInOrder(job, base, row("20.000000Z", "k3", 100));
            commitInOrder(job, base, row("30.000000Z", "k4", 200));
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(lv);
            final long[] beforeSeal = ledger(instance);
            commitInOrder(job, base, row("40.000000Z", "k5", 300));
            final long[] afterSeal = ledger(instance);
            final long captures = afterSeal[0] - beforeSeal[0];
            final long whole = captures - (afterSeal[1] - beforeSeal[1]);
            final String tag = "fused=" + isFused + " [" + projection + "] over (" + window + ")";
            Assert.assertEquals(tag + ": roots imaged whole", expectedWhole, whole);
            Assert.assertEquals(tag + ": keys imaged", 8 * whole + (captures - whole), afterSeal[2] - beforeSeal[2]);
            assertDirtySetClaims(tag, instance);

            final long declines = job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT);
            commit(job, base, row("25.000000Z", "k6", 400));
            Assert.assertEquals(
                    tag + ": the chain price declines iff a root is imaged whole",
                    expectedWhole > 0 ? 1 : 0,
                    job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT) - declines
            );
            Assert.assertEquals(
                    tag + ": the cadence tightens iff no root is imaged whole",
                    expectedWhole > 0 ? 5 * Micros.MINUTE_MICROS : Micros.MINUTE_MICROS,
                    instance.getEffectiveCheckpointDurationMicros(5 * Micros.MINUTE_MICROS)
            );
            assertNoRefreshFaults(lv);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM (" + select.replace(" ANCHOR DAILY '00:00'", "") + ") ORDER BY ts, sym",
                    "SELECT * FROM " + lv + " ORDER BY ts, sym",
                    LOG,
                    true
            );
            execute("DROP LIVE VIEW " + lv);
            drainJob(job);
        }
        execute("DROP TABLE " + base);
        drainWalQueue();
    }

    private void assertViews(String[] views, String window) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PURGE_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 5 * Micros.MINUTE_MICROS);
        assertMemoryLeak(() -> {
            setCurrentMicros(1_767_225_600_000_000L);
            for (String view : views) {
                final int split = view.lastIndexOf('|');
                assertView(view.substring(0, split), Integer.parseInt(view.substring(split + 1)), window);
            }
        });
    }

    // Method truthfulness after three seals: a dispatched, stateful function the window does not
    // own claims a dirty set exactly when it holds one, and a claim comes with the baseline that
    // clears its full scan. A ring holds none: freezeFunction walks a ring's whole map whatever
    // its dirty set names, so a set would cost a key insert per touched key and read nothing.
    private void assertDirtySetClaims(String tag, LiveViewInstance instance) {
        final WindowRecordCursorFactory windowFactory = instance.getCompiledPlan().getWindowFactory();
        final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
        final ObjList<WindowFunction> anchorable = windowFactory.getAnchorableWindowFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState() || function.isWindowStateOwned() || !isDispatched(anchorable, function)) {
                continue;
            }
            final String name = tag + " " + function.getClass().getSimpleName();
            if (function.supportsCheckpointRingState()) {
                Assert.assertNull(name + ": a ring's seal walks its whole map, so it keeps no set", function.getCheckpointDirtyPartitionMap());
                Assert.assertFalse(name + ": a ring's seal walks its whole map, so it claims no set", function.isCheckpointDirtyTracked());
            }
            Assert.assertEquals(name + ": claims a dirty set iff it holds one", function.getCheckpointDirtyPartitionMap() != null, function.isCheckpointDirtyTracked());
            if (function.isCheckpointDirtyTracked()) {
                Assert.assertFalse(name + ": a claim comes with a baseline", function.isCheckpointFullScanRequired());
            }
        }
    }

    private void commit(LiveViewRefreshJob job, String base, String rows) throws Exception {
        execute("INSERT INTO " + base + " (ts, sym, x, y, l, i, t, dt, d8, d16, d32, d64, d128, d256, v) VALUES " + rows);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // An in-order commit needs one refresh pass to emit its rows and seal its root.
    private void commitInOrder(LiveViewRefreshJob job, String base, String rows) throws Exception {
        execute("INSERT INTO " + base + " (ts, sym, x, y, l, i, t, dt, d8, d16, d32, d64, d128, d256, v) VALUES " + rows);
        drainWalQueue();
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainJob(job);
        drainWalQueue();
    }

    private String row(String second, String sym, int value) {
        final String decimal = (value % 10) + ".5m";
        return "('" + DAY + second + "', '" + sym + "', " + value + ".0, " + (value + 1) + ".0, " + value + ", " + value
                + ", '2025-06-01T00:00:00.000000Z', '2025-06-01T00:00:00.000Z'::date, " + decimal + ", " + decimal + ", " + decimal
                + ", " + decimal + ", " + decimal + ", " + decimal + ", 'v" + value + "')";
    }
}
