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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import org.junit.Assert;
import org.junit.Test;

/**
 * The adaptive checkpoint duration cadence a live view learns from an out-of-order correction.
 * <p>
 * The unit cases drive {@link LiveViewInstance#recordAdaptiveCheckpointCorrection} directly over
 * every value class of its inputs. The stream cases hand-drive a {@link LiveViewRefreshJob} over a
 * late-row stream and pin the roots it seals, as the review that motivated bounding the tightening
 * measured it.
 * <p>
 * The tightening is gated: only a view with an ANCHOR window whose stateful window functions are
 * all anchorable - the shapes a seal images incrementally - learns a shorter cadence, and never
 * below {@code max(FLUSH EVERY, 1 minute)}. Every other view records the correction depth but keeps
 * the configured cadence, so a shallow late row no longer makes it seal and keep several times the
 * checkpoint roots.
 */
public class LiveViewAdaptiveCheckpointCadenceTest extends AbstractLiveViewTest {

    // 2026-01-01T00:00:00Z, so the hand-driven clock sits near the data the tests insert.
    private static final long START_MICROS = 1_767_225_600_000_000L;
    // One anchored, one unanchored window over the same (ts, sym, x) base. The anchored view's
    // seals image only the keys a correction touched, which is why only it is tightened; the
    // unanchored view's seals image its whole map, the cost the gate exists to stop.
    private static final String VIEW_ANCHORED_SUM =
            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, sum(x) OVER w s FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String VIEW_ANCHORED_PLUS_STATELESS =
            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, sum(x) OVER w s, "
                    + "last_value(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) lv FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String VIEW_ANCHORED_SUM_COUNT =
            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, sum(x) OVER w s, count(*) OVER w c FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String VIEW_ANCHORED_SUM_FLUSH_2M =
            "CREATE LIVE VIEW lv FLUSH EVERY 2m START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, sum(x) OVER w s FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String VIEW_MIXED =
            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, sum(x) OVER w s, "
                    + "avg(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String VIEW_STATELESS_ONLY =
            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, "
                    + "last_value(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN CURRENT ROW AND CURRENT ROW) lv FROM base";
    private static final String VIEW_UNANCHORED_RANGE =
            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, "
                    + "sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '1' MINUTE PRECEDING AND CURRENT ROW) s FROM base";
    private static final String VIEW_UNANCHORED_ROWS =
            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, x, "
                    + "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM base";
    private static final String BASE_DDL = "CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL";
    private static final String BASE_DDL_KEYED = "CREATE TABLE base (ts TIMESTAMP, sym SYMBOL INDEX, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL";
    private static final String BASE_DDL_NANOS = "CREATE TABLE base (ts TIMESTAMP_NS, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL";
    // The same base with the columns the anchored families below read: a second DOUBLE, a
    // DECIMAL and the 16 DOUBLEs whose sums fill the window's inline leaf budget.
    private static final String BASE_DDL_WIDE = "CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y DOUBLE, d DECIMAL(18,2)"
            + ", q1 DOUBLE, q2 DOUBLE, q3 DOUBLE, q4 DOUBLE, q5 DOUBLE, q6 DOUBLE, q7 DOUBLE, q8 DOUBLE"
            + ", q9 DOUBLE, q10 DOUBLE, q11 DOUBLE, q12 DOUBLE, q13 DOUBLE, q14 DOUBLE, q15 DOUBLE, q16 DOUBLE"
            + ") TIMESTAMP(ts) PARTITION BY DAY WAL";

    @Test
    public void testAnchoredDailyTightensToTheOneMinuteFloor() throws Exception {
        // depth 2s halves to 1s, floored up to one minute: the anchored view is tightened.
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_ANCHORED_SUM, 5 * Micros.MINUTE_MICROS, Micros.MINUTE_MICROS);
    }

    @Test
    public void testAnchoredPlusStatelessTightensToTheOneMinuteFloor() throws Exception {
        // The stateless CURRENT ROW function images nothing, so it does not keep the view off the
        // gate: an ANCHOR window plus only anchorable-or-stateless functions is tightened.
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_ANCHORED_PLUS_STATELESS, 5 * Micros.MINUTE_MICROS, Micros.MINUTE_MICROS);
    }

    @Test
    public void testAnchoredSumCountKeyedShapeTightensToTheOneMinuteFloor() throws Exception {
        // The reported customer shape: sum + count under one ANCHOR DAILY window over an indexed,
        // non-dedup base. Both functions are anchorable, so the view is tightened.
        assertShallowCorrectionLearnsCadence(BASE_DDL_KEYED, VIEW_ANCHORED_SUM_COUNT, 5 * Micros.MINUTE_MICROS, Micros.MINUTE_MICROS);
    }

    @Test
    public void testAnchoredStreamSealsNoInOrderRootWithinTheFloor() throws Exception {
        // The same late-row stream over an anchored view. Its repair seals every ten commits keep
        // the one-minute cadence from coming due, so no in-order commit seals an extra root.
        assertStreamSealsNoInOrderRoot(VIEW_ANCHORED_SUM);
    }

    @Test
    public void testCeilingBelowTheFloorStaysAtTheCeiling() throws Exception {
        // A ceiling under one minute means no tightening at all: the floor cannot push the learned
        // cadence above the ceiling, which stays the effective value.
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_ANCHORED_SUM, 30 * Micros.SECOND_MICROS, 30 * Micros.SECOND_MICROS);
    }

    @Test
    public void testDeepCorrectionDoesNotBindTheFloor() throws Exception {
        // A four-minute correction halves to two minutes, well above the one-minute floor, so the
        // learned cadence is depth/2 whichever side of the floor the view sits on.
        assertCorrectionLearnsCadence(
                BASE_DDL,
                VIEW_ANCHORED_SUM,
                5 * Micros.MINUTE_MICROS,
                "2026-01-01T00:11:00.000000Z",
                "2026-01-01T00:07:00.000000Z",
                4 * Micros.MINUTE_MICROS,
                2 * Micros.MINUTE_MICROS
        );
    }

    @Test
    public void testFlushEveryAboveTheFloorFloorsTheLearnedCadence() throws Exception {
        // FLUSH EVERY 2m is the floor here, above the one-minute minimum: a 2s correction cannot
        // learn a cadence the refresh cycle cannot realize.
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_ANCHORED_SUM_FLUSH_2M, 5 * Micros.MINUTE_MICROS, 2 * Micros.MINUTE_MICROS);
    }

    @Test
    public void testGatedAnchoredCadenceSealImagesOnlyDirtyKeys() throws Exception {
        // The premise the gate rests on: a gated anchored view's cadence seal images only the keys
        // the interval since the last seal touched, not its whole map. A regression that made it
        // image the whole map would reintroduce, on the tightened cadence, the cost the gate exists
        // to keep off unanchored views.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 5 * Micros.MINUTE_MICROS);
        assertMemoryLeak(() -> {
            execute(BASE_DDL);
            execute(VIEW_ANCHORED_SUM_COUNT);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                setCurrentMicros(START_MICROS);
                // Five keys in one commit: the first root images all five (a complete capture).
                execute("INSERT INTO base SELECT ('2026-01-01T00:10:00.000000Z'::timestamp + x * 1_000_000)::timestamp, "
                        + "('k' || x)::symbol, 1.0 FROM long_sequence(5)");
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(5, instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged());
                final long windowImaged = instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged();
                final long windowIncremental = instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures();

                // Past the cadence, touch two of the five keys in order: the seal images exactly
                // the two dirty keys through its window root, incrementally - not the whole map.
                // sum and count fuse into the anchor group, so the imaging runs through the window
                // root (getFunctionKeysImaged stays zero).
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                execute("INSERT INTO base VALUES ('2026-01-01T00:20:00.000000Z', 'k2', 1.0), "
                        + "('2026-01-01T00:20:01.000000Z', 'k4', 1.0)");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the cadence seal must image only the two dirty keys, not the whole five-key map",
                        2,
                        instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged() - windowImaged
                );
                Assert.assertEquals(
                        "and it must be an incremental capture, not a full re-image",
                        1,
                        instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures() - windowIncremental
                );
            }
        });
    }

    @Test
    public void testIncrementalAnchoredFamiliesTighten() throws Exception {
        // Every seal of these images only the keys a correction touched: residual tracked
        // functions (a DECIMAL sum, first_value, the EMA), and a runtime-only member past the
        // leaf budget whose class tracks its keys, fused or not. The view tightens.
        final String[] projections = {"sum(d) OVER w s", "first_value(x) OVER w s", "avg(x, 'period', 5) OVER w s"};
        for (String projection : projections) {
            assertShallowCorrectionLearnsCadence(BASE_DDL_WIDE, anchoredView(projection), 5 * Micros.MINUTE_MICROS, Micros.MINUTE_MICROS);
        }
        for (boolean isFused : new boolean[]{true, false}) {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
            assertShallowCorrectionLearnsCadence(BASE_DDL_WIDE, anchoredView(sums16() + ", avg(x) OVER w s"), 5 * Micros.MINUTE_MICROS, Micros.MINUTE_MICROS);
        }
    }

    @Test
    public void testMixedViewKeepsTheConfiguredCadence() throws Exception {
        // One anchored and one bounded ROWS window: the bounded function keeps checkpoint state
        // but is not anchorable, so it images its whole map on every seal. The view stays off the
        // gate and keeps the configured cadence.
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_MIXED, 5 * Micros.MINUTE_MICROS, 5 * Micros.MINUTE_MICROS);
    }

    @Test
    public void testNanosecondBaseTightensToTheOneMinuteFloor() throws Exception {
        // The correction depth arrives in nanoseconds and converts to 2,000,000 micros; the
        // anchored view tightens to the one-minute floor exactly as over a microsecond base.
        assertShallowCorrectionLearnsCadence(BASE_DDL_NANOS, VIEW_ANCHORED_SUM, 5 * Micros.MINUTE_MICROS, Micros.MINUTE_MICROS);
    }

    @Test
    public void testRestartResetsTheLearnedCadence() throws Exception {
        // The learned cadence is in-memory: a restart drops it, so the view publishes on the
        // configured cadence until the next correction tightens it again.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 5 * Micros.MINUTE_MICROS);
        assertMemoryLeak(() -> {
            execute(BASE_DDL);
            execute(VIEW_ANCHORED_SUM);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                setCurrentMicros(START_MICROS);
                insertAndRefresh(job, "2026-01-01T00:10:00.000000Z", 1.0);
                insertAndRefresh(job, "2026-01-01T00:11:00.000000Z", 2.0);
                insertAndRefresh(job, "2026-01-01T00:10:58.000000Z", 3.0);
                Assert.assertEquals(Micros.MINUTE_MICROS, instance().getEffectiveCheckpointDurationMicros(5 * Micros.MINUTE_MICROS));
            }

            restartLiveViewsAndRecover();
            final LiveViewInstance restored = instance();
            Assert.assertEquals(Numbers.LONG_NULL, restored.getAdaptiveCheckpointDurationMicros());
            Assert.assertEquals(0, restored.getAdaptiveCheckpointCorrectionCount());
            Assert.assertEquals(5 * Micros.MINUTE_MICROS, restored.getEffectiveCheckpointDurationMicros(5 * Micros.MINUTE_MICROS));

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertAndRefresh(job, "2026-01-01T00:12:00.000000Z", 4.0);
                insertAndRefresh(job, "2026-01-01T00:13:00.000000Z", 5.0);
                insertAndRefresh(job, "2026-01-01T00:12:58.000000Z", 6.0);
                Assert.assertEquals(Micros.MINUTE_MICROS, instance().getEffectiveCheckpointDurationMicros(5 * Micros.MINUTE_MICROS));
            }
        });
    }

    @Test
    public void testStatelessOnlyViewKeepsTheConfiguredCadence() throws Exception {
        // A single stateless CURRENT ROW function and no ANCHOR window: off the gate, the view
        // keeps the configured cadence, as a base with no adaptation would.
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_STATELESS_ONLY, 5 * Micros.MINUTE_MICROS, 5 * Micros.MINUTE_MICROS);
    }

    @Test
    public void testWholeImagedAnchoredFamiliesKeepTheConfiguredCadence() throws Exception {
        // Under an ANCHOR window, yet every seal images these whole: lag keeps no dirty set,
        // rank and corr keep no baseline, an EXCLUDE CURRENT ROW call is a ring, and row_number
        // past the leaf budget is a runtime-only member that keeps no baseline, fused or not. A
        // tightened cadence would turn into whole-map images, so the view keeps the configured one.
        final String[] projections = {"lag(x) OVER w s", "rank() OVER w s", "corr(x, y) OVER w s"};
        for (String projection : projections) {
            assertShallowCorrectionLearnsCadence(BASE_DDL_WIDE, anchoredView(projection), 5 * Micros.MINUTE_MICROS, 5 * Micros.MINUTE_MICROS);
        }
        assertShallowCorrectionLearnsCadence(
                BASE_DDL_WIDE,
                "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, avg(x) OVER w s FROM base "
                        + "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW ANCHOR DAILY '00:00')",
                5 * Micros.MINUTE_MICROS,
                5 * Micros.MINUTE_MICROS
        );
        for (boolean isFused : new boolean[]{true, false}) {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
            assertShallowCorrectionLearnsCadence(BASE_DDL_WIDE, anchoredView(sums16() + ", row_number() OVER w rn"), 5 * Micros.MINUTE_MICROS, 5 * Micros.MINUTE_MICROS);
        }
    }

    @Test
    public void testUnanchoredRangeKeepsTheConfiguredCadence() throws Exception {
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_UNANCHORED_RANGE, 5 * Micros.MINUTE_MICROS, 5 * Micros.MINUTE_MICROS);
    }

    @Test
    public void testUnanchoredRowsKeepsTheConfiguredCadence() throws Exception {
        assertShallowCorrectionLearnsCadence(BASE_DDL, VIEW_UNANCHORED_ROWS, 5 * Micros.MINUTE_MICROS, 5 * Micros.MINUTE_MICROS);
    }

    @Test
    public void testUnanchoredRowsStreamSealsNoInOrderRoot() throws Exception {
        // The headline stream: 1 commit/s with a 2s-deep late row every tenth commit. Off the
        // gate, the unanchored view keeps its 5-minute cadence, so no in-order commit seals an
        // extra root over the 50-second span; only the late-row repairs seal.
        assertStreamSealsNoInOrderRoot(VIEW_UNANCHORED_ROWS);
    }

    @Test
    public void testCadenceTightensImmediatelyAndRelaxesGradually() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        final long configured = 60 * Micros.MINUTE_MICROS;

        Assert.assertEquals(Numbers.LONG_NULL, instance.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(configured, instance.getEffectiveCheckpointDurationMicros(configured));

        // Gated: the floor is max(1 min, FLUSH EVERY), but every depth here halves well above it.
        instance.recordAdaptiveCheckpointCorrection(60 * Micros.MINUTE_MICROS, configured, Micros.SECOND_MICROS, true);
        Assert.assertEquals(30 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointDurationMicros());

        instance.recordAdaptiveCheckpointCorrection(10 * Micros.MINUTE_MICROS, configured, Micros.SECOND_MICROS, true);
        Assert.assertEquals(5 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointDurationMicros());

        instance.recordAdaptiveCheckpointCorrection(40 * Micros.MINUTE_MICROS, configured, Micros.SECOND_MICROS, true);
        Assert.assertEquals(375 * Micros.SECOND_MICROS, instance.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(3, instance.getAdaptiveCheckpointCorrectionCount());
        Assert.assertEquals(40 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointLastCorrectionDepthMicros());
    }

    @Test
    public void testFlushIntervalFloorsLearnedCadence() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        // FLUSH EVERY 90s is the floor here, above the one-minute minimum: a shallow correction
        // cannot learn a cadence the refresh cycle cannot realize.
        instance.recordAdaptiveCheckpointCorrection(
                Micros.SECOND_MICROS,
                60 * Micros.MINUTE_MICROS,
                90 * Micros.SECOND_MICROS,
                true
        );
        Assert.assertEquals(90 * Micros.SECOND_MICROS, instance.getAdaptiveCheckpointDurationMicros());

        instance.recordAdaptiveCheckpointCorrection(0, 60 * Micros.MINUTE_MICROS, Micros.SECOND_MICROS, true);
        Assert.assertEquals(1, instance.getAdaptiveCheckpointCorrectionCount());
    }

    @Test
    public void testRestoredHeadDoesNotRelaxCadence() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        final long configured = 5 * Micros.MINUTE_MICROS;

        // Gated, depth 2s: halves to 1s, floored up to one minute.
        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS, true);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // writtenUs == LONG_NULL marks a head/seed this process restored rather than
        // sealed. It cost no seal, so it must not pay down the learned cadence.
        instance.setHeadCheckpoint(7, 7, 700, 0, Numbers.LONG_NULL);
        instance.recordSeedCheckpointWritten(70, 700, Numbers.LONG_NULL);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // A restored head must not consume the correction's exemption either: the
        // first seal this process writes is still the correction's own.
        instance.setHeadCheckpoint(8, 8, 800, 0, 8_000L);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));
        instance.setHeadCheckpoint(9, 9, 900, 0, 9_000L);
        Assert.assertEquals(75 * Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));
    }

    @Test
    public void testSeedSealsRelaxCadenceTowardsCeiling() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        final long configured = 5 * Micros.MINUTE_MICROS;

        // Gated, depth 2s: halves to 1s, floored up to one minute.
        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS, true);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // The seal the correction's own repair forces is exempt.
        instance.recordSeedCheckpointWritten(10, 100, 1_000L);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        instance.recordSeedCheckpointWritten(11, 110, 1_100L);
        Assert.assertEquals(75 * Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        for (int i = 0; i < 64; i++) {
            instance.recordSeedCheckpointWritten(20 + i, 200 + i, 2_000L + i);
        }
        Assert.assertEquals(configured, instance.getEffectiveCheckpointDurationMicros(configured));
    }

    @Test
    public void testSealsWithoutCorrectionsRelaxCadenceBackToCeiling() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        // Shipped defaults: cairo.live.view.checkpoint.max.duration.micros = 5 min,
        // against a FLUSH EVERY 1s view.
        final long configured = 5 * Micros.MINUTE_MICROS;

        // One row two seconds late pins the gated cadence at the one-minute floor - a 5x
        // seal-rate increase over the configured ceiling, bounded by the floor.
        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS, true);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // The seal the same correction's O3 repair forces is exempt: the tightened
        // cadence gets the one whole interval it asked for.
        instance.setHeadCheckpoint(1, 1, 100, 0, 1_000L);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        // Every seal after it, with no further correction, walks the cadence one
        // 25% step back towards the ceiling, so the tightening pays for itself.
        instance.setHeadCheckpoint(2, 2, 200, 0, 2_000L);
        Assert.assertEquals(75 * Micros.SECOND_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));

        instance.setHeadCheckpoint(3, 3, 300, 0, 3_000L);
        Assert.assertEquals(93_750_000L, instance.getEffectiveCheckpointDurationMicros(configured));

        // 1.25^n >= 5 needs 8 steps; 64 seals must land exactly on the ceiling
        // and stay there.
        for (int i = 0; i < 64; i++) {
            instance.setHeadCheckpoint(4 + i, 4 + i, 400 + i, 0, 4_000L + i);
        }
        Assert.assertEquals(configured, instance.getEffectiveCheckpointDurationMicros(configured));
        Assert.assertEquals(configured, instance.getAdaptiveCheckpointDurationMicros());

        // A fresh correction still tightens immediately.
        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, Micros.SECOND_MICROS, true);
        Assert.assertEquals(Micros.MINUTE_MICROS, instance.getEffectiveCheckpointDurationMicros(configured));
    }

    @Test
    public void testIneligibleViewRecordsCorrectionButKeepsTheCeiling() {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        final long configured = 5 * Micros.MINUTE_MICROS;

        // Off the gate (isSealIncremental == false): the floor is the ceiling, so no tightening.
        instance.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, configured, 100_000L, false);
        Assert.assertEquals(configured, instance.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(configured, instance.getEffectiveCheckpointDurationMicros(configured));
        // The correction depth and count are still recorded for live_views().
        Assert.assertEquals(1, instance.getAdaptiveCheckpointCorrectionCount());
        Assert.assertEquals(2 * Micros.SECOND_MICROS, instance.getAdaptiveCheckpointLastCorrectionDepthMicros());

        // A deeper later correction keeps it at the ceiling and bumps the count.
        instance.recordAdaptiveCheckpointCorrection(4 * Micros.MINUTE_MICROS, configured, 100_000L, false);
        Assert.assertEquals(configured, instance.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(2, instance.getAdaptiveCheckpointCorrectionCount());
        Assert.assertEquals(4 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointLastCorrectionDepthMicros());
    }

    @Test
    public void testLegacyAndNoOpInputsOfRecordCorrection() {
        final long configured = 5 * Micros.MINUTE_MICROS;
        final long flush = 100_000L;

        // A depth of zero or less records nothing: the cadence stays LONG_NULL, so
        // getEffectiveCheckpointDurationMicros falls back to the configured ceiling.
        final LiveViewInstance noOp = new LiveViewInstance(null, null, 1, false, -1);
        noOp.recordAdaptiveCheckpointCorrection(0, configured, flush, true);
        Assert.assertEquals(Numbers.LONG_NULL, noOp.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(0, noOp.getAdaptiveCheckpointCorrectionCount());
        noOp.recordAdaptiveCheckpointCorrection(-1, configured, flush, true);
        Assert.assertEquals(Numbers.LONG_NULL, noOp.getAdaptiveCheckpointDurationMicros());
        Assert.assertEquals(0, noOp.getAdaptiveCheckpointCorrectionCount());

        // A ceiling of zero fires on every eligible cycle; a negative one keeps the legacy
        // comparison semantics. Both bypass the floor, gated or not.
        for (boolean gated : new boolean[]{true, false}) {
            final LiveViewInstance zero = new LiveViewInstance(null, null, 1, false, -1);
            zero.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, 0, flush, gated);
            Assert.assertEquals(0, zero.getAdaptiveCheckpointDurationMicros());
            final LiveViewInstance negative = new LiveViewInstance(null, null, 1, false, -1);
            negative.recordAdaptiveCheckpointCorrection(2 * Micros.SECOND_MICROS, -1, flush, gated);
            Assert.assertEquals(-1, negative.getAdaptiveCheckpointDurationMicros());
        }
    }

    @Test
    public void testValueClassesOfRecordCorrection() {
        final long fiveMin = 5 * Micros.MINUTE_MICROS;
        final long flush = 100_000L; // 100ms FLUSH EVERY, the shipped value in these tests

        // Gated views floor at max(1 min, FLUSH EVERY): depth/2 below the floor is clamped up to
        // it, above it is depth/2, above twice the ceiling is the ceiling.
        assertLearnedCadence(1, fiveMin, flush, true, Micros.MINUTE_MICROS);
        assertLearnedCadence(2 * Micros.SECOND_MICROS, fiveMin, flush, true, Micros.MINUTE_MICROS);
        assertLearnedCadence(119_999_999L, fiveMin, flush, true, Micros.MINUTE_MICROS);
        assertLearnedCadence(2 * Micros.MINUTE_MICROS, fiveMin, flush, true, Micros.MINUTE_MICROS);
        assertLearnedCadence(120_000_001L, fiveMin, flush, true, 60_000_001L);
        assertLearnedCadence(4 * Micros.MINUTE_MICROS, fiveMin, flush, true, 2 * Micros.MINUTE_MICROS);
        assertLearnedCadence(10 * Micros.MINUTE_MICROS, fiveMin, flush, true, fiveMin);
        assertLearnedCadence(2 * fiveMin + 1, fiveMin, flush, true, fiveMin);

        // The floor is max(1 min, FLUSH EVERY): a FLUSH EVERY above a minute raises it; one below
        // it or zero leaves the one-minute minimum; a FLUSH EVERY above the ceiling clamps there.
        assertLearnedCadence(2 * Micros.SECOND_MICROS, fiveMin, 0, true, Micros.MINUTE_MICROS);
        assertLearnedCadence(2 * Micros.SECOND_MICROS, fiveMin, 90 * Micros.SECOND_MICROS, true, 90 * Micros.SECOND_MICROS);
        assertLearnedCadence(2 * Micros.SECOND_MICROS, fiveMin, 10 * Micros.MINUTE_MICROS, true, fiveMin);

        // A ceiling below the floor stays the ceiling: the floor cannot push above it.
        assertLearnedCadence(2 * Micros.SECOND_MICROS, 30 * Micros.SECOND_MICROS, flush, true, 30 * Micros.SECOND_MICROS);
        assertLearnedCadence(2 * Micros.SECOND_MICROS, Micros.MINUTE_MICROS, flush, true, Micros.MINUTE_MICROS);
        assertLearnedCadence(2 * Micros.SECOND_MICROS, 60 * Micros.MINUTE_MICROS, flush, true, Micros.MINUTE_MICROS);

        // Ineligible views never tighten: the learned cadence equals the ceiling whatever the depth.
        assertLearnedCadence(2 * Micros.SECOND_MICROS, fiveMin, flush, false, fiveMin);
        assertLearnedCadence(4 * Micros.MINUTE_MICROS, fiveMin, flush, false, fiveMin);
        assertLearnedCadence(2 * Micros.SECOND_MICROS, 30 * Micros.SECOND_MICROS, flush, false, 30 * Micros.SECOND_MICROS);
        assertLearnedCadence(2 * Micros.SECOND_MICROS, 60 * Micros.MINUTE_MICROS, flush, false, 60 * Micros.MINUTE_MICROS);
    }

    private static void assertLearnedCadence(
            long depthMicros,
            long ceilingMicros,
            long flushEveryMicros,
            boolean isSealIncremental,
            long expectedMicros
    ) {
        final LiveViewInstance instance = new LiveViewInstance(null, null, 1, false, -1);
        instance.recordAdaptiveCheckpointCorrection(depthMicros, ceilingMicros, flushEveryMicros, isSealIncremental);
        Assert.assertEquals(
                "depth=" + depthMicros + " ceiling=" + ceilingMicros + " flushEvery=" + flushEveryMicros + " gated=" + isSealIncremental,
                expectedMicros,
                instance.getAdaptiveCheckpointDurationMicros()
        );
    }

    private static String anchoredView(String projection) {
        return "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, " + projection
                + " FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
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

    private void assertShallowCorrectionLearnsCadence(
            String baseDdl,
            String viewSql,
            long ceilingMicros,
            long expectedCadenceMicros
    ) throws Exception {
        assertCorrectionLearnsCadence(
                baseDdl,
                viewSql,
                ceilingMicros,
                "2026-01-01T00:11:00.000000Z",
                "2026-01-01T00:10:58.000000Z",
                2 * Micros.SECOND_MICROS,
                expectedCadenceMicros
        );
    }

    private void assertCorrectionLearnsCadence(
            String baseDdl,
            String viewSql,
            long ceilingMicros,
            String frontierTs,
            String lateTs,
            long expectedDepthMicros,
            long expectedCadenceMicros
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "true");
        // Keep the row trigger off, so only the duration cadence under test decides a seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, ceilingMicros);
        assertMemoryLeak(() -> {
            execute(baseDdl);
            execute(viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                setCurrentMicros(START_MICROS);
                insertAndRefresh(job, "2026-01-01T00:10:00.000000Z", 1.0);
                insertAndRefresh(job, frontierTs, 2.0);
                // One row below the frontier: a single out-of-order correction of the given depth.
                insertAndRefresh(job, lateTs, 3.0);
                final LiveViewInstance instance = instance();
                Assert.assertEquals("one correction must be recorded", 1, instance.getAdaptiveCheckpointCorrectionCount());
                Assert.assertEquals(expectedDepthMicros, instance.getAdaptiveCheckpointLastCorrectionDepthMicros());
                Assert.assertEquals(viewSql, expectedCadenceMicros, instance.getEffectiveCheckpointDurationMicros(ceilingMicros));
            }
            // Leaves the database as it found it, so one test may run several shapes.
            execute("DROP LIVE VIEW lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            execute("DROP TABLE base");
            drainWalQueue();
        });
    }

    private void assertStreamSealsNoInOrderRoot(String viewSql) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, 5 * Micros.MINUTE_MICROS);
        assertMemoryLeak(() -> {
            execute(BASE_DDL);
            execute(viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                setCurrentMicros(START_MICROS);
                insertAndRefresh(job, "2026-01-01T00:09:00.000000Z", 1.0);
                final long entriesBefore = colLong("checkpoint_timeline_entries");
                // 50 commits one second apart, a 2s-deep late row every tenth: 50s of clock, under
                // both the 5-minute configured cadence and the one-minute gated floor.
                final long inOrderSeals = driveLateRowStream(job, 50, Micros.SECOND_MICROS, 10, 2 * Micros.SECOND_MICROS);
                Assert.assertEquals(0, inOrderSeals);
                Assert.assertTrue(
                        "the stream's late-row repairs must seal roots, so the stream exercised the O3 path",
                        colLong("checkpoint_timeline_entries") > entriesBefore
                );
            }
        });
    }

    private long colLong(String column) throws Exception {
        try (RecordCursorFactory factory = select("SELECT " + column + " FROM live_views() WHERE view_name = 'lv'")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private void drainAtStep(LiveViewRefreshJob job) {
        drainWalQueue();
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainJob(job);
        drainWalQueue();
    }

    private long driveLateRowStream(
            LiveViewRefreshJob job,
            int commits,
            long stepMicros,
            int lateEvery,
            long lateDepthMicros
    ) throws Exception {
        final long ts0 = ts("2026-01-01T00:10:00.000000Z");
        long inOrderSeals = 0;
        for (int i = 1; i <= commits; i++) {
            setCurrentMicros(currentMicros + stepMicros);
            final long genBefore = colLong("checkpoint_timeline_generation");
            execute("INSERT INTO base (ts, sym, x) VALUES (" + (ts0 + (long) i * stepMicros) + "::timestamp, 'k', 1.0)");
            drainWalQueue();
            drainJob(job);
            drainWalQueue();
            if (colLong("checkpoint_timeline_generation") != genBefore) {
                inOrderSeals++;
            }
            if (i % lateEvery == 0) {
                execute("INSERT INTO base (ts, sym, x) VALUES ("
                        + (ts0 + (long) i * stepMicros - lateDepthMicros) + "::timestamp, 'b', 1.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
        }
        return inOrderSeals;
    }

    private void insertAndRefresh(LiveViewRefreshJob job, String ts, double x) throws Exception {
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + ts + "', 'k', " + x + ")");
        drainAtStep(job);
    }

    private static LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        return instance;
    }
}
