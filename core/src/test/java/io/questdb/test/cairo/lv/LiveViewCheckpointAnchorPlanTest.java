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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the compiler-owned fixed segment boundary of an anchored live view: which
 * anchors carry one, which decline, and - for the anchor whose origin the plan reads
 * from the definition rather than from the expression - that the boundary it computes is
 * the one the runtime actually resets on.
 */
public class LiveViewCheckpointAnchorPlanTest extends AbstractLiveViewTest {

    @Before
    public void pinClockBelowTestData() {
        // Below the 2026 test data, so a START FROM NOW view admits every row it sees.
        setCurrentMicros(0L);
    }

    @Test
    public void testAnchorDailyMidnightPlansEpochAlignedDays() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView("ANCHOR DAILY '00:00'");
            final LiveViewCheckpointAnchorPlan plan = refreshAndGetPlan();
            Assert.assertNotNull("DAILY at UTC midnight must carry a fixed segment", plan);
            Assert.assertEquals('d', plan.getUnit());
            Assert.assertEquals(1, plan.getStride());
            Assert.assertEquals(0, plan.getSegmentOffset());

            final long dayStart = ts("2026-08-01T00:00:00.000000Z");
            final long nextDay = ts("2026-08-02T00:00:00.000000Z");
            Assert.assertEquals(dayStart, plan.getSegmentStart(dayStart));
            Assert.assertEquals(dayStart, plan.getSegmentStart(nextDay - 1));
            Assert.assertEquals(nextDay, plan.getSegmentEndExclusive(dayStart));
            Assert.assertEquals(nextDay, plan.getSegmentEndExclusive(nextDay - 1));
            // Segments are left-closed and right-open, so they abut without overlapping.
            Assert.assertEquals(nextDay, plan.getSegmentStart(nextDay));

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyNonMidnightPlansTheBoundaryTheRuntimeResetsOn() throws Exception {
        // The desugared form is a three-argument timestamp_floor whose origin the AST
        // carries as a cast expression, so the plan reads the origin off the definition's
        // captured DAILY time instead. The second half is what makes that reading sound:
        // the accumulator must restart at exactly the timestamp the plan calls the
        // segment end, and must not restart one microsecond earlier.
        assertMemoryLeak(() -> {
            createBaseAndView("ANCHOR DAILY '09:30'");
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T12:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-02T09:29:59.999999Z', 20, 'a'), " +
                    "('2026-08-02T09:30:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T09:30:00.000001Z', 7, 'a')");
            final LiveViewCheckpointAnchorPlan plan = refreshAndGetPlan();
            Assert.assertNotNull("DAILY without a time zone must carry a fixed segment", plan);
            Assert.assertEquals('d', plan.getUnit());
            Assert.assertEquals(1, plan.getStride());
            Assert.assertEquals(
                    9 * Micros.HOUR_MICROS + 30 * Micros.MINUTE_MICROS,
                    plan.getSegmentOffset()
            );

            final long inFirstBucket = ts("2026-08-01T12:00:00.000000Z");
            Assert.assertEquals(ts("2026-08-01T09:30:00.000000Z"), plan.getSegmentStart(inFirstBucket));
            Assert.assertEquals(
                    ts("2026-08-02T09:30:00.000000Z"),
                    plan.getSegmentEndExclusive(inFirstBucket)
            );

            // The row one microsecond below that boundary still accumulates into the
            // first segment; the row at the boundary starts a new one.
            assertQuery("SELECT ts, s FROM lv WHERE sym = 'a' ORDER BY ts").noLeakCheck().timestamp("ts").returns("ts\ts\n" +
                    "2026-08-01T12:00:00.000000Z\t10.0\n" +
                    "2026-08-02T09:29:59.999999Z\t30.0\n" +
                    "2026-08-02T09:30:00.000000Z\t5.0\n" +
                    "2026-08-02T09:30:00.000001Z\t12.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyWithTimeZonePlansTheZonesCivilDay() throws Exception {
        // A tz-aware daily anchor desugars to timestamp_floor_utc, whose buckets follow the
        // zone's civil day rather than a fixed stride. The plan describes them from the
        // zone's own transition table; LiveViewCheckpointTimeZoneAnchorPlanTest is where the
        // DST widths and the plan-versus-runtime agreement are pinned.
        assertMemoryLeak(() -> {
            createBaseAndView("ANCHOR DAILY '00:00' 'Europe/Berlin'");
            final LiveViewCheckpointAnchorPlan plan = refreshAndGetPlan();
            Assert.assertNotNull("a tz-aware DAILY anchor must carry a fixed segment too", plan);
            Assert.assertEquals('d', plan.getUnit());
            Assert.assertEquals(1, plan.getStride());
            Assert.assertEquals(0, plan.getSegmentOffset());

            // Berlin runs two hours ahead of UTC in August, so its civil day starts at
            // 22:00Z the evening before.
            final long dayStart = ts("2026-07-31T22:00:00.000000Z");
            final long nextDay = ts("2026-08-01T22:00:00.000000Z");
            Assert.assertEquals(dayStart, plan.getSegmentStart(dayStart));
            Assert.assertEquals(dayStart, plan.getSegmentStart(nextDay - 1));
            Assert.assertEquals(nextDay, plan.getSegmentEndExclusive(dayStart));
            Assert.assertEquals(nextDay, plan.getSegmentEndExclusive(nextDay - 1));
            Assert.assertEquals(nextDay, plan.getSegmentStart(nextDay));

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorBesideASlidingWindowStillPlansItsOwnSegment() throws Exception {
        // The anchor bounds the functions it resets, and only those. A bounded ROWS window
        // declared beside the anchored one keeps sliding across every bucket crossing, so
        // the segment says nothing about its state - but that is the ROWS plan's job to
        // say, not a reason to withhold the segment. The repair takes the union of the two
        // and declines only when some function is left outside both.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s, " +
                    "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r " +
                    "FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts " +
                    "ANCHOR EXPRESSION timestamp_floor('1d', ts))");
            final LiveViewCheckpointAnchorPlan plan = refreshAndGetPlan();
            Assert.assertNotNull("the anchored function's segment is still fixed", plan);
            Assert.assertEquals('d', plan.getUnit());
            Assert.assertEquals(1, plan.getStride());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorExpressionFloorPlansStridedSegments() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView("ANCHOR EXPRESSION timestamp_floor('4h', ts)");
            final LiveViewCheckpointAnchorPlan plan = refreshAndGetPlan();
            Assert.assertNotNull(plan);
            Assert.assertEquals('h', plan.getUnit());
            Assert.assertEquals(4, plan.getStride());
            Assert.assertEquals(0, plan.getSegmentOffset());

            final long inBucket = ts("2026-08-01T05:17:00.000000Z");
            Assert.assertEquals(ts("2026-08-01T04:00:00.000000Z"), plan.getSegmentStart(inBucket));
            Assert.assertEquals(ts("2026-08-01T08:00:00.000000Z"), plan.getSegmentEndExclusive(inBucket));

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCalendarUnitSegmentBoundaries() {
        // A month is not a fixed number of microseconds, but its boundaries are still
        // exact: the plan's self-check accepts them because adding one month to a bucket
        // start lands on the floor's own next boundary.
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.of('M', 1, 0, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull(plan);
        final long inFebruary = ts("2026-02-14T03:00:00.000000Z");
        Assert.assertEquals(ts("2026-02-01T00:00:00.000000Z"), plan.getSegmentStart(inFebruary));
        Assert.assertEquals(ts("2026-03-01T00:00:00.000000Z"), plan.getSegmentEndExclusive(inFebruary));
    }

    @Test
    public void testEpochAlignedSegmentBoundaries() {
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.of('h', 1, 0, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull(plan);
        Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, plan.getTimestampType());
        final long hourStart = ts("2026-08-01T05:00:00.000000Z");
        final long nextHour = ts("2026-08-01T06:00:00.000000Z");
        Assert.assertEquals(hourStart, plan.getSegmentStart(hourStart));
        Assert.assertEquals(hourStart, plan.getSegmentStart(nextHour - 1));
        Assert.assertEquals(nextHour, plan.getSegmentEndExclusive(hourStart));
        Assert.assertEquals(nextHour, plan.getSegmentEndExclusive(nextHour - 1));
    }

    @Test
    public void testNanosecondColumnSegmentBoundaries() {
        // The bounds are expressed in the designated timestamp's own units, so the same
        // anchor over a nanosecond column produces nanosecond boundaries.
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.of('d', 1, 0, ColumnType.TIMESTAMP_NANO);
        Assert.assertNotNull(plan);
        final long dayStart = ts("2026-08-01T00:00:00.000000Z") * 1000L;
        final long nextDay = ts("2026-08-02T00:00:00.000000Z") * 1000L;
        Assert.assertEquals(dayStart, plan.getSegmentStart(dayStart + 1));
        Assert.assertEquals(nextDay, plan.getSegmentEndExclusive(dayStart + 1));
    }

    @Test
    public void testOffsetAlignedSegmentBoundaries() {
        final long origin = ts("1970-01-01T09:30:00.000000Z");
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.of('d', 1, origin, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull(plan);
        final long bucketStart = ts("2026-08-01T09:30:00.000000Z");
        final long nextBucket = ts("2026-08-02T09:30:00.000000Z");
        Assert.assertEquals(bucketStart, plan.getSegmentStart(bucketStart));
        Assert.assertEquals(bucketStart, plan.getSegmentStart(nextBucket - 1));
        Assert.assertEquals(nextBucket, plan.getSegmentEndExclusive(nextBucket - 1));

        // Every row below the origin carries the origin as its anchor value, so they
        // share one segment that is open below and ends where the first bucket starts.
        final long belowOrigin = origin - 1;
        Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(belowOrigin));
        Assert.assertEquals(origin, plan.getSegmentEndExclusive(belowOrigin));
    }

    @Test
    public void testSegmentEndReportsNoFiniteBoundWhenThePeriodCannotAdvance() {
        // A nanosecond period over a microsecond column advances nothing, so the computed
        // end is not the floor's next boundary and the plan reports no finite high bound
        // rather than one at or below the row it was asked about.
        final LiveViewCheckpointAnchorPlan subResolution = LiveViewCheckpointAnchorPlan.of('n', 1, 0, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull(subResolution);
        Assert.assertEquals(
                Numbers.LONG_NULL,
                subResolution.getSegmentEndExclusive(ts("2026-08-01T00:00:00.000000Z"))
        );

        // The topmost segment has no representable end either.
        final LiveViewCheckpointAnchorPlan daily = LiveViewCheckpointAnchorPlan.of('d', 1, 0, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull(daily);
        Assert.assertEquals(Numbers.LONG_NULL, daily.getSegmentEndExclusive(Long.MAX_VALUE));
    }

    @Test
    public void testUnrecognizedAnchorExpressionsHaveNoPlan() throws Exception {
        assertMemoryLeak(() -> {
            // date_trunc floors too, but its unit is a word rather than a period literal;
            // it stays outside the recognized shape until its unit set is proven.
            assertNoPlan("ts, sym", "date_trunc('day', ts)");
            // A floor of a non-designated timestamp column says nothing about where a
            // segment sits on the designated one.
            assertNoPlan("ts, sym, other_ts", "timestamp_floor('1d', other_ts)");
            // dateadd shifts every row by the same amount, so it never repeats a value:
            // each row would be its own segment, which is no bucket boundary at all.
            assertNoPlan("ts, sym", "dateadd('d', 1, ts)");
            // The timestamp argument has to be the designated timestamp itself, not an
            // expression over it that this cannot invert.
            assertNoPlan("ts, sym", "timestamp_floor('1d', dateadd('h', 1, ts))");
        });
    }

    @Test
    public void testUnsupportedSegmentInputsDecline() {
        Assert.assertNull(LiveViewCheckpointAnchorPlan.of('z', 1, 0, ColumnType.TIMESTAMP_MICRO));
        Assert.assertNull(LiveViewCheckpointAnchorPlan.of('d', 0, 0, ColumnType.TIMESTAMP_MICRO));
        Assert.assertNull(LiveViewCheckpointAnchorPlan.of('d', -1, 0, ColumnType.TIMESTAMP_MICRO));
        Assert.assertNull(LiveViewCheckpointAnchorPlan.of('d', 1, 0, ColumnType.LONG));
    }

    @After
    public void unpinClock() {
        // currentMicros is a static that outlives the class; hand the next one a clean slate.
        setCurrentMicros(-1);
    }

    private static void createBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, other_ts TIMESTAMP, x INT, sym SYMBOL) " +
                "TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void createView(String projection, String anchorClause) throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                "SELECT " + projection + ", sum(x) OVER w AS s FROM base " +
                "WINDOW w AS (PARTITION BY sym ORDER BY ts " + anchorClause + ")");
    }

    private void assertNoPlan(String projection, String anchorExpression) throws Exception {
        createBase();
        createView(projection, "ANCHOR EXPRESSION " + anchorExpression);
        Assert.assertNull(anchorExpression, refreshAndGetPlan());
        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    private void createBaseAndView(String anchorClause) throws Exception {
        createBase();
        createView("ts, sym", anchorClause);
    }

    /**
     * Drives one refresh so the anchor function and window are compiled, then returns the
     * segment plan the compiler attached to the window. The anchor is built on the first
     * refresh that has rows to process, so this seeds one.
     */
    private LiveViewCheckpointAnchorPlan refreshAndGetPlan() throws Exception {
        execute("INSERT INTO base (ts, x, sym) VALUES ('2026-07-01T00:00:00.000000Z', 1, 'seed')");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final LiveViewWindow window = instance.getAnchorWindow();
        Assert.assertNotNull("anchor window must be built after refresh", window);
        return window.getCheckpointAnchorPlan();
    }
}
