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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.nanotime.Nanos;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers the segment boundary of a daily anchor read in a named time zone - what
 * {@code ANCHOR DAILY 'HH:MM' '<zone>'} desugars to.
 * <p>
 * The boundary matters only if it is the one the runtime anchor actually resets on, and a
 * plan that floors on the wrong local grid can still agree with itself: it re-floors its
 * own wrong boundaries consistently. So every case here asserts the plan against the
 * runtime {@code timestamp_floor_utc} function rather than against a second copy of the
 * plan's arithmetic, and does it one tick either side of the boundary, where the two
 * would first disagree.
 * <p>
 * Every case runs over both designated timestamp precisions. The origin and the two
 * bounds live in the base column's own units, and the desugared expression spells its
 * origin as a MICROSECOND constant whatever the base is, so the scaling between the two
 * is real arithmetic that a micro-only case leaves unpinned - and a nanosecond zone floor
 * additionally reads the transition table at nanosecond resolution.
 * <p>
 * Europe/Berlin in 2024 is the fixture: clocks go forward at 01:00Z on 31 March and back
 * at 01:00Z on 27 October, so the civil day starting 30 March is 23 hours wide and the one
 * starting 26 October is 25.
 */
public class LiveViewCheckpointTimeZoneAnchorPlanTest extends AbstractLiveViewTest {

    // A one-minute grid over three days: a day either side of the transition's own, so a
    // segment adjacent to it lies wholly inside the window rather than clipped by it.
    // Quarter-hour steps, not half-hour: Australia/Lord_Howe shifts by 30 minutes rather
    // than an hour, so its repeated local window is only 30 minutes wide and a half-hour
    // sweep lands on its edges instead of inside it.
    private static final int ANCHOR_SWEEP_STEP_MINUTES = 15;
    private static final int GRID_ROWS = 3 * 24 * 60;
    private static final long GRID_STEP_MICROS = Micros.MINUTE_MICROS;
    private static final String ZONE = "Europe/Berlin";

    @Test
    public void testADayLineSkipReportsNoEndInsideTheSkippedDay() throws Exception {
        // Pacific/Apia jumped from UTC-10 to UTC+14 at 2011-12-30T10:00Z, so its civil 30
        // December never happened. The 00:30 grid point of that missing day is an anchor value
        // all the same: the runtime converts it to 2011-12-30T10:30Z and hands it to every row
        // from the jump instant up to 2011-12-31T10:30Z - the first half hour of them BELOW the
        // value itself.
        //
        // The arithmetic end for a probe in that half hour is 10:30Z again, the missing day's
        // own start, and it passes the self-checks at its own two instants: the runtime reports
        // 10:30Z at the end and one tick below it. But the anchor does not change there, so the
        // end is no wall - the rows above it carry on the run the rows below it opened, and a
        // replacement stopping at 10:30Z would leave them with stale output.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final String zone = "Pacific/Apia";
            final LiveViewCheckpointAnchorPlan plan = plan("00:30", zone, timestampType);
            final long jump = ts("2011-12-30T10:00:00.000000Z", timestampType);
            final long probe = ts("2011-12-30T10:15:00.000000Z", timestampType);
            final long arithmeticEnd = ts("2011-12-30T10:30:00.000000Z", timestampType);
            final long trueEnd = ts("2011-12-31T10:30:00.000000Z", timestampType);

            // One anchor value from the jump instant to the true end, straight across the
            // arithmetic one.
            Assert.assertEquals(arithmeticEnd, runtimeAnchor("00:30", zone, jump, timestampType));
            Assert.assertEquals(arithmeticEnd, runtimeAnchor("00:30", zone, probe, timestampType));
            Assert.assertEquals(arithmeticEnd, runtimeAnchor("00:30", zone, arithmeticEnd - 1, timestampType));
            Assert.assertEquals(arithmeticEnd, runtimeAnchor("00:30", zone, arithmeticEnd, timestampType));
            Assert.assertEquals(arithmeticEnd, runtimeAnchor("00:30", zone, trueEnd - 1, timestampType));
            Assert.assertEquals(trueEnd, runtimeAnchor("00:30", zone, trueEnd, timestampType));

            // So the probe gets no end. Its start was refused already: the value sits above it.
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(probe));
            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(probe));
            // A probe at or above the value reads the true end, which is a wall.
            Assert.assertEquals(trueEnd, plan.getSegmentEndExclusive(arithmeticEnd));
        }));
    }

    @Test
    public void testAFallBackRunInsideTheRepeatedHourReportsNoStart() throws Exception {
        // 02:30 local happens twice on 27 October, and the first time is 00:30Z under CEST. The
        // anchor value 2024-10-27T00:30Z opens there and holds only until the 01:00Z fall-back
        // instant, where local time winds back to 02:00 CET - below the day's own 02:30 - and
        // the rows up to 01:30Z floor back onto 26 October's 02:30, a value BELOW that start.
        //
        // The start passes the self-checks at its own two instants: the runtime reports 00:30Z
        // there and a lower value one tick below it. But it is no wall. A replay floored there
        // starts every key from a reset, and a key whose 26 October run carries on in the
        // upper interval would have that run cut in two - so the plan reports the segment open
        // below.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE, timestampType);
            final long dayBelowStart = ts("2024-10-26T00:30:00.000000Z", timestampType);
            final long start = ts("2024-10-27T00:30:00.000000Z", timestampType);
            final long probe = ts("2024-10-27T00:45:00.000000Z", timestampType);
            final long fallBack = ts("2024-10-27T01:00:00.000000Z", timestampType);

            Assert.assertEquals(start, runtimeAnchor("02:30", ZONE, probe, timestampType));
            Assert.assertEquals(start, runtimeAnchor("02:30", ZONE, start, timestampType));
            Assert.assertEquals(dayBelowStart, runtimeAnchor("02:30", ZONE, start - 1, timestampType));
            // Above the start, the value from below it again.
            Assert.assertTrue(fallBack > start);
            Assert.assertEquals(dayBelowStart, runtimeAnchor("02:30", ZONE, fallBack, timestampType));

            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(start));
            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(probe));
            // The end was refused already: the value's run stops at the fall-back instant, well
            // below the arithmetic end.
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(probe));

            // The refusal belongs to the run inside the repeated hour, not to the day. The day
            // below has the same fall-back above its start, but the rows there carry its own
            // value, so a probe in the upper interval keeps that start.
            Assert.assertEquals(dayBelowStart, plan.getSegmentStart(fallBack));
        }));
    }

    @Test
    public void testAFallBackThatSplitsASegmentReportsNoEnd() throws Exception {
        // 02:30 local happens TWICE on 27 October: once at 00:30Z under CEST and again at
        // 01:30Z under CET. The hour between them reads 02:00..02:59 CEST and then
        // 02:00..02:29 CET, so the rows from the 01:00Z transition instant to 01:30Z floor
        // back onto 26 October's 02:30 - the anchor value the whole of 26 October carries.
        //
        // That anchor value therefore spans two disjoint intervals of the timestamp with a
        // different value in between, and the arithmetic end names only the lower one. A
        // repair bounded by it would stop below the upper interval and leave it holding
        // stale output, so the plan reports no finite end for the segment.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE, timestampType);
            final long belowFallBack = ts("2024-10-26T20:00:00.000000Z", timestampType);
            final long segmentStart = ts("2024-10-26T00:30:00.000000Z", timestampType);
            final long arithmeticEnd = ts("2024-10-27T00:30:00.000000Z", timestampType);
            final long repeated = ts("2024-10-27T01:00:00.000000Z", timestampType);

            // The start is sound and still reported: it is the lowest instant carrying the
            // value, and the runtime agrees on the value itself.
            Assert.assertEquals(segmentStart, plan.getSegmentStart(belowFallBack));
            Assert.assertEquals(segmentStart, runtimeAnchor("02:30", ZONE, belowFallBack, timestampType));

            // The end the arithmetic names does bound the value's lower interval - the
            // runtime has moved on at it and has not one tick below it - which is exactly
            // why the two checks that look only there are not enough.
            Assert.assertEquals(arithmeticEnd, runtimeAnchor("02:30", ZONE, arithmeticEnd, timestampType));
            Assert.assertEquals(segmentStart, runtimeAnchor("02:30", ZONE, arithmeticEnd - 1, timestampType));

            // And here is the upper interval, half an hour above that end.
            Assert.assertTrue(repeated > arithmeticEnd);
            Assert.assertEquals(segmentStart, runtimeAnchor("02:30", ZONE, repeated, timestampType));

            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(belowFallBack));
        }));
    }

    @Test
    public void testAFixedOffsetZoneFollowsTheSameGridAsTheRuntime() throws Exception {
        // A zone whose rules carry no transition is still read through the zone table
        // rather than folded into arithmetic, and still has to land where the runtime
        // lands. UTC is the one the desugaring itself can emit, for a non-midnight
        // ANCHOR DAILY 'HH:MM' 'UTC'.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> assertSegmentMatchesRuntime(
                "09:30",
                "UTC",
                "2024-03-31T12:00:00.000000Z",
                "2024-03-31T09:30:00.000000Z",
                "2024-04-01T09:30:00.000000Z",
                timestampType
        )));
    }

    @Test
    public void testAnOriginInsideTheSpringForwardGapKeepsTheSegmentOpenBelow() throws Exception {
        // 02:30 local does not exist on 31 March: the clocks jump from 02:00 to 03:00. The
        // runtime floors 03:00 local back onto the missing 02:30 anyway and converts it to
        // 01:30Z, so rows from the transition instant onward all carry that anchor - the
        // floor lands ABOVE some of the rows it describes.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE, timestampType);
            final long inGapDay = ts("2024-03-31T10:00:00.000000Z", timestampType);
            final long transition = ts("2024-03-31T01:00:00.000000Z", timestampType);
            final long naiveStart = ts("2024-03-31T01:30:00.000000Z", timestampType);

            // The runtime's own answer, and the reason the naive floor is not a floor: the
            // row at the transition instant sits below it and carries it all the same.
            Assert.assertEquals(naiveStart, runtimeAnchor("02:30", ZONE, inGapDay, timestampType));
            Assert.assertEquals(naiveStart, runtimeAnchor("02:30", ZONE, transition, timestampType));
            Assert.assertTrue(transition < naiveStart);

            // So the plan reports the segment open below rather than a floor that would
            // leave the transition row outside a repair bounded by it.
            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(inGapDay));
        }));
    }

    @Test
    public void testAnOriginInsideTheSpringForwardGapReportsNoEndForTheDayBelowIt() throws Exception {
        // The other half of the same hole, one day lower. Advancing the local grid names
        // 2024-03-31T02:30 as the next boundary, and the local time it names never happens,
        // so the anchor does not change there: a row half an hour past that "end" still
        // carries the segment's own value. An H below the true one would cut the segment
        // short, so the plan reports none.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE, timestampType);
            final long inSegment = ts("2024-03-30T10:00:00.000000Z", timestampType);
            final long segmentStart = ts("2024-03-30T01:30:00.000000Z", timestampType);
            final long naiveEnd = ts("2024-03-31T00:30:00.000000Z", timestampType);

            Assert.assertEquals(segmentStart, plan.getSegmentStart(inSegment));
            Assert.assertEquals(segmentStart, runtimeAnchor("02:30", ZONE, inSegment, timestampType));
            // Still the same anchor at the boundary the arithmetic would have claimed.
            Assert.assertEquals(segmentStart, runtimeAnchor("02:30", ZONE, naiveEnd, timestampType));
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(inSegment));
        }));
    }

    @Test
    public void testAnOriginInsideTheSpringForwardGapIsOrdinaryAwayFromIt() throws Exception {
        // The same anchor on a day the zone leaves alone. The declines above are scoped to
        // the transition, not to the anchor that straddles it.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> assertSegmentMatchesRuntime(
                "02:30",
                ZONE,
                "2024-06-15T12:00:00.000000Z",
                "2024-06-15T00:30:00.000000Z",
                "2024-06-16T00:30:00.000000Z",
                timestampType
        )));
    }

    @Test
    public void testATransitionDayRefusesOneBoundAndReportsTheOther() throws Exception {
        // Both bounds off ONE probe, which is the part the cases above split. Each bound
        // carries a self-check of its own, over its own instants, so the probes below each get
        // one finite bound and one refusal rather than two refusals - and each refusal widens
        // the repair on its own side. Not every transition-day probe does: a probe in the run
        // that opens inside a fall-back's repeated hour gets two refusals.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE, timestampType);

            // The day the gap ends a segment: a start the runtime agrees with, and no end.
            final long belowGap = ts("2024-03-30T10:00:00.000000Z", timestampType);
            final long belowGapStart = ts("2024-03-30T01:30:00.000000Z", timestampType);
            Assert.assertEquals(belowGapStart, plan.getSegmentStart(belowGap));
            Assert.assertEquals(belowGapStart, runtimeAnchor("02:30", ZONE, belowGap, timestampType));
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(belowGap));

            // The day the gap starts one: no start, and an end the runtime agrees with.
            final long inGap = ts("2024-03-31T10:00:00.000000Z", timestampType);
            final long inGapEnd = ts("2024-04-01T00:30:00.000000Z", timestampType);
            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(inGap));
            Assert.assertEquals(inGapEnd, plan.getSegmentEndExclusive(inGap));
            Assert.assertEquals(inGapEnd, runtimeAnchor("02:30", ZONE, inGapEnd, timestampType));
            Assert.assertNotEquals(inGapEnd, runtimeAnchor("02:30", ZONE, inGapEnd - 1, timestampType));

            // Fall back splits the pair the same way, one bound at a time: the day below it keeps
            // its start and loses its end to the second interval the repeated hour gives it. The
            // run inside the repeated hour loses both, which
            // testAFallBackRunInsideTheRepeatedHourReportsNoStart covers.
            final long belowFallBack = ts("2024-10-26T20:00:00.000000Z", timestampType);
            final long belowFallBackStart = ts("2024-10-26T00:30:00.000000Z", timestampType);
            Assert.assertEquals(belowFallBackStart, plan.getSegmentStart(belowFallBack));
            Assert.assertEquals(belowFallBackStart, runtimeAnchor("02:30", ZONE, belowFallBack, timestampType));
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(belowFallBack));
        }));
    }

    @Test
    public void testAnUnresolvableZoneDeclines() throws Exception {
        forBothPrecisions(timestampType -> {
            Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone(
                    'd', 1, 0, timestampType, "Nowhere/Atlantis"));
            // And the ordinary input checks still apply to the zone-aware factory.
            Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone('d', 0, 0, timestampType, ZONE));
            Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone('z', 1, 0, timestampType, ZONE));
        });
        Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone('d', 1, 0, ColumnType.LONG, ZONE));
    }

    @Test
    public void testBelowTheOriginTheSegmentReportsNoEnd() throws Exception {
        // A non-midnight origin clamps every local time below it to the origin itself, and the
        // first aligned bucket above it floors there too. So the rows below the origin and the
        // whole first bucket carry ONE anchor value, and the origin is no wall between them: a
        // replacement stopping there would leave the first bucket reading state from below it.
        // The end the arithmetic names for a probe below the origin - where the first bucket
        // starts - converts back to the same instant as the start, and the plan refuses it.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE, timestampType);
            final long belowOrigin = ts("1970-01-01T00:30:00.000000Z", timestampType);
            final long origin = ts("1970-01-01T01:30:00.000000Z", timestampType);
            final long inFirstBucket = ts("1970-01-01T12:00:00.000000Z", timestampType);

            Assert.assertEquals(origin, runtimeAnchor("02:30", ZONE, belowOrigin, timestampType));
            Assert.assertEquals(origin, runtimeAnchor("02:30", ZONE, origin, timestampType));
            Assert.assertEquals(origin, runtimeAnchor("02:30", ZONE, inFirstBucket, timestampType));

            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(belowOrigin));
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(belowOrigin));
        }));
    }

    @Test
    public void testEveryReportedSegmentCoversItsWholeAnchorValue() throws Exception {
        // The property the two bounds exist to carry, swept rather than spot-checked: every
        // bound the plan reports finite is a wall, and together they cover every instant the
        // runtime gives the anchor value start - no fewer. A segment that held fewer would let a
        // repair bounded by it leave rows behind.
        //
        // Each bound is held to that on its own, whatever the plan says about the other one. A
        // caller reads them separately - the repair plan floors a replay on the start of one
        // probe's segment and stops the replacement at the end of another's - so a finite start
        // beside a refused end still has to be a wall. The fall-back run inside the repeated
        // hour is where a start fails that while its end is refused anyway, and Pacific/Apia's
        // skipped day is the mirror image: an end that fails it beside a refused start.
        //
        // It may hold MORE, and that is sound. A fall-back gives one anchor value two runs,
        // and the pair a probe in the upper one gets starts at the lower one's start, so it
        // spans the run of whatever value sits between them - the shape
        // LiveViewCheckpointSegmentChangeSetTest pins for Berlin '02:30'. Both ends are still
        // walls, so the interval is a union of whole runs and a replacement bounded by it
        // reconstructs every row it re-emits. assertSegmentIsAWall() below asserts that wall
        // property rather than an upper extent.
        //
        // The sweep runs every quarter-hour wall time of the day against a three-day window
        // around each of the seven transitions below, which is what makes it a general check
        // rather than one instant's: it covers both directions, the 30-minute shift
        // Lord Howe uses as well as the usual hour, a whole skipped day, and zones on both
        // sides of UTC. The return value is how many anchor values the window found split
        // across more than one interval, and the assertions on it are what keep the sweep
        // from passing vacuously.
        assertMemoryLeak(() -> {
            // Fall back: the direction that splits an anchor value in two.
            Assert.assertTrue(
                    "Europe/Berlin fall-back must produce split anchor values",
                    assertSegmentsCoverTheirAnchorValues(ZONE, "2024-10-26T00:00:00.000000Z") > 0
            );
            Assert.assertTrue(
                    "America/New_York fall-back must produce split anchor values",
                    assertSegmentsCoverTheirAnchorValues("America/New_York", "2024-11-02T00:00:00.000000Z") > 0
            );
            Assert.assertTrue(
                    "Australia/Lord_Howe fall-back must produce split anchor values",
                    assertSegmentsCoverTheirAnchorValues("Australia/Lord_Howe", "2024-04-06T00:00:00.000000Z") > 0
            );

            // Spring forward: it skips a local window rather than repeating one, so it
            // splits nothing. The existing cases above cover what it does instead - an
            // origin inside the gap keeps its segment open below and its neighbour below
            // reports no end - and the assertion here is that the sweep agrees.
            Assert.assertEquals(
                    "spring forward must split no anchor value in Europe/Berlin",
                    0,
                    assertSegmentsCoverTheirAnchorValues(ZONE, "2024-03-30T00:00:00.000000Z")
            );
            Assert.assertEquals(
                    "spring forward must split no anchor value in America/New_York",
                    0,
                    assertSegmentsCoverTheirAnchorValues("America/New_York", "2024-03-09T00:00:00.000000Z")
            );
            Assert.assertEquals(
                    "spring forward must split no anchor value in Australia/Lord_Howe",
                    0,
                    assertSegmentsCoverTheirAnchorValues("Australia/Lord_Howe", "2024-10-05T00:00:00.000000Z")
            );

            // A whole skipped day: Pacific/Apia crossed the date line from UTC-10 to UTC+14 at
            // 2011-12-30T10:00Z. It splits nothing either, but the missing day's anchor value
            // holds from the jump instant, below the value itself, to a day above it.
            Assert.assertEquals(
                    "the skipped day must split no anchor value in Pacific/Apia",
                    0,
                    assertSegmentsCoverTheirAnchorValues("Pacific/Apia", "2011-12-29T00:00:00.000000Z")
            );
        });
    }

    @Test
    public void testMidnightSegmentIsTwentyFiveHoursWideAcrossFallBack() throws Exception {
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = assertSegmentMatchesRuntime(
                    "00:00",
                    ZONE,
                    "2024-10-27T10:00:00.000000Z",
                    "2024-10-26T22:00:00.000000Z",
                    "2024-10-27T23:00:00.000000Z",
                    timestampType
            );
            final long probe = ts("2024-10-27T10:00:00.000000Z", timestampType);
            Assert.assertEquals(
                    hours(25, timestampType),
                    plan.getSegmentEndExclusive(probe) - plan.getSegmentStart(probe)
            );
        }));
    }

    @Test
    public void testMidnightSegmentIsTwentyThreeHoursWideAcrossSpringForward() throws Exception {
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = assertSegmentMatchesRuntime(
                    "00:00",
                    ZONE,
                    "2024-03-31T10:00:00.000000Z",
                    "2024-03-30T23:00:00.000000Z",
                    "2024-03-31T22:00:00.000000Z",
                    timestampType
            );
            final long onTransitionDay = ts("2024-03-31T10:00:00.000000Z", timestampType);
            Assert.assertEquals(
                    hours(23, timestampType),
                    plan.getSegmentEndExclusive(onTransitionDay) - plan.getSegmentStart(onTransitionDay)
            );
            // The day below it is an ordinary 24 hours, so the width above is the zone's
            // and not the plan's.
            final long dayBelow = ts("2024-03-30T10:00:00.000000Z", timestampType);
            Assert.assertEquals(
                    hours(24, timestampType),
                    plan.getSegmentEndExclusive(dayBelow) - plan.getSegmentStart(dayBelow)
            );
        }));
    }

    @Test
    public void testNonMidnightOriginFloorsOnTheLocalGridRatherThanTheEpoch() throws Exception {
        // The origin is a modulus seed for the LOCAL grid, not a UTC instant. A plan that
        // dropped it - or applied it in UTC space - would floor 2024-10-26T18:00Z to the
        // previous local midnight at 2024-10-25T22:00Z, which re-floors to itself just as
        // consistently. Only the runtime can tell the two grids apart.
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = assertSegmentMatchesRuntime(
                    "12:00",
                    ZONE,
                    "2024-10-26T18:00:00.000000Z",
                    "2024-10-26T10:00:00.000000Z",
                    "2024-10-27T11:00:00.000000Z",
                    timestampType
            );
            Assert.assertNotEquals(
                    ts("2024-10-25T22:00:00.000000Z", timestampType),
                    plan.getSegmentStart(ts("2024-10-26T18:00:00.000000Z", timestampType))
            );
        }));
    }

    @Test
    public void testNonMidnightOriginTracksBothTransitions() throws Exception {
        assertMemoryLeak(() -> forBothPrecisions(timestampType -> {
            // Spring forward falls inside a 12:00-anchored segment, so it is 23 hours wide.
            assertSegmentMatchesRuntime(
                    "12:00",
                    ZONE,
                    "2024-03-30T20:00:00.000000Z",
                    "2024-03-30T11:00:00.000000Z",
                    "2024-03-31T10:00:00.000000Z",
                    timestampType
            );
            // And the segment above it is back to 24.
            assertSegmentMatchesRuntime(
                    "12:00",
                    ZONE,
                    "2024-03-31T20:00:00.000000Z",
                    "2024-03-31T10:00:00.000000Z",
                    "2024-04-01T10:00:00.000000Z",
                    timestampType
            );
        }));
    }

    @Test
    public void testTheTopmostSegmentHasNoRepresentableEnd() throws Exception {
        forBothPrecisions(timestampType -> {
            final LiveViewCheckpointAnchorPlan plan = plan("00:00", ZONE, timestampType);
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(Long.MAX_VALUE));
            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(Long.MAX_VALUE));
            // LONG_NULL is Long.MIN_VALUE, which is what an absent change bound reaches the
            // plan as; it has no segment either.
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(Numbers.LONG_NULL));
        });
    }

    /**
     * Asserts that no run of one anchor value reaches across {@code bound}: every row at or
     * above it carries an anchor value at or above it, and every row below it one below it.
     * The two value ranges are then disjoint, so no key can hold one run on both sides.
     */
    private static void assertIsAWall(LongList timestamps, LongList anchors, long bound, String where) {
        for (int i = 0, n = timestamps.size(); i < n; i++) {
            final long ts = timestamps.getQuick(i);
            final long anchor = anchors.getQuick(i);
            if (ts >= bound) {
                Assert.assertTrue("the row at " + Micros.toUSecString(ts) + " carries the anchor "
                                + Micros.toUSecString(anchor) + " from below the bound, " + where,
                        anchor >= bound);
            } else {
                Assert.assertTrue("the row at " + Micros.toUSecString(ts) + " carries the anchor "
                                + Micros.toUSecString(anchor) + " from at or above the bound, " + where,
                        anchor < bound);
            }
        }
    }

    /**
     * Runs one case over both designated timestamp precisions, so a bound expressed in the
     * column's own units is pinned in both of them.
     */
    private static void forBothPrecisions(PrecisionCase testCase) throws Exception {
        testCase.run(ColumnType.TIMESTAMP_MICRO);
        testCase.run(ColumnType.TIMESTAMP_NANO);
    }

    /**
     * {@code count} hours in {@code timestampType}'s own units.
     */
    private static long hours(int count, int timestampType) {
        return count * (ColumnType.isTimestampNano(timestampType) ? Nanos.HOUR_NANOS : Micros.HOUR_MICROS);
    }

    private static LiveViewCheckpointAnchorPlan plan(String anchorTime, String zone, int timestampType) {
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.ofTimeZone(
                'd',
                1,
                ts("1970-01-01T" + anchorTime + ":00.000000Z", timestampType),
                timestampType,
                zone
        );
        Assert.assertNotNull(
                "ANCHOR DAILY '" + anchorTime + "' '" + zone + "' must carry a segment on "
                        + ColumnType.nameOf(timestampType),
                plan
        );
        return plan;
    }

    /**
     * Parses a timestamp literal into {@code timestampType}'s own units, which is what
     * every bound the plan reports and every argument it takes is expressed in.
     */
    private static long ts(String timestamp, int timestampType) {
        final long micros = ts(timestamp);
        return ColumnType.isTimestampNano(timestampType) ? micros * Nanos.MICRO_NANOS : micros;
    }

    /**
     * The wall time {@code minuteOfDay} names, as {@code ANCHOR DAILY} spells it.
     */
    private static String wallTime(int minuteOfDay) {
        final int hour = minuteOfDay / 60;
        final int minute = minuteOfDay % 60;
        return (hour < 10 ? "0" : "") + hour + ":" + (minute < 10 ? "0" : "") + minute;
    }

    /**
     * Sweeps every quarter-hour {@code ANCHOR DAILY} wall time over a three-day window of
     * {@code zone} at one-minute resolution, and asserts of every bound the plan reports
     * finite that it is a wall - see {@link #assertSegmentIsAWall} for the parts of that. A
     * refused bound asserts nothing: a refusal costs the view only the localized path and is
     * always available.
     * <p>
     * The anchor values come from {@code timestamp_floor_utc} rather than from a second
     * copy of the plan's own arithmetic, so the grid the assertions are made against is the
     * grid the runtime resets on. Values whose run touches either end of the window are
     * skipped, since the window rather than the zone would be bounding them.
     * <p>
     * The sweep runs on the microsecond precision alone. The bounds are the same zone
     * arithmetic on both, and the cases that pin them across precisions - including the
     * split-segment refusal this exists to generalize - run {@code forBothPrecisions}.
     *
     * @param windowLo the first instant of the window, a day below the transition's own
     * @return how many anchor values the window carried in more than one interval
     */
    private int assertSegmentsCoverTheirAnchorValues(String zone, String windowLo) throws SqlException {
        final int timestampType = ColumnType.TIMESTAMP_MICRO;
        final LongList timestamps = new LongList();
        final LongList anchors = new LongList();
        final LongList runValues = new LongList();
        final LongList runLo = new LongList();
        final LongList runHi = new LongList();
        int splitValues = 0;
        for (int minuteOfDay = 0; minuteOfDay < 24 * 60; minuteOfDay += ANCHOR_SWEEP_STEP_MINUTES) {
            final String anchorTime = wallTime(minuteOfDay);
            final LiveViewCheckpointAnchorPlan plan = plan(anchorTime, zone, timestampType);
            readAnchorGrid(anchorTime, zone, windowLo, timestamps, anchors);
            final int rows = timestamps.size();
            Assert.assertEquals(GRID_ROWS, rows);

            // The maximal runs of one anchor value, in timestamp order. A value the zone
            // splits owns more than one of them.
            runValues.clear();
            runLo.clear();
            runHi.clear();
            for (int i = 0; i < rows; i++) {
                if (i > 0 && anchors.getQuick(i - 1) == anchors.getQuick(i)) {
                    runHi.setQuick(runHi.size() - 1, i);
                    continue;
                }
                runValues.add(anchors.getQuick(i));
                runLo.add(i);
                runHi.add(i);
            }

            for (int r = 0, runs = runValues.size(); r < runs; r++) {
                final long value = runValues.getQuick(r);
                boolean isSeen = false;
                for (int s = 0; s < r; s++) {
                    isSeen |= runValues.getQuick(s) == value;
                }
                if (isSeen) {
                    continue; // a later run of a value the sweep has already asserted on
                }
                // The value's whole extent, across every run it owns.
                long valueLo = Long.MAX_VALUE;
                long valueHi = Long.MIN_VALUE;
                int ownRuns = 0;
                boolean isClipped = false;
                for (int s = r; s < runs; s++) {
                    if (runValues.getQuick(s) != value) {
                        continue;
                    }
                    final int lo = (int) runLo.getQuick(s);
                    final int hi = (int) runHi.getQuick(s);
                    isClipped |= lo == 0 || hi == rows - 1;
                    valueLo = Math.min(valueLo, timestamps.getQuick(lo));
                    valueHi = Math.max(valueHi, timestamps.getQuick(hi));
                    ownRuns++;
                }
                if (ownRuns > 1) {
                    splitValues++;
                }
                if (isClipped) {
                    continue; // the window bounds this value, not the zone
                }

                for (int s = r; s < runs; s++) {
                    if (runValues.getQuick(s) != value) {
                        continue;
                    }
                    assertSegmentIsAWall(
                            plan,
                            anchorTime,
                            zone,
                            timestamps,
                            anchors,
                            timestamps.getQuick((int) runLo.getQuick(s)),
                            value,
                            valueLo,
                            valueHi
                    );
                    assertSegmentIsAWall(
                            plan,
                            anchorTime,
                            zone,
                            timestamps,
                            anchors,
                            timestamps.getQuick((int) runHi.getQuick(s)),
                            value,
                            valueLo,
                            valueHi
                    );
                }
            }
        }
        return splitValues;
    }

    /**
     * One probe of the sweep. Each bound the plan reports finite has to be a wall on its own,
     * whatever the plan reports for the other one, which is the whole of what a localized
     * repair rests on:
     * <ul>
     *     <li>{@code start} is the lowest instant carrying the probe's own anchor value, and
     *     no instant at or above it carries an anchor value from below it - so a replay from
     *     it starts every key on a reset rather than mid-run;</li>
     *     <li>{@code end} lies above the probe's anchor value's top instant, and no instant at
     *     or above it carries an anchor value from below it - so a replacement that stops there
     *     leaves nothing above it reading state the repair rewrote;</li>
     *     <li>and for either bound, no instant below it carries an anchor value from at or
     *     above it, so no run reaches across it from below either.</li>
     * </ul>
     * A fall-back breaks the first two from above, since it winds local time back and the
     * rows it repeats floor below a boundary they sit above; a skipped day breaks the last,
     * since the rows after the jump carry an anchor value above themselves.
     * <p>
     * The pair may be WIDER than the probe's own run: a fall-back gives one anchor value
     * two intervals, and the pair a probe in the upper one gets runs from the lower one's
     * start. That is sound - both ends are still walls, so the interval is a union of whole
     * runs - and the assertions here are the wall property rather than a single run's
     * extent.
     */
    private void assertSegmentIsAWall(
            LiveViewCheckpointAnchorPlan plan,
            String anchorTime,
            String zone,
            LongList timestamps,
            LongList anchors,
            long probe,
            long value,
            long valueLo,
            long valueHi
    ) {
        final String where = "ANCHOR DAILY '" + anchorTime + "' '" + zone + "' at " + Micros.toUSecString(probe);
        final long start = plan.getSegmentStart(probe);
        if (start != Long.MIN_VALUE) {
            final String startWhere = where + ", segment start " + Micros.toUSecString(start);
            Assert.assertEquals("the reported start must be an anchor value the runtime gives, " + startWhere,
                    value, start);
            Assert.assertEquals("the segment must start where its anchor value does, " + startWhere,
                    valueLo, start);
            assertIsAWall(timestamps, anchors, start, startWhere);
        }
        final long end = plan.getSegmentEndExclusive(probe);
        if (end != Numbers.LONG_NULL) {
            final String endWhere = where + ", segment end " + Micros.toUSecString(end);
            Assert.assertTrue("the segment must reach its anchor value's top instant "
                    + Micros.toUSecString(valueHi) + ", " + endWhere, valueHi < end);
            assertIsAWall(timestamps, anchors, end, endWhere);
        }
    }

    /**
     * Fills {@code timestamps} and {@code anchors} with a one-minute grid over three days
     * from {@code windowLo} and the anchor value {@code timestamp_floor_utc} gives each of
     * them - the very function {@code ANCHOR DAILY 'HH:MM' '<zone>'} desugars to.
     */
    private void readAnchorGrid(
            String anchorTime,
            String zone,
            String windowLo,
            LongList timestamps,
            LongList anchors
    ) throws SqlException {
        final String sql = "SELECT ts, timestamp_floor_utc('1d', ts, '1970-01-01T" + anchorTime
                + ":00.000000Z'::timestamp, '+00:00', '" + zone + "') AS anchor FROM ("
                + "SELECT (" + ts(windowLo) + " + (x - 1) * " + GRID_STEP_MICROS
                + ")::timestamp AS ts FROM long_sequence(" + GRID_ROWS + "))";
        timestamps.clear();
        anchors.clear();
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                timestamps.add(record.getTimestamp(0));
                anchors.add(record.getTimestamp(1));
            }
        }
    }

    /**
     * Asserts the plan's two bounds for {@code instant}, then asserts the runtime anchor
     * agrees that those bounds are where its own value changes: it holds the segment start
     * at the start, still holds it one tick below the end, and has moved on at the end
     * itself.
     */
    private LiveViewCheckpointAnchorPlan assertSegmentMatchesRuntime(
            String anchorTime,
            String zone,
            String instant,
            String expectedStart,
            String expectedEnd,
            int timestampType
    ) throws SqlException {
        final String where = instant + " on " + ColumnType.nameOf(timestampType);
        final LiveViewCheckpointAnchorPlan plan = plan(anchorTime, zone, timestampType);
        final long timestamp = ts(instant, timestampType);
        final long start = ts(expectedStart, timestampType);
        final long end = ts(expectedEnd, timestampType);
        Assert.assertEquals("segment start at " + where, start, plan.getSegmentStart(timestamp));
        Assert.assertEquals("segment end at " + where, end, plan.getSegmentEndExclusive(timestamp));

        Assert.assertEquals(
                "runtime anchor at " + where,
                start,
                runtimeAnchor(anchorTime, zone, timestamp, timestampType)
        );
        Assert.assertEquals(
                "runtime anchor at the segment start of " + where,
                start,
                runtimeAnchor(anchorTime, zone, start, timestampType)
        );
        Assert.assertEquals(
                "the runtime must still hold the segment one tick below its end, at " + where,
                start,
                runtimeAnchor(anchorTime, zone, end - 1, timestampType)
        );
        Assert.assertEquals(
                "and must have reset at the end itself, at " + where,
                end,
                runtimeAnchor(anchorTime, zone, end, timestampType)
        );
        return plan;
    }

    /**
     * The anchor value the runtime computes for one instant, through the very function the
     * desugared {@code ANCHOR DAILY 'HH:MM' '<zone>'} expression carries.
     * <p>
     * The origin stays a microsecond constant on both precisions because that is what the
     * desugaring emits whatever the base column is - {@code SqlParser.desugarDailyAnchor}
     * spells it {@code '1970-01-01THH:MM:00.000000Z'::timestamp} - so the runtime scales it
     * the same way the plan's own origin has to be scaled, and the two are compared rather
     * than assumed equal.
     */
    private long runtimeAnchor(String anchorTime, String zone, long timestamp, int timestampType) throws SqlException {
        final String cast = ColumnType.isTimestampNano(timestampType) ? "::timestamp_ns" : "::timestamp";
        final String sql = "SELECT timestamp_floor_utc('1d', " + timestamp + cast + ", '1970-01-01T"
                + anchorTime + ":00.000000Z'::timestamp, '+00:00', '" + zone + "')";
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(sql, cursor.hasNext());
            return cursor.getRecord().getTimestamp(0);
        }
    }

    @FunctionalInterface
    private interface PrecisionCase {
        void run(int timestampType) throws Exception;
    }
}
