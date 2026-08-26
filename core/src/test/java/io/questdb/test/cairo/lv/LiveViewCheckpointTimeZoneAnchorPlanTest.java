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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
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
 * plan's arithmetic, and does it one microsecond either side of the boundary, where the
 * two would first disagree.
 * <p>
 * Europe/Berlin in 2024 is the fixture: clocks go forward at 01:00Z on 31 March and back
 * at 01:00Z on 27 October, so the civil day starting 30 March is 23 hours wide and the one
 * starting 26 October is 25.
 */
public class LiveViewCheckpointTimeZoneAnchorPlanTest extends AbstractLiveViewTest {

    private static final String ZONE = "Europe/Berlin";

    @Test
    public void testAFixedOffsetZoneFollowsTheSameGridAsTheRuntime() throws Exception {
        // A zone whose rules carry no transition is still read through the zone table
        // rather than folded into arithmetic, and still has to land where the runtime
        // lands. UTC is the one the desugaring itself can emit, for a non-midnight
        // ANCHOR DAILY 'HH:MM' 'UTC'.
        assertMemoryLeak(() -> assertSegmentMatchesRuntime(
                "09:30",
                "UTC",
                "2024-03-31T12:00:00.000000Z",
                "2024-03-31T09:30:00.000000Z",
                "2024-04-01T09:30:00.000000Z"
        ));
    }

    @Test
    public void testAnOriginInsideTheSpringForwardGapKeepsTheSegmentOpenBelow() throws Exception {
        // 02:30 local does not exist on 31 March: the clocks jump from 02:00 to 03:00. The
        // runtime floors 03:00 local back onto the missing 02:30 anyway and converts it to
        // 01:30Z, so rows from the transition instant onward all carry that anchor - the
        // floor lands ABOVE some of the rows it describes.
        assertMemoryLeak(() -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE);
            final long inGapDay = ts("2024-03-31T10:00:00.000000Z");
            final long transition = ts("2024-03-31T01:00:00.000000Z");
            final long naiveStart = ts("2024-03-31T01:30:00.000000Z");

            // The runtime's own answer, and the reason the naive floor is not a floor: the
            // row at the transition instant sits below it and carries it all the same.
            Assert.assertEquals(naiveStart, runtimeAnchor("02:30", ZONE, inGapDay));
            Assert.assertEquals(naiveStart, runtimeAnchor("02:30", ZONE, transition));
            Assert.assertTrue(transition < naiveStart);

            // So the plan reports the segment open below rather than a floor that would
            // leave the transition row outside a repair bounded by it.
            Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(inGapDay));
        });
    }

    @Test
    public void testAnOriginInsideTheSpringForwardGapReportsNoEndForTheDayBelowIt() throws Exception {
        // The other half of the same hole, one day lower. Advancing the local grid names
        // 2024-03-31T02:30 as the next boundary, and the local time it names never happens,
        // so the anchor does not change there: a row half an hour past that "end" still
        // carries the segment's own value. An H below the true one would cut the segment
        // short, so the plan reports none.
        assertMemoryLeak(() -> {
            final LiveViewCheckpointAnchorPlan plan = plan("02:30", ZONE);
            final long inSegment = ts("2024-03-30T10:00:00.000000Z");
            final long segmentStart = ts("2024-03-30T01:30:00.000000Z");
            final long naiveEnd = ts("2024-03-31T00:30:00.000000Z");

            Assert.assertEquals(segmentStart, plan.getSegmentStart(inSegment));
            Assert.assertEquals(segmentStart, runtimeAnchor("02:30", ZONE, inSegment));
            // Still the same anchor at the boundary the arithmetic would have claimed.
            Assert.assertEquals(segmentStart, runtimeAnchor("02:30", ZONE, naiveEnd));
            Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(inSegment));
        });
    }

    @Test
    public void testAnOriginInsideTheSpringForwardGapIsOrdinaryAwayFromIt() throws Exception {
        // The same anchor on a day the zone leaves alone. The declines above are scoped to
        // the transition, not to the anchor that straddles it.
        assertMemoryLeak(() -> assertSegmentMatchesRuntime(
                "02:30",
                ZONE,
                "2024-06-15T12:00:00.000000Z",
                "2024-06-15T00:30:00.000000Z",
                "2024-06-16T00:30:00.000000Z"
        ));
    }

    @Test
    public void testAnUnresolvableZoneDeclines() {
        Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone(
                'd', 1, 0, ColumnType.TIMESTAMP_MICRO, "Nowhere/Atlantis"));
        // And the ordinary input checks still apply to the zone-aware factory.
        Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone('d', 0, 0, ColumnType.TIMESTAMP_MICRO, ZONE));
        Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone('z', 1, 0, ColumnType.TIMESTAMP_MICRO, ZONE));
        Assert.assertNull(LiveViewCheckpointAnchorPlan.ofTimeZone('d', 1, 0, ColumnType.LONG, ZONE));
    }

    @Test
    public void testMidnightSegmentIsTwentyFiveHoursWideAcrossFallBack() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointAnchorPlan plan = assertSegmentMatchesRuntime(
                    "00:00",
                    ZONE,
                    "2024-10-27T10:00:00.000000Z",
                    "2024-10-26T22:00:00.000000Z",
                    "2024-10-27T23:00:00.000000Z"
            );
            Assert.assertEquals(
                    25 * Micros.HOUR_MICROS,
                    plan.getSegmentEndExclusive(ts("2024-10-27T10:00:00.000000Z"))
                            - plan.getSegmentStart(ts("2024-10-27T10:00:00.000000Z"))
            );
        });
    }

    @Test
    public void testMidnightSegmentIsTwentyThreeHoursWideAcrossSpringForward() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointAnchorPlan plan = assertSegmentMatchesRuntime(
                    "00:00",
                    ZONE,
                    "2024-03-31T10:00:00.000000Z",
                    "2024-03-30T23:00:00.000000Z",
                    "2024-03-31T22:00:00.000000Z"
            );
            Assert.assertEquals(
                    23 * Micros.HOUR_MICROS,
                    plan.getSegmentEndExclusive(ts("2024-03-31T10:00:00.000000Z"))
                            - plan.getSegmentStart(ts("2024-03-31T10:00:00.000000Z"))
            );
            // The day below it is an ordinary 24 hours, so the width above is the zone's
            // and not the plan's.
            Assert.assertEquals(
                    24 * Micros.HOUR_MICROS,
                    plan.getSegmentEndExclusive(ts("2024-03-30T10:00:00.000000Z"))
                            - plan.getSegmentStart(ts("2024-03-30T10:00:00.000000Z"))
            );
        });
    }

    @Test
    public void testNonMidnightOriginFloorsOnTheLocalGridRatherThanTheEpoch() throws Exception {
        // The origin is a modulus seed for the LOCAL grid, not a UTC instant. A plan that
        // dropped it - or applied it in UTC space - would floor 2024-10-26T18:00Z to the
        // previous local midnight at 2024-10-25T22:00Z, which re-floors to itself just as
        // consistently. Only the runtime can tell the two grids apart.
        assertMemoryLeak(() -> {
            final LiveViewCheckpointAnchorPlan plan = assertSegmentMatchesRuntime(
                    "12:00",
                    ZONE,
                    "2024-10-26T18:00:00.000000Z",
                    "2024-10-26T10:00:00.000000Z",
                    "2024-10-27T11:00:00.000000Z"
            );
            Assert.assertNotEquals(
                    ts("2024-10-25T22:00:00.000000Z"),
                    plan.getSegmentStart(ts("2024-10-26T18:00:00.000000Z"))
            );
        });
    }

    @Test
    public void testNonMidnightOriginTracksBothTransitions() throws Exception {
        assertMemoryLeak(() -> {
            // Spring forward falls inside a 12:00-anchored segment, so it is 23 hours wide.
            assertSegmentMatchesRuntime(
                    "12:00",
                    ZONE,
                    "2024-03-30T20:00:00.000000Z",
                    "2024-03-30T11:00:00.000000Z",
                    "2024-03-31T10:00:00.000000Z"
            );
            // And the segment above it is back to 24.
            assertSegmentMatchesRuntime(
                    "12:00",
                    ZONE,
                    "2024-03-31T20:00:00.000000Z",
                    "2024-03-31T10:00:00.000000Z",
                    "2024-04-01T10:00:00.000000Z"
            );
        });
    }

    @Test
    public void testTheTopmostSegmentHasNoRepresentableEnd() {
        final LiveViewCheckpointAnchorPlan plan = plan("00:00", ZONE);
        Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(Long.MAX_VALUE));
        Assert.assertEquals(Long.MIN_VALUE, plan.getSegmentStart(Long.MAX_VALUE));
        // LONG_NULL is Long.MIN_VALUE, which is what an absent change bound reaches the
        // plan as; it has no segment either.
        Assert.assertEquals(Numbers.LONG_NULL, plan.getSegmentEndExclusive(Numbers.LONG_NULL));
    }

    private static LiveViewCheckpointAnchorPlan plan(String anchorTime, String zone) {
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.ofTimeZone(
                'd',
                1,
                ts("1970-01-01T" + anchorTime + ":00.000000Z"),
                ColumnType.TIMESTAMP_MICRO,
                zone
        );
        Assert.assertNotNull("ANCHOR DAILY '" + anchorTime + "' '" + zone + "' must carry a segment", plan);
        return plan;
    }

    /**
     * Asserts the plan's two bounds for {@code instant}, then asserts the runtime anchor
     * agrees that those bounds are where its own value changes: it holds the segment start
     * at the start, still holds it one microsecond below the end, and has moved on at the
     * end itself.
     */
    private LiveViewCheckpointAnchorPlan assertSegmentMatchesRuntime(
            String anchorTime,
            String zone,
            String instant,
            String expectedStart,
            String expectedEnd
    ) throws SqlException {
        final LiveViewCheckpointAnchorPlan plan = plan(anchorTime, zone);
        final long timestamp = ts(instant);
        final long start = ts(expectedStart);
        final long end = ts(expectedEnd);
        Assert.assertEquals("segment start at " + instant, start, plan.getSegmentStart(timestamp));
        Assert.assertEquals("segment end at " + instant, end, plan.getSegmentEndExclusive(timestamp));

        Assert.assertEquals("runtime anchor at " + instant, start, runtimeAnchor(anchorTime, zone, timestamp));
        Assert.assertEquals("runtime anchor at the segment start", start, runtimeAnchor(anchorTime, zone, start));
        Assert.assertEquals(
                "the runtime must still hold the segment one microsecond below its end",
                start,
                runtimeAnchor(anchorTime, zone, end - 1)
        );
        Assert.assertEquals(
                "and must have reset at the end itself",
                end,
                runtimeAnchor(anchorTime, zone, end)
        );
        return plan;
    }

    /**
     * The anchor value the runtime computes for one instant, through the very function the
     * desugared {@code ANCHOR DAILY 'HH:MM' '<zone>'} expression carries.
     */
    private long runtimeAnchor(String anchorTime, String zone, long timestamp) throws SqlException {
        final String sql = "SELECT timestamp_floor_utc('1d', " + timestamp + "::timestamp, '1970-01-01T"
                + anchorTime + ":00.000000Z'::timestamp, '+00:00', '" + zone + "')";
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(sql, cursor.hasNext());
            return cursor.getRecord().getTimestamp(0);
        }
    }
}
