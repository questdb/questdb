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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The {@code live_views()} reading a time-zone-anchored view gives an operator on a running
 * server.
 * <p>
 * {@link LiveViewCustomerShapeGuardTest} pins the same two flips off {@code LiveViewInstance}
 * under a hand-driven {@link io.questdb.cairo.lv.LiveViewRefreshJob} and a fixed clock, which
 * is where the bounds are proven. What it cannot say is what a person reads: the fixture never
 * projects a column, never starts a refresh worker and never lets wall time decide when a
 * flush lands. This case runs a real {@code ServerMain} - its own refresh pool, its own clock,
 * its own WAL apply - and takes every reading over PGWire, which is the shape the soak protocol
 * asks an operator for.
 * <p>
 * Two readings, of very different kinds:
 * <ul>
 *     <li><b>The preflight gate</b>, which is a property of the view's SELECT and settles as
 *     soon as the view compiles. A zoned daily anchor used to leave
 *     {@code checkpoint_segment_repair_gate} reading {@code incomplete dependency} for the life
 *     of the view - the compiler recognized no plan, so no route derived from a segment bound
 *     was ever available to it. It reads {@code available} now, and reads it beside the
 *     no-zone view on the same base as the control.</li>
 *     <li><b>The settled repair</b>, which needs a trickle of corrections and a checkpoint
 *     ladder under them. {@code checkpoint_repair_last_disposition} has to settle on
 *     {@code resume from anchor}, and {@code checkpoint_repair_last_denial} must never name
 *     {@code incomplete dependency} - that denial was compile-time, so a single sighting of it
 *     at any pass is the gap reopening rather than a timing artefact.</li>
 * </ul>
 * The zone is a neutral DST-observing one, and the repair readings above depend only on its
 * rules carrying transitions at all rather than on which zone it is.
 * <p>
 * {@link #testTheZoneAnchoredViewSurvivesBothDaylightSavingTransitions()} is the third reading
 * and the one that does pin the zone: it drives base rows across both of Europe/Berlin's 2026
 * transitions, so the view is built, corrected and repaired over a 23-hour civil day and a
 * 25-hour one rather than over the fixed-width days every other case here uses.
 * <p>
 * {@link #testANonMidnightZoneAnchorSurvivesBothDaylightSavingTransitions()} is the fourth, and
 * the only one whose anchor is not midnight. A midnight zone floor is monotone through both
 * transitions, so it never trips a segment self-check; {@code ANCHOR DAILY '02:30'} does, on
 * both days, which is what puts the repair on the union-range fallback behind the refusal.
 * {@link #anchorTime} is what sets it apart from the readings above - the field is
 * initialized to {@link #ANCHOR_TIME}, which every other case here keeps, and only this one
 * assigns over it.
 */
public class LiveViewTimeZoneAnchorServerTest extends AbstractBootstrapTest {

    private static final int ACCOUNT_COUNT = 8;
    // The anchor every case here used before a non-midnight one was added, and the value
    // anchorTime is initialized to. Midnight is the only wall time whose zone floor is
    // monotone through both transitions, so it is also the only one whose segment
    // self-checks never refuse.
    private static final String ANCHOR_TIME = "00:00";
    private static final String ANCHOR_ZONE = "Europe/Berlin";
    private static final String BASE = "payments";
    // A root every 16 view rows, so a few hundred rows build a ladder a million-row cadence
    // would not. It is the one production value this case moves, and it is a cadence rather
    // than a price: every switch the repair route consults stays at its shipped default.
    private static final int CHECKPOINT_ROWS = 16;
    private static final int FIRST_HOUR = 0;
    private static final int HOURS = 10;
    private static final int ROWS_PER_ACCOUNT_PER_HOUR = 4;
    // The minute the last row of an hour lands on, since one row lands per minute from 1.
    private static final int HEAD_MINUTE = ROWS_PER_ACCOUNT_PER_HOUR * ACCOUNT_COUNT;
    // Corrections the trickle makes, each one minute deeper under the batch head so no pass
    // rests on a root its own predecessor sealed.
    private static final int TRICKLE_PASSES = 8;
    // Passes at the tail of the trickle that have to read the settled disposition. The
    // earlier ones are allowed the bootstrap rebuild a view whose ladder has not spliced yet
    // owes, which is unavoidable and unchanged by this fix.
    private static final int SETTLED_PASSES = 3;
    private static final String VIEW_PLAIN = "payments_view_plain";
    private static final String VIEW_ZONED = "payments_view_zoned";
    // Europe/Berlin in 2026 puts the clocks forward at 2026-03-29T01:00Z and back at
    // 2026-10-25T01:00Z, so the civil day starting 2026-03-28T23:00Z is 23 hours wide and the
    // one starting 2026-10-24T22:00Z is 25. Every instant below is pinned to one of those two
    // days or to the day either side of it - nothing reads the machine's clock, default zone
    // or locale.
    private static final ObjList<String> DST_INSTANTS = new ObjList<>();
    // The account the two hand-computed traces below follow. The window partitions by
    // account_id, so the corrections the trickle makes cannot move a single one of its values.
    private static final String TRACE_ACCOUNT = "acct-1";
    // The account every correction lands on, for the same reason. Its rows are the only ones
    // the traces do not describe.
    private static final int TRICKLE_ACCOUNT = ACCOUNT_COUNT;
    // The minute the first correction lands on; each pass goes one minute deeper, so no pass
    // rests on a root its own predecessor sealed.
    private static final int TRICKLE_FIRST_MINUTE = 30;
    // What the zone-anchored view holds for TRACE_ACCOUNT once the trickle has settled: the
    // accumulator resets at Berlin local midnight - an hour before UTC midnight under CET, two
    // under CEST - and does not reset at either transition instant. The 7 at 2026-03-29T21:30Z
    // is the 23-hour day read end to end, and the 7 at 2026-10-25T22:30Z the 25-hour one.
    private static final String ZONED_TRACE = """
            2026-03-28T21:00:00.000000Z\t1
            2026-03-28T22:30:00.000000Z\t2
            2026-03-28T23:00:00.000000Z\t1
            2026-03-28T23:30:00.000000Z\t2
            2026-03-29T00:30:00.000000Z\t3
            2026-03-29T00:59:00.000000Z\t4
            2026-03-29T01:00:00.000000Z\t5
            2026-03-29T12:00:00.000000Z\t6
            2026-03-29T21:30:00.000000Z\t7
            2026-03-29T22:00:00.000000Z\t1
            2026-03-29T23:30:00.000000Z\t2
            2026-03-30T01:00:00.000000Z\t3
            2026-10-24T20:00:00.000000Z\t1
            2026-10-24T21:30:00.000000Z\t2
            2026-10-24T22:00:00.000000Z\t1
            2026-10-24T23:30:00.000000Z\t2
            2026-10-25T00:30:00.000000Z\t3
            2026-10-25T00:59:00.000000Z\t4
            2026-10-25T01:00:00.000000Z\t5
            2026-10-25T12:00:00.000000Z\t6
            2026-10-25T22:30:00.000000Z\t7
            2026-10-25T23:00:00.000000Z\t1
            2026-10-26T00:30:00.000000Z\t2
            """;
    // The same rows off the no-zone control, which is the anti-oracle: it resets on the UTC
    // grid instead and so disagrees at 19 of the 23 rows. No UTC-grid computation can produce
    // ZONED_TRACE, and no zone-grid one can produce this.
    private static final String PLAIN_TRACE = """
            2026-03-28T21:00:00.000000Z\t1
            2026-03-28T22:30:00.000000Z\t2
            2026-03-28T23:00:00.000000Z\t3
            2026-03-28T23:30:00.000000Z\t4
            2026-03-29T00:30:00.000000Z\t1
            2026-03-29T00:59:00.000000Z\t2
            2026-03-29T01:00:00.000000Z\t3
            2026-03-29T12:00:00.000000Z\t4
            2026-03-29T21:30:00.000000Z\t5
            2026-03-29T22:00:00.000000Z\t6
            2026-03-29T23:30:00.000000Z\t7
            2026-03-30T01:00:00.000000Z\t1
            2026-10-24T20:00:00.000000Z\t1
            2026-10-24T21:30:00.000000Z\t2
            2026-10-24T22:00:00.000000Z\t3
            2026-10-24T23:30:00.000000Z\t4
            2026-10-25T00:30:00.000000Z\t1
            2026-10-25T00:59:00.000000Z\t2
            2026-10-25T01:00:00.000000Z\t3
            2026-10-25T12:00:00.000000Z\t4
            2026-10-25T22:30:00.000000Z\t5
            2026-10-25T23:00:00.000000Z\t6
            2026-10-26T00:30:00.000000Z\t1
            """;

    // The wall time the non-midnight case anchors on. 02:30 local does not exist on
    // Europe/Berlin's spring-forward day - the clocks jump from 02:00 to 03:00 - and it
    // happens twice on the fall-back day, which is what makes the zone floor non-monotone
    // there and the plan's segment self-checks refuse.
    private static final String NON_MIDNIGHT_ANCHOR_TIME = "02:30";
    // The three hours the non-midnight case trickles corrections into, one per verdict the
    // anchor plan can return for a segment. Every one of them is a property of the zone's
    // rules rather than of the data, and assertSegmentBounds() reads each off the plan
    // before the trickle that depends on it runs.
    //
    // The gap day, whose segment B = 2026-03-29T01:30Z opens ABOVE the 01:00Z row that
    // carries it, so the plan reports no start for it.
    private static final String NON_MIDNIGHT_GAP_DAY_HOUR = "2026-03-29T12:";
    // The day below the fall-back, whose segment D = 2026-10-24T00:30Z the repeated local
    // hour SPLITS: rows at [2026-10-25T01:00Z, 2026-10-25T01:30Z) read 02:00..02:29 CET,
    // below the day's own 02:30, and floor straight back into D - above the end the
    // arithmetic names. A repair bounded there would leave them standing, so the plan
    // reports no end for D. The fixture holds no row in that upper part, and the refusal
    // does not depend on one: it is read off the zone's transitions.
    private static final String NON_MIDNIGHT_SPLIT_DAY_HOUR = "2026-10-24T21:";
    // And an ordinary segment, C = 2026-03-30T00:30Z: the day directly above the gap, far
    // enough from either transition that both bounds hold. This is the control, and the
    // only one of the three that can settle on a localized rebuild.
    private static final String NON_MIDNIGHT_ORDINARY_HOUR = "2026-03-30T12:";
    // The instants the non-midnight case drives, each one pinned to a Europe/Berlin
    // 02:30-local segment below. Nothing here reads the machine's clock, default zone or
    // locale.
    private static final ObjList<String> NON_MIDNIGHT_DST_INSTANTS = new ObjList<>();
    // What the 02:30 'Europe/Berlin' view holds for TRACE_ACCOUNT. Hand-computed off the
    // zone's rules: the accumulator resets at Berlin local 02:30, which is 01:30Z under CET
    // and 00:30Z under CEST.
    //
    // The row at 2026-03-29T01:00Z is the one this whole case exists for. Berlin jumps from
    // 02:00 to 03:00 at that instant, so the row's own local time is 03:00 and its anchor
    // floors to the 02:30 that never happened - which the runtime resolves to 01:30Z, HALF
    // AN HOUR ABOVE THE ROW ITSELF. Its count restarting at 1 while the row sits below its
    // own segment start is what a repair floored at that start would drop.
    private static final String NON_MIDNIGHT_ZONED_TRACE = """
            2026-03-28T21:00:00.000000Z\t1
            2026-03-28T22:30:00.000000Z\t2
            2026-03-28T23:00:00.000000Z\t3
            2026-03-29T00:30:00.000000Z\t4
            2026-03-29T00:59:00.000000Z\t5
            2026-03-29T01:00:00.000000Z\t1
            2026-03-29T12:00:00.000000Z\t2
            2026-03-29T21:30:00.000000Z\t3
            2026-03-29T23:30:00.000000Z\t4
            2026-03-30T01:00:00.000000Z\t1
            2026-10-24T20:00:00.000000Z\t1
            2026-10-24T23:30:00.000000Z\t2
            2026-10-25T00:30:00.000000Z\t1
            2026-10-25T00:59:00.000000Z\t2
            2026-10-25T12:00:00.000000Z\t1
            2026-10-25T22:30:00.000000Z\t2
            2026-10-26T00:30:00.000000Z\t3
            """;
    // The same rows off the no-zone control, which resets on 02:30 UTC instead and so
    // disagrees at 7 of the 17 rows. The fall-back day is the sharpest of them: the zone
    // grid opens a new segment at 2026-10-25T00:30Z and another at 01:30Z, where the UTC
    // grid carries one boundary a day.
    private static final String NON_MIDNIGHT_PLAIN_TRACE = """
            2026-03-28T21:00:00.000000Z\t1
            2026-03-28T22:30:00.000000Z\t2
            2026-03-28T23:00:00.000000Z\t3
            2026-03-29T00:30:00.000000Z\t4
            2026-03-29T00:59:00.000000Z\t5
            2026-03-29T01:00:00.000000Z\t6
            2026-03-29T12:00:00.000000Z\t1
            2026-03-29T21:30:00.000000Z\t2
            2026-03-29T23:30:00.000000Z\t3
            2026-03-30T01:00:00.000000Z\t4
            2026-10-24T20:00:00.000000Z\t1
            2026-10-24T23:30:00.000000Z\t2
            2026-10-25T00:30:00.000000Z\t3
            2026-10-25T00:59:00.000000Z\t4
            2026-10-25T12:00:00.000000Z\t1
            2026-10-25T22:30:00.000000Z\t2
            2026-10-26T00:30:00.000000Z\t3
            """;
    static {
        // The day below the spring-forward, the 23-hour day itself, and the day above it. The
        // two rows at 23:00Z and 23:30Z are the head a UTC-grid segment start would drop, and
        // the row at 2026-03-29T22:00Z the first of the next civil day - two hours before the
        // UTC day it sits in ends.
        DST_INSTANTS.add("2026-03-28T21:00:00.000000Z");
        DST_INSTANTS.add("2026-03-28T22:30:00.000000Z");
        DST_INSTANTS.add("2026-03-28T23:00:00.000000Z");
        DST_INSTANTS.add("2026-03-28T23:30:00.000000Z");
        DST_INSTANTS.add("2026-03-29T00:30:00.000000Z");
        DST_INSTANTS.add("2026-03-29T00:59:00.000000Z");
        DST_INSTANTS.add("2026-03-29T01:00:00.000000Z");
        DST_INSTANTS.add("2026-03-29T12:00:00.000000Z");
        DST_INSTANTS.add("2026-03-29T21:30:00.000000Z");
        DST_INSTANTS.add("2026-03-29T22:00:00.000000Z");
        DST_INSTANTS.add("2026-03-29T23:30:00.000000Z");
        DST_INSTANTS.add("2026-03-30T01:00:00.000000Z");
        // And the same three around the fall-back. 2026-10-25T22:30Z is the twenty-fifth hour
        // of its civil day, which a fixed 24-hour stride would have handed to the next one.
        DST_INSTANTS.add("2026-10-24T20:00:00.000000Z");
        DST_INSTANTS.add("2026-10-24T21:30:00.000000Z");
        DST_INSTANTS.add("2026-10-24T22:00:00.000000Z");
        DST_INSTANTS.add("2026-10-24T23:30:00.000000Z");
        DST_INSTANTS.add("2026-10-25T00:30:00.000000Z");
        DST_INSTANTS.add("2026-10-25T00:59:00.000000Z");
        DST_INSTANTS.add("2026-10-25T01:00:00.000000Z");
        DST_INSTANTS.add("2026-10-25T12:00:00.000000Z");
        DST_INSTANTS.add("2026-10-25T22:30:00.000000Z");
        DST_INSTANTS.add("2026-10-25T23:00:00.000000Z");
        DST_INSTANTS.add("2026-10-26T00:30:00.000000Z");

        // The same two transitions read on the 02:30 local grid. The Berlin segments the
        // rows fall into, in order:
        //   A = 2026-03-28T01:30Z (02:30 CET), the day below the gap
        //   B = 2026-03-29T01:30Z, the gap day - its 02:30 local never happens, and the
        //       runtime resolves the floor to an instant ABOVE the 01:00Z row that carries it
        //   C = 2026-03-30T00:30Z (02:30 CEST)
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-28T21:00:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-28T22:30:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-28T23:00:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-29T00:30:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-29T00:59:00.000000Z");
        // The transition instant itself, and the first row of segment B.
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-29T01:00:00.000000Z");
        // Midday of the gap day, which is where the corrections land: well inside B and a
        // long way below the frontier, so the decomposition has to place it in a closed
        // segment - and B is the segment whose start the plan refuses.
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-29T12:00:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-29T21:30:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-29T23:30:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-03-30T01:00:00.000000Z");
        // And the fall-back, where 02:30 local happens twice:
        //   D = 2026-10-24T00:30Z (02:30 CEST), the segment the second trickle corrects
        //   E1 = 2026-10-25T00:30Z, the first 02:30 local, under CEST - a segment the plan
        //        gives a start for and refuses an end for
        //   E2 = 2026-10-25T01:30Z, the second 02:30 local, under CET
        NON_MIDNIGHT_DST_INSTANTS.add("2026-10-24T20:00:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-10-24T23:30:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-10-25T00:30:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-10-25T00:59:00.000000Z");
        // 2026-10-25T01:00Z is deliberately absent here, though DST_INSTANTS above carries
        // it. On the 02:30 'Europe/Berlin' grid that row reads 02:00 local - Berlin has just
        // fallen back - so it floors to the PREVIOUS civil day's 02:30, below the anchor the
        // 00:30Z and 00:59Z rows just above already floor to. The anchor would decrease as
        // the timestamp increases, and LiveViewWindow.processRow resets the accumulator on
        // any anchor change rather than only on an increase, so the streaming view and an
        // equivalent batch recompute may well part ways on such a row. Whether they do is a
        // separate question, and not one this case is scoped to answer: the fixture stays
        // monotone in the anchor so that a failure here can only be the chain it exists to
        // prove - the DST refusal falling back to the union range.
        NON_MIDNIGHT_DST_INSTANTS.add("2026-10-25T12:00:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-10-25T22:30:00.000000Z");
        NON_MIDNIGHT_DST_INSTANTS.add("2026-10-26T00:30:00.000000Z");
    }

    // The anchor time the running case builds its views on. JUnit builds a fresh instance
    // of this class per case, so this initializer runs for each of them and every case that
    // does not assign over it keeps the midnight coverage it had before the non-midnight
    // one existed.
    private String anchorTime = ANCHOR_TIME;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    @Test
    public void testARunningServerReportsTheZoneAnchoredViewsRepairAsLocalized() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                Assert.assertTrue(
                        "the server must be running the live view refresh pool this case reads",
                        serverMain.getEngine().getConfiguration().isLiveViewRefreshEnabled()
                );
                try (Connection conn = getConnection("admin", "quest", PG_PORT)) {
                    execute(conn, "CREATE TABLE " + BASE + " ("
                            + "created_at TIMESTAMP, "
                            + "account_id SYMBOL NOCACHE INDEX CAPACITY 4, "
                            + "amount DOUBLE"
                            + ") TIMESTAMP(created_at) PARTITION BY HOUR WAL");
                    execute(conn, "INSERT INTO " + BASE + " VALUES " + hourRows(FIRST_HOUR));
                    awaitRowCount(conn, BASE, rowsPerHour());

                    // The view under test, and the plain UTC-wall-time anchor as its control.
                    // Both read the same base and are refreshed by the same pool, so a
                    // difference between the two readings below is the zone and nothing else.
                    execute(conn, createLiveView(VIEW_ZONED, ANCHOR_ZONE));
                    execute(conn, createLiveView(VIEW_PLAIN, null));
                    awaitRowCount(conn, VIEW_ZONED, rowsPerHour());
                    awaitRowCount(conn, VIEW_PLAIN, rowsPerHour());

                    // Reading one: the preflight, which the view's SELECT alone decides. The
                    // zoned anchor desugars to timestamp_floor_utc, whose buckets are 23 or
                    // 25 hours wide across a transition - the shape that used to leave the
                    // planner with no segment bound at all, and the gate naming the missing
                    // dependency for the life of the view.
                    assertGates(conn, VIEW_ZONED, "available", "available");
                    assertGates(conn, VIEW_PLAIN, "available", "available");

                    // The rest of the anchor day, one commit per hourly base partition, so
                    // the cadence seals roots inside the segment and the refresh frontier
                    // ends up standing in it.
                    for (int hour = FIRST_HOUR + 1; hour < FIRST_HOUR + HOURS; hour++) {
                        execute(conn, "INSERT INTO " + BASE + " VALUES " + hourRows(hour));
                        awaitRowCount(conn, BASE, (long) rowsPerHour() * (hour - FIRST_HOUR + 1));
                    }
                    long baseRows = (long) rowsPerHour() * HOURS;
                    awaitRowCount(conn, VIEW_ZONED, baseRows);
                    awaitRowCount(conn, VIEW_PLAIN, baseRows);

                    // Reading two: the trickle. Each correction lands a few minutes under the
                    // head of the last batch, which is the reported symptom's own shape - a
                    // row a minute or two late, forever, with no sealed root below it.
                    final StringSink readings = new StringSink();
                    // The counters the settled window opened on, so what follows measures the
                    // window rather than the whole run.
                    long rebuiltAtSettle = -1;
                    long scannedAtSettle = -1;
                    int settled = 0;
                    for (int pass = 1; pass <= TRICKLE_PASSES; pass++) {
                        execute(conn, "INSERT INTO " + BASE + " VALUES "
                                + row(FIRST_HOUR + HOURS - 1, HEAD_MINUTE - pass, 1));
                        baseRows++;
                        awaitRowCount(conn, BASE, baseRows);
                        awaitRowCount(conn, VIEW_ZONED, baseRows);
                        awaitRowCount(conn, VIEW_PLAIN, baseRows);

                        final Reading zoned = read(conn, VIEW_ZONED);
                        final Reading plain = read(conn, VIEW_PLAIN);
                        readings.put("pass ").put(pass)
                                .put(": zoned=").put(zoned.toString())
                                .put(" plain=").put(plain.toString()).put('\n');

                        // The denial that used to be reported on every repair, forever. It is
                        // a compile-time property of the anchor's shape, so one sighting at
                        // any pass is the gap rather than a slow ladder.
                        Assert.assertNotEquals(
                                "the zone anchor's dependency must stay covered, readings:\n" + readings,
                                "incomplete dependency",
                                zoned.denial
                        );
                        // The 2026-07-31 incident's own symptom was a view that went
                        // invalid; neither of these may leave the healthy state.
                        Assert.assertEquals("active", zoned.status);
                        Assert.assertEquals("active", plain.status);

                        if ("resume from anchor".equals(zoned.disposition)) {
                            if (settled == 0) {
                                rebuiltAtSettle = zoned.boundaryReplayRows;
                                scannedAtSettle = zoned.replayScanRows;
                            }
                            settled++;
                        } else {
                            settled = 0;
                        }
                        if (settled == SETTLED_PASSES) {
                            break;
                        }
                    }
                    Assert.assertEquals(
                            "the zone-anchored view's repair must settle on a localized resume, readings:\n"
                                    + readings,
                            SETTLED_PASSES,
                            settled
                    );

                    final Reading zoned = read(conn, VIEW_ZONED);
                    final Reading plain = read(conn, VIEW_PLAIN);
                    // The control settles the same way. That is the second half of the claim:
                    // the zoned shape costs what the plain shape costs at the reading an
                    // operator takes, rather than merely costing less than it used to.
                    Assert.assertEquals(
                            "the no-zone control must settle the same way, readings:\n" + readings,
                            zoned.disposition,
                            plain.disposition
                    );
                    Assert.assertTrue(
                            "a localized resume must have replayed rows, readings:\n" + readings,
                            zoned.resumeReplayRows > 0
                    );
                    // No rebuild ran across the settled window, so nothing below re-read the
                    // segment: the boundary-replay counter has not moved.
                    Assert.assertEquals(
                            "the settled passes must take the resume, not the rebuild, readings:\n" + readings,
                            rebuiltAtSettle,
                            zoned.boundaryReplayRows
                    );
                    // And the scan is the tail above the newest root rather than the view. The
                    // cadence seals a root every CHECKPOINT_ROWS view rows, so each pass reads
                    // that tail plus the rows the earlier passes inserted into it - a bound the
                    // view's own size does not enter.
                    final long scanned = zoned.replayScanRows - scannedAtSettle;
                    Assert.assertTrue(
                            "the settled passes scanned " + scanned + " of a " + baseRows
                                    + "-row view, expected the tail above the anchor, readings:\n" + readings,
                            scanned <= (long) SETTLED_PASSES * (CHECKPOINT_ROWS + TRICKLE_PASSES)
                    );

                    // And the localization did not cost a row: both views still equal a
                    // from-base recompute over their own anchor.
                    assertMatchesRecompute(conn, VIEW_ZONED, ANCHOR_ZONE);
                    assertMatchesRecompute(conn, VIEW_PLAIN, null);
                }
            }
        });
    }

    /**
     * The zone-anchored view driven end to end across both of Europe/Berlin's 2026 daylight
     * saving transitions, which is the shape no other case here reaches: every fixture in this
     * package that names a zone puts its rows in January, where a civil day is 24 hours wide
     * and a UTC-grid computation and a zone-grid one agree row for row.
     * <p>
     * Two claims, and they answer different failures:
     * <ul>
     *     <li><b>The rows.</b> {@link #ZONED_TRACE} is hand-computed from the zone's own rules
     *     and pins a 23-hour civil day and a 25-hour one read end to end; {@link #PLAIN_TRACE}
     *     is the same account off the no-zone control and disagrees at 19 of its 23 rows, so
     *     neither trace can be produced on the other's grid. {@code assertMatchesRecompute}
     *     then holds the whole view - every account, corrections included - against a from-base
     *     recompute over the same floor. A repair that bounded itself on the UTC grid rather
     *     than the zone's would start above the head of a civil day and drop the state that
     *     head carries, and the counts here are what catches it.</li>
     *     <li><b>The route.</b> Correct rows alone do not prove the anchor did anything: the
     *     conservative fallback - no plan, no segment bound, a rebuild from the view's own
     *     boundary - produces exactly the same rows, only slower. So the preflight gate, the
     *     settled disposition and the denial are read beside them, which is what separates a
     *     localized repair over a transition day from a fallback that never consulted the zone
     *     at all.</li>
     * </ul>
     */
    @Test
    public void testTheZoneAnchoredViewSurvivesBothDaylightSavingTransitions() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                Assert.assertTrue(
                        "the server must be running the live view refresh pool this case reads",
                        serverMain.getEngine().getConfiguration().isLiveViewRefreshEnabled()
                );
                try (Connection conn = getConnection("admin", "quest", PG_PORT)) {
                    execute(conn, "CREATE TABLE " + BASE + " ("
                            + "created_at TIMESTAMP, "
                            + "account_id SYMBOL NOCACHE INDEX CAPACITY 4, "
                            + "amount DOUBLE"
                            + ") TIMESTAMP(created_at) PARTITION BY HOUR WAL");
                    execute(conn, "INSERT INTO " + BASE + " VALUES " + accountRows(DST_INSTANTS.getQuick(0)));
                    long baseRows = ACCOUNT_COUNT;
                    awaitRowCount(conn, BASE, baseRows);

                    execute(conn, createLiveView(VIEW_ZONED, ANCHOR_ZONE));
                    execute(conn, createLiveView(VIEW_PLAIN, null));
                    awaitRowCount(conn, VIEW_ZONED, baseRows);
                    awaitRowCount(conn, VIEW_PLAIN, baseRows);
                    assertGates(conn, VIEW_ZONED, "available", "available");
                    assertGates(conn, VIEW_PLAIN, "available", "available");

                    // One commit per instant, so the cadence seals roots between the rows a
                    // civil day straddles rather than under the whole day at once.
                    for (int i = 1, n = DST_INSTANTS.size(); i < n; i++) {
                        execute(conn, "INSERT INTO " + BASE + " VALUES " + accountRows(DST_INSTANTS.getQuick(i)));
                        baseRows += ACCOUNT_COUNT;
                        awaitRowCount(conn, BASE, baseRows);
                    }
                    awaitRowCount(conn, VIEW_ZONED, baseRows);
                    awaitRowCount(conn, VIEW_PLAIN, baseRows);

                    // Corrections in the middle of one transition day at a time, one minute
                    // deeper each pass. Midday is well above the day's own start and well below
                    // its end, which is exactly where a segment bound taken off the UTC grid
                    // rather than the zone's would place its floor an hour too high on the
                    // spring-forward day and two hours too high on the fall-back one. The two
                    // days run as separate phases so each repair localizes inside one of them
                    // rather than inside an interval spanning both.
                    final StringSink readings = new StringSink();
                    // A midnight local boundary never falls inside the hour a fall-back
                    // repeats, so both civil days carry both bounds and both trickles may
                    // ask for the localized route. Read off the plan rather than assumed,
                    // for the same reason the non-midnight case reads it.
                    assertSegmentBounds("2026-03-29T12:", true, true);
                    assertSegmentBounds("2026-10-25T12:", true, true);
                    baseRows = trickleUntilLocalized(conn, "2026-03-29T12:", baseRows, readings);
                    baseRows = trickleUntilLocalized(conn, "2026-10-25T12:", baseRows, readings);

                    // The rows, hand-computed off the zone's rules rather than off a second
                    // copy of the server's arithmetic.
                    Assert.assertEquals(
                            "the zone-anchored view must reset on Berlin local midnight, readings:\n" + readings,
                            ZONED_TRACE,
                            trace(conn, VIEW_ZONED)
                    );
                    Assert.assertEquals(
                            "the no-zone control must reset on UTC midnight, readings:\n" + readings,
                            PLAIN_TRACE,
                            trace(conn, VIEW_PLAIN)
                    );

                    // And every other account, corrections included, against a from-base
                    // recompute over the same floor.
                    assertMatchesRecompute(conn, VIEW_ZONED, ANCHOR_ZONE);
                    assertMatchesRecompute(conn, VIEW_PLAIN, null);
                }
            }
        });
    }

    /**
     * The same two transitions driven end to end on a NON-MIDNIGHT zoned anchor, which is
     * the only anchor shape whose segment self-checks refuse - and so the only one that
     * reaches the union-range fallback behind them.
     * <p>
     * {@code ANCHOR DAILY '02:30' 'Europe/Berlin'} names a wall time the zone deletes on the
     * spring-forward day and repeats on the fall-back day, and the runtime's own
     * {@code timestamp_floor_utc} is not monotone through either. Two consequences, and both
     * are in the data below:
     * <ul>
     *     <li><b>A row below its own segment start.</b> Berlin jumps from 02:00 to 03:00 at
     *     2026-03-29T01:00Z, so the row landing on that instant reads 03:00 local, floors to
     *     the 02:30 that never happened, and carries an anchor of 2026-03-29T01:30Z - half an
     *     hour ABOVE itself. {@code LiveViewCheckpointAnchorPlan.getSegmentStart} refuses that
     *     start rather than reporting it, because a repair floored at it would recompute the
     *     segment without the row.</li>
     *     <li><b>A segment with a start and no end.</b> 02:30 local happens twice on
     *     2026-10-25, and the repeated hour winds local time back INTO the day below it:
     *     rows at [2026-10-25T01:00Z, 2026-10-25T01:30Z) read 02:00..02:29 CET and floor
     *     onto 2026-10-24's own 02:30, above the end the arithmetic names for it. So
     *     {@code getSegmentEndExclusive} refuses the end of the segment that opens at
     *     2026-10-24T00:30Z as well as the one that opens at the first of the two 02:30s.</li>
     * </ul>
     * A refusal makes {@code LiveViewCheckpointSegmentChangeSet.addRow} give up on the whole
     * decomposition, and the repair falls back to the union range it took before segments
     * existed. That chain has three links, each with a unit test of its own; what has never
     * been driven is the conjunction, on a view that actually refreshes and repairs.
     * <p>
     * So the corrections trickle into the middle of the gap day - deep below the frontier,
     * which is what forces the decomposition to place them in a closed segment and to consult
     * the plan for its bounds - and the readings are the two hand-computed traces plus
     * {@code assertMatchesRecompute}. The traces are computed off the zone's rules rather than
     * off a second copy of the server's arithmetic, and the recompute holds every account,
     * corrections included, against a from-base window over the same floor. Between them they
     * are what catches a repair that dropped the 01:00Z row or counted a correction twice: the
     * fallback delivers the same rows the decomposition would, only over a wider range, so
     * nothing but the rows can tell a working fallback from a broken one.
     * <p>
     * Three trickles run, one per verdict the plan can return, and
     * {@link #assertSegmentBounds(String, boolean, boolean)} reads that verdict off the plan
     * before each of them rather than assuming it:
     * <ul>
     *     <li>{@link #NON_MIDNIGHT_GAP_DAY_HOUR} - a segment with no start;</li>
     *     <li>{@link #NON_MIDNIGHT_SPLIT_DAY_HOUR} - a segment with no end, the one the
     *     fall-back splits;</li>
     *     <li>{@link #NON_MIDNIGHT_ORDINARY_HOUR} - a segment carrying both bounds, which
     *     has to settle on a localized rebuild.</li>
     * </ul>
     * The third is the control, and it is why the first two prove anything: it says the
     * refusals are scoped to the transitions rather than being this view declining to
     * decompose at all, which would leave the other two trickles standing on nothing. It
     * corrects 2026-03-30, the day directly above the gap - close enough to the transition
     * to make the point, far enough for both bounds to hold.
     * <p>
     * The control's segment is chosen off the plan's verdict rather than off the calendar,
     * and the premise assertion is what keeps it that way. A route verdict a live server
     * reports depends on which repair route ran, and every route but the localized one is
     * reachable for a segment the plan refuses - so pointing the control at a refused
     * segment does not fail, it flakes. That happened: the control sat on 2026-10-24 while
     * that segment carried both bounds, and the fall-back split check that later took its
     * end away turned the assertion into a race on whether the checkpoint ladder held a root
     * below the correction. Reading the bounds first turns the next such change into a
     * failure that names the bound it moved.
     */
    @Test
    public void testANonMidnightZoneAnchorSurvivesBothDaylightSavingTransitions() throws Exception {
        anchorTime = NON_MIDNIGHT_ANCHOR_TIME;
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                Assert.assertTrue(
                        "the server must be running the live view refresh pool this case reads",
                        serverMain.getEngine().getConfiguration().isLiveViewRefreshEnabled()
                );
                try (Connection conn = getConnection("admin", "quest", PG_PORT)) {
                    execute(conn, "CREATE TABLE " + BASE + " ("
                            + "created_at TIMESTAMP, "
                            + "account_id SYMBOL NOCACHE INDEX CAPACITY 4, "
                            + "amount DOUBLE"
                            + ") TIMESTAMP(created_at) PARTITION BY HOUR WAL");
                    execute(conn, "INSERT INTO " + BASE + " VALUES "
                            + accountRows(NON_MIDNIGHT_DST_INSTANTS.getQuick(0)));
                    long baseRows = ACCOUNT_COUNT;
                    awaitRowCount(conn, BASE, baseRows);

                    execute(conn, createLiveView(VIEW_ZONED, ANCHOR_ZONE));
                    execute(conn, createLiveView(VIEW_PLAIN, null));
                    awaitRowCount(conn, VIEW_ZONED, baseRows);
                    awaitRowCount(conn, VIEW_PLAIN, baseRows);
                    // A non-midnight zoned anchor still compiles to a segment plan. Without
                    // this the refusals below would be indistinguishable from a view that
                    // never had a plan to refuse with, and the whole case would be vacuous.
                    assertGates(conn, VIEW_ZONED, "available", "available");
                    assertGates(conn, VIEW_PLAIN, "available", "available");

                    // One commit per instant, so the cadence seals roots between the rows a
                    // segment straddles rather than under a whole segment at once.
                    for (int i = 1, n = NON_MIDNIGHT_DST_INSTANTS.size(); i < n; i++) {
                        execute(conn, "INSERT INTO " + BASE + " VALUES "
                                + accountRows(NON_MIDNIGHT_DST_INSTANTS.getQuick(i)));
                        baseRows += ACCOUNT_COUNT;
                        awaitRowCount(conn, BASE, baseRows);
                    }
                    awaitRowCount(conn, VIEW_ZONED, baseRows);
                    awaitRowCount(conn, VIEW_PLAIN, baseRows);

                    // What each trickle below stands on, read off the plan itself. These
                    // are pure arithmetic over the zone table - the same answer on every
                    // run - while the route a live server takes for the same correction is
                    // not, so the premise is asserted where it is deterministic.
                    assertSegmentBounds(NON_MIDNIGHT_GAP_DAY_HOUR, false, true);
                    assertSegmentBounds(NON_MIDNIGHT_SPLIT_DAY_HOUR, true, false);
                    assertSegmentBounds(NON_MIDNIGHT_ORDINARY_HOUR, true, true);

                    final StringSink readings = new StringSink();
                    // Corrections inside the gap day's own segment, whose start the plan
                    // refuses. Each one is a closed-segment row the decomposition cannot
                    // describe, so every pass here takes the union range.
                    baseRows = trickleAcrossRefusedSegment(conn, NON_MIDNIGHT_GAP_DAY_HOUR, baseRows, readings);
                    // And the same from the other side of the year, on the segment whose END
                    // the fall-back split takes away. Nothing else drives that refusal on a
                    // running server.
                    baseRows = trickleAcrossRefusedSegment(conn, NON_MIDNIGHT_SPLIT_DAY_HOUR, baseRows, readings);
                    // Then the control, on a segment the plan carries both bounds for.
                    baseRows = trickleUntilLocalized(conn, NON_MIDNIGHT_ORDINARY_HOUR, baseRows, readings);

                    // The rows, hand-computed off the zone's rules. The 1 at 2026-03-29T01:00Z
                    // beside the 2 at 12:00Z is the pair a repair floored on the refused start
                    // would break: it would recompute the segment from 01:30Z, leaving the
                    // 01:00Z row out of every count above it.
                    Assert.assertEquals(
                            "the 02:30 zone-anchored view must reset on Berlin local 02:30, readings:\n"
                                    + readings,
                            NON_MIDNIGHT_ZONED_TRACE,
                            trace(conn, VIEW_ZONED)
                    );
                    Assert.assertEquals(
                            "the no-zone control must reset on 02:30 UTC, readings:\n" + readings,
                            NON_MIDNIGHT_PLAIN_TRACE,
                            trace(conn, VIEW_PLAIN)
                    );

                    // And every other account, corrections included, against a from-base
                    // recompute over the same floor. This is the assertion that reads the
                    // corrected account, which is the one a broken fallback moves.
                    assertMatchesRecompute(conn, VIEW_ZONED, ANCHOR_ZONE);
                    assertMatchesRecompute(conn, VIEW_PLAIN, null);
                }
            }
        });
    }

    private static void assertGates(
            Connection conn,
            String view,
            String expectedSegmentGate,
            String expectedKeyedScanGate
    ) throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT checkpoint_segment_repair_gate, checkpoint_keyed_scan_gate"
                                + " FROM live_views() WHERE view_name = '" + view + "'"
                )
        ) {
            Assert.assertTrue("live_views() must carry a row for '" + view + "'", rs.next());
            Assert.assertEquals(view + " segment repair gate", expectedSegmentGate, rs.getString(1));
            Assert.assertEquals(view + " keyed scan gate", expectedKeyedScanGate, rs.getString(2));
        }
    }

    /**
     * Polls until {@code table} holds {@code expected} rows. Ingestion is asynchronous on a
     * running server twice over - the WAL apply job for the base, the refresh pool for a view -
     * so every count this case reads is a poll rather than a snapshot.
     */
    private static void awaitRowCount(Connection conn, String table, long expected) throws Exception {
        TestUtils.assertEventually(
                () -> Assert.assertEquals(
                        table + " row count",
                        expected,
                        queryLong(conn, "SELECT count() FROM " + table)
                ),
                60
        );
    }

    private static void execute(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static long queryLong(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("query returned no row: " + sql, rs.next());
            return rs.getLong(1);
        }
    }

    private static TestServerMain startServer() {
        return startWithEnvVariables(
                PropertyKey.CAIRO_LIVE_VIEW_ENABLED.getEnvVarName(), "true",
                PropertyKey.LIVE_VIEW_REFRESH_WORKER_COUNT.getEnvVarName(), "2",
                PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS.getEnvVarName(), String.valueOf(CHECKPOINT_ROWS),
                PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false"
        );
    }

    /**
     * The rows of one anchor day's {@code hour}, every account, as one INSERT tuple list.
     */
    private static String hourRows(int hour) {
        final StringBuilder rows = new StringBuilder();
        for (int i = 0; i < ROWS_PER_ACCOUNT_PER_HOUR; i++) {
            for (int account = 1; account <= ACCOUNT_COUNT; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(hour, account + i * ACCOUNT_COUNT, account));
            }
        }
        return rows.toString();
    }

    /**
     * One INSERT tuple list holding one row per account at {@code instant}, so a whole commit
     * lands on a single point of the timeline and the traces count commits rather than rows.
     */
    private static String accountRows(String instant) {
        final StringBuilder rows = new StringBuilder();
        for (int account = 1; account <= ACCOUNT_COUNT; account++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append("('").append(instant).append("', 'acct-").append(account).append("', 1.0)");
        }
        return rows.toString();
    }

    /**
     * The instant pass {@code pass} of a trickle corrects, inside the hour
     * {@code hourPrefix} names. Read both by the INSERT the pass makes and by the premise
     * assertion that asks the plan what segment it lands in, so the two cannot drift.
     */
    private static String correctionInstant(String hourPrefix, int pass) {
        return hourPrefix + String.format("%02d", TRICKLE_FIRST_MINUTE - pass) + ":00.000000Z";
    }

    /**
     * One correction of pass {@code pass} inside the hour {@code hourPrefix} names, on the
     * account no trace follows.
     */
    private static String trickleRow(String hourPrefix, int pass) {
        return "('" + correctionInstant(hourPrefix, pass) + "', 'acct-" + TRICKLE_ACCOUNT + "', 1.0)";
    }

    private static String row(int hour, int minute, int account) {
        return "('2026-01-05T" + String.format("%02d", hour) + ":" + String.format("%02d", minute)
                + ":00.000000Z', 'acct-" + account + "', 1.0)";
    }

    private static int rowsPerHour() {
        return ACCOUNT_COUNT * ROWS_PER_ACCOUNT_PER_HOUR;
    }

    /**
     * {@code TRACE_ACCOUNT}'s rows of {@code view}, one per line as
     * {@code <timestamp>\t<cumulative_count>}. Reading the count rather than the sum keeps the
     * expected values integers, and since every row carries an amount of one the two carry the
     * same information.
     */
    private static String trace(Connection conn, String view) throws SQLException {
        final StringSink sink = new StringSink();
        try (
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT created_at::varchar, cumulative_count FROM " + view
                                + " WHERE account_id = '" + TRACE_ACCOUNT + "' ORDER BY created_at"
                )
        ) {
            while (rs.next()) {
                sink.put(rs.getString(1)).put('\t').put(rs.getLong(2)).put('\n');
            }
        }
        return sink.toString();
    }

    /**
     * The view has to equal a from-base recompute bucketed on the same daily anchor its own
     * window carries, which is a different floor function once a zone is named.
     */
    private void assertMatchesRecompute(Connection conn, String view, String anchorZone) throws SQLException {
        final String origin = "'1970-01-01T" + anchorTime + ":00.000000Z'::timestamp";
        final String bucket = anchorZone == null
                ? "timestamp_floor('1d', created_at, " + origin + ")"
                : "timestamp_floor_utc('1d', created_at, " + origin + ", '+00:00', '" + anchorZone + "')";
        final String recompute = "SELECT created_at, account_id, "
                + "sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum, "
                + "count(account_id) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count "
                + "FROM (SELECT created_at, account_id, amount, " + bucket + " AS bucket FROM " + BASE + ")";
        final String stored = "SELECT created_at, account_id, cumulative_sum, cumulative_count FROM " + view;
        Assert.assertEquals(
                view + " has rows the recompute does not",
                0,
                queryLong(conn, "SELECT count() FROM ((" + stored + ") EXCEPT (" + recompute + "))")
        );
        Assert.assertEquals(
                view + " is missing rows the recompute has",
                0,
                queryLong(conn, "SELECT count() FROM ((" + recompute + ") EXCEPT (" + stored + "))")
        );
    }

    /**
     * Asserts which bounds {@link LiveViewCheckpointAnchorPlan} reports for the segment every
     * correction of the trickle at {@code hourPrefix} lands in - the premise that trickle's
     * own assertions rest on, and the one part of it that does not depend on timing.
     * <p>
     * The plan is arithmetic over the zone table and a view's own anchor: for a given anchor
     * time, zone and instant it answers the same on every run, on a loaded machine as on an
     * idle one. What a running server reports for a correction into that segment does not
     * follow from the bounds alone - it follows from the route the repair took, and the
     * routes a refused segment leaves open include ones that read no segment bound at all.
     * So a trickle asking for a particular route has to state which segment shape it is
     * asking over, and state it here rather than in a comment.
     * <p>
     * The plan is built at microsecond precision because the base table's {@code TIMESTAMP}
     * column is; the refusals themselves are a property of the zone rather than of the
     * precision, which {@code LiveViewCheckpointTimeZoneAnchorPlanTest} pins on both.
     *
     * @param hasStart whether the plan must report a start for the segment - false is the
     *                 open-below refusal a spring-forward gap produces
     * @param hasEnd   whether it must report an end - false is the {@code H = EOF} refusal a
     *                 fall-back that splits the segment produces
     */
    private void assertSegmentBounds(String hourPrefix, boolean hasStart, boolean hasEnd) {
        final LiveViewCheckpointAnchorPlan plan = LiveViewCheckpointAnchorPlan.ofTimeZone(
                'd',
                1,
                MicrosTimestampDriver.floor("1970-01-01T" + anchorTime + ":00.000000Z"),
                ColumnType.TIMESTAMP_MICRO,
                ANCHOR_ZONE
        );
        Assert.assertNotNull(
                "ANCHOR DAILY '" + anchorTime + "' '" + ANCHOR_ZONE + "' must carry a segment plan",
                plan
        );
        // Every pass of the trickle, not just the first: the passes walk one minute deeper
        // each time, and a segment boundary between two of them would put them on different
        // sides of the verdict this asserts.
        for (int pass = 1; pass <= TRICKLE_PASSES; pass++) {
            final String instant = correctionInstant(hourPrefix, pass);
            final long ts = MicrosTimestampDriver.floor(instant);
            Assert.assertEquals(
                    "the plan must " + (hasStart ? "report" : "refuse") + " a segment start at "
                            + instant + " under ANCHOR DAILY '" + anchorTime + "' '" + ANCHOR_ZONE + "'",
                    hasStart,
                    plan.getSegmentStart(ts) != Long.MIN_VALUE
            );
            Assert.assertEquals(
                    "the plan must " + (hasEnd ? "report" : "refuse") + " a segment end at "
                            + instant + " under ANCHOR DAILY '" + anchorTime + "' '" + ANCHOR_ZONE + "'",
                    hasEnd,
                    plan.getSegmentEndExclusive(ts) != Numbers.LONG_NULL
            );
        }
    }

    private String createLiveView(String view, String anchorZone) {
        return "CREATE LIVE VIEW " + view + " FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, "
                + "sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM " + BASE + " "
                + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at "
                + "ANCHOR DAILY '" + anchorTime + "'" + (anchorZone != null ? " '" + anchorZone + "'" : "") + ")";
    }

    /**
     * Trickles corrections into the hour {@code hourPrefix} names, over a segment whose
     * bounds the anchor plan REFUSES, and returns the base row count that leaves.
     * <p>
     * No disposition is required here, and that is the point: the decomposition gives up on
     * the first refused row, so every pass takes the union range - the whole-change-set route
     * that predates anchor segments and that carries no reading of its own to settle on. What
     * the loop does hold is the two readings a refusal must never move: the view stays active,
     * and the denial never names {@code incomplete dependency}, which is a compile-time
     * property of the anchor's shape rather than something a transition day can produce.
     * <p>
     * The rows the fallback leaves behind are what the caller asserts, since a fallback that
     * dropped or double-counted a correction still reports exactly what a working one does.
     */
    private long trickleAcrossRefusedSegment(
            Connection conn,
            String hourPrefix,
            long baseRows,
            StringSink readings
    ) throws Exception {
        for (int pass = 1; pass <= TRICKLE_PASSES; pass++) {
            execute(conn, "INSERT INTO " + BASE + " VALUES " + trickleRow(hourPrefix, pass));
            baseRows++;
            awaitRowCount(conn, BASE, baseRows);
            awaitRowCount(conn, VIEW_ZONED, baseRows);
            awaitRowCount(conn, VIEW_PLAIN, baseRows);

            final Reading zoned = read(conn, VIEW_ZONED);
            final Reading plain = read(conn, VIEW_PLAIN);
            readings.put(hourPrefix).put(" pass ").put(pass)
                    .put(": zoned=").put(zoned.toString())
                    .put(" plain=").put(plain.toString()).put('\n');

            Assert.assertNotEquals(
                    "the zone anchor's dependency must stay covered over a refused segment,"
                            + " readings:\n" + readings,
                    "incomplete dependency",
                    zoned.denial
            );
            Assert.assertEquals("active", zoned.status);
            Assert.assertEquals("active", plain.status);
        }
        return baseRows;
    }

    /**
     * Trickles corrections into the hour {@code hourPrefix} names until the zone-anchored
     * view's repair settles on a localized rebuild, and returns the base row count that leaves.
     * <p>
     * Settling is the route half of this case's claim. A rebuild that reports
     * {@code localized rebuild} with no denial read {@code [L, H)} off the anchor's own segment
     * bounds - the ones the zone's transition table produced. Every other outcome here is the
     * conservative path: a denial, and a read from the view's own {@code START FROM} boundary
     * that would deliver the same rows without the segment arithmetic ever being consulted. The
     * row assertions cannot tell those two apart, so this one has to.
     * <p>
     * <b>The caller owes this a segment the plan carries both bounds for</b>, asserted with
     * {@link #assertSegmentBounds(String, boolean, boolean)} rather than assumed. A refused
     * bound does not merely make the localized route less likely: it takes the route away.
     * {@code LiveViewCheckpointSegmentChangeSet.addRow} declines a row whose segment has no
     * representable start or end, so the decomposition never runs, and the union-range plan
     * behind it derives {@code H = EOF} from the same refusal - which prices the rebuild as
     * everything above {@code L} and hands the repair to a resume from any anchor the ladder
     * happens to hold below the correction. Whether it holds one is a function of how far the
     * asynchronous refresh had got, so over a refused segment this assertion is a coin toss
     * rather than a verdict. Use {@link #trickleAcrossRefusedSegment} there instead.
     */
    private long trickleUntilLocalized(
            Connection conn,
            String hourPrefix,
            long baseRows,
            StringSink readings
    ) throws Exception {
        int settled = 0;
        for (int pass = 1; pass <= TRICKLE_PASSES; pass++) {
            execute(conn, "INSERT INTO " + BASE + " VALUES " + trickleRow(hourPrefix, pass));
            baseRows++;
            awaitRowCount(conn, BASE, baseRows);
            awaitRowCount(conn, VIEW_ZONED, baseRows);
            awaitRowCount(conn, VIEW_PLAIN, baseRows);

            final Reading zoned = read(conn, VIEW_ZONED);
            final Reading plain = read(conn, VIEW_PLAIN);
            readings.put(hourPrefix).put(" pass ").put(pass)
                    .put(": zoned=").put(zoned.toString())
                    .put(" plain=").put(plain.toString()).put('\n');

            // The denial that used to be reported on every repair of a zoned anchor, forever.
            // It is a compile-time property of the anchor's shape, so one sighting at any pass
            // is the gap rather than a slow ladder.
            Assert.assertNotEquals(
                    "the zone anchor's dependency must stay covered across a transition, readings:\n"
                            + readings,
                    "incomplete dependency",
                    zoned.denial
            );
            Assert.assertEquals("active", zoned.status);
            Assert.assertEquals("active", plain.status);

            settled = "localized rebuild".equals(zoned.disposition) ? settled + 1 : 0;
            if (settled == SETTLED_PASSES) {
                break;
            }
        }
        Assert.assertEquals(
                "corrections at " + hourPrefix + " must settle on a localized rebuild - anything"
                        + " else is the conservative path, which reads from the view's own boundary"
                        + " and never consults the zone's segment bounds, readings:\n" + readings,
                SETTLED_PASSES,
                settled
        );
        return baseRows;
    }

    private Reading read(Connection conn, String view) throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT view_status, checkpoint_repair_last_disposition,"
                                + " checkpoint_repair_last_denial, o3_resume_replay_rows,"
                                + " o3_boundary_replay_rows, o3_replay_scan_rows"
                                + " FROM live_views() WHERE view_name = '" + view + "'"
                )
        ) {
            Assert.assertTrue("live_views() must carry a row for '" + view + "'", rs.next());
            final Reading reading = new Reading();
            reading.status = rs.getString(1);
            reading.disposition = rs.getString(2);
            reading.denial = rs.getString(3);
            reading.resumeReplayRows = rs.getLong(4);
            reading.boundaryReplayRows = rs.getLong(5);
            reading.replayScanRows = rs.getLong(6);
            return reading;
        }
    }

    /**
     * One {@code live_views()} row, at the columns the soak protocol reads.
     */
    private static class Reading {
        private long boundaryReplayRows;
        private String denial;
        private String disposition;
        private long replayScanRows;
        private long resumeReplayRows;
        private String status;

        @Override
        public String toString() {
            return "[" + status + " " + disposition + " / " + denial
                    + " resumed=" + resumeReplayRows
                    + " rebuilt=" + boundaryReplayRows
                    + " scanned=" + replayScanRows + "]";
        }
    }
}
