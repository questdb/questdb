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
 * The zone is a neutral DST-observing one; nothing here depends on which zone it is, only that
 * its rules carry transitions and so cannot be folded into fixed-stride arithmetic.
 */
public class LiveViewTimeZoneAnchorServerTest extends AbstractBootstrapTest {

    private static final int ACCOUNT_COUNT = 8;
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

    private static String row(int hour, int minute, int account) {
        return "('2026-01-05T" + String.format("%02d", hour) + ":" + String.format("%02d", minute)
                + ":00.000000Z', 'acct-" + account + "', 1.0)";
    }

    private static int rowsPerHour() {
        return ACCOUNT_COUNT * ROWS_PER_ACCOUNT_PER_HOUR;
    }

    /**
     * The view has to equal a from-base recompute bucketed on the same daily anchor its own
     * window carries, which is a different floor function once a zone is named.
     */
    private void assertMatchesRecompute(Connection conn, String view, String anchorZone) throws SQLException {
        final String origin = "'1970-01-01T" + ANCHOR_TIME + ":00.000000Z'::timestamp";
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

    private String createLiveView(String view, String anchorZone) {
        return "CREATE LIVE VIEW " + view + " FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, "
                + "sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM " + BASE + " "
                + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at "
                + "ANCHOR DAILY '" + ANCHOR_TIME + "'" + (anchorZone != null ? " '" + anchorZone + "'" : "") + ")";
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
