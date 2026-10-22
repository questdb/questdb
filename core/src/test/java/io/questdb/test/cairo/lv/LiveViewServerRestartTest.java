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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.e2e.RestartableQwpServer;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;

/**
 * Anchored and bounded-frame live views across a real {@code ServerMain} restart: its own refresh
 * pool, its own WAL apply, its own startup metadata hydrator and its own network ingestion. Every
 * {@code live_views()} reading goes over PGWire, and so does every row of the anchored cases. The
 * bounded-frame case reads its rows back through the client's QWP query client instead. The
 * restart route and the fault count, which no catalogue column carries, come off the server's
 * engine.
 * <p>
 * The hand-driven restart tests ({@code restartCycle()} and friends) reload the registry inside one
 * engine. They cannot show what a cold boot does: the order in which the refresh workers, the WAL
 * apply job and the metadata hydrator start, and what a refresh turn finds when it runs first.
 * <p>
 * Each case reads three things after the restart:
 * <ul>
 *     <li>the view came back on the ladder its previous process published, not on a rebuild,
 *     and reports {@code active} with no recovery phase;</li>
 *     <li>its rows equal a from-base recompute of the same daily anchor;</li>
 *     <li>it keeps tracking: in-order and late commits after the restart still recompute, and no
 *     refresh cycle faulted, bar the one drift an unreferenced ADD COLUMN costs by design. A
 *     fault self-heals into a recompute from the applied base, which the row oracle would match
 *     either way.</li>
 * </ul>
 * Every base keeps {@code (sym, ts)} unique, so the cumulative values a window computes do not
 * depend on the order in which equal timestamps arrive, and the text of the two ordered result
 * sets can be compared outright. Equal timestamps across symbols are deliberate: they put an
 * in-order commit on the newest root's own timestamp after the root sealed, which a restore has
 * to replay rather than skip.
 */
public class LiveViewServerRestartTest extends AbstractBootstrapTest {

    // The row trigger of the seal cadence. Each seed commit carries this many rows and waits for
    // its flush, so every seed commit seals a root of its own.
    private static final int CHECKPOINT_ROWS = 4;
    private static final String DEDUP_BASE = "quotes";
    private static final int DEDUP_SEED_COMMITS = 6;
    private static final String DEDUP_VIEW = "quotes_lv";
    private static final long FIRST_TS = MicrosTimestampDriver.floor("2026-02-02T00:00:00.000000Z");
    private static final String IN_MEMORY_VIEW = "trades_lv_mem";
    private static final Log LOG = LogFactory.getLog(LiveViewServerRestartTest.class);
    private static final String PLAIN_BASE = "trades";
    private static final String PLAIN_VIEW = "trades_lv";
    // The rows of every seed commit, an hour apart, so eight commits span the anchor's day
    // boundary and the window resets in the middle of the ladder.
    private static final int ROWS_PER_COMMIT = CHECKPOINT_ROWS;
    private static final int SEED_COMMITS = 8;
    private static final int SEED_ROWS = SEED_COMMITS * ROWS_PER_COMMIT;
    private static final int DEDUP_SEED_ROWS = DEDUP_SEED_COMMITS * ROWS_PER_COMMIT;
    // The auto-flush batches of the two senders. Neither divides the symbols of a tick, so the
    // stream's commits end half-way through a timestamp and the next commit carries on at it,
    // and the backfill's late rows of a round spread over several commits.
    private static final int SENDER_BACKFILL_FLUSH_ROWS = 3;
    private static final int SENDER_BACKFILL_ROWS_PER_ROUND = 7;
    private static final String SENDER_BASE = "ticks";
    // The SYMBOL capacity the base starts at. The senders bring SENDER_SYMBOLS keys over the
    // rounds, which crosses the 0.8 auto-scale threshold at 4, 7 and 13 keys and grows the
    // capacity to 8, 16 and then 32 in separate rounds.
    private static final int SENDER_INITIAL_SYMBOL_CAPACITY = 4;
    // The round from which both senders send a field the base does not have yet, which turns
    // into an ADD COLUMN mid-stream.
    private static final int SENDER_NEW_FIELD_ROUND = 3;
    private static final int SENDER_ROUNDS = 7;
    private static final int SENDER_STREAM_FLUSH_ROWS = 7;
    private static final int SENDER_SYMBOLS = 16;
    private static final long SENDER_TICK_MICROS = 40 * Micros.MINUTE_MICROS;
    private static final int SENDER_TICKS_PER_ROUND = 6;
    private static final String SENDER_VIEW = "ticks_lv";
    private static final int STAT_BACKFILL_ROWS_PER_ROUND = 5;
    private static final String STAT_BASE = "fx";
    private static final String STAT_RANGE_COLUMNS = "sym, ts::varchar, px, qty, c, sp, ks, cvl, vsx, cx";
    // T1b over a RANGE frame in the issue's lv2 spelling, with T1a and T2 on the same frame, plus
    // T1a and T1b over a lagged frame and over one that excludes the current row.
    private static final String STAT_RANGE_QUERY = """
            SELECT ts, sym, px, qty,
                corr(px, qty) OVER w AS c,
                stddev_pop(px) OVER w AS sp,
                ksum(px) OVER w AS ks,
                covar_pop(px, qty) OVER wl AS cvl,
                var_samp(px) OVER wx AS vsx,
                covar_samp(px, qty) OVER wx AS cx
            FROM fx
            WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE '1' MINUTE PRECEDING),
                wl AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' MINUTE PRECEDING AND '30' SECOND PRECEDING),
                wx AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '40' SECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)""";
    private static final String STAT_RANGE_VIEW = "fx_range_lv";
    private static final int STAT_REPAIR_ROUNDS = 6;
    private static final String STAT_ROWS_COLUMNS = "sym, ts::varchar, px, qty, vol, cv, vp, sx, cr";
    // T1a and T1b over a ROWS frame in the issue's lv1 spelling, plus a lagged frame and one that
    // excludes the current row.
    private static final String STAT_ROWS_QUERY = """
            SELECT ts, sym, px, qty,
                stddev_samp(px) OVER w AS vol,
                covar_samp(px, qty) OVER w AS cv,
                var_pop(px) OVER wl AS vp,
                stddev(px) OVER wx AS sx,
                corr(px, qty) OVER wx AS cr
            FROM fx
            WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS 19 PRECEDING),
                wl AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING),
                wx AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 6 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)""";
    private static final String STAT_ROWS_VIEW = "fx_rows_lv";
    private static final int STAT_SYMBOLS = 3;
    private static final int STAT_TAIL_ROUNDS = 3;
    // Six ticks span the one-minute frame. A round gives every symbol six in-order rows, so the
    // 20-row frame fills within the first rounds and slides from then on.
    private static final long STAT_TICK_MICROS = 10 * Micros.SECOND_MICROS;
    private static final int STAT_TICKS_PER_ROUND = 6;
    private static final int SYMBOL_COUNT = 3;
    private static final int TIMEOUT_SECONDS = 60;
    private static final LogCapture capture = new LogCapture();
    private int httpPort;
    private int pgPort;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> {
            // Free ports rather than the class-wide offset, so a run on a shared machine does not
            // land on another JVM's server. The configuration names a bind address for every
            // service, so the two that startServer() switches off get a free port as well.
            final IntList ports = new IntList();
            while (ports.size() < 4) {
                final int port = RestartableQwpServer.pickFreePort();
                if (!ports.contains(port)) {
                    ports.add(port);
                }
            }
            httpPort = ports.getQuick(0);
            pgPort = ports.getQuick(2);
            createDummyConfiguration(httpPort, ports.getQuick(1), pgPort, ports.getQuick(3), root);
        });
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testADedupBaseUpsertAfterARestartAheadOfTheHydratorReplacesTheRow() throws Exception {
        // A deduplicating base makes the refresh worker drain the applied base rather than the
        // raw WAL, because the raw WAL holds the pre-dedup stream: an upsert at the frontier
        // timestamp would append there as a second row. isDedupBase decides that once per cycle.
        // It used to read the metadata catalogue, which the startup hydrator fills table by table
        // on its own thread, so a cycle that ran before the hydrator reached the base answered
        // "not deduplicating" and duplicated the upsert. It now reads the base table's own
        // metadata, which needs no catalogue.
        //
        // On a catalogue this small the server's hydrator finishes within milliseconds of the
        // start, long before any commit can follow the restart. So the case lets the hydrator
        // finish, then empties the catalogue the way clearCache() leaves it - the state the
        // hydrator starts from - and drives the upsert through the server while the base is
        // still missing from it. The two cold-catalogue checks around the upsert keep the case
        // honest: something that warmed the base in between would make it a test of nothing.
        assertMemoryLeak(() -> {
            final long frontierTs = seedTs(DEDUP_SEED_ROWS - 1);
            final String beforeRestart;
            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                execute(conn, createBase(DEDUP_BASE, "", " DEDUP UPSERT KEYS(ts, sym)"));
                execute(conn, createAnchoredView(DEDUP_VIEW, DEDUP_BASE, ""));
                for (int commit = 0; commit < DEDUP_SEED_COMMITS; commit++) {
                    execute(conn, insertInto(DEDUP_BASE, seedRows(commit)));
                    awaitCaughtUp(serverMain, conn, DEDUP_BASE, DEDUP_VIEW);
                }
                assertLadder(conn, DEDUP_VIEW);
                assertViewMatchesRecompute(conn, DEDUP_VIEW, DEDUP_BASE);
                assertNoRefreshFaults(serverMain, DEDUP_VIEW);
                beforeRestart = readRows(conn, viewRowsSql(DEDUP_VIEW));
            }

            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                serverMain.awaitStartup();
                final CairoEngine engine = serverMain.getEngine();
                assertRestartRestoredFromTimeline(serverMain, conn, DEDUP_VIEW);
                TestUtils.assertEquals(
                        DEDUP_VIEW + " must come back with the rows it had",
                        beforeRestart,
                        readRows(conn, viewRowsSql(DEDUP_VIEW))
                );

                final TableToken baseToken = engine.verifyTableName(DEDUP_BASE);
                try (MetadataCacheWriter cacheRW = engine.getMetadataCache().writeLock()) {
                    cacheRW.clearCache();
                }
                Assert.assertNull("the case needs a cold catalogue; something warmed it", cachedTable(engine, baseToken));

                // The upsert replaces the frontier row: same timestamp, same symbol, new price.
                execute(conn, insertInto(DEDUP_BASE, row(frontierTs, symbol(DEDUP_SEED_ROWS - 1), 1000)));
                awaitCaughtUp(serverMain, conn, DEDUP_BASE, DEDUP_VIEW);
                Assert.assertNull(
                        "the drain must have read the upsert while the catalogue was still cold; something warmed it",
                        cachedTable(engine, baseToken)
                );
                assertViewMatchesRecompute(conn, DEDUP_VIEW, DEDUP_BASE);

                // And the view keeps tracking: an upsert below the frontier, then the next day.
                execute(conn, insertInto(DEDUP_BASE, row(seedTs(10), symbol(10), 2000)));
                execute(conn, insertInto(DEDUP_BASE, rows(DEDUP_SEED_ROWS, DEDUP_SEED_ROWS + ROWS_PER_COMMIT)));
                awaitCaughtUp(serverMain, conn, DEDUP_BASE, DEDUP_VIEW);
                assertViewMatchesRecompute(conn, DEDUP_VIEW, DEDUP_BASE);
                Assert.assertEquals(
                        "an upsert must not add a row",
                        DEDUP_SEED_ROWS + ROWS_PER_COMMIT,
                        queryLong(conn, "SELECT count() FROM " + DEDUP_VIEW)
                );
                assertActive(conn, DEDUP_VIEW);
                assertNoRefreshFaults(serverMain, DEDUP_VIEW);
            }
        });
    }

    @Test
    public void testAnchoredViewsRestoreTheirLadderAcrossARestartAndKeepTracking() throws Exception {
        assertMemoryLeak(() -> {
            final String plainBeforeRestart;
            final String inMemoryBeforeRestart;
            final long frontierTs = seedTs(SEED_ROWS - 1);
            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                execute(conn, createBase(PLAIN_BASE, "", ""));
                execute(conn, createAnchoredView(PLAIN_VIEW, PLAIN_BASE, ""));
                execute(conn, createAnchoredView(IN_MEMORY_VIEW, PLAIN_BASE, "IN MEMORY 1h "));
                for (int commit = 0; commit < SEED_COMMITS; commit++) {
                    execute(conn, insertInto(PLAIN_BASE, seedRows(commit)));
                    awaitCaughtUp(serverMain, conn, PLAIN_BASE, PLAIN_VIEW);
                    awaitCaughtUp(serverMain, conn, PLAIN_BASE, IN_MEMORY_VIEW);
                }
                // A single row on the newest root's own timestamp, under a symbol the root does not
                // hold there. One row is below the cadence's row trigger, so no seal records it:
                // the restore has to replay it from the base WAL above the root.
                execute(conn, insertInto(PLAIN_BASE, row(frontierTs, symbol(SEED_ROWS), 100)));
                awaitCaughtUp(serverMain, conn, PLAIN_BASE, PLAIN_VIEW);
                awaitCaughtUp(serverMain, conn, PLAIN_BASE, IN_MEMORY_VIEW);

                assertLadder(conn, PLAIN_VIEW);
                assertLadder(conn, IN_MEMORY_VIEW);
                assertViewMatchesRecompute(conn, PLAIN_VIEW, PLAIN_BASE);
                assertViewMatchesRecompute(conn, IN_MEMORY_VIEW, PLAIN_BASE);
                assertNoRefreshFaults(serverMain, PLAIN_VIEW);
                assertNoRefreshFaults(serverMain, IN_MEMORY_VIEW);
                plainBeforeRestart = readRows(conn, viewRowsSql(PLAIN_VIEW));
                inMemoryBeforeRestart = readRows(conn, viewRowsSql(IN_MEMORY_VIEW));
            }

            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                assertRestartRestoredFromTimeline(serverMain, conn, PLAIN_VIEW);
                assertRestartRestoredFromTimeline(serverMain, conn, IN_MEMORY_VIEW);
                // Both restores stood on the newest root and replayed the tie above it: the one
                // row no seal recorded.
                capture.drain();
                capture.assertLoggedRE("restored live view from checkpoint timeline \\[view=" + PLAIN_VIEW + ", .*replayedRows=1]");
                capture.assertLoggedRE("restored live view from checkpoint timeline \\[view=" + IN_MEMORY_VIEW + ", .*replayedRows=1]");
                TestUtils.assertEquals(
                        PLAIN_VIEW + " must come back with the rows it had",
                        plainBeforeRestart,
                        readRows(conn, viewRowsSql(PLAIN_VIEW))
                );
                TestUtils.assertEquals(
                        IN_MEMORY_VIEW + " must come back with the rows it had",
                        inMemoryBeforeRestart,
                        readRows(conn, viewRowsSql(IN_MEMORY_VIEW))
                );
                assertViewMatchesRecompute(conn, PLAIN_VIEW, PLAIN_BASE);
                assertViewMatchesRecompute(conn, IN_MEMORY_VIEW, PLAIN_BASE);

                // In-order rows that open on the frontier timestamp under its third symbol, then
                // late rows: one in the closed first day and one in the open second day, inside
                // the IN MEMORY hour. A runtime the restore put back at the wrong root would
                // answer the next cumulative values wrong; one that skipped the tie above would
                // answer the frontier group's count wrong.
                final StringSink inOrder = new StringSink();
                inOrder.put(row(frontierTs, symbol(SEED_ROWS + 1), 200)).put(", ")
                        .put(rows(SEED_ROWS, SEED_ROWS + ROWS_PER_COMMIT - 1));
                execute(conn, insertInto(PLAIN_BASE, inOrder));
                execute(conn, insertInto(PLAIN_BASE, row(seedTs(5) + 30 * Micros.MINUTE_MICROS, symbol(0), 300)
                        + ", " + row(seedTs(SEED_ROWS + 1) + 30 * Micros.MINUTE_MICROS, symbol(2), 400)));
                awaitCaughtUp(serverMain, conn, PLAIN_BASE, PLAIN_VIEW);
                awaitCaughtUp(serverMain, conn, PLAIN_BASE, IN_MEMORY_VIEW);
                assertViewMatchesRecompute(conn, PLAIN_VIEW, PLAIN_BASE);
                assertViewMatchesRecompute(conn, IN_MEMORY_VIEW, PLAIN_BASE);
                assertActive(conn, PLAIN_VIEW);
                assertActive(conn, IN_MEMORY_VIEW);
                assertNoRefreshFaults(serverMain, PLAIN_VIEW);
                assertNoRefreshFaults(serverMain, IN_MEMORY_VIEW);
            }
        });
    }

    @Test
    public void testBoundedFrameStatViewsFedBySendersRestoreAcrossARestart() throws Exception {
        // The statistical window functions over bounded frames, end to end through the real
        // client: stddev and variance (T1a) and covar and corr (T1b) over ROWS and RANGE frames,
        // and ksum (T2) over a RANGE frame. A QWP stream feeds the base in timestamp order and an
        // ILP-over-HTTP backfill sends late rows below its frontier, which the views repair. Some
        // rows leave px or qty out, so the frames hold NULLs: the ROWS ring keeps a NaN
        // placeholder for each, and the RANGE ring and every accumulator skip them.
        //
        // After the restart each view must come back on the timeline its previous process
        // sealed, not on a rebuild, and keep computing on the frame state it restored: the next
        // in-order rows read restored rows in their frames, and late rows after them still
        // repair. The QWP query client reads every result back.
        //
        // Every value here is a whole number far below 2^53, so each running sum is exact
        // whatever order it is summed in, and a view row equals its recompute bit for bit on any
        // route. testBoundedFrameStatViewsFedBySendersRestoreBitExactlyOnFractionalData sends
        // fractional values, where only the order the plain query slides its frames in gives
        // its bits.
        assertBoundedFrameStatViewsRestoreAcrossARestart(false);
    }

    @Test
    public void testBoundedFrameStatViewsFedBySendersRestoreBitExactlyOnFractionalData() throws Exception {
        // testBoundedFrameStatViewsFedBySendersRestoreAcrossARestart with prices in cents near
        // a level per symbol and sizes in thousandths, the late rows included, so every add to
        // and drop from a running sum rounds. Each view holds a function whose formula subtracts
        // nearly equal sums, which declines the localized repair for the whole view: every late
        // row resumes from a sealed root or rebuilds from the view's boundary, sliding the
        // frames in the order the plain query slides them, and the restart restores the sums the
        // previous process sealed verbatim. A route that re-summed a frame would leave its sums
        // a few ULPs off, which the cancelling formulas scale into differing digits, so the QWP
        // read-back must still equal the recompute's line for line.
        assertBoundedFrameStatViewsRestoreAcrossARestart(true);
    }

    @Test
    public void testTwoSendersIntoAnInMemoryViewBaseMatchARecomputeAcrossARestart() throws Exception {
        // Two real senders on two connections into one base: a QWP stream in timestamp order, and
        // an ILP-over-HTTP backfill of late rows below its frontier. Both auto-flush in batches
        // that cut a tick's symbols in two, and every round of the stream stops half-way through
        // a tick, so the root the view seals at the end of a round always has a tie above it:
        // the next round's first commit carries on at that timestamp. Over the rounds the stream
        // brings new symbols past the base's SYMBOL capacity three times, and from round
        // SENDER_NEW_FIELD_ROUND on both senders send a field the base does not have, which ILP
        // and QWP turn into an ADD COLUMN in the middle of the stream.
        //
        // Each round waits for the base to apply the stream, and for the view to consume it,
        // before the late rows land, so the repair they trigger reads a base whose metadata has
        // already moved: the capacity growth, which the view must take without a fault, and the
        // ADD COLUMN, which costs the view the designed recompile and in-place restore.
        assertMemoryLeak(() -> {
            final SenderWorkload workload = new SenderWorkload();
            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                execute(conn, createBase(SENDER_BASE, " CAPACITY " + SENDER_INITIAL_SYMBOL_CAPACITY, ""));
                execute(conn, createAnchoredView(SENDER_VIEW, SENDER_BASE, "IN MEMORY 1h "));
                try (Sender streamer = newStreamer(); Sender backfiller = newBackfiller()) {
                    for (int round = 0; round < SENDER_ROUNDS; round++) {
                        sendRound(serverMain, conn, workload, streamer, backfiller, round);
                    }
                    // A tie the restart has to replay from the base WAL above the newest root.
                    workload.streamTie(streamer, SENDER_ROUNDS - 1);
                    awaitRowCount(conn, SENDER_BASE, workload.rowCount);
                    awaitCaughtUp(serverMain, conn, SENDER_BASE, SENDER_VIEW);
                }

                // The premises the workload promises, read off the base and the view: the
                // capacity grew past where it started, the new field landed as a column, and late
                // rows reached a repair rather than only the in-order append.
                Assert.assertEquals(
                        "the senders must have doubled the base SYMBOL capacity three times",
                        SENDER_INITIAL_SYMBOL_CAPACITY * 8,
                        queryLong(conn, "SELECT symbolCapacity FROM table_columns('" + SENDER_BASE + "') WHERE \"column\" = 'sym'")
                );
                Assert.assertTrue(
                        "the new field must have landed as a column",
                        queryLong(conn, "SELECT count() FROM " + SENDER_BASE + " WHERE qty IS NOT NULL") > 0
                );
                Assert.assertTrue(
                        "late rows must have reached a repair",
                        queryLong(conn, "SELECT o3_replay_scan_rows FROM live_views() WHERE view_name = '" + SENDER_VIEW + "'") > 0
                );
                // The round boundary put a commit on the newest root's own timestamp after it
                // sealed. The next seal then replaces that root rather than stacking a second one
                // on the same timestamp, which is also why this view's ladder stays short.
                capture.drain();
                capture.assertLogged("live view seal replaced the previous root, whose timestamp group grew after it was sealed [view="
                        + SENDER_VIEW + ',');
                assertViewMatchesRecompute(conn, SENDER_VIEW, SENDER_BASE);
                assertActive(conn, SENDER_VIEW);
                assertOnlyAddColumnDriftFaults(serverMain, SENDER_VIEW);
            }

            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                assertRestartRestoredFromTimeline(serverMain, conn, SENDER_VIEW);
                capture.drain();
                capture.assertLoggedRE("restored live view from checkpoint timeline \\[view=" + SENDER_VIEW + ", .*replayedRows=1]");
                assertViewMatchesRecompute(conn, SENDER_VIEW, SENDER_BASE);
                // Fresh connections after the restart, one more round each. The first commit
                // carries on at the timestamp the last round stopped half-way through, on top of
                // the root the restore came back on.
                try (Sender streamer = newStreamer(); Sender backfiller = newBackfiller()) {
                    sendRound(serverMain, conn, workload, streamer, backfiller, SENDER_ROUNDS);
                }
                assertViewMatchesRecompute(conn, SENDER_VIEW, SENDER_BASE);
                assertActive(conn, SENDER_VIEW);
                assertNoRefreshFaults(serverMain, SENDER_VIEW);
            }
        });
    }

    /**
     * Asserts {@code live_views()} reads the view as a healthy, settled one: active, no recovery
     * phase, no invalidation reason, and none of the seal, repair or row-count faults the catalogue
     * counts.
     */
    private static void assertActive(Connection conn, String view) throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT view_status, checkpoint_recovery_phase, invalidation_reason,"
                                + " checkpoint_seal_failures, checkpoint_repair_failures, checkpoint_row_count_mismatches"
                                + " FROM live_views() WHERE view_name = '" + view + "'"
                )
        ) {
            Assert.assertTrue("live_views() must carry a row for '" + view + "'", rs.next());
            Assert.assertEquals(view + " view_status", "active", rs.getString(1));
            Assert.assertNull(view + " checkpoint_recovery_phase", rs.getString(2));
            Assert.assertNull(view + " invalidation_reason", rs.getString(3));
            Assert.assertEquals(view + " checkpoint_seal_failures", 0, rs.getLong(4));
            Assert.assertEquals(view + " checkpoint_repair_failures", 0, rs.getLong(5));
            Assert.assertEquals(view + " checkpoint_row_count_mismatches", 0, rs.getLong(6));
        }
    }

    /**
     * Asserts the view's checkpoint timeline holds at least three roots, so a restart has a ladder
     * to pick from rather than a single seed boundary.
     */
    private static void assertLadder(Connection conn, String view) throws SQLException {
        final long entries = queryLong(
                conn,
                "SELECT checkpoint_timeline_entries FROM live_views() WHERE view_name = '" + view + "'"
        );
        LOG.info().$("live view ladder [view=").$(view).$(", entries=").$(entries).I$();
        Assert.assertTrue(view + " must have sealed a ladder, entries=" + entries, entries > 2);
    }

    private static void assertNoRefreshFaults(TestServerMain serverMain, String view) {
        final LiveViewInstance instance = serverMain.getEngine().getLiveViewRegistry().getViewInstance(view);
        Assert.assertNotNull("live view '" + view + "' is not registered", instance);
        Assert.assertEquals(
                "live view '" + view + "' refresh cycles must not fault: a fault self-heals into a"
                        + " full recompute from the applied base, so a recompute oracle passes either way",
                0L,
                instance.getRefreshFaultCount()
        );
    }

    /**
     * Asserts the one refresh fault the view counted is the one an unreferenced ADD COLUMN costs
     * by design. The raw-WAL drain walks past the structural commit without recompiling, so the
     * late rows of that round are the first read of the base through a plan compiled before the
     * column existed: the refresh context refuses the reader as drift, and the view recompiles and
     * restores its runtime from its own timeline in place, logging the restore under the
     * metadata-change cause. A SYMBOL capacity growth must cost nothing, so a fault it caused
     * would show up here as a second drift, and a drift recovery that rebuilt the view from the
     * applied base instead of restoring it would show up as a missing restore.
     */
    private static void assertOnlyAddColumnDriftFaults(TestServerMain serverMain, String view) {
        final LiveViewInstance instance = serverMain.getEngine().getLiveViewRegistry().getViewInstance(view);
        Assert.assertNotNull("live view '" + view + "' is not registered", instance);
        capture.drain();
        final long faults = instance.getRefreshFaultCount();
        final int driftRestores = capture.countOccurrences(
                "live view restored its runtime from the checkpoint timeline [view=" + view + ", cause=base table metadata change,"
        );
        LOG.info().$("live view refresh faults [view=").$(view)
                .$(", faults=").$(faults)
                .$(", driftRestores=").$(driftRestores).I$();
        Assert.assertEquals("the ADD COLUMN must cost '" + view + "' exactly one refresh fault", 1, faults);
        Assert.assertEquals("the drift must restore '" + view + "' in place", 1, driftRestores);
        capture.assertNotLogged("live view recomputed window state from applied base [view=" + view + ',');
    }

    /**
     * Asserts the restarted server registered the view, ran its restart recovery off the
     * timeline the previous process sealed, and reports it settled.
     */
    private static void assertRestartRestoredFromTimeline(TestServerMain serverMain, Connection conn, String view) throws Exception {
        final CairoEngine engine = serverMain.getEngine();
        Assert.assertTrue(view + " must be registered after the restart", engine.getLiveViewRegistry().hasView(view));
        final TableToken token = engine.getTableTokenIfExists(view);
        Assert.assertNotNull(view + " must keep its table token", token);
        Assert.assertTrue(view + " must still be a live view", token.isLiveView());
        // The route is recorded when the attempt ends, so a route other than none is the signal
        // the attempt has finished.
        TestUtils.assertEventually(
                () -> {
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
                    Assert.assertNotNull(instance);
                    Assert.assertNotEquals(
                            view + " restart recovery must finish",
                            LiveViewCheckpointRecoveryPhase.ROUTE_NONE,
                            instance.getCheckpointRestoreRoute()
                    );
                },
                TIMEOUT_SECONDS
        );
        AbstractLiveViewTest.assertRestoredFromTimeline(view, engine.getLiveViewRegistry().getViewInstance(view));
        assertActive(conn, view);
    }

    /**
     * Asserts the premises the bounded-frame case relies on, read off the view: some rows carry a
     * NULL px and some a NULL qty, every window column has values to compare, and late rows reached
     * a repair rather than only the in-order append.
     */
    private static void assertStatPremises(Connection conn, String view, String... statColumns) throws SQLException {
        final long rowCount = queryLong(conn, "SELECT count() FROM " + view);
        Assert.assertTrue(view + " must hold rows with a NULL px", queryLong(conn, "SELECT count(px) FROM " + view) < rowCount);
        Assert.assertTrue(view + " must hold rows with a NULL qty", queryLong(conn, "SELECT count(qty) FROM " + view) < rowCount);
        for (String column : statColumns) {
            Assert.assertTrue(
                    view + '.' + column + " must carry values",
                    queryLong(conn, "SELECT count(" + column + ") FROM " + view) > 0
            );
        }
        Assert.assertTrue(
                "late rows must have reached a repair of " + view,
                queryLong(conn, "SELECT o3_replay_scan_rows FROM live_views() WHERE view_name = '" + view + "'") > 0
        );
    }

    /**
     * Compares the view against a from-base recompute of the same window, row for row. ANCHOR is
     * live-view syntax, so the daily bucket is written out as a partition term.
     */
    private static void assertViewMatchesRecompute(Connection conn, String view, String base) throws SQLException {
        final String recompute = """
                SELECT sym, ts::varchar, price, cumulative_sum, cumulative_count FROM (
                    SELECT ts, sym, price,
                        sum(price) OVER (PARTITION BY sym, bucket ORDER BY ts
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum,
                        count(sym) OVER (PARTITION BY sym, bucket ORDER BY ts
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count
                    FROM (
                        SELECT ts, sym, price, timestamp_floor('1d', ts, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket
                        FROM %s
                    )
                ) ORDER BY 1, 2
                """.formatted(base);
        TestUtils.assertEquals(
                view + " must equal a from-base recompute",
                readRows(conn, recompute),
                readRows(conn, viewRowsSql(view))
        );
    }

    /**
     * Waits until the view has consumed every base commit made so far and its own table has
     * applied what it wrote. A read serves the in-memory lead as well as the table, so a row count
     * can match the base a whole FLUSH EVERY interval before the flush lands; the consumed seqTxn
     * cannot.
     */
    private static void awaitCaughtUp(TestServerMain serverMain, Connection conn, String base, String view) throws Exception {
        serverMain.awaitTable(base);
        final long baseSeqTxn = queryLong(conn, "SELECT sequencerTxn FROM wal_tables() WHERE name = '" + base + "'");
        TestUtils.assertEventually(
                () -> {
                    final long consumed = queryLong(
                            conn,
                            "SELECT lv_consumed_seqtxn FROM live_views() WHERE view_name = '" + view + "'"
                    );
                    Assert.assertTrue(
                            view + " must consume base seqTxn " + baseSeqTxn + ", consumed " + consumed,
                            consumed >= baseSeqTxn
                    );
                },
                TIMEOUT_SECONDS
        );
        serverMain.awaitTable(view);
    }

    private static void awaitRowCount(Connection conn, String table, long expected) throws Exception {
        TestUtils.assertEventually(
                () -> Assert.assertEquals(table + " row count", expected, queryLong(conn, "SELECT count() FROM " + table)),
                TIMEOUT_SECONDS
        );
    }

    private static void awaitStatViewsCaughtUp(TestServerMain serverMain, Connection conn, StatWorkload workload) throws Exception {
        awaitRowCount(conn, STAT_BASE, workload.rowCount);
        awaitCaughtUp(serverMain, conn, STAT_BASE, STAT_ROWS_VIEW);
        awaitCaughtUp(serverMain, conn, STAT_BASE, STAT_RANGE_VIEW);
    }

    /**
     * The late rows of one bounded-frame round, waited out until the base has applied them and
     * both views have consumed them.
     */
    private static void backfillStatRound(
            TestServerMain serverMain,
            Connection conn,
            StatWorkload workload,
            Sender backfiller
    ) throws Exception {
        workload.backfill(backfiller);
        awaitStatViewsCaughtUp(serverMain, conn, workload);
    }

    /**
     * The metadata catalogue's entry for {@code token}, read without hydrating it: any lookup that
     * hydrates on demand would answer its own question.
     */
    private static Object cachedTable(CairoEngine engine, TableToken token) {
        try (MetadataCacheReader metaRO = engine.getMetadataCache().readLock()) {
            return metaRO.getTable(token);
        }
    }

    private static String createAnchoredView(String view, String base, String inMemoryClause) {
        return """
                CREATE LIVE VIEW %s FLUSH EVERY 100ms %sSTART FROM BEGINNING AS
                SELECT ts, sym, price, sum(price) OVER w AS cumulative_sum, count(sym) OVER w AS cumulative_count
                FROM %s
                WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')
                """.formatted(view, inMemoryClause, base);
    }

    private static String createBase(String base, String symbolCapacityClause, String dedupClause) {
        return "CREATE TABLE " + base + " (ts TIMESTAMP, sym SYMBOL" + symbolCapacityClause + ", price DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL" + dedupClause;
    }

    private static String createStatView(String view, String query) {
        return "CREATE LIVE VIEW " + view + " FLUSH EVERY 100ms START FROM BEGINNING AS\n" + query;
    }

    private static void execute(Connection conn, CharSequence sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    private static String insertInto(String base, CharSequence rows) {
        return "INSERT INTO " + base + " (ts, sym, price) VALUES " + rows;
    }

    private static long queryLong(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("query returned no row: " + sql, rs.next());
            return rs.getLong(1);
        }
    }

    /**
     * Every row of {@code sql}, one line each, tab-separated, in the order the query returns them.
     */
    private static String readRows(Connection conn, String sql) throws SQLException {
        final StringSink sink = new StringSink();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            final int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        sink.put('\t');
                    }
                    final String value = rs.getString(i);
                    sink.put(value != null ? value : "null");
                }
                sink.put('\n');
            }
        }
        return sink.toString();
    }

    private static String row(long ts, String sym, double price) {
        return "(" + ts + "::timestamp, '" + sym + "', " + price + ")";
    }

    /**
     * Seed rows {@code [lo, hi)}: one an hour from {@link #FIRST_TS}, the symbols taking the hours
     * in turn, each priced at its own ordinal so no two rows carry the same value.
     */
    private static String rows(int lo, int hi) {
        final StringSink sink = new StringSink();
        for (int i = lo; i < hi; i++) {
            if (i > lo) {
                sink.put(", ");
            }
            sink.put(row(seedTs(i), symbol(i), i + 1));
        }
        return sink.toString();
    }

    /**
     * One round of the sender workload: the stream, then the late rows, each waited out until the
     * base has applied it and the view has consumed it.
     */
    private static void sendRound(
            TestServerMain serverMain,
            Connection conn,
            SenderWorkload workload,
            Sender streamer,
            Sender backfiller,
            int round
    ) throws Exception {
        workload.stream(streamer, round);
        awaitRowCount(conn, SENDER_BASE, workload.rowCount);
        awaitCaughtUp(serverMain, conn, SENDER_BASE, SENDER_VIEW);
        workload.backfill(backfiller, round);
        awaitRowCount(conn, SENDER_BASE, workload.rowCount);
        awaitCaughtUp(serverMain, conn, SENDER_BASE, SENDER_VIEW);
    }

    private static String seedRows(int commit) {
        return rows(commit * ROWS_PER_COMMIT, (commit + 1) * ROWS_PER_COMMIT);
    }

    private static long seedTs(int row) {
        return FIRST_TS + row * Micros.HOUR_MICROS;
    }

    private static String statViewRowsSql(String view, String columns) {
        return "SELECT " + columns + " FROM " + view + " ORDER BY 1, 2";
    }

    /**
     * The in-order rows of one bounded-frame round, waited out until the base has applied them and
     * both views have consumed them.
     */
    private static void streamStatRound(
            TestServerMain serverMain,
            Connection conn,
            StatWorkload workload,
            Sender streamer
    ) throws Exception {
        workload.stream(streamer);
        awaitStatViewsCaughtUp(serverMain, conn, workload);
    }

    private static String symbol(int row) {
        return "s" + (row % SYMBOL_COUNT);
    }

    private static String viewRowsSql(String view) {
        return "SELECT sym, ts::varchar, price, cumulative_sum, cumulative_count FROM " + view + " ORDER BY 1, 2";
    }

    /**
     * Feeds both bounded-frame views through the real senders, in-order rounds and backfills of
     * late rows, restarts the server, and asserts each view comes back on its timeline and keeps
     * equal to a from-base recompute, read back over QWP, through more rounds. With
     * {@code isFractional} every value the senders write has a fractional part.
     */
    private void assertBoundedFrameStatViewsRestoreAcrossARestart(boolean isFractional) throws Exception {
        assertMemoryLeak(() -> {
            final StatWorkload workload = new StatWorkload(isFractional);
            final String rowsBeforeRestart;
            final String rangeBeforeRestart;
            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                execute(conn, "CREATE TABLE " + STAT_BASE + " (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty DOUBLE)"
                        + " TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute(conn, createStatView(STAT_ROWS_VIEW, STAT_ROWS_QUERY));
                execute(conn, createStatView(STAT_RANGE_VIEW, STAT_RANGE_QUERY));
                try (Sender streamer = newStreamer(); Sender backfiller = newBackfiller()) {
                    for (int round = 0; round < STAT_REPAIR_ROUNDS; round++) {
                        streamStatRound(serverMain, conn, workload, streamer);
                        backfillStatRound(serverMain, conn, workload, backfiller);
                    }
                    // A late row moves every later ROWS frame of its symbol, so the ROWS view
                    // repairs it with a full replay from the applied base, which leaves it a
                    // single root. In-order rounds with no late rows let both views seal a ladder
                    // for the restart to pick from.
                    for (int round = 0; round < STAT_TAIL_ROUNDS; round++) {
                        streamStatRound(serverMain, conn, workload, streamer);
                    }
                }

                assertStatPremises(conn, STAT_ROWS_VIEW, "vol", "cv", "vp", "sx", "cr");
                assertStatPremises(conn, STAT_RANGE_VIEW, "c", "sp", "ks", "cvl", "vsx", "cx");
                if (isFractional) {
                    Assert.assertEquals(
                            "every px and qty the senders wrote must have a fractional part",
                            0,
                            queryLong(conn, "SELECT count() FROM " + STAT_BASE
                                    + " WHERE (px IS NOT NULL AND px = round(px, 0)) OR (qty IS NOT NULL AND qty = round(qty, 0))")
                    );
                }
                assertLadder(conn, STAT_ROWS_VIEW);
                assertLadder(conn, STAT_RANGE_VIEW);
                assertStatViewMatchesRecompute(STAT_ROWS_VIEW, STAT_ROWS_QUERY, STAT_ROWS_COLUMNS);
                assertStatViewMatchesRecompute(STAT_RANGE_VIEW, STAT_RANGE_QUERY, STAT_RANGE_COLUMNS);
                assertActive(conn, STAT_ROWS_VIEW);
                assertActive(conn, STAT_RANGE_VIEW);
                assertNoRefreshFaults(serverMain, STAT_ROWS_VIEW);
                assertNoRefreshFaults(serverMain, STAT_RANGE_VIEW);
                rowsBeforeRestart = readRowsOverQwp(statViewRowsSql(STAT_ROWS_VIEW, STAT_ROWS_COLUMNS));
                rangeBeforeRestart = readRowsOverQwp(statViewRowsSql(STAT_RANGE_VIEW, STAT_RANGE_COLUMNS));
            }

            try (TestServerMain serverMain = startServer(); Connection conn = connect()) {
                assertRestartRestoredFromTimeline(serverMain, conn, STAT_ROWS_VIEW);
                assertRestartRestoredFromTimeline(serverMain, conn, STAT_RANGE_VIEW);
                TestUtils.assertEquals(
                        STAT_ROWS_VIEW + " must come back with the rows it had",
                        rowsBeforeRestart,
                        readRowsOverQwp(statViewRowsSql(STAT_ROWS_VIEW, STAT_ROWS_COLUMNS))
                );
                TestUtils.assertEquals(
                        STAT_RANGE_VIEW + " must come back with the rows it had",
                        rangeBeforeRestart,
                        readRowsOverQwp(statViewRowsSql(STAT_RANGE_VIEW, STAT_RANGE_COLUMNS))
                );

                // Fresh connections after the restart, one more round each. In-order rows first:
                // every frame of the first ones reaches back into rows from before the restart,
                // which only the restored state holds. Then late rows, which the views repair on
                // top of it.
                try (Sender streamer = newStreamer(); Sender backfiller = newBackfiller()) {
                    streamStatRound(serverMain, conn, workload, streamer);
                    assertStatViewMatchesRecompute(STAT_ROWS_VIEW, STAT_ROWS_QUERY, STAT_ROWS_COLUMNS);
                    assertStatViewMatchesRecompute(STAT_RANGE_VIEW, STAT_RANGE_QUERY, STAT_RANGE_COLUMNS);
                    backfillStatRound(serverMain, conn, workload, backfiller);
                }
                assertStatViewMatchesRecompute(STAT_ROWS_VIEW, STAT_ROWS_QUERY, STAT_ROWS_COLUMNS);
                assertStatViewMatchesRecompute(STAT_RANGE_VIEW, STAT_RANGE_QUERY, STAT_RANGE_COLUMNS);
                assertActive(conn, STAT_ROWS_VIEW);
                assertActive(conn, STAT_RANGE_VIEW);
                assertNoRefreshFaults(serverMain, STAT_ROWS_VIEW);
                assertNoRefreshFaults(serverMain, STAT_RANGE_VIEW);
            }
        });
    }

    /**
     * Compares the view against a from-base recompute of the query it was created with, row for
     * row, both read through the client's QWP query client.
     */
    private void assertStatViewMatchesRecompute(String view, String query, String columns) {
        TestUtils.assertEquals(
                view + " must equal a from-base recompute",
                readRowsOverQwp("SELECT " + columns + " FROM (" + query + ") ORDER BY 1, 2"),
                readRowsOverQwp(statViewRowsSql(view, columns))
        );
    }

    private Connection connect() throws SQLException {
        return getConnection("admin", "quest", pgPort);
    }

    private Sender newBackfiller() {
        return Sender.builder(Sender.Transport.HTTP)
                .address("localhost:" + httpPort)
                .autoFlushRows(SENDER_BACKFILL_FLUSH_ROWS)
                .build();
    }

    private Sender newStreamer() {
        return Sender.builder(Sender.Transport.WEBSOCKET)
                .address("localhost:" + httpPort)
                .autoFlushRows(SENDER_STREAM_FLUSH_ROWS)
                .build();
    }

    /**
     * Every row of {@code sql} as the client's QWP query client decodes it, one line each,
     * tab-separated, in the order the query returns them. A DOUBLE prints its shortest round-trip
     * text, so two equal lines carry the same bits.
     */
    private String readRowsOverQwp(String sql) {
        final StringSink sink = new StringSink();
        final StringSink error = new StringSink();
        try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", httpPort)) {
            client.connect();
            client.execute(sql, new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    for (int row = 0, rowCount = batch.getRowCount(); row < rowCount; row++) {
                        for (int col = 0, columnCount = batch.getColumnCount(); col < columnCount; col++) {
                            if (col > 0) {
                                sink.put('\t');
                            }
                            if (batch.isNull(col, row)) {
                                sink.put("null");
                                continue;
                            }
                            switch (batch.getColumnWireType(col)) {
                                case QwpConstants.TYPE_DOUBLE -> sink.put(batch.getDoubleValue(col, row));
                                case QwpConstants.TYPE_SYMBOL, QwpConstants.TYPE_VARCHAR -> sink.put(batch.getString(col, row));
                                default -> Assert.fail("unexpected wire type " + batch.getColumnWireType(col)
                                        + " of column " + batch.getColumnName(col) + " in: " + sql);
                            }
                        }
                        sink.put('\n');
                    }
                }

                @Override
                public void onEnd(long totalRows) {
                }

                @Override
                public void onError(byte status, String message) {
                    error.put("status=").put(status).put(", message=").put(message);
                }

                @Override
                public void onFailoverReset(QwpServerInfo newNode) {
                    // The client re-submits the query from its first batch.
                    sink.clear();
                }
            });
        }
        Assert.assertEquals("QWP query failed: " + sql, "", error.toString());
        return sink.toString();
    }

    private TestServerMain startServer() {
        return startWithEnvVariables(
                PropertyKey.CAIRO_LIVE_VIEW_ENABLED.getEnvVarName(), "true",
                PropertyKey.LIVE_VIEW_REFRESH_WORKER_COUNT.getEnvVarName(), "2",
                PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS.getEnvVarName(), String.valueOf(CHECKPOINT_ROWS),
                PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false"
        );
    }

    /**
     * The rows the two senders write, and the count the base must reach. Deterministic, so a
     * failure replays as it ran.
     * <p>
     * The streamer walks one row per {@code (tick, symbol)}, in tick order, the symbols of a tick
     * in turn. A round stops half-way through the symbols of the tick that follows its own
     * {@link #SENDER_TICKS_PER_ROUND} ticks, and the next round carries on from there. A tick
     * carries more symbols the later its round, which is what grows the SYMBOL capacity.
     * <p>
     * The backfiller writes late rows below the frontier the streamer just left, each a whole
     * number of seconds past a tick and at a serial offset of its own, so no late row shares
     * {@code (sym, ts)} with any other row. A round's first late row lands one tick below the
     * frontier, inside the view's IN MEMORY hour; the rest spread over the closed segments below.
     */
    private static class SenderWorkload {
        private int backfillSerial;
        private int nextSymbol;
        private int nextTick;
        private long rowCount;

        private static double price(int tick, int symbol) {
            return (tick * 31 + symbol * 7) % 97 + 1;
        }

        private static String senderSymbol(int symbol) {
            return "k" + (symbol < 10 ? "0" : "") + symbol;
        }

        // The symbols every tick of the round that owns it carries.
        private static int symbolsAt(int tick) {
            return Math.min(SENDER_SYMBOLS, SENDER_INITIAL_SYMBOL_CAPACITY - 1 + 2 * (tick / SENDER_TICKS_PER_ROUND));
        }

        private static long tickTs(int tick) {
            return FIRST_TS + tick * SENDER_TICK_MICROS;
        }

        private void backfill(Sender backfiller, int round) {
            // The tick the stream stopped in is the frontier; every late row sits below it.
            final int frontierTick = nextTick;
            for (int i = 0; i < SENDER_BACKFILL_ROWS_PER_ROUND; i++) {
                final int tick = i == 0 ? frontierTick - 1 : (round * 7 + i * 5) % (frontierTick - 1);
                final int symbol = (round * 3 + i * 5) % symbolsAt(tick);
                backfillSerial++;
                backfiller.table(SENDER_BASE)
                        .symbol("sym", senderSymbol(symbol))
                        .doubleColumn("price", 1000 + backfillSerial);
                if (round >= SENDER_NEW_FIELD_ROUND) {
                    backfiller.longColumn("qty", -backfillSerial);
                }
                backfiller.at(tickTs(tick) + backfillSerial * Micros.SECOND_MICROS, ChronoUnit.MICROS);
                rowCount++;
            }
            backfiller.flush();
        }

        private void stream(Sender streamer, int round) {
            final int stopTick = (round + 1) * SENDER_TICKS_PER_ROUND;
            final int stopSymbol = symbolsAt(stopTick) / 2;
            while (nextTick < stopTick || nextSymbol < stopSymbol) {
                streamRow(streamer, round);
            }
            streamer.flush();
        }

        private void streamRow(Sender streamer, int round) {
            streamer.table(SENDER_BASE)
                    .symbol("sym", senderSymbol(nextSymbol))
                    .doubleColumn("price", price(nextTick, nextSymbol));
            if (round >= SENDER_NEW_FIELD_ROUND) {
                streamer.longColumn("qty", nextTick);
            }
            streamer.at(tickTs(nextTick), ChronoUnit.MICROS);
            rowCount++;
            if (++nextSymbol == symbolsAt(nextTick)) {
                nextTick++;
                nextSymbol = 0;
            }
        }

        // One more row of the tick the stream stopped half-way through, on its own commit: on the
        // newest root's own timestamp, and below the cadence's row trigger, so no seal records it.
        private void streamTie(Sender streamer, int round) {
            streamRow(streamer, round);
            streamer.flush();
        }
    }

    /**
     * The rows the bounded-frame case writes, and the count the base must reach. Deterministic, so
     * a failure replays as it ran. Every value is a whole number, unless the workload is
     * fractional: then every price is a level per symbol plus an odd number of half cents, and
     * every size one plus a whole number of thousandths short of two, so no value is whole.
     * <p>
     * The streamer writes one row per {@code (tick, symbol)}, in tick order, the symbols of a tick
     * in turn, {@link #STAT_TICKS_PER_ROUND} ticks a round. Some rows leave px out and others qty,
     * at ticks that differ by symbol.
     * <p>
     * The backfiller writes late rows below the frontier the streamer just left. A round's first
     * late row lands in the tick just below the frontier; the rest spread over older ticks. Each
     * sits one second and a serial number of milliseconds past its tick, so no late row shares
     * {@code (sym, ts)} with any other row. Every fourth late row leaves px out.
     */
    private static class StatWorkload {
        private final boolean isFractional;
        private int backfillSerial;
        private int nextTick;
        private int round;
        private long rowCount;

        private StatWorkload(boolean isFractional) {
            this.isFractional = isFractional;
        }

        // A price within two units of its symbol's level, in cents plus half a cent, so the
        // frame's mean dwarfs its spread and the variance formulas cancel most of their digits.
        private static double fractionalPx(int symbol, int cents) {
            return 1000.005 + symbol * 250 + cents % 200 * 0.01;
        }

        // One plus a whole number of thousandths short of two.
        private static double fractionalQty(int thousandths) {
            return 1 + (thousandths % 999 + 1) * 0.001;
        }

        private static boolean isPxNull(int tick, int symbol) {
            return (tick + 2 * symbol) % 7 == 3;
        }

        private static boolean isQtyNull(int tick, int symbol) {
            return (tick + symbol) % 9 == 5;
        }

        private static String statSymbol(int symbol) {
            return "x" + symbol;
        }

        private static long tickTs(int tick) {
            return FIRST_TS + tick * STAT_TICK_MICROS;
        }

        private void backfill(Sender backfiller) {
            // The tick the stream stopped at is the frontier; every late row sits below it.
            final int frontierTick = nextTick;
            for (int i = 0; i < STAT_BACKFILL_ROWS_PER_ROUND; i++) {
                final int tick = i == 0 ? frontierTick - 1 : (round * 7 + i * 3) % (frontierTick - 1);
                final int symbol = (round + i) % STAT_SYMBOLS;
                backfillSerial++;
                backfiller.table(STAT_BASE).symbol("sym", statSymbol(symbol));
                if (backfillSerial % 4 != 0) {
                    backfiller.doubleColumn("px", backfillPx(symbol));
                }
                backfiller.doubleColumn("qty", backfillQty());
                backfiller.at(
                        tickTs(tick) + Micros.SECOND_MICROS + backfillSerial * Micros.MILLI_MICROS,
                        ChronoUnit.MICROS
                );
                rowCount++;
            }
            backfiller.flush();
            round++;
        }

        private double backfillPx(int symbol) {
            return isFractional ? fractionalPx(symbol, backfillSerial * 7) : 100 + backfillSerial % 50;
        }

        private double backfillQty() {
            return isFractional ? fractionalQty(backfillSerial * 13) : 50 + backfillSerial % 30;
        }

        private double px(int tick, int symbol) {
            return isFractional ? fractionalPx(symbol, tick * 37 + symbol * 11) : (tick * 31 + symbol * 7) % 97 + 1;
        }

        private double qty(int tick, int symbol) {
            return isFractional ? fractionalQty(tick * 17 + symbol * 11) : (tick * 17 + symbol * 11) % 89 + 1;
        }

        private void stream(Sender streamer) {
            final int stopTick = nextTick + STAT_TICKS_PER_ROUND;
            for (; nextTick < stopTick; nextTick++) {
                for (int symbol = 0; symbol < STAT_SYMBOLS; symbol++) {
                    streamer.table(STAT_BASE).symbol("sym", statSymbol(symbol));
                    if (!isPxNull(nextTick, symbol)) {
                        streamer.doubleColumn("px", px(nextTick, symbol));
                    }
                    if (!isQtyNull(nextTick, symbol)) {
                        streamer.doubleColumn("qty", qty(nextTick, symbol));
                    }
                    streamer.at(tickTs(nextTick), ChronoUnit.MICROS);
                    rowCount++;
                }
            }
            streamer.flush();
        }
    }
}
