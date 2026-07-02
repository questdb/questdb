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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * REPLACE_RANGE data commits on a live view's base table. A replace-range
 * commit atomically deletes every base row inside {@code [rangeLo, rangeHi)}
 * and inserts the commit's own rows in their place. Unlike DROP PARTITION or
 * TRUNCATE (non-DATA operations, deliberately frozen by the LV), a replace
 * commit is a DATA commit: the refresh worker must converge the view onto the
 * post-replace base state, exactly as it does for a dedup replacement.
 * <p>
 * The hazard: the raw-WAL forward drain reads only the commit's inserted rows.
 * The deletion side of the replace is visible only through the commit's range
 * metadata. A replace whose inserted rows all sit above the view's frontier
 * (or a row-less pure-delete replace) presents nothing below
 * {@code latestSeenTs}, so a drain that never consults the range would
 * forward-append the new rows and keep the derived rows of the deleted band
 * forever - silent ghosts a recompute does not have. The drain therefore
 * treats the commit's range low (clamped to the view's lower bound) as its
 * effective minimum timestamp, non-strictly against the frontier, and routes
 * such commits to the O3 replay, which re-reads the applied (post-replace)
 * base.
 */
public class LiveViewBaseReplaceRangeTest extends AbstractCairoTest {

    // > FLUSH EVERY 100ms so a per-cycle clock bump never defers a flush.
    private static final long CLOCK_ADVANCE_MICROS = 250_000;

    // Pin the test clock below all test data before each test. A non-BACKFILL view's
    // lower bound is the CREATE wall-clock moment, and the forward-append refresh path
    // drops rows below it. The test data is timestamped in the past, so without a
    // pinned clock every row would be dropped as pre-CREATE.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testPureDeleteReplaceRangeRemovesEmittedRows() throws Exception {
        // A row-less replace commit is a pure delete: it carries no data rows at
        // all (its min timestamp reads Long.MAX_VALUE), so only the range
        // metadata reveals that emitted rows are gone. The view must drop the
        // deleted band's derived rows and renumber, not keep them as ghosts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3), " +
                        "('2026-01-01T00:00:04.000000Z', 4)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n4\n");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:02.000000Z"),
                            ts("2026-01-01T00:00:04.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:04.000000Z\t4\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeAboveFrontierAppendsForward() throws Exception {
        // A replace band strictly above every existing row deletes nothing the
        // view has seen: the commit's rows flow through the plain forward-append
        // path and the view converges without a replay.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:05.000000Z"), 5);
                    appendRow(walWriter, ts("2026-01-01T00:00:06.000000Z"), 6);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:04.000000Z"),
                            ts("2026-01-01T00:00:10.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t3\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t4\n" +
                            "2026-01-01T00:00:06.000000Z\t6\t5\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeCoveringPendingRowsConverges() throws Exception {
        // The view lags the base: two commits sit in the base WAL un-refreshed
        // when a replace commit covers them (the base apply may even skip them
        // entirely via calculateSkipTransactionCount, so their rows never
        // materialize in the base table). The drain first emits the pending
        // rows - raising the frontier into the replace band - and must then
        // route the replace commit to the replay, erasing the just-emitted
        // ghosts. The final view equals the recompute over the applied base.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n2\n");

                // Pending: committed to the base WAL, never refreshed into the view.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:03.000000Z', 3)");
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:04.000000Z', 4)");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:05.000000Z"), 5);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:03.000000Z"),
                            ts("2026-01-01T00:00:06.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                assertQuery("SELECT count() FROM base")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3\n");

                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeDeletingEmittedRowsBelowItsNewRowsConverges() throws Exception {
        // The ghost-row shape: the view has emitted rows at 00:02 and 00:03; a
        // replace commit covers [00:02, 00:05) but its only inserted row sits at
        // 00:04, ABOVE the frontier (00:03). The commit deletes the two emitted
        // rows from the base while presenting nothing out of order, so a drain
        // that ignores the range would forward-append the 00:04 row and keep the
        // deleted rows' derived output forever. The view must instead replay onto
        // the post-replace base: rows at 00:01 and 00:04, renumbered 1..2.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3\n");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:04.000000Z"), 4);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:02.000000Z"),
                            ts("2026-01-01T00:00:05.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                assertQuery("SELECT ts, x FROM base")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tx\n" +
                                "2026-01-01T00:00:01.000000Z\t1\n" +
                                "2026-01-01T00:00:04.000000Z\t4\n");

                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(SELECT ts, x, row_number() OVER () AS rn FROM base) ORDER BY 1",
                    "(lv) ORDER BY 1",
                    LOG,
                    true
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeOnDedupBaseConverges() throws Exception {
        // Same ghost shape over a DEDUP base, which routes refreshes through the
        // coupled applied-reader path (or its provably-clean raw-WAL fast path).
        // Whichever route the cycle takes must see the replace commit's deletion
        // through the range metadata and converge onto the post-replace base.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'A', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 'A', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 'A', 3)");
                drainWalQueue();
                refreshCycle(job);
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3\n");

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    TableWriter.Row row = walWriter.newRow(ts("2026-01-01T00:00:04.000000Z"));
                    row.putSym(1, "A");
                    row.putLong(2, 10);
                    row.append();
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:02.000000Z"),
                            ts("2026-01-01T00:00:05.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            // The bounded frame restarts over the post-replace base: the deleted
            // rows at 00:02 / 00:03 no longer contribute to the 00:04 row's sum.
            assertQuery("SELECT ts, sym, x, s FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\ts\n" +
                            "2026-01-01T00:00:01.000000Z\tA\t1\t1.0\n" +
                            "2026-01-01T00:00:04.000000Z\tA\t10\t11.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeStartingExactlyAtFrontierDeletesFrontierRow() throws Exception {
        // Boundary case for the non-strict frontier comparison: the replace band
        // starts EXACTLY at the frontier timestamp, so it deletes the frontier
        // row itself while its inserted row sits above. A strict range-low check
        // (rangeLo < latestSeenTs) would miss this commit and keep the deleted
        // frontier row as a ghost.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 1), " +
                        "('2026-01-01T00:00:02.000000Z', 2), " +
                        "('2026-01-01T00:00:03.000000Z', 3)");
                drainWalQueue();
                refreshCycle(job);

                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:04.000000Z"), 4);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:03.000000Z"),
                            ts("2026-01-01T00:00:05.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t1\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t2\n" +
                            "2026-01-01T00:00:04.000000Z\t4\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    // Appends one (ts, x) row through a WalWriter without committing; the caller
    // decides the commit mode (plain vs replace-range).
    private static void appendRow(WalWriter walWriter, long ts, long x) {
        TableWriter.Row row = walWriter.newRow(ts);
        row.putLong(1, x);
        row.append();
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    private static long ts(String timestamp) {
        return MicrosTimestampDriver.floor(timestamp);
    }

    private void assertLiveViewValid() {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertFalse("LV must stay valid after a base replace-range commit", instance.isInvalid());
    }

    // Pumps the refresh job until no further LV WAL work is produced, advancing
    // the clock each pass so deferred flushes land, and applying the LV's own
    // WAL after each burst.
    private void driveRefreshToQuiescence(LiveViewRefreshJob job) {
        for (int i = 0; i < 512; i++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed) {
                break;
            }
        }
    }

    // One refresh cycle past the FLUSH EVERY rate-limit: advances the clock so
    // the commit is not deferred, runs the job, and applies the LV WAL.
    private void refreshCycle(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainJob(job);
        drainWalQueue();
    }
}
