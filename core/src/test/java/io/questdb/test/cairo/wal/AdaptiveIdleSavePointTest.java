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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * An adaptive table keeps what its older save point names: the WAL above it, superseded and dropped partition
 * versions. On a table that gets no further write, one more save point about one save point interval after the
 * last one puts both save points on the current state, and the purges free the rest. A busy table takes no extra
 * save point, an idle table takes one per idle spell, and nothing happens outside ADAPTIVE, with no periodic save
 * points, or on a replica.
 */
public class AdaptiveIdleSavePointTest extends AbstractCairoTest {
    private static final long INTERVAL_MS = 1000;

    @Test
    public void testBusyTableTakesNoExtraSavePoint() throws Exception {
        // A write every half interval, each followed by a WAL purge pass: the save points stay one per interval.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken tt = engine.verifyTableName("t");
                final SavePointCounter counter = new SavePointCounter(tt);
                for (int i = 0; i < 12; i++) {
                    tick(INTERVAL_MS / 2);
                    execute("INSERT INTO t VALUES ('2024-01-01T00:00:0" + (i % 10) + ".000000Z', " + i + ")");
                    drainWalQueue();
                    counter.check();
                    drainPurgeJob();
                    drainWalQueue();
                    counter.check();
                }
                Assert.assertEquals("one save point per interval of writes", 6, counter.count);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testCleanRestartKeepsCommitWithoutWalTransaction() throws Exception {
        // A writer change that moves the table's txn but not its seqTxn (a partition removed, a partition converted
        // to Parquet, a row committed through the writer itself), made after the idle spell and followed by a clean
        // restart before the next idle save point: the restart keeps the change, since no WAL replay can redo it.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                final String[] changes = {"remove partition", "convert partition to parquet", "writer commit"};
                final ObjList<TableToken> tokens = new ObjList<>();
                for (int i = 0; i < changes.length; i++) {
                    tokens.add(createDays("t" + i, false));
                }
                for (int j = 0; j < 4; j++) {
                    idlePass(INTERVAL_MS, purgeJob);
                }
                // All changes after the idle spells, so no idle save point of another table's pass takes them.
                final ObjList<String> before = new ObjList<>();
                for (int i = 0; i < changes.length; i++) {
                    final TableToken tt = tokens.getQuick(i);
                    final SeqTxnTracker tracker = tracker(tt);
                    Assert.assertEquals(changes[i] + ", idle spell over", tracker.getPinnedEpochTxn(), tracker.getOtherPinnedEpochTxn());
                    try (TableWriter writer = getWriter(tt)) {
                        final long day = MicrosTimestampDriver.floor("2024-01-01");
                        switch (i) {
                            case 0 -> Assert.assertTrue(writer.removePartition(day));
                            case 1 -> {
                                Assert.assertTrue(writer.convertPartitionNativeToParquet(day, null, 0.01));
                                writer.advanceDurableEpochAfter("convert partition");
                            }
                            default -> {
                                final TableWriter.Row row = writer.newRow(day + 3_600_000_000L);
                                row.putLong(1, 5);
                                row.append();
                                writer.commit();
                            }
                        }
                    }
                    before.add(tableState(engine, tt));
                }
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                engine.releaseInactiveTableSequencers();
                final StringSink failures = new StringSink();
                try (CairoEngine restarted = new CairoEngine(configuration)) {
                    for (int i = 0; i < tokens.size(); i++) {
                        final String after = tableState(restarted, tokens.getQuick(i));
                        if (!before.getQuick(i).equals(after)) {
                            failures.put(changes[i]).put(": before restart ").put(before.getQuick(i))
                                    .put(", after ").put(after).put('\n');
                        }
                    }
                }
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleLiveViewPurgesWalBelowNewestSavePoint() throws Exception {
        // A live view takes save points of its own WAL, applied by its refresh worker. Once its base table gets no
        // further write, the refresh worker takes the idle save point, and the view's WAL that only the older save
        // point needed goes, with no write to the base table.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            // Below the rows, so the view materializes them.
            setCurrentMicros(MicrosTimestampDriver.floor("2026-05-31T00:00:00.000000Z"));
            try (LiveViewRefreshJob refreshJob = new LiveViewRefreshJob(0, engine, 1)) {
                execute("CREATE TABLE base (ts TIMESTAMP, x INT, pg SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                        + "SELECT ts, x, count(*) OVER (PARTITION BY pg ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
                final TableToken lv = engine.verifyTableName("lv");
                // Three base writes an interval apart: each flush writes a block of the view's WAL, and each apply of
                // it takes a save point.
                for (int i = 0; i < 3; i++) {
                    tick(INTERVAL_MS);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:0" + i + ":00.000000Z', " + (i + 1) + ")");
                    drainWalQueue();
                    runJob(refreshJob);
                    tick(INTERVAL_MS);
                    runJob(refreshJob);
                    engine.releaseInactive();
                }
                final SeqTxnTracker tracker = tracker(lv);
                Assert.assertNotEquals("two save points apart", tracker.getPinnedEpochTxn(), tracker.getOtherPinnedEpochTxn());
                // The older save point keeps the WAL of the newest block, above it.
                final ObjList<String> kept = walDirs(lv);
                Assert.assertTrue(kept.size() > 0);
                for (int i = 0; i < 2; i++) {
                    tick(INTERVAL_MS);
                    drainPurgeJob();
                    runJob(refreshJob);
                    engine.releaseInactive();
                }
                final StringSink failures = new StringSink();
                if (tracker.getPinnedEpochTxn() != tracker.getOtherPinnedEpochTxn()) {
                    failures.put("live view idle for two intervals: its save points differ\n");
                }
                checkOnDisk(failures, "live view idle for two intervals", lv, kept, false);
                Assert.assertEquals("", failures.toString());
                execute("DROP LIVE VIEW lv");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleRequestNotTakenIsAskedOncePerInterval() throws Exception {
        // The idle save point cannot be taken while another holder keeps the writer: the purge job asks again one
        // interval after its last request, not at every pass, and the save point is taken once the writer is free.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                final TableToken tt = createDays("t", false);
                int requests = 0;
                try (TableWriter ignore = engine.getWriter(tt, "rebase")) {
                    Assert.assertNotNull(ignore);
                    long lastRequestTs = tracker(tt).getIdleEpochRequestTs();
                    // Eight passes over four intervals.
                    for (int i = 0; i < 8; i++) {
                        idlePass(INTERVAL_MS / 2, null);
                        if (tracker(tt).getIdleEpochRequestTs() != lastRequestTs) {
                            lastRequestTs = tracker(tt).getIdleEpochRequestTs();
                            requests++;
                        }
                    }
                }
                Assert.assertEquals("requests over four intervals", 4, requests);
                for (int i = 0; i < 2; i++) {
                    idlePass(INTERVAL_MS, null);
                }
                Assert.assertEquals("taken once the writer is free", tracker(tt).getPinnedEpochTxn(), tracker(tt).getOtherPinnedEpochTxn());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleRequestRacingSuspensionTakesNoSavePoint() throws Exception {
        // The purge job asks, then the table is suspended before the apply run: the run neither applies nor takes
        // a save point on the suspended table. After RESUME WAL the next pass asks again.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                final TableToken tt = createDays("t", false);
                final SavePointCounter counter = new SavePointCounter(tt);
                tick(INTERVAL_MS);
                drainPurgeJob();
                Assert.assertTrue(tracker(tt).isIdleEpochRequested());
                engine.getTableSequencerAPI().suspendTable(tt, ErrorTag.NONE, "test");
                drainWalQueue();
                counter.check();
                Assert.assertEquals("save points on the suspended table", 0, counter.count);
                Assert.assertFalse("the request is consumed", tracker(tt).isIdleEpochRequested());
                execute("ALTER TABLE t RESUME WAL");
                for (int i = 0; i < 2; i++) {
                    idlePass(INTERVAL_MS, null);
                    counter.check();
                }
                Assert.assertEquals("save points after RESUME WAL", 1, counter.count);
                Assert.assertEquals(tracker(tt).getPinnedEpochTxn(), tracker(tt).getOtherPinnedEpochTxn());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleRequestWithWriterHeldLogsNoUnsolicitedLock() throws Exception {
        // The writer is held for another reason (as REBASE or a demote hold it) when the idle request's apply run
        // comes: with nothing to apply that is not abnormal, so no CRITICAL "unsolicited table lock". The
        // request is asked again later and taken once the writer is free.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            startClock();
            capture.start();
            try {
                final TableToken tt = createDays("t", false);
                try (TableWriter ignore = engine.getWriter(tt, "rebase")) {
                    Assert.assertNotNull(ignore);
                    idlePass(INTERVAL_MS, null);
                }
                capture.drain();
                capture.assertNotLogged("unsolicited table lock");
                for (int i = 0; i < 2; i++) {
                    idlePass(INTERVAL_MS, null);
                }
                Assert.assertEquals(tracker(tt).getPinnedEpochTxn(), tracker(tt).getOtherPinnedEpochTxn());
            } finally {
                capture.stop();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleRequestsReachEveryTableUnderChurn() throws Exception {
        // More tables go idle per pass than the cap allows: busy tables written every two intervals go idle in
        // between, and some tables are written twice and then never again. Each table that stays idle gets its
        // save point within a bounded number of passes, wherever it sits in the sweep order.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 1);
        assertMemoryLeak(() -> {
            startClock();
            // One job for every pass, as on a server: it carries the sweep position from one pass to the next.
            try (WalPurgeJob purgeJob = new WalPurgeJob(engine, configuration.getFilesFacade(), configuration.getMicrosecondClock())) {
                final int cap = WalPurgeJob.IDLE_SAVE_POINT_REQUESTS_PER_PASS;
                final int churnCount = 5 * cap;
                final int idleCount = cap;
                final int tableCount = churnCount + idleCount;
                final ObjList<TableToken> tables = new ObjList<>();
                for (int i = 0; i < tableCount; i++) {
                    execute("CREATE TABLE t" + i + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    tables.add(engine.verifyTableName("t" + i));
                }
                // Two save points per table, an interval apart, so each table's older save point is behind.
                for (int day = 1; day < 3; day++) {
                    tick(INTERVAL_MS);
                    for (int i = 0; i < tableCount; i++) {
                        execute("INSERT INTO t" + i + " VALUES ('2024-01-0" + day + "T00:00:00.000000Z', " + day + ")");
                    }
                    drainWalQueue();
                }
                tick(INTERVAL_MS);
                // Two rounds of tableCount / cap passes, and the pass that starts the second round at the head.
                final int maxPasses = 2 * ((tableCount + cap - 1) / cap) + 1;
                final int[] servedAt = new int[idleCount];
                Arrays.fill(servedAt, -1);
                for (int pass = 0; pass < 2 * maxPasses; pass++) {
                    for (int c = 0; c < churnCount; c++) {
                        if ((pass + c) % 4 == 0) {
                            execute("INSERT INTO t" + c + " VALUES ('2024-01-03T00:00:00.000000Z', " + pass + ")");
                        }
                    }
                    drainWalQueue();
                    purgeJob.drain(0);
                    drainWalQueue();
                    int served = 0;
                    for (int i = 0; i < idleCount; i++) {
                        final SeqTxnTracker tracker = tracker(tables.getQuick(churnCount + i));
                        if (servedAt[i] < 0 && tracker.getPinnedEpochTxn() == tracker.getOtherPinnedEpochTxn()) {
                            servedAt[i] = pass;
                        }
                        if (servedAt[i] >= 0) {
                            served++;
                        }
                    }
                    if (served == idleCount) {
                        break;
                    }
                    tick(INTERVAL_MS / 2);
                }
                final StringSink failures = new StringSink();
                for (int i = 0; i < idleCount; i++) {
                    if (servedAt[i] < 0 || servedAt[i] >= maxPasses) {
                        failures.put('t').put(churnCount + i).put(" has its idle save point at pass ").put(servedAt[i]).put('\n');
                    }
                }
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleRequestsSpreadOverPurgePasses() throws Exception {
        // Many tables go idle at once, as after a restart: each purge pass asks for a capped number of idle save
        // points, and the tables not asked are asked at the next passes until every table has its save point.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                final int cap = WalPurgeJob.IDLE_SAVE_POINT_REQUESTS_PER_PASS;
                final int tableCount = 2 * cap + cap / 2;
                final ObjList<TableToken> tables = new ObjList<>();
                for (int i = 0; i < tableCount; i++) {
                    execute("CREATE TABLE t" + i + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    tables.add(engine.verifyTableName("t" + i));
                }
                // Two save points per table, an interval apart, so each table's older save point is behind.
                for (int day = 1; day < 3; day++) {
                    tick(INTERVAL_MS);
                    for (int i = 0; i < tableCount; i++) {
                        execute("INSERT INTO t" + i + " VALUES ('2024-01-0" + day + "T00:00:00.000000Z', " + day + ")");
                    }
                    drainWalQueue();
                }
                tick(INTERVAL_MS);
                final int[] expected = {cap, cap, tableCount - 2 * cap, 0};
                for (int pass = 0; pass < expected.length; pass++) {
                    drainPurgeJob();
                    int asked = 0;
                    for (int i = 0; i < tableCount; i++) {
                        if (tracker(tables.getQuick(i)).isIdleEpochRequested()) {
                            asked++;
                        }
                    }
                    Assert.assertEquals("requests in pass " + pass, expected[pass], asked);
                    drainWalQueue();
                }
                for (int i = 0; i < tableCount; i++) {
                    final SeqTxnTracker tracker = tracker(tables.getQuick(i));
                    Assert.assertEquals("t" + i + " has its idle save point", tracker.getPinnedEpochTxn(), tracker.getOtherPinnedEpochTxn());
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleSpellTakesOneSavePoint() throws Exception {
        // After the last write's save point, several intervals of idleness take exactly one more, and the writer
        // close after them takes none. A last write that took no save point (a tail) first gets the save point the
        // close would have taken, then the one more.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                final StringSink failures = new StringSink();
                for (int tail = 0; tail < 2; tail++) {
                    final String table = "t" + tail;
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    final TableToken tt = engine.verifyTableName(table);
                    tick(INTERVAL_MS);
                    execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                    tick(tail == 0 ? INTERVAL_MS : INTERVAL_MS / 2);
                    execute("INSERT INTO " + table + " VALUES ('2024-01-01T01:00:00.000000Z', 2)");
                    drainWalQueue();
                    final SavePointCounter counter = new SavePointCounter(tt);
                    for (int i = 0; i < 10; i++) {
                        idlePass(INTERVAL_MS / 2, null);
                        counter.check();
                    }
                    engine.releaseAllWriters();
                    counter.check();
                    final int expected = tail == 0 ? 1 : 2;
                    if (counter.count != expected) {
                        failures.put(tail == 0 ? "last write took a save point" : "last write left a tail")
                                .put(": expected ").put(expected).put(" save points while idle, got ").put(counter.count).put('\n');
                    }
                    if (tracker(tt).getDurableEpochSeqTxn() != tracker(tt).getWriterTxn()) {
                        failures.put(table).put(": the newest save point is not at the applied seqTxn\n");
                    }
                }
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleTablePurgesWalBelowNewestSavePoint() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken tt = engine.verifyTableName("t");
                for (int i = 0; i < 3; i++) {
                    tick(INTERVAL_MS);
                    execute("INSERT INTO t VALUES ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
                    drainWalQueue();
                }
                engine.releaseAllWalWriters();
                drainPurgeJob();
                Assert.assertTrue("the older save point keeps the WAL above it", walExists(tt));
                tick(INTERVAL_MS / 2);
                drainPurgeJob();
                drainWalQueue();
                drainPurgeJob();
                Assert.assertTrue("idle for half an interval", walExists(tt));
                tick(INTERVAL_MS / 2);
                drainPurgeJob();
                drainWalQueue();
                drainPurgeJob();
                Assert.assertFalse("idle for one interval, the WAL is purged", walExists(tt));
                assertQuery("SELECT count() FROM t")
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                3
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleTableRemovesColumnVersionLeftByUpdate() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try (ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(engine)) {
                final TableToken tt = createDays("t", false);
                final ObjList<String> left = columnFiles(tt);
                tick(INTERVAL_MS);
                execute("UPDATE t SET v = 9 WHERE ts < '2024-01-02'");
                drainWalQueue();
                columnPurgeJob.run();
                final StringSink failures = new StringSink();
                checkOnDisk(failures, "the older save point names it", tt, left, true);
                idlePass(INTERVAL_MS / 2, null);
                columnPurgeJob.run();
                checkOnDisk(failures, "idle for half an interval", tt, left, true);
                idlePass(INTERVAL_MS / 2, null);
                columnPurgeJob.run();
                checkOnDisk(failures, "idle for one interval", tt, left, false);
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
            // The column purge job holds the writer of its own log table while open.
            assertQuery("SELECT sum(v) FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            26
                            """);
        });
    }

    @Test
    public void testIdleTableRemovesLeftPartitionVersions() throws Exception {
        // DROP PARTITION (by list and by a predicate), a TTL eviction and an O3 rewrite each leave a 2024-01-01
        // version that the older save point names. With no write after the change, it goes about one interval later.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        // The O3 rewrite leaves an older version of the partition only as a whole-partition rewrite: with
        // merge-append on (the harness default) the late row appends a piece to the same version. Pin the
        // production default.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            startClock();
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                final String[] changes = {
                        "ALTER TABLE %s DROP PARTITION LIST '2024-01-01'",
                        "ALTER TABLE %s DROP PARTITION WHERE ts < '2024-01-02'",
                        "INSERT INTO %s VALUES ('2024-01-04T12:00:00.000000Z', 4)",
                        "INSERT INTO %s VALUES ('2024-01-01T01:00:00.000000Z', 4)"
                };
                final StringSink failures = new StringSink();
                for (int i = 0; i < changes.length; i++) {
                    final String change = String.format(changes[i], "t" + i);
                    final TableToken tt = createDays("t" + i, i == 2);
                    final ObjList<String> left = versionDirs(tt);
                    tick(INTERVAL_MS);
                    execute(change);
                    drainWalQueue();
                    purgeJob.drain(0);
                    checkOnDisk(failures, change + ", the older save point names it", tt, left, true);
                    idlePass(INTERVAL_MS / 2, purgeJob);
                    checkOnDisk(failures, change + ", idle for half an interval", tt, left, true);
                    idlePass(INTERVAL_MS / 2, purgeJob);
                    checkOnDisk(failures, change + ", idle for one interval", tt, left, false);
                }
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleTableRemovesSupersededPostingFiles() throws Exception {
        // DEDUP commits into the partition a save point indexed reseal its covering posting index in place, so the
        // superseded value files wait for the save points that name them. With no write after the last commit,
        // they go after the idle save points, at the posting purge's next retry.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        // Only the whole-partition rewrite reseals the index in place: with merge-append on (the harness default)
        // the covering index grows by a generation per commit and supersedes no value file. Pin the production
        // default.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1);
        assertMemoryLeak(() -> {
            startClock();
            try (PostingSealPurgeJob postingPurgeJob = new PostingSealPurgeJob(engine)) {
                execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) "
                        + "PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, s)");
                // 2024-01-01, then four commits of ten rows into 2024-01-02, the first three an interval apart, each
                // with its own save point, the last one a tail.
                tick(INTERVAL_MS);
                execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 0), ('2024-01-02T00:00:00.000000Z', 'b', 0)");
                drainWalQueue();
                for (int i = 0; i < 4; i++) {
                    if (i < 3) {
                        tick(INTERVAL_MS);
                    }
                    final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
                    for (int j = 0; j < 10; j++) {
                        sql.append(j > 0 ? ", " : "").append("('2024-01-02T0").append(i + 1).append(":0").append(j)
                                .append(":00.000000Z', '").append((char) ('a' + j % 4)).append("', ").append(i * 10 + j).append(')');
                    }
                    execute(sql);
                    drainWalQueue();
                    runPostingPurgeJob(postingPurgeJob);
                }
                Assert.assertTrue("superseded posting files wait for the save points", postingPurgeJob.getOutstandingPurgeTasks() > 0);
                for (int i = 0; i < 3; i++) {
                    idlePass(INTERVAL_MS, null);
                    runPostingPurgeJob(postingPurgeJob);
                }
                Assert.assertEquals("superseded posting files left after three idle intervals", 0, postingPurgeJob.getOutstandingPurgeTasks());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testIdleTableRemovesVersionLeftByCommitWithoutWalTransaction() throws Exception {
        // A writer change that moves the table's txn but not its seqTxn (a partition removed, a partition converted
        // to Parquet, a row committed through the writer itself), made after the idle spell left both save points
        // on the same state, leaves a 2024-01-01 version that a save point names. It goes at the next idle save
        // point.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        // The writer's own late row leaves an older version of the partition only as a whole-partition rewrite:
        // with merge-append on (the harness default) it appends a piece to the same version. Pin the production
        // default.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            startClock();
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                final String[] changes = {"remove partition", "convert partition to parquet", "writer commit"};
                final StringSink failures = new StringSink();
                for (int i = 0; i < changes.length; i++) {
                    final TableToken tt = createDays("t" + i, false);
                    for (int j = 0; j < 4; j++) {
                        idlePass(INTERVAL_MS, purgeJob);
                    }
                    final SeqTxnTracker tracker = tracker(tt);
                    Assert.assertEquals(changes[i] + ", idle spell over", tracker.getPinnedEpochTxn(), tracker.getOtherPinnedEpochTxn());
                    final ObjList<String> left = versionDirs(tt);
                    try (TableWriter writer = getWriter(tt)) {
                        final long day = MicrosTimestampDriver.floor("2024-01-01");
                        switch (i) {
                            case 0 -> Assert.assertTrue(writer.removePartition(day));
                            case 1 -> {
                                // As ALTER TABLE CONVERT PARTITION and the cold storage switch do.
                                Assert.assertTrue(writer.convertPartitionNativeToParquet(day, null, 0.01));
                                writer.advanceDurableEpochAfter("convert partition");
                            }
                            default -> {
                                final TableWriter.Row row = writer.newRow(day + 3_600_000_000L);
                                row.putLong(1, 5);
                                row.append();
                                writer.commit();
                            }
                        }
                    }
                    purgeJob.drain(0);
                    checkOnDisk(failures, changes[i] + ", both save points name it", tt, left, true);
                    for (int j = 0; j < 2; j++) {
                        idlePass(INTERVAL_MS, purgeJob);
                    }
                    checkOnDisk(failures, changes[i] + ", idle for two intervals", tt, left, false);
                }
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testInvalidLiveViewTakesNoIdleRequest() throws Exception {
        // An invalid live view's refresh worker never takes its save point, so the request would stay pending and take
        // a slot of the per-pass cap every interval. The purge job does not ask it.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2026-05-31T00:00:00.000000Z"));
            try (LiveViewRefreshJob refreshJob = new LiveViewRefreshJob(0, engine, 1)) {
                execute("CREATE TABLE base (ts TIMESTAMP, x INT, pg SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                        + "SELECT ts, x, count(*) OVER (PARTITION BY pg ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");
                final TableToken lv = engine.verifyTableName("lv");
                for (int i = 0; i < 3; i++) {
                    tick(INTERVAL_MS);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:0" + i + ":00.000000Z', " + (i + 1) + ")");
                    drainWalQueue();
                    runJob(refreshJob);
                    tick(INTERVAL_MS);
                    runJob(refreshJob);
                    engine.releaseInactive();
                }
                final SeqTxnTracker tracker = tracker(lv);
                Assert.assertNotEquals("two save points apart", tracker.getPinnedEpochTxn(), tracker.getOtherPinnedEpochTxn());
                engine.getLiveViewRegistry().getViewInstance("lv").markInvalid("test", 0);
                int asked = 0;
                for (int i = 0; i < 4; i++) {
                    tick(INTERVAL_MS);
                    drainPurgeJob();
                    if (tracker.isIdleEpochRequested()) {
                        asked++;
                    }
                    runJob(refreshJob);
                    engine.releaseInactive();
                }
                Assert.assertEquals("requests to the invalid live view over four intervals", 0, asked);
                execute("DROP LIVE VIEW lv");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testNoIdleSavePointOnReplica() throws Exception {
        assertNoIdleSavePoint("adaptive", INTERVAL_MS, true);
    }

    @Test
    public void testNoIdleSavePointOutsideAdaptive() throws Exception {
        assertNoIdleSavePoint("nosync", INTERVAL_MS, false);
    }

    @Test
    public void testNoIdleSavePointWithoutPeriodicSavePoints() throws Exception {
        assertNoIdleSavePoint("adaptive", -1, false);
    }

    @Test
    public void testStartupColumnPurgeKeepsLiveVersionOfRewoundUpdate() throws Exception {
        // A purge log row whose update is past the committed txn, as a recovery rewind to a save point before the
        // update leaves it, names the version the table uses now. The startup purge neither deletes it nor keeps it
        // for a retry, so the running job does not delete it once both save points have moved past the update.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                final TableToken tt = createDays("t", false);
                final long updateTxn = tableTxn(tt) + 1;
                final Path liveFile = logUpdatePastCommittedTxn(tt);
                try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                    Assert.assertTrue("startup purge deleted " + liveFile, Files.exists(liveFile));
                    Assert.assertEquals("tasks kept for a retry", 0, job.getOutstandingPurgeTasks());
                    execute("INSERT INTO t VALUES ('2024-01-03T03:00:00.000000Z', 4)");
                    drainWalQueue();
                    for (int i = 0; i < 4; i++) {
                        idlePass(INTERVAL_MS / 2, null);
                        job.run();
                    }
                    final SeqTxnTracker tracker = tracker(tt);
                    Assert.assertTrue("save points past the update", tracker.getPinnedEpochTxn() >= updateTxn && tracker.getOtherPinnedEpochTxn() >= updateTxn);
                    tick(INTERVAL_MS);
                    job.run();
                    Assert.assertTrue("running purge deleted " + liveFile, Files.exists(liveFile));
                }
            } finally {
                setCurrentMicros(-1);
            }
            assertQuery("SELECT sum(v) FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            14
                            """);
        });
    }

    @Test
    public void testStartupColumnPurgeKeepsLiveVersionOfUpdateLostWithoutSavePoint() throws Exception {
        // The same row on a table without save points, as a power loss in nosync leaves it, or a nosync start
        // after an adaptive one: recovery rewinds the table and leaves adaptive, which drops the pins, before the
        // purge job starts. With no save point check to stop it, only the committed txn keeps the live version.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            startClock();
            try {
                final TableToken tt = createDays("t", false);
                final Path liveFile = logUpdatePastCommittedTxn(tt);
                Assert.assertEquals("pinned save point", -1, tracker(tt).getPinnedEpochTxn());
                try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                    Assert.assertTrue("startup purge deleted " + liveFile, Files.exists(liveFile));
                    Assert.assertEquals("tasks kept for a retry", 0, job.getOutstandingPurgeTasks());
                }
            } finally {
                setCurrentMicros(-1);
            }
            assertQuery("SELECT sum(v) FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            10
                            """);
        });
    }

    @Test
    public void testStartupColumnPurgeKeepsTwoHeldTasksApart() throws Exception {
        // The startup purge groups log rows into tasks by their timestamp. It writes each held task back at its own
        // original timestamp, so every later start reads the two tasks again instead of merging them into one.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                final TableToken tt = createDays("t", false);
                final ObjList<String> left = columnFiles(tt);
                tick(INTERVAL_MS);
                execute("UPDATE t SET v = 9 WHERE ts < '2024-01-02'");
                execute("UPDATE t SET v = 8 WHERE ts >= '2024-01-03'");
                drainWalQueue();
                final String logTable;
                try (ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(engine)) {
                    columnPurgeJob.run();
                    logTable = columnPurgeJob.getLogTableName();
                }
                final String openTasksSql = "SELECT count_distinct(ts) tasks, count() - count(completed) open FROM '" + logTable + "'";
                // No leak check: its engine.clear() would drop the table's txn tracker, which holds the pins.
                assertQuery(openTasksSql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                tasks\topen
                                2\t2
                                """);
                final StringSink failures = new StringSink();
                for (int restart = 1; restart <= 3; restart++) {
                    try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                        Assert.assertEquals("restart " + restart + ", tasks kept for a retry", 2, job.getOutstandingPurgeTasks());
                    }
                    checkOnDisk(failures, "restart " + restart, tt, left, true);
                    assertQuery(openTasksSql)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    tasks\topen
                                    2\t2
                                    """);
                }
                try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                    idlePass(INTERVAL_MS / 2, null);
                    idlePass(INTERVAL_MS / 2, null);
                    final SeqTxnTracker tracker = tracker(tt);
                    Assert.assertEquals("idle spell over", tracker.getPinnedEpochTxn(), tracker.getOtherPinnedEpochTxn());
                    tick(INTERVAL_MS);
                    job.run();
                    checkOnDisk(failures, "retry, both save points past the updates", tt, left, false);
                    Assert.assertEquals("retry, tasks kept for a retry", 0, job.getOutstandingPurgeTasks());
                }
                assertQuery(openTasksSql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                tasks\topen
                                2\t0
                                """);
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
            assertQuery("SELECT sum(v) FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            36
                            """);
        });
    }

    @Test
    public void testStartupColumnPurgeKeepsVersionNamedBySavePoint() throws Exception {
        // The column purge at server start skips the reader check, so the save point check alone keeps the column
        // version an UPDATE left while the older save point still names it. The startup purge then truncates its
        // log, so it writes the held task back, and the running job removes the files once the save points move on.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
        assertMemoryLeak(() -> {
            startClock();
            try {
                final TableToken tt = createDays("t", false);
                final ObjList<String> left = columnFiles(tt);
                tick(INTERVAL_MS);
                execute("UPDATE t SET v = 9 WHERE ts < '2024-01-02'");
                drainWalQueue();
                final String logTable;
                try (ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(engine)) {
                    columnPurgeJob.run();
                    logTable = columnPurgeJob.getLogTableName();
                }
                // No leak check: its engine.clear() would drop the table's txn tracker, which holds the pins.
                assertQuery("SELECT count() FROM '" + logTable + "' WHERE completed = null")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                1
                                """);
                final StringSink failures = new StringSink();
                checkOnDisk(failures, "logged for purge", tt, left, true);
                // A new job runs the logged purge in the startup mode, then truncates the log.
                try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                    checkOnDisk(failures, "startup purge, the older save point names it", tt, left, true);
                    Assert.assertEquals("startup purge, tasks kept for a retry", 1, job.getOutstandingPurgeTasks());
                    assertQuery("SELECT count() FROM '" + logTable + "' WHERE completed = null")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    1
                                    """);
                    tick(INTERVAL_MS / 10);
                    job.run();
                    checkOnDisk(failures, "retry, the older save point names it", tt, left, true);
                    Assert.assertEquals("retry, tasks kept for a retry", 1, job.getOutstandingPurgeTasks());
                    idlePass(INTERVAL_MS / 2, null);
                    idlePass(INTERVAL_MS / 2, null);
                    final SeqTxnTracker tracker = tracker(tt);
                    Assert.assertEquals("idle spell over", tracker.getPinnedEpochTxn(), tracker.getOtherPinnedEpochTxn());
                    tick(INTERVAL_MS);
                    job.run();
                    checkOnDisk(failures, "retry, both save points past the update", tt, left, false);
                    Assert.assertEquals("retry, tasks kept for a retry", 0, job.getOutstandingPurgeTasks());
                }
                assertQuery("SELECT count(), count(completed) FROM '" + logTable + "'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count\tcount1
                                1\t1
                                """);
                Assert.assertEquals("", failures.toString());
            } finally {
                setCurrentMicros(-1);
            }
            assertQuery("SELECT sum(v) FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            26
                            """);
        });
    }

    private static void checkOnDisk(StringSink failures, String context, TableToken tt, ObjList<String> dirs, boolean expected) {
        for (int i = 0, n = dirs.size(); i < n; i++) {
            if (Files.exists(Path.of(configuration.getDbRoot(), tt.getDirName(), dirs.getQuick(i))) != expected) {
                failures.put(context).put(": ").put(dirs.getQuick(i)).put(expected ? " is gone" : " is still on disk").put('\n');
            }
        }
    }

    // The files of column v in the 2024-01-01 partition now, relative to the table directory.
    private static ObjList<String> columnFiles(TableToken tt) throws Exception {
        final ObjList<String> files = new ObjList<>();
        final ObjList<String> dirs = versionDirs(tt);
        for (int i = 0, n = dirs.size(); i < n; i++) {
            final String dir = dirs.getQuick(i);
            try (Stream<Path> s = Files.list(Path.of(configuration.getDbRoot(), tt.getDirName(), dir))) {
                s.map(p -> p.getFileName().toString()).filter(n2 -> n2.startsWith("v.")).forEach(f -> files.add(dir + "/" + f));
            }
        }
        Assert.assertTrue(files.size() > 0);
        return files;
    }

    // One WAL purge pass, then the apply queue and the partition purge, after the clock moves on.
    private static void idlePass(long ms, O3PartitionPurgeJob purgeJob) {
        tick(ms);
        drainPurgeJob();
        drainWalQueue();
        if (purgeJob != null) {
            purgeJob.drain(0);
        }
    }

    // Logs a purge of the 2024-01-02 column v version the table uses now, for an update one txn past the committed
    // txn, and returns that version's data file.
    private static Path logUpdatePastCommittedTxn(TableToken tt) throws Exception {
        final String logTable;
        try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
            logTable = job.getLogTableName();
        }
        final long partitionTs = MicrosTimestampDriver.floor("2024-01-02");
        final long txn;
        final long columnVersion;
        final long partitionNameTxn;
        final long truncateVersion;
        try (TableReader reader = engine.getReader(tt)) {
            txn = reader.getTxn();
            columnVersion = reader.getColumnVersionReader().getColumnNameTxn(partitionTs, 1);
            partitionNameTxn = reader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTs);
            truncateVersion = reader.getTxFile().getTruncateVersion();
        }
        execute("INSERT INTO '" + logTable + "' VALUES (" + (currentMicros - 1000) + ", '" + tt.getDirName() + "', 'v', "
                + tt.getTableId() + ", " + truncateVersion + ", " + ColumnType.LONG + ", " + PartitionBy.DAY + ", "
                + (txn + 1) + ", " + columnVersion + ", " + partitionTs + ", " + partitionNameTxn + ", NULL)");
        final Path liveFile = Path.of(
                configuration.getDbRoot(),
                tt.getDirName(),
                partitionNameTxn > -1 ? "2024-01-02." + partitionNameTxn : "2024-01-02",
                columnVersion > -1 ? "v.d." + columnVersion : "v.d"
        );
        Assert.assertTrue(liveFile.toString(), Files.exists(liveFile));
        return liveFile;
    }

    // Runs the live view refresh job until it reports no work, in a bounded burst.
    private static void runJob(LiveViewRefreshJob job) {
        //noinspection StatementWithEmptyBody
        for (int i = 0; i < 64 && job.run(); i++) {
        }
    }

    private static void runPostingPurgeJob(PostingSealPurgeJob job) {
        for (int i = 0; i < 4; i++) {
            Os.sleep(1);
            job.run();
        }
    }

    private static void startClock() {
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
    }

    // The table's rows, partitions and txn as a reader of the given engine sees them.
    private static String tableState(CairoEngine eng, TableToken tt) {
        try (TableReader reader = eng.getReader(tt)) {
            return "rows=" + reader.size() + " partitions=" + reader.getPartitionCount() + " txn=" + reader.getTxn();
        }
    }

    private static long tableTxn(TableToken tt) {
        try (TableReader reader = engine.getReader(tt)) {
            return reader.getTxn();
        }
    }

    private static void tick(long ms) {
        setCurrentMicros(currentMicros + ms * 1000L);
    }

    private static SeqTxnTracker tracker(TableToken tt) {
        return engine.getTableSequencerAPI().getTxnTracker(tt);
    }

    // The 2024-01-01 directories on disk now.
    private static ObjList<String> versionDirs(TableToken tt) throws Exception {
        final ObjList<String> dirs = new ObjList<>();
        try (Stream<Path> s = Files.list(Path.of(configuration.getDbRoot(), tt.getDirName()))) {
            s.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("2024-01-01")).forEach(dirs::add);
        }
        Assert.assertTrue(dirs.size() > 0);
        return dirs;
    }

    // The table's WAL directories now, by WAL id.
    private static ObjList<String> walDirs(TableToken tt) throws Exception {
        final ObjList<String> dirs = new ObjList<>();
        try (Stream<Path> s = Files.list(Path.of(configuration.getDbRoot(), tt.getDirName()))) {
            s.map(p -> p.getFileName().toString()).filter(n -> n.matches("wal\\d+"))
                    .sorted(Comparator.comparingInt(n -> Integer.parseInt(n.substring(3)))).forEach(dirs::add);
        }
        return dirs;
    }

    private static boolean walExists(TableToken tt) {
        return Files.exists(Path.of(configuration.getDbRoot(), tt.getDirName(), "wal1"));
    }

    private void assertNoIdleSavePoint(String mode, long intervalMs, boolean replica) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, intervalMs);
        assertMemoryLeak(() -> {
            startClock();
            try {
                execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken tt = engine.verifyTableName("t");
                for (int i = 0; i < 2; i++) {
                    tick(INTERVAL_MS);
                    execute("INSERT INTO t VALUES ('2024-01-01T0" + i + ":00:00.000000Z', " + i + ")");
                    drainWalQueue();
                }
                if (replica) {
                    engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
                }
                final SavePointCounter counter = new SavePointCounter(tt);
                for (int i = 0; i < 6; i++) {
                    idlePass(INTERVAL_MS / 2, null);
                    counter.check();
                }
                Assert.assertEquals(mode + ", interval " + intervalMs + (replica ? ", replica" : ""), 0, counter.count);
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
                setCurrentMicros(-1);
            }
        });
    }

    // Three days in two batches a save point interval apart, so the table has two save points that both name
    // 2024-01-01.
    private TableToken createDays(String table, boolean ttl) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY"
                + (ttl ? " TTL 2 DAYS" : "") + " WAL");
        tick(INTERVAL_MS);
        execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-01T02:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2), ('2024-01-03T00:00:00.000000Z', 3)");
        drainWalQueue();
        tick(INTERVAL_MS);
        execute("INSERT INTO " + table + " VALUES ('2024-01-03T01:00:00.000000Z', 3)");
        drainWalQueue();
        return engine.verifyTableName(table);
    }

    // Counts save points by the pin slot, which each save point flips; at most one save point falls between checks.
    private static class SavePointCounter {
        private final TableToken tt;
        private int count;
        private boolean slotA;

        SavePointCounter(TableToken tt) {
            this.tt = tt;
            this.slotA = tracker(tt).isPinnedEpochSlotA();
        }

        void check() {
            final boolean a = tracker(tt).isPinnedEpochSlotA();
            if (a != slotA) {
                count++;
                slotA = a;
            }
        }
    }
}
