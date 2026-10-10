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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
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
    public void testIdleTableRemovesVersionLeftByCommitWithoutWalTransaction() throws Exception {
        // A writer change that moves the table's txn but not its seqTxn (a partition removed, a partition converted
        // to Parquet, a row committed through the writer itself), made after the idle spell left both save points
        // on the same state, leaves a 2024-01-01 version that a save point names. It goes at the next idle save
        // point.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, INTERVAL_MS);
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

    private static void startClock() {
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
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
