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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.TxnScoreboardV2;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * An ADAPTIVE table's durable-epoch pin must outlive idle eviction of its pooled {@link TxnScoreboard}.
 * <p>
 * The pin keeps every file the published epoch names on disk, because a restart rewinds the table to that
 * epoch and replays the WAL from it. The pin used to live only in the pooled scoreboard, which
 * {@code CairoEngine.releaseInactive()} (the maintenance job's call) frees as soon as nothing references
 * it; the replacement board came up blank, so the next O3 rewrite, UPDATE, dedup upsert, squash, DROP
 * PARTITION or TTL deleted files the epoch still named. A kill -9 before the next epoch then left the table
 * suspended with a partition missing or, when the lost version was the epoch's last partition, silently
 * serving zero rows.
 * <p>
 * Each scenario idles the table with {@code engine.releaseInactive()} -- not {@code releaseAllWriters()},
 * which keeps the pooled board and would hide the bug -- mutates it, then models a kill -9: no graceful close
 * epoch, fresh scoreboard and tracker, production recovery, WAL replay. The rewound table must read as the
 * epoch, and the replayed table as the pre-crash table.
 */
public class EpochPinEvictionTest extends AbstractCairoTest {
    private static final String BASE_INSERT = """
            insert into x values
            ('2024-01-01T00:00:00.000000Z', 1),
            ('2024-01-01T01:00:00.000000Z', 2),
            ('2024-01-01T02:00:00.000000Z', 3),
            ('2024-01-02T00:00:00.000000Z', 4),
            ('2024-01-02T01:00:00.000000Z', 5)
            """;
    private static final String BASE_ROWS = """
            ts\tv
            2024-01-01T00:00:00.000000Z\t1
            2024-01-01T01:00:00.000000Z\t2
            2024-01-01T02:00:00.000000Z\t3
            2024-01-02T00:00:00.000000Z\t4
            2024-01-02T01:00:00.000000Z\t5
            """;
    private static final String BASE_ROWS_WITH_LATE_ROW = """
            ts\tv
            2024-01-01T00:00:00.000000Z\t1
            2024-01-01T00:30:00.000000Z\t99
            2024-01-01T01:00:00.000000Z\t2
            2024-01-01T02:00:00.000000Z\t3
            2024-01-02T00:00:00.000000Z\t4
            2024-01-02T01:00:00.000000Z\t5
            """;
    private static final String CREATE_TABLE = "create table x (ts timestamp, v long) timestamp(ts) partition by day wal";
    private static final long FEB_1 = 1_706_745_600_000_000L; // 2024-02-01T00:00:00Z
    private static final String LATE_ROW_INSERT = "insert into x values ('2024-01-01T00:30:00.000000Z', 99)";
    private static final long MINUTE_MICROS = 60_000_000L;

    @Test
    public void testCloseEpochThenLateRowUnderDefaultCadence() throws Exception {
        // Pure defaults (60 s interval, 5M rows), no injection. The idle tick closes the writer, whose
        // close-time epoch covers an un-epoched tail and restarts the cadence clock, then frees the board.
        // A late row 10 s later is inside the interval, so its batch publishes no epoch and the window stays
        // open until the next write.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final long t0 = 1_790_000_000_000_000L;
        assertMemoryLeak(() -> {
            try {
                Assert.assertEquals(60_000L, engine.getConfiguration().getAdaptiveEpochIntervalMs());
                setCurrentMicros(t0);
                execute(CREATE_TABLE);
                execute("""
                        insert into x values
                        ('2024-01-01T00:00:00.000000Z', 1),
                        ('2024-01-01T01:00:00.000000Z', 2),
                        ('2024-01-01T02:00:00.000000Z', 3),
                        ('2024-01-02T00:00:00.000000Z', 4)
                        """);
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("x");

                setCurrentMicros(t0 + MINUTE_MICROS / 2);
                execute("insert into x values ('2024-01-02T01:00:00.000000Z', 5)");
                drainWalQueue();

                setCurrentMicros(t0 + 12 * MINUTE_MICROS);
                evictIdleScoreboard(tt);
                Assert.assertEquals(
                        "the close-time epoch must cover the tail",
                        2,
                        engine.getTableSequencerAPI().getTxnTracker(tt).getDurableEpochSeqTxn()
                );

                setCurrentMicros(t0 + 12 * MINUTE_MICROS + 10_000_000L);
                execute(LATE_ROW_INSERT);
                drainWalQueue();
                assertPartitionDirExists(tt, "2024-01-01");

                killAndRestart(tt);
                assertTable(BASE_ROWS);
                replayWal(tt);
                assertTable(BASE_ROWS_WITH_LATE_ROW);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDedupUpsertAfterIdleEviction() throws Exception {
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal dedup upsert keys(ts)");
            execute(BASE_INSERT);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            evictIdleScoreboard(tt);

            execute("insert into x values ('2024-01-01T01:00:00.000000Z', 222)");
            drainWalQueue();
            assertPartitionDirExists(tt, "2024-01-01");

            killAndRestart(tt);
            assertTable(BASE_ROWS);
            replayWal(tt);
            assertTable("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-01T01:00:00.000000Z\t222
                    2024-01-01T02:00:00.000000Z\t3
                    2024-01-02T00:00:00.000000Z\t4
                    2024-01-02T01:00:00.000000Z\t5
                    """);
        });
    }

    @Test
    public void testDropPartitionAfterIdleEviction() throws Exception {
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute(CREATE_TABLE);
            execute(BASE_INSERT);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            evictIdleScoreboard(tt);

            execute("alter table x drop partition list '2024-01-01'");
            drainWalQueue();
            assertPartitionDirExists(tt, "2024-01-01");

            killAndRestart(tt);
            assertTable(BASE_ROWS);
            replayWal(tt);
            assertTable("""
                    ts\tv
                    2024-01-02T00:00:00.000000Z\t4
                    2024-01-02T01:00:00.000000Z\t5
                    """);
        });
    }

    @Test
    public void testLateRowIntoLastPartitionAfterIdleEviction() throws Exception {
        // The lost version would be the epoch's LAST partition. Before the fix, a production JVM (-da) replayed
        // "successfully" over a re-created, zero-filled partition and served the rows as 1970-01-01 zeros.
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute(CREATE_TABLE);
            execute("""
                    insert into x values
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T01:00:00.000000Z', 2),
                    ('2024-01-01T02:00:00.000000Z', 3)
                    """);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            evictIdleScoreboard(tt);

            execute(LATE_ROW_INSERT);
            drainWalQueue();
            assertPartitionDirExists(tt, "2024-01-01");

            killAndRestart(tt);
            assertTable("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-01T01:00:00.000000Z\t2
                    2024-01-01T02:00:00.000000Z\t3
                    """);
            replayWal(tt);
            assertTable("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-01T00:30:00.000000Z\t99
                    2024-01-01T01:00:00.000000Z\t2
                    2024-01-01T02:00:00.000000Z\t3
                    """);
        });
    }

    @Test
    public void testLateRowIntoNonLastPartitionAfterIdleEviction() throws Exception {
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute(CREATE_TABLE);
            execute(BASE_INSERT);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            evictIdleScoreboard(tt);

            execute(LATE_ROW_INSERT);
            drainWalQueue();
            assertPartitionDirExists(tt, "2024-01-01");

            killAndRestart(tt);
            assertTable(BASE_ROWS);
            replayWal(tt);
            assertTable(BASE_ROWS_WITH_LATE_ROW);
        });
    }

    @Test
    public void testPartitionRemovalHonoursTrackerPinMissingFromScoreboard() throws Exception {
        // Defense in depth, independent of idle eviction: the synchronous removal gate must consult the
        // tracker's record of the pin, as the async purge jobs do, so a scoreboard that loses the pin some
        // other way still cannot turn into an inline delete of a partition version the epoch names.
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute(CREATE_TABLE);
            execute(BASE_INSERT);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            dropEpochPinFromScoreboard(tt);

            execute(LATE_ROW_INSERT);
            drainWalQueue();
            assertPartitionDirExists(tt, "2024-01-01");

            killAndRestart(tt);
            assertTable(BASE_ROWS);
            replayWal(tt);
            assertTable(BASE_ROWS_WITH_LATE_ROW);
        });
    }

    @Test
    public void testRecoveryPinSurvivesIdleEvictionBeforeReplay() throws Exception {
        // The pin recovery places sits in a board nothing references until the table is next opened, and the
        // maintenance job's first run at startup evicts it. Replay then rewrote the epoch's day 1 inline, and
        // a second kill before the replay's own epoch broke the table.
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            try {
                execute(CREATE_TABLE);
                execute(BASE_INSERT);
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("x");
                execute(LATE_ROW_INSERT);
                drainWalQueue();

                killAndRestart(tt);
                evictIdleScoreboard(tt);

                // Model the second kill anywhere before the replay batch's epoch marker.
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
                replayWal(tt);
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
                assertPartitionDirExists(tt, "2024-01-01");

                killAndRestart(tt);
                assertTable(BASE_ROWS);
                replayWal(tt);
                assertTable(BASE_ROWS_WITH_LATE_ROW);
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    @Test
    public void testSplitSquashAfterIdleEviction() throws Exception {
        // The first late batch rewrites the day, the second splits it, and the squash that follows folds the
        // split back into a new version of the day; the roll into the next day seals it.
        setAdaptiveWithFirstBatchEpochOnly();
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1);
        assertMemoryLeak(() -> {
            execute("create table x (i int, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x select cast(x as int) i, timestamp_sequence('2024-02-01T00', 60 * 1000000L) ts from long_sequence(600)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            final String epochRows = snapshot();
            evictIdleScoreboard(tt);

            execute("insert into x select cast(9000 + x as int) i, timestamp_sequence('2024-02-01T05:00', 1000000L) ts from long_sequence(50)");
            drainWalQueue();
            execute("insert into x select cast(8000 + x as int) i, timestamp_sequence('2024-02-01T07:00', 1000000L) ts from long_sequence(50)");
            drainWalQueue();
            execute("insert into x select cast(7000 + x as int) i, timestamp_sequence('2024-02-02T00:00', 1000000L) ts from long_sequence(5)");
            drainWalQueue();
            assertPartitionDirExists(tt, "2024-02-01");
            final String liveRows = snapshot();

            killAndRestart(tt);
            assertTable(epochRows);
            replayWal(tt);
            assertTable(liveRows);
        });
    }

    @Test
    public void testSquashCopiesWhenTrackerPinMissingFromScoreboard() throws Exception {
        // Defense in depth for the squash gate: an epoch pin the scoreboard lost must still classify the
        // range as held by the epoch, so the squash copies into a new partition version instead of appending
        // into the existing one in place.
        setAdaptiveWithFirstBatchEpochOnly();
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1);
        assertMemoryLeak(() -> {
            execute("create table x (i int, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x select cast(x as int) i, timestamp_sequence('2024-02-01T00', 60 * 1000000L) ts from long_sequence(600)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            final String epochRows = snapshot();
            dropEpochPinFromScoreboard(tt);

            execute("insert into x select cast(9000 + x as int) i, timestamp_sequence('2024-02-01T05:00', 1000000L) ts from long_sequence(50)");
            drainWalQueue();
            final long versionBeforeSquash = liveNameTxn(tt, FEB_1);
            // Splits the day again, and the squash that follows folds the split back into the day.
            execute("insert into x select cast(8000 + x as int) i, timestamp_sequence('2024-02-01T07:00', 1000000L) ts from long_sequence(50)");
            drainWalQueue();
            Assert.assertNotEquals(
                    "the squash must copy into a new version of the day, not append into the existing one",
                    versionBeforeSquash,
                    liveNameTxn(tt, FEB_1)
            );
            final String liveRows = snapshot();

            killAndRestart(tt);
            assertTable(epochRows);
            replayWal(tt);
            assertTable(liveRows);
        });
    }

    @Test
    public void testTtlAfterIdleEviction() throws Exception {
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day ttl 2 days wal");
            execute(BASE_INSERT);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            evictIdleScoreboard(tt);

            execute("insert into x values ('2024-01-05T00:00:00.000000Z', 7)");
            drainWalQueue();
            assertPartitionDirExists(tt, "2024-01-01");

            killAndRestart(tt);
            assertTable(BASE_ROWS);
            replayWal(tt);
            assertTable("""
                    ts\tv
                    2024-01-05T00:00:00.000000Z\t7
                    """);
        });
    }

    @Test
    public void testUpdateAfterIdleEviction() throws Exception {
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute(CREATE_TABLE);
            execute(BASE_INSERT);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            evictIdleScoreboard(tt);

            execute("update x set v = v + 100 where ts < '2024-01-02'");
            drainWalQueue();
            assertPartitionFileExists(tt, "2024-01-01", "v.d");

            killAndRestart(tt);
            assertTable(BASE_ROWS);
            replayWal(tt);
            assertTable("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t101
                    2024-01-01T01:00:00.000000Z\t102
                    2024-01-01T02:00:00.000000Z\t103
                    2024-01-02T00:00:00.000000Z\t4
                    2024-01-02T01:00:00.000000Z\t5
                    """);
        });
    }

    @Test
    public void testUpdateHonoursTrackerPinMissingFromScoreboard() throws Exception {
        // Defense in depth for the synchronous column purge UPDATE runs after its commit.
        setAdaptiveWithFirstBatchEpochOnly();
        assertMemoryLeak(() -> {
            execute(CREATE_TABLE);
            execute(BASE_INSERT);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            dropEpochPinFromScoreboard(tt);

            execute("update x set v = v + 100 where ts < '2024-01-02'");
            drainWalQueue();
            assertPartitionFileExists(tt, "2024-01-01", "v.d");

            killAndRestart(tt);
            assertTable(BASE_ROWS);
            replayWal(tt);
            assertTable("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t101
                    2024-01-01T01:00:00.000000Z\t102
                    2024-01-01T02:00:00.000000Z\t103
                    2024-01-02T00:00:00.000000Z\t4
                    2024-01-02T01:00:00.000000Z\t5
                    """);
        });
    }

    private static void assertPartitionDirExists(TableToken tt, String partitionDir) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tt).concat(partitionDir);
            Assert.assertTrue(
                    "the partition version the durable epoch names must survive [path=" + path + ']',
                    configuration.getFilesFacade().exists(path.$())
            );
        }
    }

    private static void assertPartitionFileExists(TableToken tt, String partitionDir, String fileName) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tt).concat(partitionDir).concat(fileName);
            Assert.assertTrue(
                    "the column version the durable epoch names must survive [path=" + path + ']',
                    configuration.getFilesFacade().exists(path.$())
            );
        }
    }

    // Strips the epoch pin from the pooled scoreboard the table writer holds, leaving the tracker's record
    // of it intact: a scoreboard that lost its pin through some lifecycle gap other than idle eviction.
    private static void dropEpochPinFromScoreboard(TableToken tt) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
        final long pinnedTxn = tracker.getPinnedEpochTxn();
        Assert.assertTrue("the first batch must publish and pin an epoch", pinnedTxn > -1);
        try (TxnScoreboard scoreboard = engine.getTxnScoreboard(tt)) {
            scoreboard.releaseTxn(tracker.isPinnedEpochSlotA() ? TxnScoreboard.EPOCH_ID_A : TxnScoreboard.EPOCH_ID_B, pinnedTxn);
            Assert.assertEquals(0, ((TxnScoreboardV2) scoreboard).getActiveReaderCount(pinnedTxn));
        }
        Assert.assertEquals(pinnedTxn, tracker.getPinnedEpochTxn());
    }

    // What EngineMaintenanceJob does to an idle table: close its writer and readers, then free every
    // scoreboard nothing references. Asserts the pooled board really was replaced, so no scenario can pass
    // because the old board happened to stay pooled.
    private static void evictIdleScoreboard(TableToken tt) {
        final TxnScoreboard before;
        try (TxnScoreboard scoreboard = engine.getTxnScoreboard(tt)) {
            before = scoreboard;
        }
        engine.releaseInactive();
        try (TxnScoreboard scoreboard = engine.getTxnScoreboard(tt)) {
            Assert.assertNotSame("idle eviction must free the pooled scoreboard", before, scoreboard);
        }
    }

    // kill -9 and restart: no graceful close epoch; the process's scoreboards and sequencer trackers die with
    // it; then the production recovery pass. WAL replay is left to the caller.
    private static void killAndRestart(TableToken tt) {
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
        } finally {
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
        engine.getTxnScoreboardPool().remove(tt);
        engine.getTableSequencerAPI().resetForReboot(tt);
        new RecoveryCoordinator(engine).recover();
    }

    private static long liveNameTxn(TableToken tt, long partitionTimestamp) {
        try (Path path = new Path(); TxReader txReader = new TxReader(configuration.getFilesFacade())) {
            path.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            Assert.assertTrue(txReader.unsafeLoadAll());
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    return txReader.getPartitionNameTxn(i);
                }
            }
        }
        throw new AssertionError("no such partition: " + partitionTimestamp);
    }

    private static void replayWal(TableToken tt) {
        engine.notifyWalTxnRepublisher(tt);
        drainWalQueue();
        Assert.assertFalse("WAL replay must not suspend the table", engine.getTableSequencerAPI().isSuspended(tt));
    }

    // Only the first apply batch publishes an epoch (a fresh table's first batch always does); every later
    // batch stays inside the epoch window.
    private static void setAdaptiveWithFirstBatchEpochOnly() {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1_000_000_000L);
    }

    private static String snapshot() throws Exception {
        printSql("x");
        return sink.toString();
    }

    private void assertTable(String expected) throws Exception {
        // noLeakCheck: the builder's own leak check starts with engine.clear(), which would drop the pins and
        // trackers these scenarios are about. The enclosing assertMemoryLeak() covers leaks.
        assertQuery("x").noLeakCheck().timestamp("ts").expectSize().returns(expected);
        // A reader opened between recovery and replay holds the board; production has none at that point.
        engine.releaseAllReaders();
    }
}
