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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * An adaptive replica ({@link LocalDurabilityPolicy#REPLICA_SKIP}) advances no durable epoch and keeps no WAL
 * purge floor, and nothing downloads purged WAL again. So a replica must never keep an anchor that startup would
 * rewind a table to: the replay would need WAL the purge job deleted, and the table would come back suspended
 * and truncated to the anchor. Each test models one Enterprise lifecycle sequence with the OSS primitives
 * Enterprise calls:
 * <ul>
 *   <li>ReplicaRoleState ctor: {@code engine.setLocalDurabilityPolicy(REPLICA_SKIP)}</li>
 *   <li>ReplicaRoleState.openLoops (the demote clear): mark for enrolment, then remove the anchor, for every
 *       table present at that moment</li>
 *   <li>WalEvents.registerTable: the same enrolment the OSS CREATE path performs (modelled here by CREATE)</li>
 *   <li>ReplicaRoleState.close / PrimaryRoleState ctor: {@code setLocalDurabilityPolicy(ALWAYS_ON)}</li>
 *   <li>boot: {@code RecoveryCoordinator.recover()} under ALWAYS_ON, before any role state exists</li>
 * </ul>
 * Every scenario purges applied WAL and then restarts. The table must come back with every applied row, and
 * must not be suspended.
 */
public class ReplicaSkipAnchorTest extends AbstractCairoTest {

    @Test
    public void testAlwaysOnLongEpochIntervalKeepsWalAndReplays() throws Exception {
        // Control: the primary's tracker floor keeps the WAL above a long-lived anchor, and recovery replays it.
        run(() -> {
            setAdaptive(true);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000L);
            execute("create table ctl_long (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("ctl_long");
            insertAndRelease("ctl_long", 1, 5, false);
            drainPurgeJob();
            crashRestartAndAssertNoLoss(tt, false, 5);
        });
    }

    @Test
    public void testAnchorOnDiskUnderReplicaSkipKeepsItsWal() throws Exception {
        // An anchor still on disk when the node becomes a replica: here the table goes idle before the demote
        // clear gets to it (or the clear fails). The primary's floor kept WAL 2..3 above the anchor at 1. Once
        // REPLICA_SKIP drops the tracker floor, the purge job must still keep that WAL, because startup rewinds
        // the table to the anchor and replays it.
        run(() -> {
            setAdaptive(true);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000L);
            execute("create table idle_anchor (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("idle_anchor");
            // The first batch epochs (lastEpochTs == 0); the next two stay above the anchor, and the writer stays
            // pooled, so no close-time epoch catches them up.
            insertAndRelease("idle_anchor", 1, 3, false);
            Assert.assertEquals("setup: anchor must lag the applied WAL", 1, readAnchorSeqTxn(tt));
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            drainPurgeJob();
            Assert.assertEquals("the anchor must still be on disk", 1, readAnchorSeqTxn(tt));
            crashRestartAndAssertNoLoss(tt, false, 3);
        });
    }

    @Test
    public void testAnchoredBeforeTenureAndClearedByOpenLoops() throws Exception {
        // Control: anchored during a primary tenure, then the demote clear runs; apply continues under
        // REPLICA_SKIP, WAL is purged, crash.
        run(() -> {
            setAdaptive(true);
            execute("create table pre_tenure (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("pre_tenure");
            insertAndRelease("pre_tenure", 1, 2, true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            clearLikeOpenLoops(tt);
            insertAndRelease("pre_tenure", 3, 5, true);
            drainPurgeJob();
            crashRestartAndAssertNoLoss(tt, false, 5);
        });
    }

    @Test
    public void testCreateUnderReplicaSkipCrashAfterPurge() throws Exception {
        // The reviewer's probe: a table created (Enterprise: registered by the downloader) while the node is a
        // replica, applied in several batches with idle eviction, WAL purged, then a crash.
        run(() -> {
            setAdaptive(true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("create table rs (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("rs");
            insertAndRelease("rs", 1, 5, true);
            drainPurgeJob();
            Assert.assertEquals("a replica must be able to purge its applied WAL", 0, countWalDirs(tt));
            crashRestartAndAssertNoLoss(tt, false, 5);
        });
    }

    @Test
    public void testCreateUnderReplicaSkipEnrolsWithoutAnchor() throws Exception {
        setAdaptive(true);
        assertMemoryLeak(() -> {
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                execute("create table no_anchor (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("no_anchor");
                Assert.assertFalse("a replica must not publish an anchor", fileExists(tt, TableUtils.SNAPSHOT_FILE_NAME));
                Assert.assertTrue("the enrolment marker must stand in for it", fileExists(tt, RecoveryCoordinator.RESTORE_ENROL_FILE_NAME));
                Assert.assertEquals(CommitMode.ADAPTIVE, readEnrolledCommitMode(tt));
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    @Test
    public void testDemoteAndPromoteBetweenBatchesPublishesBaseline() throws Exception {
        // The pooled writer sees neither role change: the demote clear removes its anchor, and the promote puts
        // the policy back to ALWAYS_ON before the next batch. The writer must still notice, and publish a
        // baseline before it applies anything lazily over an anchorless table.
        run(() -> {
            setAdaptive(true);
            execute("create table flap (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("flap");
            insertAndRelease("flap", 1, 2, false);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            clearLikeOpenLoops(tt);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            suppressCadenceEpoch(tt);
            insertAndRelease("flap", 3, 3, false);
            Assert.assertEquals("the baseline must be published at the cut before the batch", 2, readAnchorSeqTxn(tt));
            Assert.assertFalse("the baseline must consume the enrolment marker", fileExists(tt, RecoveryCoordinator.RESTORE_ENROL_FILE_NAME));
            crashRestartAndAssertNoLoss(tt, false, 3);
        });
    }

    @Test
    public void testEnrolmentUnderReplicaSkipCrashAfterPurge() throws Exception {
        // A table created and applied while the instance ran NOSYNC (not enrolled, no anchor), then the node runs
        // as an adaptive replica: its first writer enrols it during the tenure.
        run(() -> {
            setAdaptive(false);
            execute("create table enrol (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("enrol");
            insertAndRelease("enrol", 1, 2, true);
            setAdaptive(true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            clearLikeOpenLoops(tt);
            insertAndRelease("enrol", 3, 5, true);
            Assert.assertEquals(CommitMode.ADAPTIVE, readEnrolledCommitMode(tt));
            Assert.assertFalse("enrolment on a replica must not publish an anchor", fileExists(tt, TableUtils.SNAPSHOT_FILE_NAME));
            drainPurgeJob();
            crashRestartAndAssertNoLoss(tt, false, 5);
        });
    }

    @Test
    public void testFailedDemoteClearAnchorIsReplacedBeforeApply() throws Exception {
        // The demote clear skipped this table (it logs and moves on when a delete fails), so the primary-tenure
        // anchor at 2 survives into the replica tenure. The next batch must not be applied on top of it.
        run(() -> {
            setAdaptive(true);
            execute("create table noclear (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("noclear");
            insertAndRelease("noclear", 1, 2, true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            insertAndRelease("noclear", 3, 3, true);
            Assert.assertFalse("the anchor must be gone before the replica applies on top of it", fileExists(tt, TableUtils.SNAPSHOT_FILE_NAME));
            Assert.assertTrue("the enrolment marker must replace it", fileExists(tt, RecoveryCoordinator.RESTORE_ENROL_FILE_NAME));
            insertAndRelease("noclear", 4, 5, true);
            drainPurgeJob();
            crashRestartAndAssertNoLoss(tt, false, 5);
        });
    }

    @Test
    public void testGracefulRestartAfterIdleEvictionUnderReplicaSkip() throws Exception {
        // No crash at all. A table created during the tenure has its writer evicted as idle while REPLICA_SKIP is
        // active, so no close-time epoch runs. Graceful shutdown restores ALWAYS_ON first, but there is no writer
        // left to flush, and startup recovery runs on every boot.
        run(() -> {
            setAdaptive(true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("create table graceful (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("graceful");
            insertAndRelease("graceful", 1, 5, true);
            drainPurgeJob();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            engine.releaseAllWriters();
            crashRestartAndAssertNoLoss(tt, true, 5);
        });
    }

    @Test
    public void testPromotedIdleTableSurvivesCrashAfterPurge() throws Exception {
        // Replica tenure (new table, WAL purged), then promotion; the table gets no write after it, and the new
        // primary crashes.
        run(() -> {
            setAdaptive(true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("create table promo_idle (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("promo_idle");
            insertAndRelease("promo_idle", 1, 5, true);
            drainPurgeJob();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            drainPurgeJob();
            crashRestartAndAssertNoLoss(tt, false, 5);
        });
    }

    @Test
    public void testPromotionPublishesBaselineBeforeFirstLazyApply() throws Exception {
        // After promotion a table the replica left without an anchor must get a real baseline before the first
        // commit is applied lazily; the cadence epoch after the batch is too late for that batch.
        run(() -> {
            setAdaptive(true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("create table promo_write (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("promo_write");
            insertAndRelease("promo_write", 1, 5, true);
            drainPurgeJob();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            suppressCadenceEpoch(tt);
            // Keep the writer pooled: evicting it would run the close-time epoch at seqTxn 6.
            insertAndRelease("promo_write", 6, 6, false);
            Assert.assertEquals("the baseline must be published at the cut before the batch", 5, readAnchorSeqTxn(tt));
            Assert.assertFalse("the baseline must consume the enrolment marker", fileExists(tt, RecoveryCoordinator.RESTORE_ENROL_FILE_NAME));
            drainPurgeJob();
            crashRestartAndAssertNoLoss(tt, false, 6);
        });
    }

    @Test
    public void testRebaseUnderReplicaSkipCrashAfterPurge() throws Exception {
        // The REBASE WAL clone is a new table at seqTxn 0 (Enterprise runs the replica variant, REBASE WAL INTO,
        // to follow the primary), so on a replica it must come out with the marker instead of an anchor too.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        run(() -> {
            setAdaptive(true);
            execute("create table rb (ts timestamp, v long) timestamp(ts) partition by day wal");
            insertAndRelease("rb", 1, 2, true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("alter table rb suspend wal");
            execute("alter table rb rebase wal");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("rb");
            Assert.assertFalse("a clone made on a replica must not carry an anchor", fileExists(tt, TableUtils.SNAPSHOT_FILE_NAME));
            Assert.assertTrue("the enrolment marker must stand in for it", fileExists(tt, RecoveryCoordinator.RESTORE_ENROL_FILE_NAME));
            Assert.assertEquals(CommitMode.ADAPTIVE, readEnrolledCommitMode(tt));
            insertAndRelease("rb", 3, 5, true);
            drainPurgeJob();
            crashRestartAndAssertNoLoss(tt, false, 5);
        });
    }

    @Test
    public void testTenureTableWriterPooledAtGracefulShutdown() throws Exception {
        // Control: graceful shutdown with the writer still pooled; the close-time epoch runs under ALWAYS_ON.
        run(() -> {
            setAdaptive(true);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            execute("create table pooled (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("pooled");
            insertAndRelease("pooled", 1, 5, false);
            drainPurgeJob();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            engine.releaseAllWriters();
            crashRestartAndAssertNoLoss(tt, true, 5);
        });
    }

    private static void clearLikeOpenLoops(TableToken tt) {
        try (Path path = new Path()) {
            final int rootLen = path.of(configuration.getDbRoot()).concat(tt).size();
            RecoveryCoordinator.markRestoredForEnrolment(configuration.getFilesFacade(), path, rootLen);
            RecoveryCoordinator.removeAdaptiveEpochArtifacts(configuration.getFilesFacade(), path, rootLen);
        }
    }

    private static int countWalDirs(TableToken tt) {
        final java.io.File tableDir = new java.io.File(configuration.getDbRoot(), tt.getDirName());
        final String[] names = tableDir.list();
        int count = 0;
        if (names != null) {
            for (String name : names) {
                if (name.startsWith("wal") && new java.io.File(tableDir, name).isDirectory()) {
                    count++;
                }
            }
        }
        return count;
    }

    private static boolean fileExists(TableToken tt, CharSequence fileName) {
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt).concat(fileName);
            return configuration.getFilesFacade().exists(p.$());
        }
    }

    private static void insertAndRelease(String name, int from, int to, boolean evictTableWriter) throws Exception {
        for (int i = from; i <= to; i++) {
            execute("insert into " + name + " values ('2024-01-0" + i + "T00:00:00.000000Z', " + i + ")");
            drainWalQueue();
            if (evictTableWriter) {
                // idle eviction of both the WalWriter and the TableWriter
                engine.releaseInactive();
            } else {
                engine.releaseAllWalWriters();
            }
        }
    }

    private static long readAnchorSeqTxn(TableToken tt) {
        if (!fileExists(tt, TableUtils.SNAPSHOT_FILE_NAME)) {
            return -1;
        }
        try (Path p = new Path(); SnapshotMarker marker = new SnapshotMarker(configuration)) {
            p.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(p.$());
            return marker.tryLoad() ? marker.getEpochSeqTxn() : -1;
        }
    }

    private static long readDiskSeqTxn(TableToken tt) {
        try (TxReader tx = new TxReader(configuration.getFilesFacade()); Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            tx.ofRO(p.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            tx.unsafeLoadAll();
            return tx.getSeqTxn();
        }
    }

    private static int readEnrolledCommitMode(TableToken tt) {
        try (TableReaderMetadata metadata = new TableReaderMetadata(configuration, tt)) {
            metadata.loadMetadata();
            return metadata.getEnrolledCommitMode();
        }
    }

    private static void setAdaptive(boolean adaptive) {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, adaptive ? "adaptive" : "nosync");
    }

    private static void suppressCadenceEpoch(TableToken tt) {
        // A long interval and a recent last epoch, so the only epoch the next batch can publish is the one the
        // writer takes before it applies.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000L);
        engine.getTableSequencerAPI().getTxnTracker(tt).setLastEpochTs(configuration.getMicrosecondClock().getTicks() / 1000L);
    }

    private void crashRestartAndAssertNoLoss(TableToken tt, boolean graceful, long expectedRows) throws Exception {
        if (!graceful) {
            // crash: in-memory state dropped with no close-time epoch
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseAllWalWriters();
        engine.getTxnScoreboardPool().remove(tt);
        // boot: the policy is the OSS default until a role state installs another one
        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
        final long diskBefore = readDiskSeqTxn(tt);
        engine.getTableSequencerAPI().resetForReboot(tt);
        new RecoveryCoordinator(engine).recover();
        final long diskAfterRecover = readDiskSeqTxn(tt);
        engine.notifyWalTxnRepublisher(tt);
        drainWalQueue();
        Assert.assertFalse(
                "table must not be suspended after restart [diskSeqTxn before=" + diskBefore
                        + ", afterRecover=" + diskAfterRecover + ", afterReplay=" + readDiskSeqTxn(tt) + ']',
                engine.getTableSequencerAPI().isSuspended(tt)
        );
        assertQuery("select count() from " + tt.getTableName())
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + expectedRows + "\n");
    }

    private void run(Scenario scenario) throws Exception {
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        assertMemoryLeak(() -> {
            try {
                scenario.run();
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    @FunctionalInterface
    private interface Scenario {
        void run() throws Exception;
    }
}
