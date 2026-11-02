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
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A node demoted to replica while its WAL apply runs a partition operation, or between a DROP PARTITION and the
 * next save point. Partition work that waits for a save point (a DETACH's temporary name, an ATTACH's source, an
 * owed partition purge, the required completion) waits while the table's save point may be on disk, and is
 * finished when the writer removes the save point (TableWriter.removeSavePointForReplica). No operation fails
 * because the policy changed under it, and a crash before the removal replays cleanly.
 */
public class ReplicaSkipPartitionWorkTest extends AbstractCairoTest {
    private static final int FLIP_IN_COMPLETION = 2;
    private static final int FLIP_NONE = 0;
    private static final int FLIP_ON_FIRST_ATTACH_LINK = 3;
    private static final int FLIP_ON_SECOND_DETACH_LINK = 1;
    private static volatile int flipWhere = FLIP_NONE;
    private static volatile boolean isDetachedNameTakenOnFlip;
    private static volatile boolean isDetachingRenameFailing;
    private static volatile boolean isManifestRemovalFailing;
    private static volatile boolean isManifestRemovalFailingOnFlip;
    private static volatile String tableDirOnFlip;
    private static final TestFilesFacadeImpl FLIP_FF = new TestFilesFacadeImpl() {
        @Override
        public boolean exists(LPSZ path) {
            if (flipWhere == FLIP_IN_COMPLETION
                    && Utf8s.containsAscii(path, DurableEpochManifest.FILE_NAME)
                    && isInStack("completeDurableEpochAfter")) {
                flip();
            }
            return super.exists(path);
        }

        @Override
        public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
            if (flipWhere == FLIP_ON_SECOND_DETACH_LINK && Utf8s.containsAscii(dst, "2024-01-02")) {
                flip();
            } else if (flipWhere == FLIP_ON_FIRST_ATTACH_LINK && Utf8s.containsAscii(src, "2024-01-01.attachable")) {
                // After the first partition's attach decided how to move its source, before the second one decides
                flip();
            }
            return super.hardLinkDirRecursive(src, dst, dirMode);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isManifestRemovalFailing && Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                return false;
            }
            return super.removeQuiet(name);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            if (isDetachingRenameFailing && Utf8s.containsAscii(from, TableUtils.DETACHING_DIR_MARKER)) {
                isDetachingRenameFailing = false;
                return Files.FILES_RENAME_ERR_OTHER;
            }
            return super.rename(from, to);
        }
    };

    @Test
    public void testControlAlwaysOnThroughout() throws Exception {
        runDetach(FLIP_NONE, false, (tt, writerBefore) -> {
            assertNotSuspended(tt);
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            Assert.assertTrue("a primary keeps its save point", savePointExists(tt));
            assertCompletionNotRequired(tt);
        });
    }

    @Test
    public void testControlReplicaSkipThroughout() throws Exception {
        runDetach(FLIP_NONE, true, (tt, writerBefore) -> {
            assertNotSuspended(tt);
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            Assert.assertFalse("a replica keeps no save point", savePointExists(tt));
            assertCompletionNotRequired(tt);
        });
    }

    @Test
    public void testDemoteBetweenAttachesRemovesKeptSources() throws Exception {
        runAdaptive(() -> {
            final TableToken tt = createTableWithFourPartitions();
            execute("alter table t detach partition list '2024-01-01','2024-01-02'");
            drainWalQueue();
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            for (String partition : new String[]{"2024-01-01", "2024-01-02"}) {
                java.nio.file.Files.move(
                        tableDir(tt).resolve(partition + TableUtils.DETACHED_DIR_MARKER),
                        tableDir(tt).resolve(partition + configuration.getAttachPartitionSuffix())
                );
            }
            execute("alter table t attach partition list '2024-01-01','2024-01-02'");
            flipWhere = FLIP_ON_FIRST_ATTACH_LINK;
            drainWalQueue();
            Assert.assertEquals("the flip must land in the attach", FLIP_NONE, flipWhere);

            assertNotSuspended(tt);
            Assert.assertEquals("no source may stay next to its attached partition", "[]", moveDirs(tt));
            Assert.assertFalse(savePointExists(tt));
            assertCompletionNotRequired(tt);
            assertQuery("select count() from t").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
        });
    }

    @Test
    public void testDemoteBetweenDetachesFinishesBoth() throws Exception {
        runDetach(FLIP_ON_SECOND_DETACH_LINK, false, (tt, writerBefore) -> {
            assertNotSuspended(tt);
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            Assert.assertFalse(savePointExists(tt));
            assertCompletionNotRequired(tt);
            assertAppliesOnSameWriter(tt, writerBefore);
        });
    }

    @Test
    public void testDemoteBetweenDetachesRemovalFailsThenCrash() throws Exception {
        assertCrashBeforeSavePointRemovalReplays(false);
    }

    @Test
    public void testDemoteBetweenDetachesRemovalFailsThenReopenAndCrash() throws Exception {
        // The writer open between the flip and the removal must not name the partitions either.
        assertCrashBeforeSavePointRemovalReplays(true);
    }

    @Test
    public void testDemoteBetweenDetachesRemovalFailsThenRetries() throws Exception {
        runDetachWithFailedRemoval((tt, writerBefore) -> {
            // The next batch's reconcile removes the save point and finishes the deferred work.
            isManifestRemovalFailing = false;
            assertAppliesOnSameWriter(tt, writerBefore);
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            Assert.assertFalse(savePointExists(tt));
            assertCompletionNotRequired(tt);
        });
    }

    @Test
    public void testDemoteBetweenDropAndSavePointPurgesDroppedVersions() throws Exception {
        runAdaptive(() -> {
            final TableToken tt = createTableWithDroppedPartition();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            // A replica batch: its reconcile removes the save point and queues the owed purge.
            execute("insert into t values ('2024-01-04T01:00:00.000000Z', 5)");
            drainWalQueue();
            Assert.assertFalse(savePointExists(tt));
            drainPartitionPurge();
            Assert.assertEquals("dropped versions after the replica batch", "[]", droppedVersionDirs(tt));
        });
    }

    @Test
    public void testDemoteClearBetweenDropAndSavePointPurgesDroppedVersions() throws Exception {
        runAdaptive(() -> {
            final TableToken tt = createTableWithDroppedPartition();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            // What the Enterprise demote clear does for an idle table
            try (TableWriter writer = engine.getWriterUnsafe(tt, "demote clear")) {
                writer.reconcileDurableEpochAnchor();
            }
            Assert.assertFalse(savePointExists(tt));
            drainPartitionPurge();
            Assert.assertEquals("dropped versions after the demote clear", "[]", droppedVersionDirs(tt));
        });
    }

    @Test
    public void testDemoteInFirstBatchKeepsMovesUnderCreationBaseline() throws Exception {
        // Before its first save point a table has only its CREATE baseline, which no pin records.
        runAdaptive(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2), "
                    + "('2024-01-03T00:00:00.000000Z', 3), ('2024-01-04T00:00:00.000000Z', 4)");
            final TableToken tt = engine.verifyTableName("t");
            Assert.assertTrue("setup: the CREATE baseline", savePointExists(tt));
            Assert.assertTrue("setup: no pin", engine.getTableSequencerAPI().getTxnTracker(tt).getPinnedEpochTxn() < 0);
            tableDirOnFlip = tableDir(tt).toString();
            isManifestRemovalFailingOnFlip = true;
            execute("alter table t detach partition list '2024-01-01','2024-01-02'");
            flipWhere = FLIP_ON_SECOND_DETACH_LINK;
            drainWalQueue();
            Assert.assertEquals("the flip must land in the DETACH", FLIP_NONE, flipWhere);
            assertNotSuspended(tt);
            Assert.assertTrue(savePointExists(tt));
            Assert.assertEquals("[2024-01-01.2.detaching, 2024-01-02.3.detaching]", moveDirs(tt));
            isManifestRemovalFailing = false;
            execute("insert into t values ('2024-01-04T01:00:00.000000Z', 5)");
            drainWalQueue();
            assertNotSuspended(tt);
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            Assert.assertFalse(savePointExists(tt));
            assertCompletionNotRequired(tt);
        });
    }

    @Test
    public void testDemoteInsideDetachCompletionFinishesBoth() throws Exception {
        runDetach(FLIP_IN_COMPLETION, false, (tt, writerBefore) -> {
            assertNotSuspended(tt);
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            Assert.assertFalse(savePointExists(tt));
            assertCompletionNotRequired(tt);
            assertAppliesOnSameWriter(tt, writerBefore);
        });
    }

    @Test
    public void testDetachNameTakenAtSavePointRemovalSuspendsUntilResume() throws Exception {
        isDetachedNameTakenOnFlip = true;
        runDetach(FLIP_IN_COMPLETION, false, (tt, writerBefore) -> {
            assertSuspendedWith(tt, "already exists, move it away");
            Assert.assertFalse("the save point goes before any partition is named", savePointExists(tt));
            Assert.assertTrue("the requirement stays while a partition has no name",
                    engine.getTableSequencerAPI().getTxnTracker(tt).isDurableEpochCompletionRequired());
            java.nio.file.Files.delete(tableDir(tt).resolve("2024-01-01" + TableUtils.DETACHED_DIR_MARKER));
            resumeAndAssertBothDetached(tt);
        });
    }

    @Test
    public void testDetachRenameFailureAtSavePointRemovalSuspendsUntilResume() throws Exception {
        runAdaptive(() -> {
            final TableToken tt = createTableWithFourPartitions();
            execute("alter table t detach partition list '2024-01-01','2024-01-02'");
            flipWhere = FLIP_IN_COMPLETION;
            isDetachingRenameFailing = true;
            drainWalQueue();
            Assert.assertEquals("the flip must land in the completion", FLIP_NONE, flipWhere);
            Assert.assertFalse("the rename failure must be hit", isDetachingRenameFailing);
            assertSuspendedWith(tt, "partition is detached, its files are in");
            Assert.assertFalse(savePointExists(tt));
            resumeAndAssertBothDetached(tt);
        });
    }

    @Test
    public void testOwedPurgeWithFullQueueLeavesVersionsToWriterOpen() throws Exception {
        runAdaptive(() -> {
            final TableToken tt = createTableWithDroppedPartition();
            execute("create table filler (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken filler = engine.verifyTableName("filler");
            int queued = 0;
            while (TableUtils.schedulePurgeO3Partitions(engine.getMessageBus(), filler, ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                queued++;
            }
            Assert.assertTrue(queued > 0);
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
                execute("insert into t values ('2024-01-04T01:00:00.000000Z', 5)");
                drainWalQueue();
                capture.drain();
                capture.assertLogged("could not queue the partition purge the removed save points owed");
            } finally {
                capture.stop();
            }
            // A full purge queue does not fail the removal or the batch.
            assertNotSuspended(tt);
            Assert.assertFalse(savePointExists(tt));
            assertCompletionNotRequired(tt);
            drainPartitionPurge();
            Assert.assertNotEquals("nothing queued the purge", "[]", droppedVersionDirs(tt));
            // As for any table whose purge did not fit the queue, the next writer open removes the versions.
            engine.releaseAllWriters();
            try (TableWriter writer = getWriter(tt)) {
                Assert.assertNotNull(writer);
            }
            Assert.assertEquals("dropped versions after the writer open", "[]", droppedVersionDirs(tt));
        });
    }

    private static void assertCompletionNotRequired(TableToken tt) {
        Assert.assertFalse("completion requirement",
                engine.getTableSequencerAPI().getTxnTracker(tt).isDurableEpochCompletionRequired());
    }

    private static void assertNotSuspended(TableToken tt) throws Exception {
        if (engine.getTableSequencerAPI().isSuspended(tt)) {
            Assert.fail("table is suspended: " + walTablesRow());
        }
    }

    private static void assertSuspendedWith(TableToken tt, String message) throws Exception {
        Assert.assertTrue("table must be suspended", engine.getTableSequencerAPI().isSuspended(tt));
        TestUtils.assertContains(walTablesRow(), message);
    }

    private static TableToken createTableWithDroppedPartition() throws Exception {
        final TableToken tt = createTableWithFourPartitions();
        execute("alter table t drop partition list '2024-01-01'");
        drainWalQueue();
        drainPartitionPurge();
        Assert.assertNotEquals("setup: a save point still names the dropped version", "[]", droppedVersionDirs(tt));
        return tt;
    }

    private static TableToken createTableWithFourPartitions() throws Exception {
        execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
        execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2), "
                + "('2024-01-03T00:00:00.000000Z', 3), ('2024-01-04T00:00:00.000000Z', 4)");
        drainWalQueue();
        final TableToken tt = engine.verifyTableName("t");
        Assert.assertTrue("setup: the table has a save point", savePointExists(tt));
        return tt;
    }

    private static void drainPartitionPurge() {
        try (O3PartitionPurgeJob job = new O3PartitionPurgeJob(engine, 1)) {
            job.drain(0);
        }
    }

    private static String droppedVersionDirs(TableToken tt) {
        return listDirs(tt, name -> name.startsWith("2024-01-01") && !name.contains(".detach") && !name.contains(".attachable"));
    }

    private static void flip() {
        flipWhere = FLIP_NONE;
        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
        if (isManifestRemovalFailingOnFlip) {
            isManifestRemovalFailing = true;
        }
        if (isDetachedNameTakenOnFlip) {
            try {
                java.nio.file.Files.createDirectories(Paths.get(tableDirOnFlip, "2024-01-01" + TableUtils.DETACHED_DIR_MARKER));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static boolean isInStack(String method) {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
            if (method.equals(e.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    private static String listDirs(TableToken tt, java.util.function.Predicate<String> filter) {
        try (Stream<java.nio.file.Path> entries = java.nio.file.Files.list(tableDir(tt))) {
            return Arrays.toString(entries.map(p -> p.getFileName().toString()).filter(filter).sorted().toArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String moveDirs(TableToken tt) {
        return listDirs(tt, name -> name.contains(".detach") || name.contains(".attachable"));
    }

    private static boolean savePointExists(TableToken tt) {
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt);
            return DurableEpochManifest.exists(configuration.getFilesFacade(), p, p.size());
        }
    }

    private static java.nio.file.Path tableDir(TableToken tt) {
        return Paths.get(configuration.getDbRoot(), tt.getDirName());
    }

    private static String walTablesRow() throws Exception {
        final StringSink sink = new StringSink();
        printSql("select suspended, writerTxn, sequencerTxn, errorMessage from wal_tables() where name = 't'", sink);
        return sink.toString();
    }

    private void assertAppliesOnSameWriter(TableToken tt, TableWriter writerBefore) throws Exception {
        execute("insert into t values ('2024-01-04T01:00:00.000000Z', 5)");
        drainWalQueue();
        assertNotSuspended(tt);
        try (TableWriter writer = getWriter(tt)) {
            Assert.assertSame("the writer must not have been reopened", writerBefore, writer);
        }
        assertQuery("select count() from t").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
    }

    private void assertCrashBeforeSavePointRemovalReplays(boolean reopenBeforeCrash) throws Exception {
        runDetachWithFailedRemoval((tt, writerBefore) -> {
            if (reopenBeforeCrash) {
                engine.releaseAllWriters();
                try (TableWriter writer = getWriter(tt)) {
                    Assert.assertNotNull(writer);
                }
                Assert.assertEquals("a writer open must not name a partition the save point does not cover",
                        "[2024-01-01.2.detaching, 2024-01-02.3.detaching]", moveDirs(tt));
            }
            // A process kill: the writers close without a save point, then a boot runs recovery before the role
            // is installed, and the replica applies the rest of its WAL.
            isManifestRemovalFailing = false;
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            engine.getTableSequencerAPI().resetForReboot(tt);
            new RecoveryCoordinator(engine).recover();
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            engine.notifyWalTxnRepublisher(tt);
            drainWalQueue();
            assertNotSuspended(tt);
            Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
            Assert.assertFalse(savePointExists(tt));
            assertQuery("select count() from t").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
        });
    }

    private void resumeAndAssertBothDetached(TableToken tt) throws Exception {
        execute("alter table t resume wal");
        drainWalQueue();
        assertNotSuspended(tt);
        Assert.assertEquals("[2024-01-01.detached, 2024-01-02.detached]", moveDirs(tt));
        assertCompletionNotRequired(tt);
        assertQuery("select count() from t").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
    }

    private void runAdaptive(TestUtils.LeakProneCode code) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);
        assertMemoryLeak(FLIP_FF, () -> {
            try {
                tableDirOnFlip = null;
                code.run();
            } finally {
                flipWhere = FLIP_NONE;
                isDetachedNameTakenOnFlip = false;
                isDetachingRenameFailing = false;
                isManifestRemovalFailing = false;
                isManifestRemovalFailingOnFlip = false;
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    private void runDetach(int flipAt, boolean replicaSkipThroughout, DetachCheck check) throws Exception {
        runAdaptive(() -> {
            final TableToken tt = createTableWithFourPartitions();
            tableDirOnFlip = tableDir(tt).toString();
            final TableWriter writerBefore;
            try (TableWriter writer = getWriter(tt)) {
                writerBefore = writer;
            }
            if (replicaSkipThroughout) {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            }
            execute("alter table t detach partition list '2024-01-01','2024-01-02'");
            flipWhere = flipAt;
            drainWalQueue();
            Assert.assertEquals("the flip must land in the DETACH", FLIP_NONE, flipWhere);
            check.run(tt, writerBefore);
        });
    }

    private void runDetachWithFailedRemoval(DetachCheck check) throws Exception {
        isManifestRemovalFailingOnFlip = true;
        runDetach(FLIP_ON_SECOND_DETACH_LINK, false, (tt, writerBefore) -> {
            // The save point stays, so no partition may take its final name yet.
            assertNotSuspended(tt);
            Assert.assertTrue(savePointExists(tt));
            Assert.assertEquals("[2024-01-01.2.detaching, 2024-01-02.3.detaching]", moveDirs(tt));
            check.run(tt, writerBefore);
        });
    }

    @FunctionalInterface
    private interface DetachCheck {
        void run(TableToken tt, TableWriter writerBefore) throws Exception;
    }
}
