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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.crash.CrashFaultFilesFacade;
import io.questdb.test.std.SyncAttributingFilesFacade;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * Unit-level oracle for {@link RecoveryCoordinator}: a durable epoch was taken at seqTxn=K, then more
 * rows were applied LAZILY (advancing {@code _txn} to K+M without a new epoch). Recovery must REWIND
 * {@code _txn}/{@code _cv} to the durable epoch cut by copying the immutable {@code _txn.epoch} /
 * {@code _cv.epoch} back over them, so the table opens at exactly {@code epoch.seqTxn}.
 */
public class RecoveryCoordinatorTest extends AbstractCairoTest {
    private static final int PAGE_SIZE_4K = 4096;

    @Test
    public void testManifestPartitionLookupClosesHandlesOnChecksumFailure() throws Exception {
        assertManifestPartitionLookupOwnership("checksum");
    }

    @Test
    public void testManifestPartitionLookupClosesHandlesOnOpenFailure() throws Exception {
        assertManifestPartitionLookupOwnership("open");
    }

    @Test
    public void testManifestPartitionLookupClosesHandlesOnReadFailure() throws Exception {
        assertManifestPartitionLookupOwnership("read");
    }

    @Test
    public void testManifestPartitionLookupClosesHandlesOnRssLimit() throws Exception {
        assertManifestPartitionLookupOwnership("rss");
    }

    @Test
    public void testManifestPartitionLookupClosesHandlesOnSuccess() throws Exception {
        assertManifestPartitionLookupOwnership("none");
    }

    @Test
    public void testPurgeClosesManifestHandlesOnRssLimit() throws Exception {
        assertPurgeManifestOwnership(true);
    }

    @Test
    public void testPurgeClosesManifestHandlesWithoutRssLimit() throws Exception {
        assertPurgeManifestOwnership(false);
    }

    @Test
    public void testRewriteManifestHeaderClosesHandleOnReadFailure() throws Exception {
        assertRewriteManifestHeaderOwnership(false, true);
    }

    @Test
    public void testRewriteManifestHeaderClosesHandleOnRssLimit() throws Exception {
        assertRewriteManifestHeaderOwnership(true, false);
    }

    @Test
    public void testRewriteManifestHeaderClosesHandleOnSuccess() throws Exception {
        assertRewriteManifestHeaderOwnership(false, false);
    }

    private void assertManifestPartitionLookupOwnership(String fault) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE manifest_lookup (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO manifest_lookup VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("manifest_lookup");
            try (TableWriter writer = getWriter(token)) {
                writer.advanceDurableEpoch(1L);
            }
            final List<Long> open = new ArrayList<>();
            final AtomicInteger acquisitions = new AtomicInteger();
            final AtomicInteger sectionReads = new AtomicInteger();
            final FilesFacade probe = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ path) {
                    // The second open of each lookup is the saved-image acquisition, after a valid header.
                    if (fault.equals("open") && acquisitions.get() % 2 == 1) {
                        acquisitions.incrementAndGet();
                        return -1;
                    }
                    long fd = super.openRO(path);
                    if (fd >= 0) {
                        acquisitions.incrementAndGet();
                        open.add(fd);
                    }
                    return fd;
                }

                @Override
                public long read(long fd, long address, long len, long offset) {
                    if (offset == DurableEpochManifest.FILE_SIZE) {
                        sectionReads.incrementAndGet();
                        if (fault.equals("read")) {
                            return -1;
                        }
                        long read = super.read(fd, address, len, offset);
                        if (fault.equals("checksum") && read == len) {
                            Unsafe.getUnsafe().putByte(address, (byte) (Unsafe.getUnsafe().getByte(address) ^ 1));
                        }
                        return read;
                    }
                    return super.read(fd, address, len, offset);
                }

                @Override
                public boolean close(long fd) {
                    Assert.assertTrue("close without acquisition", open.remove(Long.valueOf(fd)));
                    return super.close(fd);
                }
            };
            try (Path path = new Path()) {
                final int rootLen = path.of(configuration.getDbRoot()).concat(token).size();
                final long previousLimit = Unsafe.getRssMemLimit();
                final long beforeMemory = Unsafe.getMemUsed();
                final long beforeFds = io.questdb.std.Files.getOpenFileCount();
                try {
                    if (fault.equals("rss")) {
                        // The 120-byte header fits, but even the creation baseline's _txn image does not.
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 256);
                    }
                    for (int i = 0; i < 8; i++) {
                        // This partition is absent: true under a fault must be conservative retention.
                        Assert.assertEquals(!fault.equals("none"), DurableEpochManifest.namesPartition(
                                probe, path, rootLen, token.getTableId(), 0, -1));
                        Assert.assertEquals(rootLen, path.size());
                        Assert.assertEquals(beforeMemory, Unsafe.getMemUsed());
                        Assert.assertTrue("manifest handles retained=" + open.size(), open.isEmpty());
                        Assert.assertEquals(beforeFds, io.questdb.std.Files.getOpenFileCount());
                    }
                    Assert.assertTrue("must open the saved image", acquisitions.get() >= 16);
                    Assert.assertEquals(fault.equals("rss") || fault.equals("open"), sectionReads.get() == 0);
                } finally {
                    Unsafe.setRssMemLimit(previousLimit);
                    // Rescue only after observing production's balance, including on a failing regression.
                    for (long fd : new ArrayList<>(open)) {
                        probe.close(fd);
                    }
                }
            }
        });
    }

    private void assertPurgeManifestOwnership(boolean hasRssLimit) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicInteger acquisitions = new AtomicInteger();
        final AtomicInteger sectionReads = new AtomicInteger();
        final List<Long> open = new ArrayList<>();
        final FilesFacade probe = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ path) {
                long fd = super.openRO(path);
                if (armed.get() && fd >= 0 && Utf8s.containsAscii(path, DurableEpochManifest.FILE_NAME)) {
                    acquisitions.incrementAndGet();
                    open.add(fd);
                    if (hasRssLimit) {
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 136);
                    }
                }
                return fd;
            }

            @Override
            public long read(long fd, long address, long len, long offset) {
                if (open.contains(fd) && offset == DurableEpochManifest.FILE_SIZE) {
                    sectionReads.incrementAndGet();
                }
                return super.read(fd, address, len, offset);
            }

            @Override
            public boolean close(long fd) {
                open.remove(Long.valueOf(fd));
                return super.close(fd);
            }
        };
        assertMemoryLeak(probe, () -> {
            execute("CREATE TABLE manifest_purge (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO manifest_purge VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("manifest_purge");
            final long partitionNameTxn;
            try (TableWriter writer = getWriter(token)) {
                writer.advanceDurableEpoch(1L);
                partitionNameTxn = writer.getTxWriter().getPartitionNameTxn(0);
            }
            execute("ALTER TABLE manifest_purge DROP PARTITION LIST '2024-01-01'");
            drainWalQueue();
            final long previousLimit = Unsafe.getRssMemLimit();
            try (O3PartitionPurgeJob purge = new O3PartitionPurgeJob(engine, 1); Path partitionPath = new Path()) {
                partitionPath.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(partitionPath, ColumnType.TIMESTAMP, PartitionBy.DAY,
                        MicrosTimestampDriver.INSTANCE.parseFloorLiteral("2024-01-01"), partitionNameTxn);
                Assert.assertTrue(probe.exists(partitionPath.$()));
                final long beforeMemory = Unsafe.getRssMemUsed();
                final long beforeFds = io.questdb.std.Files.getOpenFileCount();
                armed.set(true);
                try {
                    while (purge.run()) {
                        Os.pause();
                    }
                    Assert.assertTrue("purge must open the saved image", acquisitions.get() >= 2);
                    Assert.assertEquals(hasRssLimit, sectionReads.get() == 0);
                    Assert.assertEquals(beforeMemory, Unsafe.getRssMemUsed());
                    Assert.assertTrue("purge manifest handles retained=" + open.size(), open.isEmpty());
                    Assert.assertEquals(beforeFds, io.questdb.std.Files.getOpenFileCount());
                    Assert.assertTrue("fallback still needs the dropped partition", probe.exists(partitionPath.$()));
                } finally {
                    armed.set(false);
                    Unsafe.setRssMemLimit(previousLimit);
                    for (long fd : new ArrayList<>(open)) {
                        probe.close(fd);
                    }
                }
            }
            assertQuery("SELECT v FROM manifest_purge").expectSize().returns("v\n2\n");
        });
    }

    private void assertRewriteManifestHeaderOwnership(boolean hasRssLimit, boolean hasReadFailure) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE manifest_rewrite (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("manifest_rewrite");
            final List<Long> open = new ArrayList<>();
            final AtomicInteger acquisitions = new AtomicInteger();
            final FilesFacade probe = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ path, int opts) {
                    long fd = super.openRW(path, opts);
                    if (fd >= 0) {
                        acquisitions.incrementAndGet();
                        open.add(fd);
                        if (hasRssLimit) {
                            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + DurableEpochManifest.FILE_SIZE - 1);
                        }
                    }
                    return fd;
                }

                @Override
                public long read(long fd, long address, long len, long offset) {
                    return hasReadFailure ? -1 : super.read(fd, address, len, offset);
                }

                @Override
                public boolean close(long fd) {
                    Assert.assertTrue("close without acquisition", open.remove(Long.valueOf(fd)));
                    return super.close(fd);
                }
            };
            try (Path path = new Path()) {
                final int rootLen = path.of(configuration.getDbRoot()).concat(token).size();
                final long previousLimit = Unsafe.getRssMemLimit();
                final long beforeMemory = Unsafe.getMemUsed();
                final long beforeFds = io.questdb.std.Files.getOpenFileCount();
                try {
                    for (int i = 0; i < 8; i++) {
                        try {
                            DurableEpochManifest.rewriteHeader(probe, path, rootLen, 0, true);
                            Assert.assertFalse(hasRssLimit || hasReadFailure);
                        } catch (CairoException e) {
                            Assert.assertTrue(hasRssLimit || hasReadFailure);
                            Assert.assertEquals(hasRssLimit, e.isOutOfMemory());
                        }
                        Assert.assertEquals(beforeMemory, Unsafe.getMemUsed());
                        Assert.assertTrue("rewrite handles retained=" + open.size(), open.isEmpty());
                        Assert.assertEquals(beforeFds, io.questdb.std.Files.getOpenFileCount());
                        Assert.assertEquals(rootLen, path.size());
                    }
                    Assert.assertEquals(8, acquisitions.get());
                } finally {
                    Unsafe.setRssMemLimit(previousLimit);
                    for (long fd : new ArrayList<>(open)) {
                        probe.close(fd);
                    }
                }
                Assert.assertTrue(DurableEpochManifest.validateSections(configuration, token, path, rootLen, 0, 0, 0));
            }
        });
    }

    /**
     * A table created while the instance ran nosync has no anchor: the generation-zero baseline is
     * published at CREATE only when the effective mode is already adaptive. Turning adaptive on globally --
     * unpinning {@code cairo.commit.mode}, or upgrading to a build where it is the default -- must not
     * refuse that table. It has nothing to roll forward: enrollment is what permits lazy apply, and it
     * publishes the anchor first, so a table that was never enrolled cannot be ahead of a cut.
     *
     */
    @Test
    public void testTableCreatedUnderNosyncEnrolsOnGlobalSwitchToAdaptive() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        try {
            assertMemoryLeak(() -> {
                execute("create table mode_flip (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into mode_flip values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();

                final TableToken token = engine.verifyTableName("mode_flip");
                try (Path path = new Path()) {
                    path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(io.questdb.cairo.DurableEpochManifest.FILE_NAME + ".0");
                    Assert.assertFalse(
                            "precondition: a nosync-created table has no anchor",
                            TestFilesFacadeImpl.INSTANCE.exists(path.$())
                    );
                }

                engine.releaseAllWriters();
                engine.releaseAllReaders();
                // A live SeqTxnTracker still remembers the mode this table was written under, which would let
                // recovery resolve the mode from memory and skip the table entirely -- the flip would not be
                // modelled at all. Drop the cached sequencer and tracker, as a cold start has.
                engine.getTableSequencerAPI().resetForReboot(token);

                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
                // Boots. A table without a save point is not enrolled.
                new RecoveryCoordinator(engine).recover();

                // The first writer to open the table enrols it: a baseline at the LIVE cut.
                try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                    Assert.assertEquals(CommitMode.ADAPTIVE, writer.getEffectiveCommitMode());
                }
                engine.releaseAllWriters();

                try (Path path = new Path()) {
                    path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(io.questdb.cairo.DurableEpochManifest.FILE_NAME + ".0");
                    Assert.assertTrue("enrollment must publish the anchor",
                            TestFilesFacadeImpl.INSTANCE.exists(path.$()));
                }
                // The row written under nosync is still there, and the anchor now published is one an ordinary
                // startup validates rather than merely tolerates.
                new RecoveryCoordinator(engine).recover();
                try (io.questdb.cairo.TableReader reader = engine.getReader(token)) {
                    Assert.assertEquals("the row written under nosync must survive enrollment", 1L, reader.size());
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    @Test
    public void testRecoveryDisableSwitchFailsClosedForAdaptiveTable() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, "false");
            try {
                execute("create table disabled_recovery (ts timestamp, v long) timestamp(ts) partition by day wal");
                engine.releaseAllWriters();
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("disabled adaptive recovery must not expose possibly torn live state");
                } catch (CairoException expected) {
                    TestUtils.assertContains(expected.getFlyweightMessage(), "refusing unsafe startup");
                }
            } finally {
                setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, "true");
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            }
        });
    }

    @Test
    public void testEnrolmentAfterSavePointRemovalHandsOverEpochPin() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            try {
                execute("create table checkpoint_pin (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into checkpoint_pin values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final TableToken token = engine.verifyTableName("checkpoint_pin");
                final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
                final long priorEpochTxn = tracker.getPinnedEpochTxn();
                Assert.assertTrue(priorEpochTxn >= 0);

                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
                execute("insert into checkpoint_pin values ('2024-01-01T01:00:00.000000Z', 2)");
                drainWalQueue();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                try (Path path = new Path()) {
                    final int rootLen = path.of(configuration.getDbRoot()).concat(token).size();
                    RecoveryCoordinator.removeAdaptiveEpochArtifacts(configuration.getFilesFacade(), path, rootLen);
                }

                // A restore removes the save point; the table's next writer enrols it.
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("a table without a save point keeps the pin until it enrols", priorEpochTxn, tracker.getPinnedEpochTxn());
                try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                    Assert.assertNotNull(writer);
                }
                final long newEpochTxn = tracker.getPinnedEpochTxn();
                Assert.assertTrue(newEpochTxn > priorEpochTxn);
                try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
                    Assert.assertTrue("the superseded pin must be released",
                            scoreboard.isRangeAvailable(priorEpochTxn, priorEpochTxn + 1));
                    Assert.assertFalse("the enrolment baseline must remain pinned",
                            scoreboard.isRangeAvailable(newEpochTxn, newEpochTxn + 1));
                }
            } finally {
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            }
        });
    }

    @Test
    public void testRecoverRestoresTxnToEpochCut() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            // Epoch interval -1 => the auto-epoch never fires; we drive the durable cut explicitly so the
            // epoch is recorded at a KNOWN seqTxn (K), then apply more after it without a new epoch.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

                execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
                // K rows, fully applied.
                for (int i = 0; i < 3; i++) {
                    execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("r");

                final long epochSeqTxn;
                try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                    epochSeqTxn = w.getSeqTxn();
                }
                Assert.assertTrue("epoch must be at seqTxn >= 3", epochSeqTxn >= 3);

                // M more rows, applied lazily AFTER the epoch (no new epoch fires).
                for (int i = 3; i < 7; i++) {
                    execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Sanity: _txn now reflects the post-epoch frontier (seqTxn advanced past the epoch).
                final long frontierSeqTxn = readTxnSeqTxn(tt);
                Assert.assertTrue("post-epoch _txn seqTxn must be > epoch seqTxn",
                        frontierSeqTxn > epochSeqTxn);

                // Release writers so RecoveryCoordinator can rewrite _txn/_cv unobstructed.
                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // ACT: roll forward / restore the durable cut.
                new RecoveryCoordinator(engine).recover();

                // ASSERT: _txn was rewound to exactly the epoch cut.
                final long restoredSeqTxn = readTxnSeqTxn(tt);
                Assert.assertEquals("recovery must restore _txn to the epoch cut", epochSeqTxn, restoredSeqTxn);

                // ASSERT: recoveryIncarnation was bumped exactly once (one successful restore).
                final long incarnation = engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation();
                Assert.assertEquals("recoveryIncarnation must be 1 after one successful restore", 1L, incarnation);
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * SP-F Task 3: {@code wal_adaptive_recovery_events} is the engine-wide aggregate counter of
     * "a table was rewound to its durable epoch at recovery" — incremented once per successful
     * validated restore, co-located with the per-table {@code bumpRecoveryIncarnation()} call. This
     * mirrors {@link #testRecoverRestoresTxnToEpochCut()}'s arrange (the happy-path rewind) and asserts
     * the global counter moves; the per-table {@code _txn}/{@code recoveryIncarnation} assertions for
     * this same scenario are already covered there.
     */
    @Test
    public void testRecoverIncrementsRecoveryEventsMetric() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            // Epoch interval -1 => the auto-epoch never fires; we drive the durable cut explicitly so the
            // epoch is recorded at a KNOWN seqTxn (K), then apply more after it without a new epoch.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

                execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
                // K rows, fully applied.
                for (int i = 0; i < 3; i++) {
                    execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("r");

                final long epochSeqTxn;
                try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                    epochSeqTxn = w.getSeqTxn();
                }
                Assert.assertTrue("epoch must be at seqTxn >= 3", epochSeqTxn >= 3);

                // M more rows, applied lazily AFTER the epoch (no new epoch fires).
                for (int i = 3; i < 7; i++) {
                    execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Sanity: _txn now reflects the post-epoch frontier (seqTxn advanced past the epoch).
                final long frontierSeqTxn = readTxnSeqTxn(tt);
                Assert.assertTrue("post-epoch _txn seqTxn must be > epoch seqTxn",
                        frontierSeqTxn > epochSeqTxn);

                // Release writers so RecoveryCoordinator can rewrite _txn/_cv unobstructed.
                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // ACT + ASSERT: the global recovery-events counter must increment for this successful
                // validated restore (the metric is the only new behavior under test here; the per-table
                // _txn/recoveryIncarnation outcomes for this exact scenario are asserted in
                // testRecoverRestoresTxnToEpochCut).
                long before = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events_total");
                new RecoveryCoordinator(engine).recover();
                long after = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events_total");
                assertTrue("a table rewound to its durable epoch at recovery must increment the counter", after > before);
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * A clean restart has nothing to recover. The writer's close epoch copies the live {@code _meta},
     * {@code _cv} and {@code _txn} as they stand, so recovery finds the live files already at the epoch cut.
     * It must not rewrite or fsync them, and must not count the boot as a recovery, which operators read as
     * a crash. It still pins the epoch, which protects the cut and bounds WAL purge, and makes the selected save
     * point headers durable: one syncfs per start where syncfs is filesystem-wide, one fsync per table elsewhere.
     */
    @Test
    public void testCleanRestartRestoresNothing() throws Exception {
        assertCleanRestartRestoresNothing("clean_restart");
    }

    /**
     * A clean restart is not a recovery, whichever save point the live files stand at: the creation baseline of a
     * table nothing has written yet, or the save point a table created outside adaptive takes when its writer
     * enrols it. Recovery restores nothing, counts nothing and changes no file of the table, at this start and at
     * the next.
     */
    @Test
    public void testCleanRestartAtFirstSavePointCountsNoRecovery() throws Exception {
        try {
            assertMemoryLeak(() -> {
                final List<String> failures = new ArrayList<>();
                for (String origin : new String[]{"creation", "enrolment"}) {
                    final boolean enrolment = "enrolment".equals(origin);
                    setCommitMode(enrolment ? "nosync" : "adaptive");
                    final String name = "clean_" + origin;
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    final TableToken tt = engine.verifyTableName(name);
                    if (enrolment) {
                        execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                        drainWalQueue();
                        engine.releaseAllWriters();
                        setCommitMode("adaptive");
                        try (TableWriter writer = getWriter(tt)) {
                            Assert.assertNotNull(writer);
                        }
                    }
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();
                    Assert.assertTrue("precondition: the " + origin + " published a save point", hasSavePointFile(tt));
                    for (int start = 1; start < 3; start++) {
                        final Map<String, byte[]> before = readTableFiles(tt, true);
                        final long events = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events_total");
                        engine.getTxnScoreboardPool().remove(tt);
                        engine.getTableSequencerAPI().resetForReboot(tt);
                        new RecoveryCoordinator(engine).recover();
                        final long counted = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events_total") - events;
                        final long incarnation = engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation();
                        final List<String> changed = changedFiles(before, readTableFiles(tt, true));
                        if (counted != 0 || incarnation != 0 || !changed.isEmpty()) {
                            failures.add(origin + ", start " + start + ": recovery events +" + counted + ", incarnation " + incarnation + ", changed " + changed);
                        }
                    }
                    assertQuery("SELECT count() FROM " + name)
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n" + (enrolment ? 1 : 0) + "\n");
                }
                Assert.assertEquals("a clean restart is not a recovery", "[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * A lineage discard removes the save point and publishes none: the table is then not enrolled, and its first
     * writer enrols it at the live cut. A second start in between leaves the table as it is.
     */
    @Test
    public void testLineageDiscardLeavesEnrolmentToWriter() throws Exception {
        org.junit.Assume.assumeFalse("replants files over existing destinations, which Windows forbids", Os.isWindows());
        try {
            assertMemoryLeak(() -> {
                setCommitMode("adaptive");
                execute("create table lin_writer (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into lin_writer values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                execute("insert into lin_writer values ('2024-09-02T00:00:00.000000Z', 2)");
                drainWalQueue();
                final TableToken pre = engine.verifyTableName("lin_writer");
                Assert.assertTrue("precondition: a save point beyond seqTxn 0", readTxnSeqTxn(pre) > 0);
                copyEpochArtifacts(pre, false);

                execute("alter table lin_writer set type bypass wal");
                engine.releaseInactive();
                engine.load();
                execute("alter table lin_writer set type wal", sqlExecutionContext);
                engine.releaseInactive();
                engine.load();

                final TableToken tt = engine.verifyTableName("lin_writer");
                Assert.assertEquals("the conversion re-seeds the lineage at seqTxn 0", 0L, readTxnSeqTxn(tt));
                copyEpochArtifacts(tt, true);

                // A restart: a fresh scoreboard and tracker, so the conversion's baseline pin is gone.
                engine.getTxnScoreboardPool().remove(tt);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("the discarded save point must not rewind the live cut", 0L, readTxnSeqTxn(tt));
                Assert.assertFalse("a lineage discard publishes no save point", hasSavePointFile(tt));
                Assert.assertFalse("a lineage discard leaves no enrolment marker", epochArtifactExists(tt, "_epoch.enrol", ""));

                final Map<String, byte[]> before = readTableFiles(tt, false);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("a second start leaves a table that is not enrolled as it is",
                        "[]", changedFiles(before, readTableFiles(tt, false)).toString());

                try (TableWriter writer = getWriter(tt)) {
                    Assert.assertNotNull(writer);
                }
                try (Path p = new Path()) {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt);
                    final io.questdb.cairo.DurableEpochManifest.Candidate[] saved = io.questdb.cairo.DurableEpochManifest.loadCandidates(
                            engine.getConfiguration().getFilesFacade(), p, p.size(), tt.getTableId());
                    Assert.assertTrue("the writer enrols the table", saved.length > 0);
                    Assert.assertEquals("the writer enrols at the live cut", 0L, saved[0].epochSeqTxn);
                }
                execute("insert into lin_writer values ('2024-09-03T00:00:00.000000Z', 3)");
                drainWalQueue();
                assertQuery("select count() from lin_writer")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n3\n");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    @Test
    public void testNonPartitionedWalTableOnNonWideSyncfsPlatform() throws Exception {
        // A non-partitioned table names its default partition before the first row creates its directory: the
        // initial save point on a file-by-file flush platform must not walk a baseline that holds no rows.
        final BaselineBarrierFacade ff = new BaselineBarrierFacade(false);
        assertMemoryLeak(ff, () -> {
            setCommitMode("adaptive");
            TestUtils.createTable(engine, new TableModel(configuration, "none_wal", PartitionBy.NONE)
                    .col("v", ColumnType.LONG)
                    .timestamp("ts")
                    .wal());
            execute("INSERT INTO none_wal VALUES (1, '2024-09-01T00:00:00.000000Z')");
            drainWalQueue();
            assertQuery("SELECT * FROM none_wal")
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            v\tts
                            1\t2024-09-01T00:00:00.000000Z
                            """);
        });
    }

    /**
     * The boot cost outside adaptive: for each WAL table without a save point, recovery makes at most two existence
     * checks and no other file system call. Measured as the difference between a database of one such table and
     * one of eleven.
     */
    @Test
    public void testNosyncBootCostsAtMostTwoExistenceChecksPerWalTable() throws Exception {
        assertMemoryLeak(() -> {
            setCommitMode("nosync");
            final int extraTables = 10;
            for (int i = 0; i <= extraTables; i++) {
                execute("CREATE TABLE boot_cost_" + i + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO boot_cost_" + i + " VALUES ('2024-09-01T00:00:00.000000Z', " + i + ')');
                if (i == 0) {
                    drainWalQueue();
                }
            }
            drainWalQueue();
            final Map<String, Integer> eleven = countRecoveryCalls();
            for (int i = 1; i <= extraTables; i++) {
                execute("DROP TABLE boot_cost_" + i);
            }
            drainWalQueue();
            final Map<String, Integer> one = countRecoveryCalls();
            final Map<String, Integer> perTable = new TreeMap<>();
            for (Map.Entry<String, Integer> e : eleven.entrySet()) {
                final int delta = e.getValue() - one.getOrDefault(e.getKey(), 0);
                if (delta != 0) {
                    perTable.put(e.getKey(), delta);
                }
            }
            final int existenceChecks = perTable.getOrDefault("exists", 0);
            perTable.remove("exists");
            Assert.assertTrue("at most two existence checks per WAL table, got " + existenceChecks + " for " + extraTables + " tables",
                    existenceChecks <= 2 * extraTables);
            Assert.assertEquals("no other file system call per WAL table, for " + extraTables + " tables and "
                    + existenceChecks + " existence checks", "{}", perTable.toString());
        });
    }

    /**
     * A power loss tore {@code _meta} while recovery wrote the save point's copy over it, and the next start runs
     * outside adaptive. The table still has a valid save point, so it is enrolled: the start restores it, boots, and
     * the WAL replays every row, whatever the tear left: an empty file, half of the copy, or the copy's length
     * without its bytes.
     */
    @Test
    public void testNosyncStartAfterTornMetaRestoreBoots() throws Exception {
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000L);
        try {
            assertMemoryLeak(() -> {
                final List<String> failures = new ArrayList<>();
                for (String tear : new String[]{"empty", "half", "zeros"}) {
                    setCommitMode("adaptive");
                    final String name = "torn_meta_" + tear;
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                    final TableToken tt = engine.verifyTableName(name);
                    try (TableWriter writer = getWriter(tt)) {
                        writer.advanceDurableEpoch(engine.getConfiguration().getMicrosecondClock().getTicks() / 1000L);
                    }
                    final long savePointSeqTxn = readTxnSeqTxn(tt);
                    final byte[] savedMeta = readTableFiles(tt, true).get(TableUtils.META_FILE_NAME);
                    execute("ALTER TABLE " + name + " ADD COLUMN c INT");
                    execute("INSERT INTO " + name + " VALUES ('2024-09-02T00:00:00.000000Z', 2, 7)");
                    drainWalQueue();
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();
                    Assert.assertEquals("precondition: the save point is still the one whose _meta was read",
                            savePointSeqTxn, newestSavePointSeqTxn(tt));
                    Assert.assertTrue("precondition: the live files are ahead of the save point", readTxnSeqTxn(tt) > savePointSeqTxn);
                    final byte[] torn = switch (tear) {
                        case "empty" -> new byte[0];
                        case "half" -> Arrays.copyOf(Arrays.copyOf(savedMeta, savedMeta.length / 2), savedMeta.length);
                        default -> new byte[savedMeta.length];
                    };
                    Files.write(Paths.get(engine.getConfiguration().getDbRoot().toString(), tt.getDirName(), TableUtils.META_FILE_NAME), torn);

                    setCommitMode("nosync");
                    engine.getTxnScoreboardPool().remove(tt);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    try {
                        new RecoveryCoordinator(engine).recover();
                    } catch (CairoException e) {
                        failures.add(tear + " _meta: the start refused: " + e.getFlyweightMessage());
                        continue;
                    }
                    engine.notifyWalTxnRepublisher(tt);
                    drainWalQueue();
                    if (engine.getTableSequencerAPI().isSuspended(tt)) {
                        failures.add(tear + " _meta: the table is suspended");
                        continue;
                    }
                    try {
                        assertQuery("SELECT * FROM " + name)
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tv\tc
                                        2024-09-01T00:00:00.000000Z\t1\tnull
                                        2024-09-02T00:00:00.000000Z\t2\t7
                                        """);
                    } catch (Throwable th) {
                        failures.add(tear + " _meta: the table reads back wrong: " + th.getMessage());
                    }
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * The safe way back from adaptive: an adaptive run, a clean stop, then a start outside adaptive in which nothing
     * writes the table, so no writer opens it. The start itself leaves the table durable without its save point: a
     * power loss right after it, with no recovery to follow (an older release), finds every byte the start left in the
     * table's files, and no save point.
     */
    @Test
    public void testSafeWayBackLeavesUnwrittenTableDurableWithoutSavePoint() throws Exception {
        org.junit.Assume.assumeFalse("the crash model rewrites files under a live engine, which Windows forbids", Os.isWindows());
        final CrashFaultFilesFacade crashFf = new CrashFaultFilesFacade();
        crashFf.setDbRoot(root);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        try {
            assertMemoryLeak(crashFf, () -> {
                final List<String> failures = new ArrayList<>();
                for (String mode : new String[]{"nosync", "sync"}) {
                    setCommitMode("adaptive");
                    final String name = "way_back_" + mode;
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    final TableToken tt = engine.verifyTableName(name);
                    crashFf.markDurableBaseline(engine.getConfiguration().getDbRoot());
                    execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                    execute("INSERT INTO " + name + " VALUES ('2024-09-02T00:00:00.000000Z', 2), ('2024-09-03T00:00:00.000000Z', 3)");
                    drainWalQueue();
                    Assert.assertFalse("precondition: the instrument sees the lazily applied bytes a power loss would lose",
                            bytesLostAtPowerLoss(crashFf, tt, readTableFiles(tt, true)).isEmpty());
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();
                    Assert.assertEquals("precondition: the clean stop leaves a save point at the live cut", readTxnSeqTxn(tt), newestSavePointSeqTxn(tt));

                    setCommitMode(mode);
                    engine.getTxnScoreboardPool().remove(tt);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    if (hasSavePointFile(tt)) {
                        failures.add(mode + " start: the save point remains");
                    }
                    final Map<String, byte[]> started = readTableFiles(tt, true);
                    final List<String> lost = bytesLostAtPowerLoss(crashFf, tt, started);
                    crashFf.powerLoss();
                    crashFf.crash(engine.getConfiguration().getDbRoot());
                    lost.addAll(changedPrefixes(started, readTableFiles(tt, true)));
                    if (!lost.isEmpty()) {
                        failures.add(mode + " start: a power loss after it loses bytes of " + lost);
                    }
                    engine.getTableSequencerAPI().resetForReboot(tt);
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * A save point whose {@code _txn} and {@code _meta} sections describe different table shapes must be
     * rejected, not adopted. Every checksum passes: the sections are byte-for-byte what was saved. Only the
     * content checks, run on the restored live files, compare {@code _txn}'s symbol count with the symbol
     * columns {@code _meta} declares. The skew is produced at save time (a stale one-symbol {@code _meta} is
     * served in place of the live one, at the live metadata version), which is the state a cut taken inside
     * {@code changeColumnType}'s structural window would leave. Recovery must fall back to the older, consistent
     * generation, whose replay floor lies before the ALTER that added the second symbol column.
     */
    @Test
    public void testRecoveryRejectsSavePointWhoseTxnAndMetaDisagreeOnSymbolColumns() throws Exception {
        final StaleMetaFacade staleFf = new StaleMetaFacade();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            assertMemoryLeak(staleFf, () -> {
                Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
                execute("create table skew (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
                for (int i = 0; i < 4; i++) {
                    execute("insert into skew values ('2024-10-01T0" + i + ":00:00.000000Z', 's" + i + "', " + i + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("skew");
                final long consistentSeqTxn;
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                    consistentSeqTxn = w.getSeqTxn();
                }

                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path live = new Path(); Path stale = new Path()) {
                    live.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.META_FILE_NAME);
                    stale.of(engine.getConfiguration().getDbRoot()).concat("stale_meta_one_symbol");
                    Assert.assertTrue(ff.copy(live.$(), stale.$()) >= 0);

                    execute("alter table skew add column sym2 symbol");
                    for (int i = 4; i < 8; i++) {
                        execute("insert into skew values ('2024-10-01T0" + i + ":00:00.000000Z', 's" + i + "', " + i + ", 'x')");
                    }
                    drainWalQueue();
                    // Equal metadata versions, as the real defect produced: otherwise the version check rejects
                    // the save point first and the symbol check is never reached.
                    try (TableMetadata meta = engine.getTableMetadata(tt);
                         io.questdb.cairo.vm.api.MemoryMARW mem = io.questdb.cairo.vm.Vm.getCMARWInstance()) {
                        mem.smallFile(ff, stale.$(), MemoryTag.MMAP_DEFAULT);
                        TableUtils.resetMetadataVersion(mem, meta.getMetadataVersion());
                    }

                    staleFf.arm(live.toString(), stale.toString());
                    try (TableWriter w = getWriter(tt)) {
                        w.advanceDurableEpoch(2L);
                    } finally {
                        staleFf.disarm();
                    }
                    Assert.assertTrue("the stale _meta was never served, so no skewed save point was written", staleFf.served > 0);
                }

                for (int i = 8; i < 11; i++) {
                    execute("insert into skew values ('2024-10-02T0" + (i - 8) + ":00:00.000000Z', 's" + i + "', " + i + ", 'x')");
                }
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();

                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("recovery must fall back to the consistent generation", consistentSeqTxn, readTxnSeqTxn(tt));
                // A second start before the replay: the rejected generation is still listed first and is ahead of
                // the live _txn the fallback rewound, which must not read as a wrong-lineage state.
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("a second start must stay on the consistent generation", consistentSeqTxn, readTxnSeqTxn(tt));

                engine.getTxnScoreboardPool().remove(tt);
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
                assertQuery("select count(), count(sym2) from skew").noRandomAccess().expectSize().returns("count\tcount1\n11\t7\n");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testRecoveryRestoresMetadataMatchingEpochTxnAfterStructuralWal() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                execute("create table schema_epoch (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken token = engine.verifyTableName("schema_epoch");
                try (Path markerPath = new Path()) {
                    final int rootLen = markerPath.of(configuration.getDbRoot()).concat(token).size();
                    final io.questdb.cairo.DurableEpochManifest.Candidate[] saved = io.questdb.cairo.DurableEpochManifest.loadCandidates(configuration.getFilesFacade(), markerPath, rootLen, token.getTableId());
                    Assert.assertEquals("creation must publish generation zero", 1, saved.length);
                    Assert.assertTrue("creation epoch must bind its metadata payload", io.questdb.cairo.DurableEpochManifest.validateSections(
                            configuration, token, markerPath, rootLen, 0, saved[0].epochSeqTxn, saved[0].epochTxn));
                }
                execute("alter table schema_epoch add column extra long");
                drainWalQueue();

                try (Path epochMetaPath = new Path(); TableReaderMetadata epochMetadata = new TableReaderMetadata(configuration)) {
                    epochMetaPath.of(configuration.getDbRoot()).concat("saved" + TableUtils.META_FILE_NAME);
                    TestUtils.copySavePointSection(configuration.getFilesFacade(), configuration.getDbRoot(), token, 0, TableUtils.META_FILE_NAME, epochMetaPath.$());
                    epochMetadata.loadMetadata(epochMetaPath.$());
                    Assert.assertEquals("creation epoch metadata must remain immutable", 0, epochMetadata.getMetadataVersion());
                }

                long liveMetadataVersion;
                try (TableMetadata metadata = engine.getTableMetadata(token)) {
                    liveMetadataVersion = metadata.getMetadataVersion();
                }
                Assert.assertTrue("structural WAL must advance live metadata beyond the creation epoch",
                        liveMetadataVersion > 0);

                engine.releaseAllWriters();
                engine.releaseAllReaders();
                try (Path epochMetaPath = new Path(); TableReaderMetadata epochMetadata = new TableReaderMetadata(configuration)) {
                    epochMetaPath.of(configuration.getDbRoot()).concat("saved" + TableUtils.META_FILE_NAME);
                    TestUtils.copySavePointSection(configuration.getFilesFacade(), configuration.getDbRoot(), token, 0, TableUtils.META_FILE_NAME, epochMetaPath.$());
                    epochMetadata.loadMetadata(epochMetaPath.$());
                    Assert.assertEquals("writer release must not mutate epoch metadata", 0, epochMetadata.getMetadataVersion());
                    epochMetaPath.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                    configuration.getFilesFacade().remove(epochMetaPath.$());
                    Assert.assertFalse("simulate crash during metadata swap", configuration.getFilesFacade().exists(epochMetaPath.$()));
                }
                new RecoveryCoordinator(engine).recover();

                final long restoredMetadataVersion;
                try (Path metaPath = new Path(); TableReaderMetadata metadata = new TableReaderMetadata(configuration);
                     TxReader txn = new TxReader(configuration.getFilesFacade())) {
                    metaPath.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                    metadata.loadMetadata(metaPath.$());
                    restoredMetadataVersion = metadata.getMetadataVersion();
                    metaPath.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                    txn.ofRO(metaPath.$(), metadata.getTimestampType(), metadata.getPartitionBy());
                    Assert.assertTrue(txn.unsafeLoadAll());
                    Assert.assertEquals("restored _meta and _txn must describe the same schema cut",
                            restoredMetadataVersion, txn.getMetadataVersion());
                }
                Assert.assertEquals("creation baseline must restore metadata version zero", 0, restoredMetadataVersion);
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * The interrupted swap of {@link #testRecoverRepairsMetaSwapInterruptedBetweenRenames()} on an enrolled
     * adaptive table: the {@code _todo_} repair runs first, so the durable epoch is restored over a readable
     * {@code _meta}, and the writer later finds no {@code _meta.prev} to roll back over the restored cut.
     */
    @Test
    public void testRecoverRepairsInterruptedMetaSwapBeforeAdaptiveRestore() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                final long epochSeqTxn = buildAdaptiveLazyGapTable("swap_adaptive");
                final TableToken tt = engine.verifyTableName("swap_adaptive");
                crashMetaSwap(tt, false);
                assertMetaSwapFiles(tt, false, true);

                new RecoveryCoordinator(engine).recover();

                assertMetaSwapFiles(tt, true, false);
                Assert.assertEquals("recovery must restore _txn to the epoch cut", epochSeqTxn, readTxnSeqTxn(tt));
                drainWalQueue();
                try (TableReader reader = engine.getReader(tt)) {
                    Assert.assertEquals("WAL replay must rebuild every row past the epoch", 7L, reader.size());
                }
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * First start after switching an instance to adaptive: a table created under nosync was never enrolled,
     * so it has no anchor and no adaptive state. A {@code _meta} that cannot be read there must not stop the
     * instance from starting; nothing can open the table without it, so it fails on its own when opened.
     */
    @Test
    public void testAdaptiveBootLeavesNeverEnrolledTableWithUnreadableMetaToFail() throws Exception {
        assertBootLeavesTablesWithUnreadableMetaToFail("nosync", "adaptive");
    }

    /**
     * A table whose {@code _meta} cannot be read must not stop a nosync instance from starting, exactly as
     * before adaptive recovery existed. Recovery reads {@code _meta} of every WAL table, because a table left
     * enrolled by adaptive must be rolled forward whatever the instance runs now, but a table it cannot read
     * is left to fail when opened.
     */
    @Test
    public void testNosyncBootLeavesTablesWithUnreadableMetaToFail() throws Exception {
        assertBootLeavesTablesWithUnreadableMetaToFail("nosync", "nosync");
    }

    @Test
    public void testSyncBootLeavesTablesWithUnreadableMetaToFail() throws Exception {
        assertBootLeavesTablesWithUnreadableMetaToFail("sync", "sync");
    }

    /**
     * A crash between TableWriter's two metadata renames leaves no {@code _meta}, only {@code _meta.prev} and a
     * {@code _todo_} armed to restore it. On a table without a save point recovery does not read {@code _meta}:
     * the instance starts and the writer restores it when it opens.
     */
    @Test
    public void testRecoverRepairsMetaSwapInterruptedBetweenRenames() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            execute("CREATE TABLE swap_gap (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO swap_gap VALUES ('2024-09-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("swap_gap");
            crashMetaSwap(tt, false);
            assertMetaSwapFiles(tt, false, true);

            // Boot a whole engine over the crashed root, the way ServerMain does.
            try (CairoEngine rebooted = new CairoEngine(configuration)) {
                Assert.assertNotNull(rebooted.getTableTokenIfExists("swap_gap"));
            }
            assertMetaSwapFiles(tt, false, true);

            try (io.questdb.cairo.TableWriter writer = getWriter(tt)) {
                Assert.assertNotNull(writer);
            }
            assertMetaSwapFiles(tt, true, false);
            Assert.assertEquals("the pre-ALTER schema must be restored", 2, readMetaColumnCount(tt));
            // The table opens and takes the interrupted ALTER again.
            execute("ALTER TABLE swap_gap ADD COLUMN c INT");
            execute("INSERT INTO swap_gap VALUES ('2024-09-01T01:00:00.000000Z', 2, 3)");
            drainWalQueue();
            assertQuery("SELECT * FROM swap_gap")
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv\tc
                            2024-09-01T00:00:00.000000Z\t1\tnull
                            2024-09-01T01:00:00.000000Z\t2\t3
                            """);
        });
    }

    /**
     * A {@code _todo_} restore entry whose txn pair does not match the live {@code _txn} is stale: TableWriter
     * ignores it, and recovery must never roll {@code _meta} back on its strength.
     */
    @Test
    public void testRecoverIgnoresStaleMetaRestoreTodo() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            execute("CREATE TABLE swap_stale (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO swap_stale VALUES ('2024-09-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("swap_stale");
            crashMetaSwap(tt, true);
            // Keep the entry internally consistent, but name a txn the table is not at.
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            pokeLong(ff, tt, TableUtils.TODO_FILE_NAME, 0, 1_000_000);
            pokeLong(ff, tt, TableUtils.TODO_FILE_NAME, 24, 1_000_000);

            new RecoveryCoordinator(engine).recover();

            assertMetaSwapFiles(tt, true, true);
            Assert.assertEquals("a stale _todo_ must leave _meta alone", 3, readMetaColumnCount(tt));
        });
    }

    /**
     * A crash after the second rename, before {@code _txn} commits, leaves the uncommitted ALTER's {@code _meta}
     * in place. On a table without a save point recovery leaves it, and the writer rolls it back when it opens.
     */
    @Test
    public void testRecoverRollsBackMetaSwapInterruptedBeforeTxnCommit() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            execute("CREATE TABLE swap_uncommitted (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO swap_uncommitted VALUES ('2024-09-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("swap_uncommitted");
            crashMetaSwap(tt, true);
            assertMetaSwapFiles(tt, true, true);
            Assert.assertEquals("precondition: _meta holds the uncommitted ALTER", 3, readMetaColumnCount(tt));

            new RecoveryCoordinator(engine).recover();
            assertMetaSwapFiles(tt, true, true);

            try (io.questdb.cairo.TableWriter writer = getWriter(tt)) {
                Assert.assertNotNull(writer);
            }
            assertMetaSwapFiles(tt, true, false);
            Assert.assertEquals("the uncommitted ALTER must be rolled back", 2, readMetaColumnCount(tt));
            assertQuery("SELECT * FROM swap_uncommitted")
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-09-01T00:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testCreationBaselineAndPreviousGenerationFallback() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                execute("create table baseline (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken token = engine.verifyTableName("baseline");
                try (Path p = new Path()) {
                    p.of(engine.getConfiguration().getDbRoot()).concat(token);
                    io.questdb.cairo.DurableEpochManifest.Candidate[] candidates = io.questdb.cairo.DurableEpochManifest.loadCandidates(engine.getConfiguration().getFilesFacade(), p, p.size(), token.getTableId());
                    Assert.assertEquals(1, candidates.length);
                    Assert.assertEquals(0, candidates[0].epochSeqTxn);
                    Assert.assertEquals(0, candidates[0].generation);
                }

                execute("insert into baseline values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                    writer.advanceDurableEpoch(2L);
                    Assert.assertTrue(writer.getSeqTxn() > 0);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();

                final io.questdb.std.FilesFacade files = engine.getConfiguration().getFilesFacade();
                try (Path p = new Path()) {
                    // Cut generation one's file at its _txn section: the sections are gone, the header stays.
                    final long[] section = TestUtils.savePointSection(files, p, engine.getConfiguration().getDbRoot(), token, 1, TableUtils.TXN_FILE_NAME);
                    final long fd = files.openRW(p.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertTrue(files.truncate(fd, section[0]));
                    } finally {
                        files.close(fd);
                    }
                }

                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("torn newest generation must fall back to creation baseline", 0, readTxnSeqTxn(token));
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    @Test
    public void testSavePointAfterFallbackOverwritesTheDamagedGeneration() throws Exception {
        // An idle table stands on two generations of one cut (the idle save point), listed generation 0 first. When
        // generation 0 fails its checksums, recovery selects generation 1 and the table stands on it. The next save
        // point must overwrite generation 0, the damaged one: writing into generation 1 would truncate the only
        // intact generation, and a power loss inside that write would leave the table without a trustworthy one.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                execute("create table tie (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("tie");
                execute("insert into tie values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final long cutSeqTxn;
                final long cutTxn;
                try (TableWriter writer = getWriter(tt)) {
                    // Generation 1, then generation 0 at the same cut, as the idle save point does.
                    writer.advanceDurableEpoch(1L);
                    writer.advanceDurableEpoch(2L);
                    cutSeqTxn = writer.getSeqTxn();
                    cutTxn = writer.getTxn();
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                try (Path p = new Path()) {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt);
                    final io.questdb.cairo.DurableEpochManifest.Candidate[] tie = io.questdb.cairo.DurableEpochManifest.loadCandidates(engine.getConfiguration().getFilesFacade(), p, p.size(), tt.getTableId());
                    Assert.assertEquals("precondition: two generations", 2, tie.length);
                    Assert.assertEquals("precondition: generation 0 is listed first on a tie", 0, tie[0].generation);
                    Assert.assertEquals("precondition: both generations hold the cut", tie[0].epochTxn, tie[1].epochTxn);
                }
                assertSavePointHolds(tt, 1, cutSeqTxn, cutTxn);
                corruptSavePointSection(tt, 0);

                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("recovery must stand on the intact generation", cutSeqTxn, readTxnSeqTxn(tt));

                execute("insert into tie values ('2024-09-02T00:00:00.000000Z', 2)");
                drainWalQueue();
                final long nextSeqTxn;
                final long nextTxn;
                try (TableWriter writer = getWriter(tt)) {
                    writer.advanceDurableEpoch(3L);
                    nextSeqTxn = writer.getSeqTxn();
                    nextTxn = writer.getTxn();
                }
                Assert.assertTrue("precondition: the save point names a later cut", nextSeqTxn > cutSeqTxn);
                assertSavePointHolds(tt, 1, cutSeqTxn, cutTxn);
                assertSavePointHolds(tt, 0, nextSeqTxn, nextTxn);
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    @Test
    public void testRecoverFailsOnSavePointReadErrorInsteadOfFallingBack() throws Exception {
        // A read error on the newest generation at start is not a failed checksum. Falling back to the older
        // generation would rewind the table over a transient fault, and the WAL after the older cut may be gone.
        // The start fails with the I/O error, retryable, and leaves the live files as they are.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            final int simErrno = 5;
            final SavePointMmapFaultFacade failingFf = new SavePointMmapFaultFacade(1, 1, simErrno);
            final FilesFacade ffBefore = AbstractCairoTest.ff;
            try {
                buildAdaptiveLazyGapTable("ioerr");
                final TableToken tt = engine.verifyTableName("ioerr");
                final long liveSeqTxn = readTxnSeqTxn(tt);
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                AbstractCairoTest.ff = failingFf;
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("startup recovery must fail on a save point read error, not fall back");
                } catch (CairoException expected) {
                    Assert.assertEquals(simErrno, expected.getErrno());
                }
                AbstractCairoTest.ff = ffBefore;
                Assert.assertEquals("the injected fault must fire", 1, failingFf.failures);
                Assert.assertEquals("a read error must not rewind the table", liveSeqTxn, readTxnSeqTxn(tt));
                Assert.assertEquals("a failed start must not bump recoveryIncarnation",
                        0L, engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation());
            } finally {
                AbstractCairoTest.ff = ffBefore;
                engine.resetDurabilityFailure();
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    @Test
    public void testRecoverReportsSavePointReadErrorInsteadOfPostDatesRefusal() throws Exception {
        // The state an earlier fallback leaves: the live _txn equals the older generation's section and the newer
        // generation is ahead of it. The start recognises it by reading the older generation; a read error there
        // is not evidence of a wrong lineage, so the start must fail with the I/O error, not refuse with the
        // post-dates message whose manual step is for another cause.
        org.junit.Assume.assumeFalse("the live _txn is rewritten by hand, which Windows forbids", Os.isWindows());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            final int simErrno = 5;
            // The second mmap of generation 1 in the start: its section check, then the comparison with the live _txn.
            final SavePointMmapFaultFacade failingFf = new SavePointMmapFaultFacade(1, 2, simErrno);
            final FilesFacade ffBefore = AbstractCairoTest.ff;
            try {
                execute("create table rejected (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("rejected");
                for (int i = 0; i < 3; i++) {
                    execute("insert into rejected values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                final long olderSeqTxn;
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                    olderSeqTxn = w.getSeqTxn();
                }
                for (int i = 3; i < 6; i++) {
                    execute("insert into rejected values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(2L);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                // The live _txn and _cv as the older generation saved them: the rewind of a fallback.
                restoreLiveFileFromSavePoint(tt, 1, TableUtils.TXN_FILE_NAME);
                restoreLiveFileFromSavePoint(tt, 1, TableUtils.COLUMN_VERSION_FILE_NAME);
                Assert.assertEquals("precondition: the live _txn is at the older cut", olderSeqTxn, readTxnSeqTxn(tt));

                AbstractCairoTest.ff = failingFf;
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("startup recovery must fail on a save point read error");
                } catch (CairoException expected) {
                    Assert.assertFalse("a read error is not a wrong-lineage verdict: " + expected.getFlyweightMessage(),
                            io.questdb.std.Chars.contains(expected.getFlyweightMessage(), "post-dates"));
                    Assert.assertEquals(simErrno, expected.getErrno());
                }
                AbstractCairoTest.ff = ffBefore;
                Assert.assertEquals("the injected fault must fire", 1, failingFf.failures);
                Assert.assertEquals("a failed start must leave the live _txn untouched", olderSeqTxn, readTxnSeqTxn(tt));
            } finally {
                AbstractCairoTest.ff = ffBefore;
                engine.resetDurabilityFailure();
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * Regression: a regular VIEW token is {@code isView()==true} AND {@code isWal()==true} (see
     * {@code CreateViewOperationImpl}), so it slips past the loop's {@code !isWal()} filter. But a view has
     * no {@code _meta}/{@code _txn}/{@code _cv}/data/epoch, and its {@code ViewState} is NOT hydrated when
     * {@code recover()} runs (at {@code CairoEngine.completeInit}, right after the name registry is loaded
     * but BEFORE views are compiled). Before the fix, resolving the view's metadata threw
     * {@code view does not exist} on the view token and failed boot. {@code recover()} must skip regular
     * views (mat-views, {@code isView()==false}, are still recovered).
     */
    @Test
    public void testRecoverSkipsRegularViewWithUnhydratedState() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                execute("create table base (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into base values ('2024-09-01T00:00:00.000000Z', 1)");
                execute("create view v as (select * from base)");
                drainWalQueue();

                final TableToken viewToken = engine.verifyTableName("v");
                // The two flags that make a regular view slip into the recovery loop:
                Assert.assertTrue("precondition: v must be a regular VIEW", viewToken.isView());
                Assert.assertTrue("precondition: view token is WAL", viewToken.isWal());

                // Reproduce the boot condition precisely. At completeInit, recover() runs against an
                // enumerable view token (the name registry is loaded) whose ViewState is NOT yet hydrated —
                // views compile lazily, after recover() — so getViewMetadata() returns null.
                engine.getViewStateStore().removeViewState(viewToken);
                Assert.assertNull("view state must be absent (pre-hydration boot condition)",
                        engine.getViewStateStore().getViewState(viewToken));

                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // ACT: before the fix this threw `view does not exist [view=v]` and failed boot.
                new RecoveryCoordinator(engine).recover();

                // ASSERT: the view was skipped, never treated as a recoverable adaptive table.
                final long incarnation = engine.getTableSequencerAPI().getTxnTracker(viewToken).getRecoveryIncarnation();
                Assert.assertEquals("a regular view must not be recovered", 0L, incarnation);
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * Restore/PITR coexistence: a durable epoch is a PAST cut, so in one lineage the live {@code _txn} is
     * always at or ahead of it. If a backup/checkpoint/PITR restore rewinds the table BENEATH a stale
     * epoch it forgot to clear, the on-disk epoch ends up AHEAD of the (restored) live {@code _txn}.
     * Recovery must NOT roll {@code _txn} forward to that stale, higher-lineage epoch — doing so resurrects
     * the discarded lineage and leaves {@code _txn} ahead of the restored sequencer. Here we simulate the
     * restore as the file copies it physically is (restore the earlier {@code _txn}/{@code _cv} over the
     * live ones while leaving {@code _snapshot}/{@code .epoch} from a LATER cut in place).
     */
    @Test
    public void testRecoverSkipsEpochAheadOfRestoredTxn() throws Exception {
        // The epoch-ahead-of-live guard is platform-neutral, but this test's physical restore simulation
        // is not: it reads a live _txn QuestDB holds locked (Windows ReadFile fails on the byte-0 lock)
        // and copies over an existing destination (Windows ff.copy refuses one). Covered by POSIX legs.
        org.junit.Assume.assumeFalse("restore simulation rewrites live files, which Windows forbids", Os.isWindows());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
                // Earlier cut: apply 3 rows, then capture (back up) _txn/_cv — the "backup".
                for (int i = 0; i < 3; i++) {
                    execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("r");
                final long restoredSeqTxn = readTxnSeqTxn(tt);
                copyTableFile(tt, TableUtils.TXN_FILE_NAME, "_txn.bak");
                copyTableFile(tt, TableUtils.COLUMN_VERSION_FILE_NAME, "_cv.bak");

                // Later cut: apply 3 more rows, then take a durable epoch at this LATER seqTxn.
                for (int i = 3; i < 6; i++) {
                    execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                final long epochSeqTxn;
                try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                    epochSeqTxn = w.getSeqTxn();
                }
                Assert.assertTrue("epoch cut must be ahead of the earlier/backup cut", epochSeqTxn > restoredSeqTxn);

                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // Simulate the restore: bring _txn/_cv back to the EARLIER cut, but (the bug) leave the LATER
                // _snapshot/_txn.epoch/_cv.epoch trio in place.
                copyTableFile(tt, "_txn.bak", TableUtils.TXN_FILE_NAME);
                copyTableFile(tt, "_cv.bak", TableUtils.COLUMN_VERSION_FILE_NAME);
                Assert.assertEquals("precondition: live _txn rewound to the earlier (restored) cut",
                        restoredSeqTxn, readTxnSeqTxn(tt));

                // ACT: a stale marker that post-dates restored live state is untrusted and must abort startup.
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("recovery must fail closed on an epoch ahead of restored live state");
                } catch (CairoException expected) {
                    TestUtils.assertContains(expected.getFlyweightMessage(), "post-dates live state");
                }

                Assert.assertEquals("failed-closed recovery must leave the restored _txn untouched",
                        restoredSeqTxn, readTxnSeqTxn(tt));
                Assert.assertEquals("a rejected stale epoch must not bump recoveryIncarnation",
                        0L, engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation());
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * Every page mix of a torn {@code _txn} must recover to the epoch cut. ADAPTIVE never msyncs {@code _txn}
     * between epochs, and the kernel writes dirty mmap pages back in no fixed order. After a power loss, each 4 KiB
     * page of {@code _txn} can therefore hold its content at the epoch, after the first commit since the
     * epoch, or after the second. The table has 130 partitions, so its {@code _txn} spans three pages.
     * <p>
     * In some mixes page 0 still holds the epoch's version word while a later page holds the second commit's
     * rewrite of the same A/B slot. The latest slot is then torn, and {@code unsafeLoadAll()} falls back to
     * the predecessor slot, whose record predates the epoch. The freshness guard must treat that as a crash,
     * not as a restore to another lineage. After the sweep, the WAL must replay the table to the full row
     * count.
     */
    @Test
    public void testRecoverRestoresEpochUnderEveryTornTxnPageMix() throws Exception {
        org.junit.Assume.assumeFalse("page-mix simulation rewrites live files, which Windows forbids", Os.isWindows());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                final int partitionCount = 130;
                final long firstDay = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
                execute("CREATE TABLE torn (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO torn SELECT timestamp_sequence(" + firstDay + ", " + Micros.DAY_MICROS + ") ts, x v" +
                        " FROM long_sequence(" + partitionCount + ")");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("torn");
                final long lastDay = firstDay + (partitionCount - 1) * Micros.DAY_MICROS;
                for (int i = 1; i <= 3; i++) {
                    execute("INSERT INTO torn VALUES (" + (lastDay + i * Micros.HOUR_MICROS) + "::timestamp, " + (1000 + i) + ")");
                    drainWalQueue();
                }

                final long epochSeqTxn;
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                    epochSeqTxn = w.getSeqTxn();
                }
                final java.nio.file.Path txnPath = Paths.get(
                        engine.getConfiguration().getDbRoot(), tt.getDirName(), TableUtils.TXN_FILE_NAME
                );
                final byte[][] images = new byte[3][];
                images[0] = Files.readAllBytes(txnPath);
                for (int i = 1; i <= 2; i++) {
                    execute("INSERT INTO torn VALUES (" + (lastDay + (3 + i) * Micros.HOUR_MICROS) + "::timestamp, " + (1003 + i) + ")");
                    drainWalQueue();
                    images[i] = Files.readAllBytes(txnPath);
                }
                Assert.assertEquals("a later commit must not move the epoch", epochSeqTxn, readTxnSeqTxn(tt) - 2);
                Assert.assertTrue("the _txn record must span several pages", images[2].length > 2 * PAGE_SIZE_4K);

                engine.releaseAllWriters();
                engine.releaseAllReaders();
                copyTableFile(tt, TableUtils.COLUMN_VERSION_FILE_NAME, "_cv.live");

                final int pageCount = (images[2].length + PAGE_SIZE_4K - 1) / PAGE_SIZE_4K;
                final int[] mix = new int[pageCount];
                int combinations = 0;
                int fallbacksBelowEpoch = 0;
                do {
                    final byte[] torn = new byte[images[2].length];
                    for (int p = 0; p < pageCount; p++) {
                        final byte[] src = images[mix[p]];
                        final int lo = p * PAGE_SIZE_4K;
                        final int hi = Math.min(Math.min(lo + PAGE_SIZE_4K, torn.length), src.length);
                        if (lo < hi) {
                            System.arraycopy(src, lo, torn, lo, hi - lo);
                        }
                    }
                    Files.write(txnPath, torn);
                    copyTableFile(tt, "_cv.live", TableUtils.COLUMN_VERSION_FILE_NAME);
                    if (isTornFallbackBelow(tt, epochSeqTxn)) {
                        fallbacksBelowEpoch++;
                    }

                    try {
                        new RecoveryCoordinator(engine).recover();
                    } catch (CairoException e) {
                        Assert.fail("recovery refused page mix " + Arrays.toString(mix) + ": " + e.getFlyweightMessage());
                    }
                    Assert.assertEquals("page mix " + Arrays.toString(mix) + " must recover to the epoch cut",
                            epochSeqTxn, readTxnSeqTxn(tt));
                    combinations++;
                } while (nextMix(mix, images.length));

                Assert.assertTrue("the sweep must reach the torn fallback below the epoch, " + combinations + " mixes",
                        fallbacksBelowEpoch > 0);

                // The last mix left the table at the epoch cut; rebuild its pins before replay after the reboot.
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                drainWalQueue();
                assertQuery("SELECT count() c, sum(v) s FROM torn")
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                c\ts
                                135\t13530
                                """);
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * The save point a REBASE WAL clone or a conversion to WAL publishes names the table's existing partitions, so it
     * is pinned from the moment it is published: an O3 commit that rewrites such a partition before the next save
     * point must not remove the version the save point names, and a kill then restores it and replays every row,
     * keeping the table's min timestamp. The REBASE row replays the clone's two empty seed transactions too.
     */
    @Test
    public void testRebaseAndConversionBaselinesKeepTheirPartitions() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        try {
            assertMemoryLeak(() -> {
                setCommitMode("adaptive");
                // No periodic or close-time save point: the baseline stays the newest one.
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
                final String rows = "('2024-09-01T01:00:00.000000Z', 1), ('2024-09-01T02:00:00.000000Z', 2), ('2024-09-01T03:00:00.000000Z', 3)";
                for (String name : new String[]{"rebased", "converted"}) {
                    if (name.equals("rebased")) {
                        execute("CREATE TABLE rebased (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                        execute("INSERT INTO rebased VALUES " + rows);
                        drainWalQueue();
                        execute("ALTER TABLE rebased SUSPEND WAL");
                        execute("ALTER TABLE rebased REBASE WAL");
                        drainWalQueue();
                    } else {
                        execute("CREATE TABLE converted (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                        execute("INSERT INTO converted VALUES " + rows);
                        execute("ALTER TABLE converted SET TYPE WAL");
                        engine.releaseInactive();
                        engine.load();
                    }
                    final TableToken tt = engine.verifyTableName(name);
                    Assert.assertEquals(name + ": the baseline is the newest save point", 0, newestSavePointSeqTxn(tt));
                    // An O3 row rewrites the partition the baseline names.
                    execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:30:00.000000Z', 0)");
                    drainWalQueue();
                    engine.releaseAllReaders();
                    try (O3PartitionPurgeJob job = new O3PartitionPurgeJob(engine, 1)) {
                        //noinspection StatementWithEmptyBody
                        while (job.run()) {
                        }
                    }
                    Assert.assertTrue(name + ": the partition version the save point names is kept",
                            Files.exists(Paths.get(engine.getConfiguration().getDbRoot(), tt.getDirName(), "2024-09-01")));
                    // A kill: no close-time save point. For the REBASE row, recovery replays the two empty seed
                    // transactions in one block with the O3 row.
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.getTxnScoreboardPool().remove(tt);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    engine.notifyWalTxnRepublisher(tt);
                    drainWalQueue();
                    Assert.assertFalse(name + ": table suspended", engine.getTableSequencerAPI().isSuspended(tt));
                    assertQuery("SELECT table_min_timestamp, table_max_timestamp FROM tables() WHERE table_name = '" + name + "'")
                            .noRandomAccess()
                            .returns("""
                                    table_min_timestamp\ttable_max_timestamp
                                    2024-09-01T00:30:00.000000Z\t2024-09-01T03:00:00.000000Z
                                    """);
                    assertQuery("SELECT v FROM " + name)
                            .expectSize()
                            .returns("""
                                    v
                                    0
                                    1
                                    2
                                    3
                                    """);
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    @Test
    public void testRebaseCloneAndConversionToWalLeaveValidCreationBaseline() throws Exception {
        assertCreationBaselinesValid(Os.isLinux());
    }

    @Test
    public void testRebaseCloneAndConversionToWalLeaveValidCreationBaselineOnNonWideSyncfsPlatform() throws Exception {
        assertCreationBaselinesValid(false);
    }

    /**
     * The start that converts a table without rows to WAL creates its sequencer files. The baseline of an empty
     * table runs no partition flush and no syncfs, so its sequencer flush is the only one: without it a power loss
     * before the first commit empties {@code txn_seq/}, the next start skips the finished conversion, and every
     * write to the table fails.
     */
    @Test
    public void testEmptyTableConversionToWalSurvivesPowerLoss() throws Exception {
        org.junit.Assume.assumeFalse("the crash model rewrites files under a live engine, which Windows forbids", Os.isWindows());
        final CrashFaultFilesFacade crashFf = new CrashFaultFilesFacade();
        crashFf.setDbRoot(root);
        try {
            assertMemoryLeak(crashFf, () -> {
                setCommitMode("adaptive");
                execute("CREATE TABLE empty_conv (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("ALTER TABLE empty_conv SET TYPE WAL");
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                // The clean stop after the ALTER is durable; the start that converts is not.
                crashFf.markDurableBaseline(engine.getConfiguration().getDbRoot());
                new RecoveryCoordinator(engine).recover();
                engine.load();
                final TableToken tt = engine.verifyTableName("empty_conv");
                Assert.assertTrue("precondition: converted", tt.isWal());
                final List<String> sequencerFiles = listSequencerFiles(tt);
                Assert.assertFalse("precondition: the conversion created the sequencer", sequencerFiles.isEmpty());
                crashFf.powerLoss();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                crashFf.crash(engine.getConfiguration().getDbRoot());
                Assert.assertEquals("sequencer files after a power loss", sequencerFiles, listSequencerFiles(tt));

                // The next start, in startup order, then the first row.
                engine.getTxnScoreboardPool().remove(tt);
                engine.getTableSequencerAPI().resetForReboot(tt);
                engine.getTableNameRegistry().reload();
                new RecoveryCoordinator(engine).recover();
                engine.load();
                execute("INSERT INTO empty_conv VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
                assertQuery("SELECT v FROM empty_conv").expectSize().returns("v\n1\n");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * A table-type conversion RESETS the WAL lineage: {@code TableConverter} zeroes {@code _txn.seqTxn}
     * and re-seeds {@code txn_seq} from txn 0. The durable epoch taken under the PREVIOUS lineage is then
     * unreachable, and left on disk it post-dates the live cut, which is a refusal that takes the whole
     * instance down over a supported operator workflow. The converter must therefore clear the anchor and
     * publish a replacement, the way the REBASE WAL clone does.
     */
    @Test
    public void testTypeConversionRoundTripLeavesInstanceBootable() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            assertMemoryLeak(() -> {
                execute("create table conv (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into conv values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                execute("insert into conv values ('2024-09-02T00:00:00.000000Z', 2)");
                drainWalQueue();
                Assert.assertTrue("precondition: the WAL table has an anchor beyond seqTxn 0",
                        readTxnSeqTxn(engine.verifyTableName("conv")) > 0);

                execute("alter table conv set type bypass wal");
                engine.releaseInactive();
                engine.load();

                execute("alter table conv set type wal", sqlExecutionContext);
                engine.releaseInactive();
                engine.load();

                final TableToken tt = engine.verifyTableName("conv");
                Assert.assertTrue("the converted table must carry its own anchor",
                        epochArtifactExists(tt, io.questdb.cairo.DurableEpochManifest.FILE_NAME + ".0", ""));
                Assert.assertEquals("the conversion re-seeds the lineage at seqTxn 0", 0L, readTxnSeqTxn(tt));

                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();

                try (io.questdb.cairo.TableReader reader = engine.getReader(tt)) {
                    Assert.assertEquals("both rows must survive the round trip", 2L, reader.size());
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * Kill, then convert: an adaptive table applies one row lazily after its creation save point, the server is
     * killed with {@code SET TYPE BYPASS WAL} pending, and the next start converts it. The row is kept whether the
     * kill left the lazily written {@code _txn} in place, or a power loss reverted it to the save point, so that
     * recovery has nothing to restore.
     */
    @Test
    public void testConversionToNonWalAfterKillKeepsTheRow() throws Exception {
        org.junit.Assume.assumeFalse("the test copies over a live file, which Windows forbids", Os.isWindows());
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(() -> {
                setCommitMode("adaptive");
                final List<String> failures = new ArrayList<>();
                for (boolean txnReverted : new boolean[]{false, true}) {
                    final String name = txnReverted ? "kill_reverted" : "kill";
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    final TableToken tt = engine.verifyTableName(name);
                    copyTableFile(tt, TableUtils.TXN_FILE_NAME, "_txn.bak");
                    execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                    execute("ALTER TABLE " + name + " SET TYPE BYPASS WAL");
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();
                    if (txnReverted) {
                        copyTableFile(tt, "_txn.bak", TableUtils.TXN_FILE_NAME);
                    }
                    removeTableFile(tt, "_txn.bak");

                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    engine.load();
                    final TableToken converted = engine.verifyTableName(name);
                    final long rows = rowCount(converted);
                    if (converted.isWal() || rows != 1) {
                        failures.add(name + ": wal=" + converted.isWal() + ", rows=" + rows);
                    }
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * Committed WAL transactions that were never applied are applied before the conversion deletes the WAL, in
     * every mode and for a table that has no save point.
     */
    @Test
    public void testConversionToNonWalAppliesUnappliedWalInEveryMode() throws Exception {
        try {
            assertMemoryLeak(() -> {
                final List<String> failures = new ArrayList<>();
                for (String mode : new String[]{"nosync", "async", "sync"}) {
                    setCommitMode(mode);
                    final String name = "unapplied_" + mode;
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                    // Committed and acknowledged, not applied: the server stops before the apply job reaches them.
                    execute("INSERT INTO " + name + " VALUES ('2024-09-02T00:00:00.000000Z', 2), ('2024-09-03T00:00:00.000000Z', 3)");
                    execute("ALTER TABLE " + name + " SET TYPE BYPASS WAL");
                    final TableToken tt = engine.verifyTableName(name);
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();

                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    engine.load();
                    final TableToken converted = engine.verifyTableName(name);
                    final long rows = rowCount(converted);
                    if (converted.isWal() || rows != 3) {
                        failures.add(mode + ": wal=" + converted.isWal() + ", rows=" + rows);
                    }
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * A conversion that fails with an ordinary error on a start outside adaptive leaves the table a WAL table with its
     * save point, which recovery skipped for the conversion. The run that follows applies and purges WAL, and the next
     * start rewinds the table to that save point again, so the WAL after it must still be there.
     */
    @Test
    public void testConversionToNonWalFailedOnNosyncStartKeepsEveryRow() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicBoolean failRemoval = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (failRemoval.get() && Utf8s.containsAscii(name, io.questdb.cairo.DurableEpochManifest.FILE_NAME)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        try {
            assertMemoryLeak(ff, () -> {
                buildAdaptiveLazyGapTable("conv");
                final TableToken tt = engine.verifyTableName("conv");
                execute("ALTER TABLE conv SET TYPE BYPASS WAL");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.releaseAllWalWriters();

                // A NOSYNC start whose conversion fails on an ordinary error.
                setCommitMode("nosync");
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                failRemoval.set(true);
                try {
                    engine.load();
                } finally {
                    failRemoval.set(false);
                }
                Assert.assertTrue("precondition: the conversion failed, the table is still a WAL table", engine.verifyTableName("conv").isWal());
                Assert.assertTrue("precondition: the save point outlived the NOSYNC start", hasSavePointFile(tt));

                // The NOSYNC run: a row, the apply, a WAL purge.
                execute("INSERT INTO conv VALUES ('2024-09-02T00:00:00.000000Z', 100)");
                drainWalQueue();
                engine.releaseAllWalWriters();
                engine.releaseAllWriters();
                drainPurgeJob();
                assertQuery("SELECT count() FROM conv").noRandomAccess().expectSize().returns("count\n8\n");

                // The next start; the conversion succeeds this time.
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.getTxnScoreboardPool().remove(tt);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                engine.load();
                Assert.assertFalse("the table must be converted", engine.verifyTableName("conv").isWal());
                assertQuery("SELECT count() FROM conv").noRandomAccess().expectSize().returns("count\n8\n");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A kill inside the conversion body, before or after it removes the sequencer, in every start mode and whether
     * the WAL was purged or not, leaves a table the next start converts with every row once: the table's
     * {@code _txn} keeps its replayed seqTxn until the WAL is gone, so the next start finds nothing to replay. The
     * kill is modelled by a failure at the sequencer's removal, which leaves {@code _convert} for the next start.
     */
    @Test
    public void testConversionToNonWalKillAroundSequencerRemovalKeepsEveryRow() throws Exception {
        final AtomicBoolean killBeforeRemoval = new AtomicBoolean();
        final AtomicBoolean killAfterRemoval = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean rmdir(Path name, boolean haltOnError) {
                if (Utf8s.endsWithAscii(name, WalUtils.SEQ_DIR)) {
                    if (killBeforeRemoval.get()) {
                        throw CairoException.critical(0).put("kill before the sequencer is removed [path=").put(name).put(']');
                    }
                    if (killAfterRemoval.get()) {
                        super.rmdir(name, haltOnError);
                        throw CairoException.critical(0).put("kill after the sequencer is removed [path=").put(name).put(']');
                    }
                }
                return super.rmdir(name, haltOnError);
            }
        };
        try {
            assertMemoryLeak(ff, () -> {
                final List<String> failures = new ArrayList<>();
                for (String mode : new String[]{"nosync", "adaptive"}) {
                    for (boolean afterRemoval : new boolean[]{false, true}) {
                        for (boolean purge : new boolean[]{false, true}) {
                            final String label = mode + " start, kill " + (afterRemoval ? "after" : "before") + " the removal, WAL " + (purge ? "purged" : "kept");
                            final String name = "kill_" + mode + (afterRemoval ? "_after" : "_before") + (purge ? "_purged" : "_kept");
                            setCommitMode("nosync");
                            execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                            for (int i = 0; i < 3; i++) {
                                execute("INSERT INTO " + name + " VALUES ('2024-09-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
                                drainWalQueue();
                                engine.releaseAllWalWriters();
                            }
                            if (purge) {
                                drainPurgeJob();
                            }
                            final TableToken tt = engine.verifyTableName(name);
                            execute("ALTER TABLE " + name + " SET TYPE BYPASS WAL");
                            engine.releaseAllWriters();
                            engine.releaseAllReaders();
                            engine.releaseAllWalWriters();

                            setCommitMode(mode);
                            engine.getTxnScoreboardPool().remove(tt);
                            engine.getTableSequencerAPI().resetForReboot(tt);
                            new RecoveryCoordinator(engine).recover();
                            (afterRemoval ? killAfterRemoval : killBeforeRemoval).set(true);
                            try {
                                engine.load();
                            } finally {
                                killBeforeRemoval.set(false);
                                killAfterRemoval.set(false);
                            }
                            if (!engine.verifyTableName(name).isWal()) {
                                failures.add(label + ": the modelled kill did not stop the conversion");
                                continue;
                            }

                            engine.releaseAllWriters();
                            engine.releaseAllReaders();
                            engine.getTxnScoreboardPool().remove(tt);
                            engine.getTableSequencerAPI().resetForReboot(tt);
                            new RecoveryCoordinator(engine).recover();
                            engine.load();
                            final TableToken converted = engine.verifyTableName(name);
                            final long rows = rowCount(converted);
                            if (converted.isWal() || rows != 3) {
                                failures.add(label + ": at the next start wal=" + converted.isWal() + ", rows=" + rows);
                            }
                            engine.releaseAllReaders();
                        }
                    }
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * A failed flush of the replayed WAL stops the engine, whether it is the conversion's own flush or the apply
     * job's save point inside the replay (at the shipped interval). The production callback halts the process in
     * that call, and the next start, with a healthy disk, rewinds to the save point, replays again and converts
     * the table with every row.
     */
    @Test
    public void testConversionToNonWalReplayFlushFailureStopsTheEngine() throws Exception {
        assertReplayFlushFailureKeepsEveryRow(TableUtils.TXN_FILE_NAME, -1, true);
        assertReplayFlushFailureKeepsEveryRow("s.k", -1, true);
        assertReplayFlushFailureKeepsEveryRow(WalUtils.TXNLOG_FILE_NAME, 60_000, true);
        assertReplayFlushFailureKeepsEveryRow("s.k", 60_000, true);
    }

    /**
     * A failure of the conversion's flush that is not a data-sync failure does not stop the engine: the table stays
     * a WAL table and the next start converts it with every row.
     */
    @Test
    public void testConversionToNonWalReplayPlainFailureRetriesAtNextStart() throws Exception {
        assertReplayFlushFailureKeepsEveryRow(TableUtils.TXN_FILE_NAME, -1, false);
    }

    /**
     * A transaction that fails to apply for good stops the replay, and the conversion is refused: the table stays a
     * WAL table with its {@code _convert}, and every transaction after the failing one is still in its WAL.
     */
    @Test
    public void testConversionToNonWalRefusedAtFailingTransaction() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(() -> {
                buildAdaptiveLazyGapTable("conv");
                final TableToken tt = engine.verifyTableName("conv");
                // One more transaction, in a WAL of its own, which the test then breaks.
                final String brokenWal;
                try (
                        io.questdb.cairo.wal.WalWriter held = engine.getWalWriter(tt);
                        io.questdb.cairo.wal.WalWriter broken = engine.getWalWriter(tt)
                ) {
                    Assert.assertNotEquals(held.getWalName(), broken.getWalName());
                    brokenWal = broken.getWalName();
                    final io.questdb.cairo.TableWriter.Row row = broken.newRow(MicrosTimestampDriver.floor("2024-09-01T08:00:00.000000Z"));
                    row.putLong(1, 7);
                    row.append();
                    broken.commit();
                }
                drainWalQueue();
                assertQuery("SELECT count() FROM conv").noRandomAccess().expectSize().returns("count\n8\n");

                execute("ALTER TABLE conv SET TYPE BYPASS WAL");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.releaseInactiveTableSequencers();
                try (Path path = new Path()) {
                    path.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(brokenWal);
                    Assert.assertTrue(engine.getConfiguration().getFilesFacade().rmdir(path));
                }

                rebootAndConvert(tt);

                final TableToken after = engine.verifyTableName("conv");
                Assert.assertTrue("the conversion must be refused", after.isWal());
                Assert.assertTrue("the refused conversion keeps its _convert", epochArtifactExists(tt, WalUtils.CONVERT_FILE_NAME, ""));
                Assert.assertTrue("the refused conversion keeps the save point", hasSavePointFile(tt));
                Assert.assertEquals("the WAL keeps every transaction", 8L, engine.getTableSequencerAPI().lastTxn(tt));
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A conversion refused on a start outside adaptive, at a transaction that fails for good, leaves adaptive at
     * the refusal: the replayed rows are flushed, the save point removed and its pin released, so a power loss
     * right after it keeps every byte, and the next start does not rewind below a transaction that
     * {@code RESUME WAL FROM TXN} skipped. After that step the next start converts the table with every row the skip
     * did not name.
     */
    @Test
    public void testConversionToNonWalRefusedOutsideAdaptiveClearedByResumeFromTxn() throws Exception {
        org.junit.Assume.assumeFalse("the crash model rewrites files under a live engine, which Windows forbids", Os.isWindows());
        final CrashFaultFilesFacade crashFf = new CrashFaultFilesFacade();
        crashFf.setDbRoot(root);
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(crashFf, () -> {
                buildAdaptiveLazyGapTable("conv");
                final TableToken tt = engine.verifyTableName("conv");
                // Transaction 8 is structural, so the replay commits every transaction before the broken transaction 9,
                // which goes into a WAL of its own; transaction 10 follows. Transaction 9 loses a column file, so it
                // fails when it is applied.
                execute("ALTER TABLE conv ADD COLUMN extra INT");
                drainWalQueue();
                final String brokenWal;
                try (
                        io.questdb.cairo.wal.WalWriter held = engine.getWalWriter(tt);
                        io.questdb.cairo.wal.WalWriter broken = engine.getWalWriter(tt)
                ) {
                    Assert.assertNotEquals(held.getWalName(), broken.getWalName());
                    brokenWal = broken.getWalName();
                    final io.questdb.cairo.TableWriter.Row row = broken.newRow(MicrosTimestampDriver.floor("2024-09-01T08:00:00.000000Z"));
                    row.putLong(1, 7);
                    row.append();
                    broken.commit();
                }
                execute("INSERT INTO conv VALUES ('2024-09-01T09:00:00.000000Z', 9, 9)");
                drainWalQueue();
                assertQuery("SELECT count() FROM conv").noRandomAccess().expectSize().returns("count\n9\n");

                execute("ALTER TABLE conv SET TYPE BYPASS WAL");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.releaseAllWalWriters();
                engine.releaseInactiveTableSequencers();
                try (Path path = new Path()) {
                    path.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(brokenWal).concat("0").concat("v.d");
                    Assert.assertTrue(engine.getConfiguration().getFilesFacade().removeQuiet(path.$()));
                }

                setCommitMode("nosync");
                crashFf.markDurableBaseline(engine.getConfiguration().getDbRoot());
                engine.getTxnScoreboardPool().remove(tt);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                engine.load();
                Assert.assertTrue("the conversion must be refused", engine.verifyTableName("conv").isWal());
                Assert.assertTrue("the refused conversion keeps its _convert", epochArtifactExists(tt, WalUtils.CONVERT_FILE_NAME, ""));
                Assert.assertTrue("precondition: the replay stopped at the broken transaction", engine.getTableSequencerAPI().isSuspended(tt));
                Assert.assertEquals("the replay applied every transaction before the broken one", 8L, readTxnSeqTxn(tt));
                Assert.assertFalse("a refusal outside adaptive removes the save point", hasSavePointFile(tt));
                Assert.assertEquals("a refusal outside adaptive releases the pin", -1L, engine.getTableSequencerAPI().getTxnTracker(tt).getPinnedEpochTxn());
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                final Map<String, byte[]> files = new TreeMap<>(readTableFiles(tt, true));
                files.keySet().removeIf(file -> !file.contains(java.io.File.separator)
                        && !file.equals(TableUtils.TXN_FILE_NAME)
                        && !file.equals(TableUtils.COLUMN_VERSION_FILE_NAME)
                        && !file.equals(TableUtils.META_FILE_NAME));
                Assert.assertEquals("a power loss after the refusal keeps every byte", "[]", bytesLostAtPowerLoss(crashFf, tt, files).toString());

                // The manual step: give up the broken transaction, let the apply catch up, restart.
                execute("ALTER TABLE conv RESUME WAL FROM TXN 10");
                drainWalQueue();
                engine.releaseAllWalWriters();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                drainPurgeJob();
                engine.getTxnScoreboardPool().remove(tt);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                engine.load();
                final TableToken converted = engine.verifyTableName("conv");
                Assert.assertFalse("the next start converts the table", converted.isWal());
                Assert.assertEquals("every row the skip did not name", 8L, rowCount(converted));
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A replay that stops early, here at a transaction that fails to apply and suspends the table, refuses the
     * conversion in every mode, whether the table has a save point or not: it stays a WAL table with its
     * {@code _convert} and its WAL. An adaptive start keeps the save point for the next replay; a start outside
     * adaptive flushes the table and removes it. Once the cause is fixed and the table is resumed and caught up, the
     * next start converts it with every row.
     */
    @Test
    public void testConversionToNonWalRefusedWhileReplayStopsKeepsEveryRow() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final FilesFacade failingFf = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get() && Utf8s.containsAscii(name, "2024-09-02")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(failingFf, () -> {
                final List<String> failures = new ArrayList<>();
                // The mode the table is written in, then the mode of the start that converts it.
                for (String[] modes : new String[][]{{"adaptive", "adaptive"}, {"adaptive", "nosync"}, {"nosync", "nosync"}}) {
                    final String label = modes[0] + " table, " + modes[1] + " start";
                    final String name = "refused_" + modes[0] + '_' + modes[1];
                    setCommitMode(modes[0]);
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    for (int i = 0; i < 3; i++) {
                        execute("INSERT INTO " + name + " VALUES ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                    }
                    drainWalQueue();
                    final TableToken tt = engine.verifyTableName(name);
                    if (modes[0].equals("adaptive")) {
                        try (TableWriter w = getWriter(tt)) {
                            w.advanceDurableEpoch(1L);
                        }
                    }
                    for (int i = 3; i < 7; i++) {
                        execute("INSERT INTO " + name + " VALUES ('2024-09-02T0" + i + ":00:00.000000Z', " + i + ")");
                    }
                    execute("ALTER TABLE " + name + " SET TYPE BYPASS WAL");
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();

                    setCommitMode(modes[1]);
                    armed.set(true);
                    try {
                        engine.getTxnScoreboardPool().remove(tt);
                        engine.getTableSequencerAPI().resetForReboot(tt);
                        new RecoveryCoordinator(engine).recover();
                        engine.load();
                    } finally {
                        armed.set(false);
                    }
                    if (!engine.verifyTableName(name).isWal() || !epochArtifactExists(tt, WalUtils.CONVERT_FILE_NAME, "")) {
                        failures.add(label + ": the conversion went ahead with " + rowCount(engine.verifyTableName(name)) + " rows");
                        continue;
                    }
                    if (modes[0].equals("adaptive") && hasSavePointFile(tt) != modes[1].equals("adaptive")) {
                        failures.add(label + ": the refused conversion " + (hasSavePointFile(tt) ? "kept" : "removed") + " the save point");
                    }

                    // The manual step: the cause is fixed, the table resumed and caught up, then a restart.
                    execute("ALTER TABLE " + name + " RESUME WAL");
                    drainWalQueue();
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();
                    drainPurgeJob();
                    // A restart starts with an empty scoreboard.
                    engine.getTxnScoreboardPool().remove(tt);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    engine.load();
                    final TableToken converted = engine.verifyTableName(name);
                    final long rows = rowCount(converted);
                    if (converted.isWal() || rows != 7) {
                        failures.add(label + ": after the manual step wal=" + converted.isWal() + ", rows=" + rows);
                    }
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A sequencer that cannot be read at the converting start refuses the conversion of a table with a save point,
     * in every start mode: recovery has rewound the table to its save point, so converting without the replay would
     * drop every row after it. The table stays a WAL table with its {@code _convert}, its WAL and its save point,
     * and the next start, with a readable sequencer, converts it with every row.
     */
    @Test
    public void testConversionToNonWalRefusedWhileSequencerUnreadable() throws Exception {
        final AtomicBoolean failSeq = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failSeq.get() && Utf8s.containsAscii(name, WalUtils.SEQ_DIR)) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (failSeq.get() && Utf8s.containsAscii(name, WalUtils.SEQ_DIR)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(ff, () -> {
                final List<String> failures = new ArrayList<>();
                for (String mode : new String[]{"adaptive", "nosync"}) {
                    setCommitMode("adaptive");
                    final String name = "unreadable_" + mode;
                    buildAdaptiveLazyGapTable(name);
                    final TableToken tt = engine.verifyTableName(name);
                    execute("ALTER TABLE " + name + " SET TYPE BYPASS WAL");
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();

                    setCommitMode(mode);
                    engine.getTxnScoreboardPool().remove(tt);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    failSeq.set(true);
                    try {
                        engine.load();
                    } finally {
                        failSeq.set(false);
                    }
                    if (!engine.verifyTableName(name).isWal() || !epochArtifactExists(tt, WalUtils.CONVERT_FILE_NAME, "")) {
                        failures.add(mode + " start: the conversion went ahead with " + rowCount(engine.verifyTableName(name)) + " rows");
                        continue;
                    }
                    if (!hasSavePointFile(tt)) {
                        failures.add(mode + " start: the refused conversion removed the save point");
                    }

                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.getTxnScoreboardPool().remove(tt);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    engine.load();
                    final TableToken converted = engine.verifyTableName(name);
                    final long rows = rowCount(converted);
                    if (converted.isWal() || rows != 7) {
                        failures.add(mode + " start: at the next start wal=" + converted.isWal() + ", rows=" + rows);
                    }
                    engine.releaseAllReaders();
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A kill in the middle of the replay leaves the save point, so the next start rewinds the table to it and
     * replays again: however often the replay is interrupted, the start that finishes it keeps every row.
     */
    @Test
    public void testConversionToNonWalReplaysAgainAfterKillMidReplay() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicReference<String> markerPath = new AtomicReference<>();
        final AtomicReference<Boolean> markerAtKill = new AtomicReference<>();
        final FilesFacade killingFf = new SyncAttributingFilesFacade() {
            @Override
            public void fsync(long fd) {
                killIfArmed(pathOfFd(fd));
                super.fsync(fd);
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                if (!async) {
                    killIfArmed(pathOfAddress(addr));
                }
                super.msync(addr, len, async);
            }

            // Kill at the conversion's final replay flush, not at the recovered writer's earlier coverage flush.
            // The facade names every file with '/' separators, Windows included.
            private void killIfArmed(String path) {
                if (path.endsWith("/" + TableUtils.TXN_FILE_NAME) && armed.get()) {
                    try (Path p = new Path(); TxReader tx = new TxReader(this)) {
                        tx.ofRO(p.of(path.replace('/', io.questdb.std.Files.SEPARATOR)).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                        if (tx.unsafeLoadAll() && tx.getSeqTxn() == 7 && armed.compareAndSet(true, false)) {
                            markerAtKill.set(exists(p.of(markerPath.get()).$()));
                            throw new Error("simulated kill");
                        }
                    }
                }
            }
        };
        try {
            assertMemoryLeak(killingFf, () -> {
                buildAdaptiveLazyGapTable("conv");
                final TableToken tt = engine.verifyTableName("conv");
                execute("ALTER TABLE conv SET TYPE BYPASS WAL");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path marker = new Path()) {
                    // The replay marker of older builds, which made the start after a kill convert without a replay.
                    marker.of(engine.getConfiguration().getDbRoot()).concat(tt).concat("_convert.replay");
                    markerPath.set(marker.toString());
                    final StringBuilder starts = new StringBuilder();
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    armed.set(true);
                    try {
                        engine.load();
                        starts.append("start 1 went on");
                    } catch (Error e) {
                        starts.append("start 1 killed");
                    }
                    // A kill runs no clean-up: put back the marker as it was at the kill.
                    if (Boolean.TRUE.equals(markerAtKill.get()) && !ff.exists(marker.$())) {
                        Assert.assertTrue(ff.touch(marker.$()));
                    }
                    engine.releaseInactive();
                    engine.closeNameRegistry();
                    for (int start = 2; start < 4; start++) {
                        markerAtKill.set(null);
                        try (CairoEngine rebooted = new CairoEngine(configuration)) {
                            // The constructor runs recovery; load() runs the conversion, as EngineEnvelope.start()
                            // does. The second start is killed in the replay again, the third one runs to the end.
                            armed.set(start == 2);
                            try {
                                rebooted.load();
                                final TableToken after = rebooted.verifyTableName("conv");
                                try (TableReader reader = rebooted.getReader(after)) {
                                    starts.append(", start ").append(start).append(": wal=").append(after.isWal()).append(", rows=").append(reader.size());
                                }
                            } catch (Error e) {
                                starts.append(", start ").append(start).append(" killed");
                            }
                        }
                        if (Boolean.TRUE.equals(markerAtKill.get()) && !ff.exists(marker.$())) {
                            Assert.assertTrue(ff.touch(marker.$()));
                        }
                    }
                    armed.set(false);
                    engine.reloadTableNames(null);
                    Assert.assertEquals("start 1 killed, start 2 killed, start 3: wal=false, rows=7", starts.toString());
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * Startup recovery rewinds an adaptive table to its save point and relies on WAL replay for every transaction
     * after it. A pending {@code SET TYPE BYPASS WAL} deletes that WAL on the same startup, so the conversion
     * replays it first, then flushes the table, removes the save point and releases its pin, in every start mode:
     * a start outside adaptive leaves the save point of such a table to the conversion. A power loss after the
     * conversion keeps every row.
     */
    @Test
    public void testConversionToNonWalReplaysWalRewoundByRecovery() throws Exception {
        org.junit.Assume.assumeFalse("the crash model rewrites files under a live engine, which Windows forbids", Os.isWindows());
        final CrashFaultFilesFacade crashFf = new CrashFaultFilesFacade();
        crashFf.setDbRoot(root);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        // A clean close must not take a save point, or there is nothing left to rewind.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(crashFf, () -> {
                final List<String> failures = new ArrayList<>();
                for (String mode : new String[]{"nosync", "adaptive"}) {
                    setCommitMode("adaptive");
                    final String name = "conv_" + mode;
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    crashFf.markDurableBaseline(engine.getConfiguration().getDbRoot());
                    for (int i = 0; i < 3; i++) {
                        execute("INSERT INTO " + name + " VALUES ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                    }
                    drainWalQueue();
                    final TableToken tt = engine.verifyTableName(name);
                    try (TableWriter w = getWriter(tt)) {
                        w.advanceDurableEpoch(1L);
                    }
                    for (int i = 3; i < 7; i++) {
                        execute("INSERT INTO " + name + " VALUES ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                    }
                    drainWalQueue();
                    execute("ALTER TABLE " + name + " SET TYPE BYPASS WAL");
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseAllWalWriters();

                    setCommitMode(mode);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                    Assert.assertEquals("precondition: recovery rewound the table to its save point", 3L, readTxnSeqTxn(tt));
                    Assert.assertTrue("precondition: recovery pinned the save point", tracker.getPinnedEpochTxn() > -1);
                    engine.load();

                    final TableToken converted = engine.verifyTableName(name);
                    final long rows = rowCount(converted);
                    if (converted.isWal() || rows != 7) {
                        failures.add(mode + " start: wal=" + converted.isWal() + ", rows=" + rows);
                    }
                    if (hasSavePointFile(tt)) {
                        failures.add(mode + " start: the save point remains");
                    }
                    if (tracker.getPinnedEpochTxn() > -1) {
                        failures.add(mode + " start: the save point's pin remains");
                    }
                    crashFf.powerLoss();
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    if (mode.equals("nosync")) {
                        // A start outside adaptive writes the conversion itself at its own grade, but the replayed
                        // rows are flushed all the same: the column files keep every byte at a power loss.
                        final Map<String, byte[]> columnFiles = new TreeMap<>(readTableFiles(tt, true));
                        columnFiles.keySet().removeIf(file -> !file.contains(java.io.File.separator));
                        final List<String> lost = bytesLostAtPowerLoss(crashFf, tt, columnFiles);
                        if (!lost.isEmpty()) {
                            failures.add(mode + " start: a power loss after the conversion loses bytes of " + lost);
                        }
                        continue;
                    }
                    crashFf.crash(engine.getConfiguration().getDbRoot());
                    final long rowsAfterPowerLoss = rowCount(converted);
                    if (rowsAfterPowerLoss != 7) {
                        failures.add(mode + " start: " + rowsAfterPowerLoss + " rows after a power loss after the conversion");
                    }
                    engine.releaseAllReaders();
                }
                Assert.assertEquals("[]", failures.toString());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A table whose save point files both fail their checks refuses the start, and the message names the manual
     * step. When both fail their section checks the live files are untouched: after a clean stop or a kill, moving
     * both files out of the table directory lets the table start with its rows. When recovery restored a generation
     * over the live files before its content checks failed, the live files no longer hold the table's last state,
     * so the message names only the restore from a backup or a checkpoint.
     */
    @Test
    public void testDamagedSavePointRefusalNamesItsManualStep() throws Exception {
        final AtomicReference<String> failCvOpenOf = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                final String dirName = failCvOpenOf.get();
                if (dirName != null && Utf8s.containsAscii(name, dirName)
                        && Utf8s.endsWithAscii(name, java.io.File.separator + TableUtils.COLUMN_VERSION_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            assertMemoryLeak(ff, () -> {
                execute("CREATE TABLE damaged (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO damaged VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("damaged");
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                corruptSavePointSection(tt, 0);
                corruptSavePointSection(tt, 1);

                engine.getTableSequencerAPI().resetForReboot(tt);
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("a table with no trustworthy save point must refuse the start");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "no trustworthy adaptive epoch generation; refusing unsafe live-state fallback [table=damaged, dir="
                                    + tt.getDirName() + "]; after a clean stop or a process kill, move _epoch.manifest.0 and "
                                    + "_epoch.manifest.1 out of the table directory and start again; after a power loss, restore "
                                    + "the table from a backup or a checkpoint"
                    );
                }

                moveSavePointFileOut(tt, 0);
                moveSavePointFileOut(tt, 1);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                drainWalQueue();
                assertQuery("SELECT count() FROM damaged").noRandomAccess().expectSize().returns("count\n1\n");

                // Both generations pass their section checks and are restored over the live files, then fail their
                // content checks (the live _cv cannot be read).
                execute("CREATE TABLE rewritten (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO rewritten VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final TableToken rewritten = engine.verifyTableName("rewritten");
                try (TableWriter w = getWriter(rewritten)) {
                    w.advanceDurableEpoch(1L);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.getTableSequencerAPI().resetForReboot(rewritten);
                failCvOpenOf.set(rewritten.getDirName());
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("a table with no trustworthy save point must refuse the start");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "no trustworthy adaptive epoch generation; refusing unsafe live-state fallback [table=rewritten, dir="
                                    + rewritten.getDirName() + "]; recovery has rewritten the live table files, restore the "
                                    + "table from a backup or a checkpoint"
                    );
                } finally {
                    failCvOpenOf.set(null);
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * A save point ahead of a cleanly loading live {@code _txn} refuses the start, and the message names the file to
     * move out. Without it, the start restores the older save point and replays the WAL after it.
     */
    @Test
    public void testPostDatedSavePointRefusalNamesItsManualStep() throws Exception {
        org.junit.Assume.assumeFalse("the test copies over live files, which Windows forbids", Os.isWindows());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE ahead (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 0; i < 3; i++) {
                    execute("INSERT INTO ahead VALUES ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("ahead");
                copyTableFile(tt, TableUtils.TXN_FILE_NAME, "_txn.bak");
                copyTableFile(tt, TableUtils.COLUMN_VERSION_FILE_NAME, "_cv.bak");
                for (int i = 3; i < 6; i++) {
                    execute("INSERT INTO ahead VALUES ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                final int newest;
                try (Path p = new Path()) {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt);
                    newest = io.questdb.cairo.DurableEpochManifest.loadCandidates(engine.getConfiguration().getFilesFacade(), p, p.size(), tt.getTableId())[0].generation;
                }
                copyTableFile(tt, "_txn.bak", TableUtils.TXN_FILE_NAME);
                copyTableFile(tt, "_cv.bak", TableUtils.COLUMN_VERSION_FILE_NAME);
                removeTableFile(tt, "_txn.bak");
                removeTableFile(tt, "_cv.bak");

                engine.getTableSequencerAPI().resetForReboot(tt);
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("a save point ahead of the live _txn must refuse the start");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "adaptive epoch post-dates live state; refusing wrong-lineage recovery [table=ahead, dir="
                                    + tt.getDirName() + ", epochSeqTxn=6]; move _epoch.manifest." + newest
                                    + " out of the table directory and start again"
                    );
                }

                moveSavePointFileOut(tt, newest);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();
                assertQuery("SELECT count() FROM ahead").noRandomAccess().expectSize().returns("count\n6\n");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * A WAL table dropped while a {@code SET TYPE WAL} request is still pending is resurrected by
     * {@code TableConverter} on the next boot (questdb/questdb#7649, an OSS bug that predates adaptive
     * commit). The resurrection re-seeds the lineage exactly as any other conversion does, so recovery must
     * find a table it can validate rather than a stale anchor that fails the startup closed. Written to pass
     * either way, so it keeps its value once the upstream resurrection is fixed.
     */
    @Test
    public void testDroppedTableResurrectedByConversionDoesNotBlockStartup() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            assertMemoryLeak(() -> {
                execute("create table zt (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into zt values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                execute("insert into zt values ('2024-09-02T00:00:00.000000Z', 2)");
                drainWalQueue();

                execute("alter table zt set type bypass wal");
                execute("alter table zt set type wal", sqlExecutionContext);
                execute("drop table zt");
                Assert.assertNull("precondition: dropped before the restart", engine.getTableTokenIfExists("zt"));

                engine.releaseInactive();
                engine.load();

                final TableToken resurrected = engine.getTableTokenIfExists("zt");
                if (resurrected != null) {
                    engine.getTableSequencerAPI().resetForReboot(resurrected);
                }
                new RecoveryCoordinator(engine).recover();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * A database written by a build that reset the WAL lineage WITHOUT clearing the anchor (every build
     * before the {@code TableConverter} fix above) still carries an epoch beyond the sequencer frontier.
     * Such an epoch names a cut no replay can reach, so it is a superseded lineage, not the lost-write case
     * the freshness guard fails closed on: recovery must discard it instead of refusing to start, and the next
     * writer enrols the table at the live cut. The stale anchor is restored here from copies taken before the conversion, which is
     * exactly the on-disk state such an upgrade presents.
     */
    @Test
    public void testEpochBeyondSequencerFrontierIsDiscarded() throws Exception {
        org.junit.Assume.assumeFalse("replants files over existing destinations, which Windows forbids", Os.isWindows());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            assertMemoryLeak(() -> {
                execute("create table lin (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into lin values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                execute("insert into lin values ('2024-09-02T00:00:00.000000Z', 2)");
                drainWalQueue();

                final TableToken pre = engine.verifyTableName("lin");
                final long staleEpochSeqTxn = readTxnSeqTxn(pre);
                Assert.assertTrue("precondition: anchor beyond seqTxn 0", staleEpochSeqTxn > 0);
                copyEpochArtifacts(pre, false);

                execute("alter table lin set type bypass wal");
                engine.releaseInactive();
                engine.load();
                execute("alter table lin set type wal", sqlExecutionContext);
                engine.releaseInactive();
                engine.load();

                final TableToken tt = engine.verifyTableName("lin");
                Assert.assertEquals("the conversion re-seeds the lineage at seqTxn 0", 0L, readTxnSeqTxn(tt));
                // Replant the pre-conversion anchor in full: the state an upgraded database arrives in.
                copyEpochArtifacts(tt, true);

                // A restart: a fresh scoreboard and tracker, so the conversion's baseline pin is gone.
                engine.getTxnScoreboardPool().remove(tt);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();

                Assert.assertEquals("the discarded anchor must not rewind the live cut", 0L, readTxnSeqTxn(tt));
                Assert.assertFalse("the discarded anchor must be removed", hasSavePointFile(tt));
                try (io.questdb.cairo.TableReader reader = engine.getReader(tt)) {
                    Assert.assertEquals("rows must survive the discarded anchor", 2L, reader.size());
                }
                try (io.questdb.cairo.TableWriter writer = getWriter(tt)) {
                    Assert.assertNotNull(writer);
                }
                Assert.assertTrue("the next writer enrols the table", hasSavePointFile(tt));
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * An out-of-process restore put back a backup of a quiet table, taken right after a save point, sequencer
     * included, but left the newer save point files in place. The live {@code _txn} then equals the older
     * generation's cut while the newer generation is beyond the restored sequencer: it belongs to the lineage the
     * restore discarded, so recovery must discard the save point and re-enrol, not take it for a generation its
     * own fallback rejected. Kept, the stale generation is restored over the new lineage's rows once the
     * sequencer passes its cut.
     */
    @Test
    public void testRestoreLeavingNewerSavePointOfDiscardedLineageIsDiscarded() throws Exception {
        org.junit.Assume.assumeFalse("replants files over existing destinations, which Windows forbids", Os.isWindows());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(() -> {
                execute("create table oop (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into oop values ('2024-09-01T01:00:00.000000Z', 1)");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("oop");
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.releaseInactive();
                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                final int mkDirMode = engine.getConfiguration().getMkDirMode();
                try (Path table = new Path(); Path backup = new Path(); Path newer = new Path(); Path from = new Path(); Path to = new Path()) {
                    table.of(engine.getConfiguration().getDbRoot()).concat(tt);
                    backup.of(engine.getConfiguration().getDbRoot()).concat("oop_backup");
                    newer.of(engine.getConfiguration().getDbRoot()).concat("oop_newer");
                    Assert.assertEquals(0, ff.copyRecursive(table, backup, mkDirMode));
                    for (int i = 2; i < 5; i++) {
                        execute("insert into oop values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
                    }
                    drainWalQueue();
                    try (TableWriter w = getWriter(tt)) {
                        w.advanceDurableEpoch(2L);
                    }
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.releaseInactive();
                    Assert.assertEquals(0, ff.rename(table.$(), newer.$()));
                    Assert.assertEquals(0, ff.copyRecursive(backup, table, mkDirMode));
                    for (String name : new String[]{"_epoch.manifest.0", "_epoch.manifest.1"}) {
                        to.of(table).concat(name);
                        ff.removeQuiet(to.$());
                        Assert.assertTrue(ff.copy(from.of(newer).concat(name).$(), to.$()) >= 0);
                    }
                }
                engine.getTableSequencerAPI().resetForReboot(tt);
                engine.getTxnScoreboardPool().remove(tt);

                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("the restored cut must stay", 1, readTxnSeqTxn(tt));
                for (int i = 0; i < 3; i++) {
                    execute("insert into oop values ('2024-09-02T0" + i + ":00:00.000000Z', " + (100 + i) + ")");
                }
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.getTxnScoreboardPool().remove(tt);

                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();
                assertQuery("select v from oop").expectSize().returns("v\n1\n100\n101\n102\n");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * Finding C2 (invariant pin) — locks the {@code TxReader.unsafeLoadAll} SLOT-SELECTION property that
     * {@code RecoveryCoordinator.isEpochAheadOfLiveTxn}'s C2 assert relies on: {@code unsafeLoadAll} returns
     * the VERSION-SELECTED (latest) A/B slot when it is intact, and ONLY its IMMEDIATE predecessor
     * (version - 1) when the latest is torn — never an older slot. So {@code unsafeReadVersion() -
     * getVersion()} is exactly 0 on a clean load and exactly 1 on the torn-latest fallback. The recovery guard
     * checks lineage only on diff 0. The version word is durably floored at the epoch cut, so on a single
     * lineage the latest slot never loads below the epoch, and a refusal always means a stale epoch from
     * another lineage. The predecessor slot (diff 1) can predate the epoch on a single lineage, so the guard
     * treats a diff of 1 as a crash.
     * <p>
     * The recovery guard's clean-load branch (diff 0) is exercised end-to-end by
     * {@link #testRecoverRestoresTxnToEpochCut} (proceed) and {@link #testRecoverSkipsEpochAheadOfRestoredTxn}
     * (refuse), and its fallback branch by {@link #testRecoverRestoresEpochUnderEveryTornTxnPageMix}. This
     * test pins the slot selection behind the fallback branch (diff 1) directly and deterministically, using the
     * proven two-commit {@code TxWriter} torn-body pattern (A and B both hold a valid checksummed record), so
     * a regression that narrowed the assert to "must be the latest" (which would wrongly reject the
     * legitimate torn-latest fallback) or widened slot selection to return an older slot is caught here.
     */
    @Test
    public void testUnsafeLoadAllReturnsLatestOrImmediatePredecessorSlot() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String tableName = "slotpin";
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            // Two commits (fixedRowCount 100 then 200) => the A and B areas each hold a valid, body-checksummed
            // record, so the version-selected latest is 200 and its immediate predecessor is 100.
            final TableModel model = new TableModel(engine.getConfiguration(), tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            final int timestampType = TableUtils.getTimestampType(model);
            final ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
            try (Path path = new Path(); TxWriter txWriter = new TxWriter(ff, engine.getConfiguration())) {
                final TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
                txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
                txWriter.updatePartitionSizeByTimestamp(0, 10);
                txWriter.updatePartitionSizeByTimestamp(Micros.HOUR_MICROS, 11);
                txWriter.setMaxTimestamp(Micros.HOUR_MICROS);
                txWriter.reset(100L, txWriter.getTransientRowCount(), txWriter.getMaxTimestamp(), symbolCounts);
                txWriter.reset(200L, txWriter.getTransientRowCount(), txWriter.getMaxTimestamp(), symbolCounts);
            }

            final TableToken tableToken = engine.verifyTableName(tableName);

            // (1) CLEAN load returns the version-selected (latest) slot -> diff 0.
            final long latestBaseOffset;
            try (Path path = new Path(); TxReader tx = new TxReader(ff)) {
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
                tx.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue(tx.unsafeLoadAll());
                Assert.assertEquals("clean load must return the latest committed record", 200L, tx.getFixedRowCount());
                Assert.assertEquals("clean load must return the version-selected (latest) slot: diff 0",
                        0L, tx.unsafeReadVersion() - tx.getVersion());
                latestBaseOffset = tx.getBaseOffset();
            }

            // Tear ONLY the latest slot's fixedRowCount, leaving its checksum stale (positional write, no
            // truncation) — the realistic torn-latest post-crash cut; the predecessor slot stays intact.
            pokeLongTxn(ff, tableToken, latestBaseOffset + TableUtils.TX_OFFSET_FIXED_ROW_COUNT_64, 0xdead_beefL);

            // (2) TORN-LATEST load falls back to the IMMEDIATE predecessor -> diff EXACTLY 1, returning the
            // prior (100) record — never an older slot.
            try (Path path = new Path(); TxReader tx = new TxReader(ff)) {
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
                tx.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue("torn-latest _txn must load via the A/B predecessor fallback", tx.unsafeLoadAll());
                Assert.assertEquals("fallback must return the prior (predecessor) record", 100L, tx.getFixedRowCount());
                Assert.assertEquals("torn-latest fallback must return the IMMEDIATE predecessor: diff 1",
                        1L, tx.unsafeReadVersion() - tx.getVersion());
            }
        });
    }

    @Test
    public void testDetachFirstCoverFallbackAndReopen() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicInteger publications = new AtomicInteger();
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade facade = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailing.get() && Utf8s.containsAscii(name, io.questdb.cairo.DurableEpochManifest.FILE_NAME)
                        && publications.incrementAndGet() > 1) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(facade, () -> {
            execute("CREATE TABLE detach_cover (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO detach_cover VALUES ('2024-01-01', 1), ('2024-01-01T01:00', 2), ('2024-01-02', 3), ('2024-01-03', 4)");
            TestUtils.drainWalQueue(engine);
            final TableToken token = engine.verifyTableName("detach_cover");
            try (TableWriter writer = getWriter(token)) {
                writer.advanceDurableEpoch(1);
            }
            isFailing.set(true);
            try {
                execute("ALTER TABLE detach_cover DETACH PARTITION LIST '2024-01-01'");
                TestUtils.drainWalQueue(engine);
                Assert.assertTrue("required second publication must suspend apply", engine.getTableSequencerAPI().isSuspended(token));
                Assert.assertEquals("one published cover, then one failed attempt", 2, publications.get());
                final io.questdb.cairo.DurableEpochManifest.Candidate[] cuts = operationCompletionCuts(token);
                Assert.assertEquals(2, cuts.length);
                Assert.assertTrue("exactly the newest cut covers the detach", cuts[0].epochTxn > cuts[1].epochTxn);
                Assert.assertFalse("first cover must not expose the final artifact", epochArtifactExists(token, "2024-01-01", TableUtils.DETACHED_DIR_MARKER));
                engine.releaseAllWriters();
                try (TableWriter ignored = getWriter(token)) {
                    Assert.fail("reopen must complete the outstanding operation before returning a writer");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                }
                Assert.assertEquals("reopen attempts the missing cover", 3, publications.get());
                Assert.assertFalse("failed reopen must not finalize from the newest pin alone", epochArtifactExists(token, "2024-01-01", TableUtils.DETACHED_DIR_MARKER));
                Assert.assertEquals("failed construction must release its pool entry", 0, engine.getBusyWriterCount());
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                isFailing.set(false);
                corruptSavePointSection(token, cuts[0].generation);
                engine.getTxnScoreboardPool().remove(token);
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
            } finally {
                isFailing.set(false);
            }
            execute("ALTER TABLE detach_cover RESUME WAL");
            TestUtils.drainWalQueue(engine);
            assertQuery("SELECT * FROM detach_cover").expectSize().timestamp("ts").returns("ts\tid\n2024-01-02T00:00:00.000000Z\t3\n2024-01-03T00:00:00.000000Z\t4\n");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertTrue("replay must finish the detached artifact", epochArtifactExists(token, "2024-01-01", TableUtils.DETACHED_DIR_MARKER));
            Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(token), readTxnSeqTxn(token));
            for (int i = 0; i < 2; i++) {
                final io.questdb.cairo.DurableEpochManifest.Candidate[] cuts = operationCompletionCuts(token);
                Assert.assertTrue("every selectable cut covers the completed detach", cuts.length > 0);
                for (io.questdb.cairo.DurableEpochManifest.Candidate cut : cuts) {
                    Assert.assertEquals(readTxnSeqTxn(token), cut.epochSeqTxn);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                if (cuts.length == 2) {
                    corruptSavePointSection(token, cuts[0].generation);
                }
                engine.getTxnScoreboardPool().remove(token);
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                TestUtils.drainWalQueue(engine);
                assertQuery("SELECT id FROM detach_cover ORDER BY id").expectSize().returns("id\n3\n4\n");
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            }
        });
    }

    @Test
    public void testDetachFinalizationFailureRetriesAfterReopen() throws Exception {
        assertDetachFinalizationFailureRetries(false);
    }

    @Test
    public void testDetachFinalizationFailureRetriesAfterRestart() throws Exception {
        assertDetachFinalizationFailureRetries(true);
    }

    /**
     * The creation baselines other than CREATE: an {@code ALTER TABLE ... REBASE WAL} clone and a conversion to WAL
     * each leave the new table with a valid save point at its live cut, and a restart accepts it and keeps every row.
     * Both baselines name a populated partition, whose files the conversion's source wrote without a flush, so
     * generation zero is published only after a barrier covers them.
     */
    private void assertCreationBaselinesValid(boolean isWide) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        final BaselineBarrierFacade ff = new BaselineBarrierFacade(isWide);
        try {
            assertMemoryLeak(ff, () -> {
                setCommitMode("adaptive");
                execute("CREATE TABLE rebased (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO rebased VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                execute("ALTER TABLE rebased SUSPEND WAL");
                ff.reset();
                execute("ALTER TABLE rebased REBASE WAL");
                drainWalQueue();
                ff.assertBaselineFlushedFirst("2024-09-01");
                execute("CREATE TABLE converted (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO converted VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                execute("ALTER TABLE converted SET TYPE WAL");
                engine.releaseInactive();
                ff.reset();
                engine.load();
                ff.assertBaselineFlushedFirst("2024-09-01");
                for (String name : new String[]{"rebased", "converted"}) {
                    final TableToken tt = engine.verifyTableName(name);
                    Assert.assertTrue(name + ": a WAL table", tt.isWal());
                    Assert.assertEquals(name + ": a valid save point at the live cut", readTxnSeqTxn(tt), newestSavePointSeqTxn(tt));
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                    engine.getTxnScoreboardPool().remove(tt);
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    engine.notifyWalTxnRepublisher(tt);
                    drainWalQueue();
                    assertQuery("SELECT * FROM " + name)
                            .expectSize()
                            .timestamp("ts")
                            .returns("""
                                    ts\tv
                                    2024-09-01T00:00:00.000000Z\t1
                                    """);
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    private void assertDetachFinalizationFailureRetries(boolean restart) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade facade = new TestFilesFacadeImpl() {
            @Override
            public int hardLinkDirRecursive(Path src, Path dst, int mode) {
                return isFailing.get() && Utf8s.endsWithAscii(dst, TableUtils.DETACHED_DIR_MARKER)
                        ? -1 : super.hardLinkDirRecursive(src, dst, mode);
            }

            @Override
            public int copyRecursive(Path src, Path dst, int mode) {
                return isFailing.get() && Utf8s.endsWithAscii(dst, TableUtils.DETACHED_DIR_MARKER)
                        ? -1 : super.copyRecursive(src, dst, mode);
            }
        };
        assertMemoryLeak(facade, () -> {
            execute("CREATE TABLE detach_retry (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO detach_retry VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            TestUtils.drainWalQueue(engine);
            final TableToken token = engine.verifyTableName("detach_retry");
            isFailing.set(true);
            try {
                execute("ALTER TABLE detach_retry DETACH PARTITION LIST '2024-01-01'");
                TestUtils.drainWalQueue(engine);
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
                Assert.assertFalse(epochArtifactExists(token, "2024-01-01", TableUtils.DETACHED_DIR_MARKER));
                final io.questdb.cairo.DurableEpochManifest.Candidate[] cuts = operationCompletionCuts(token);
                Assert.assertEquals(2, cuts.length);
                Assert.assertEquals(cuts[0].epochTxn, cuts[1].epochTxn);
                engine.releaseAllWriters();
                if (restart) {
                    engine.releaseAllReaders();
                    engine.getTxnScoreboardPool().remove(token);
                    engine.getTableSequencerAPI().resetForReboot(token);
                    new RecoveryCoordinator(engine).recover();
                    Assert.assertFalse("suspension is not persisted", engine.getTableSequencerAPI().isSuspended(token));
                }
                Assert.assertThrows(CairoException.class, () -> execute("ALTER TABLE detach_retry RESUME WAL"));
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            } finally {
                isFailing.set(false);
            }
            execute("ALTER TABLE detach_retry RESUME WAL");
            TestUtils.drainWalQueue(engine);
            Assert.assertTrue(epochArtifactExists(token, "2024-01-01", TableUtils.DETACHED_DIR_MARKER));
            assertQuery("SELECT id FROM detach_retry").expectSize().returns("id\n2\n");
        });
    }

    @Test
    public void testDetachRejectsUnrelatedDestination() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE detach_existing (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO detach_existing VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            TestUtils.drainWalQueue(engine);
            final TableToken token = engine.verifyTableName("detach_existing");
            final java.nio.file.Path destination = Paths.get(configuration.getDbRoot().toString(), token.getDirName(), "2024-01-01" + TableUtils.DETACHED_DIR_MARKER);
            Files.createDirectories(destination);
            Files.writeString(destination.resolve("unrelated"), "not this detach");
            try (TableWriter writer = getWriter(token)) {
                Assert.assertEquals(io.questdb.cairo.AttachDetachStatus.DETACH_ERR_ALREADY_DETACHED,
                        writer.detachPartition(MicrosTimestampDriver.floor("2024-01-01")));
            }
            assertQuery("SELECT id FROM detach_existing ORDER BY id").expectSize().returns("id\n1\n2\n");
            Assert.assertEquals("not this detach", Files.readString(destination.resolve("unrelated")));
        });
    }

    @Test
    public void testPlainResumeDoesNotAcquireWriter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE resume_busy (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO resume_busy VALUES ('2024-01-01', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("resume_busy");
            final CountDownLatch acquired = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final PoolListener listener = engine.getPoolListener();
            engine.setPoolListener((source, thread, table, event, segment, position) -> {
                if (source == PoolListener.SRC_WRITER
                        && event == PoolListener.EV_GET && token.equals(table)) {
                    acquired.countDown();
                    TestUtils.await(release);
                }
            });
            final Thread apply = new Thread(() -> {
                try {
                    TestUtils.drainWalQueue(engine);
                } catch (Throwable th) {
                    failure.set(th);
                } finally {
                    acquired.countDown();
                    Path.clearThreadLocals();
                }
            });
            try {
                execute("INSERT INTO resume_busy VALUES ('2024-01-02', 2)");
                apply.start();
                TestUtils.await(acquired);
                Assert.assertNull(failure.get());
                Assert.assertEquals(1, engine.getBusyWriterCount());
                execute("ALTER TABLE resume_busy SUSPEND WAL");
                execute("ALTER TABLE resume_busy RESUME WAL");
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            } finally {
                release.countDown();
                apply.join();
                engine.setPoolListener(listener);
            }
            Assert.assertNull(failure.get());
            drainWalQueue();
            assertQuery("SELECT id FROM resume_busy ORDER BY id").expectSize().returns("id\n1\n2\n");
        });
    }

    @Test
    public void testPlainResumeDuringForceDropKeepsSuspensionReason() throws Exception {
        assertPlainResumeDuringForceDrop(true);
    }

    @Test
    public void testPlainResumeDuringForceDropKeepsTableRunning() throws Exception {
        assertPlainResumeDuringForceDrop(false);
    }

    @Test
    public void testPlainResumeOnReplicaDoesNotAcquireWriter() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE resume_replica (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO resume_replica VALUES ('2024-01-01', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("resume_replica");
            // Boot as a replica does: recovery arms the required completion before any role exists, then the
            // replica role opens and reconciles each writer, which never runs that completion.
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.getTableSequencerAPI().resetForReboot(token);
            new RecoveryCoordinator(engine).recover();
            final var tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertTrue(tracker.isDurableEpochCompletionRequired());
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            final PoolListener listener = engine.getPoolListener();
            try {
                try (TableWriter writer = engine.getWriterUnsafe(token, "demote clear")) {
                    writer.reconcileDurableEpochAnchor();
                }
                Assert.assertTrue(tracker.isDurableEpochCompletionRequired());
                final CountDownLatch acquired = new CountDownLatch(1);
                final CountDownLatch release = new CountDownLatch(1);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final Thread apply = new Thread(() -> {
                    try {
                        TestUtils.drainWalQueue(engine);
                    } catch (Throwable th) {
                        failure.set(th);
                    } finally {
                        acquired.countDown();
                        Path.clearThreadLocals();
                    }
                });
                engine.setPoolListener((source, thread, table, event, segment, position) -> {
                    if (source == PoolListener.SRC_WRITER && event == PoolListener.EV_GET
                            && token.equals(table) && Thread.currentThread() == apply) {
                        acquired.countDown();
                        TestUtils.await(release);
                    }
                });
                try {
                    execute("INSERT INTO resume_replica VALUES ('2024-01-02', 2)");
                    apply.start();
                    TestUtils.await(acquired);
                    Assert.assertNull(failure.get());
                    Assert.assertEquals(1, engine.getBusyWriterCount());
                    // Succeeds while the apply job holds the writer, so it took none.
                    execute("ALTER TABLE resume_replica RESUME WAL");
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
                } finally {
                    release.countDown();
                    apply.join();
                    engine.setPoolListener(listener);
                }
                Assert.assertNull(failure.get());
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
                assertQuery("SELECT id FROM resume_replica ORDER BY id").expectSize().returns("id\n1\n2\n");
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    private void assertPlainResumeDuringForceDrop(boolean isSuspendedBefore) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final AtomicReference<TableToken> blockedToken = new AtomicReference<>();
        final AtomicReference<Thread> holder = new AtomicReference<>();
        final AtomicBoolean isBlocked = new AtomicBoolean();
        final CountDownLatch inCompletion = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final FilesFacade facade = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                maybeBlock(name);
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                maybeBlock(name);
                return super.openRW(name, opts);
            }

            // Parks FORCE DROP inside its required completion, while it holds the writer.
            private void maybeBlock(LPSZ name) {
                final TableToken token = blockedToken.get();
                if (token != null && Thread.currentThread() == holder.get()
                        && Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)
                        && engine.getTableSequencerAPI().getTxnTracker(token).isDurableEpochCompletionRequired()
                        && isBlocked.compareAndSet(false, true)) {
                    inCompletion.countDown();
                    TestUtils.await(release);
                }
            }
        };
        assertMemoryLeak(facade, () -> {
            execute("CREATE TABLE resume_force (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO resume_force VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("resume_force");
            final String walTablesQuery = "SELECT suspended, errorTag, errorMessage FROM wal_tables() WHERE name = 'resume_force'";
            final String operatorSuspension = "suspended\terrorTag\terrorMessage\ntrue\tDISK FULL\toriginal operator reason\n";
            if (isSuspendedBefore) {
                execute("ALTER TABLE resume_force SUSPEND WAL WITH 'DISK FULL', 'original operator reason'");
            }
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final Thread forceDrop = new Thread(() -> {
                try (var context = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute("ALTER TABLE resume_force FORCE DROP PARTITION LIST '2024-01-01'", context);
                } catch (Throwable th) {
                    failure.set(th);
                } finally {
                    inCompletion.countDown();
                    Path.clearThreadLocals();
                }
            });
            blockedToken.set(token);
            holder.set(forceDrop);
            forceDrop.start();
            try {
                TestUtils.await(inCompletion);
                Assert.assertNull(failure.get());
                Assert.assertTrue(isBlocked.get());
                Assert.assertEquals(1, engine.getBusyWriterCount());
                final CairoException e = Assert.assertThrows(CairoException.class, () -> execute("ALTER TABLE resume_force RESUME WAL"));
                // Nothing was attempted, so the suspension state and its reason stay as they were.
                Assert.assertEquals(isSuspendedBefore, engine.getTableSequencerAPI().isSuspended(token));
                if (isSuspendedBefore) {
                    assertQuery(walTablesQuery).noLeakCheck().noRandomAccess().returns(operatorSuspension);
                }
                Assert.assertTrue(e instanceof EntryUnavailableException);
                TestUtils.assertContains(e.getFlyweightMessage(), "table busy");
            } finally {
                release.countDown();
                forceDrop.join();
            }
            Assert.assertNull(failure.get());
            execute("INSERT INTO resume_force VALUES ('2024-01-02T01:00:00.000000Z', 3)");
            drainWalQueue();
            Assert.assertEquals(isSuspendedBefore, engine.getTableSequencerAPI().isSuspended(token));
            if (isSuspendedBefore) {
                assertQuery(walTablesQuery).noLeakCheck().noRandomAccess().returns(operatorSuspension);
                execute("ALTER TABLE resume_force RESUME WAL");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            }
            assertQuery("SELECT id FROM resume_force ORDER BY id").expectSize().returns("id\n2\n3\n");
        });
    }

    @Test
    public void testPlainResumeOfUnenrolledTable() throws Exception {
        assertResumeOfUnenrolledTable(false, false);
    }

    @Test
    public void testResumeFromTxnOfUnenrolledTable() throws Exception {
        assertResumeOfUnenrolledTable(true, false);
    }

    @Test
    public void testResumeAfterFailedEnrolment() throws Exception {
        assertResumeOfUnenrolledTable(false, true);
    }

    private void assertResumeOfUnenrolledTable(boolean fromTxn, boolean failEnrolment) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade facade = new TestFilesFacadeImpl() {
            @Override
            public boolean isSyncfsFileSystemWide() {
                // Exercise the per-file sequencer barrier used on non-Linux systems.
                return false;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailing.get() && Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(facade, () -> {
            execute("CREATE TABLE resume_enrol (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO resume_enrol VALUES ('2024-01-01', 1)");
            drainWalQueue();
            engine.clear();
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final TableToken token = restarted.verifyTableName("resume_enrol");
                final var context = TestUtils.createSqlExecutionCtx(restarted);
                final java.nio.file.Path tableDir = Paths.get(configuration.getDbRoot().toString(), token.getDirName());
                Assert.assertFalse(Files.exists(tableDir.resolve(DurableEpochManifest.FILE_NAME + ".0")));
                Assert.assertFalse(Files.exists(tableDir.resolve(DurableEpochManifest.FILE_NAME + ".1")));
                Assert.assertEquals(0, restarted.getBusyWriterCount());
                if (failEnrolment) {
                    restarted.execute("INSERT INTO resume_enrol VALUES ('2024-01-02', 2)");
                    isFailing.set(true);
                    TestUtils.drainWalQueue(restarted);
                    isFailing.set(false);
                    Assert.assertTrue(restarted.getTableSequencerAPI().isSuspended(token));
                } else {
                    restarted.execute("ALTER TABLE resume_enrol SUSPEND WAL");
                }
                final long nextTxn = restarted.getTableSequencerAPI().lastTxn(token) + 1;
                restarted.execute("ALTER TABLE resume_enrol RESUME WAL" + (fromTxn ? " FROM TXN " + nextTxn : ""));
                Assert.assertFalse(restarted.getTableSequencerAPI().isSuspended(token));
                TestUtils.drainWalQueue(restarted);
                assertQuery("SELECT id FROM resume_enrol ORDER BY id")
                        .withEngine(restarted).withContext(context).noLeakCheck().expectSize()
                        .returns(failEnrolment ? "id\n1\n2\n" : "id\n1\n");
                Assert.assertTrue(Files.exists(tableDir.resolve(DurableEpochManifest.FILE_NAME + ".0"))
                        || Files.exists(tableDir.resolve(DurableEpochManifest.FILE_NAME + ".1")));
            }
        });
    }

    @Test
    public void testResumeCompletionRejectsFencedPublication() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE resume_fence (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO resume_fence VALUES ('2024-01-01', 1)");
            TestUtils.drainWalQueue(engine);
            final TableToken token = engine.verifyTableName("resume_fence");
            execute("ALTER TABLE resume_fence SUSPEND WAL");
            execute("INSERT INTO resume_fence VALUES ('2024-01-02', 2)");
            final long resumeFrom = engine.getTableSequencerAPI().lastTxn(token) + 1;
            engine.getTableSequencerAPI().getTxnTracker(token).fenceDurableFrontier(CairoException.dataSyncFailureCarry(5, "test fence"));
            try {
                Assert.assertThrows(CairoException.class, () -> execute("ALTER TABLE resume_fence RESUME WAL FROM TXN " + resumeFrom));
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
                Assert.assertThrows(CairoException.class, () -> execute("ALTER TABLE resume_fence RESUME WAL"));
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            } finally {
                engine.releaseAllWriters();
                engine.getTxnScoreboardPool().remove(token);
                engine.getTableSequencerAPI().resetForReboot(token);
            }
        });
    }

    @Test
    public void testResumeSecondCoverFailureAndPlainRetry() throws Exception {
        assertResumeCompletionRetry(true, false, false);
    }

    @Test
    public void testResumeSecondCoverFailureAndRepeatedFromRetry() throws Exception {
        assertResumeCompletionRetry(false, false, false);
    }

    @Test
    public void testResumeSecondCoverFailureAndPlainRetryAfterRestart() throws Exception {
        assertResumeCompletionRetry(true, true, false);
    }

    @Test
    public void testResumeSecondCoverFailureAndApplyAfterRestart() throws Exception {
        assertResumeCompletionRetry(true, true, true);
    }

    private void assertResumeCompletionRetry(boolean plainRetry, boolean restart, boolean applyBeforeResume) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicInteger publications = new AtomicInteger();
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade facade = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailing.get() && Utf8s.containsAscii(name, io.questdb.cairo.DurableEpochManifest.FILE_NAME)
                        && publications.incrementAndGet() > 1) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(facade, () -> {
            execute("CREATE TABLE resume_cover (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO resume_cover VALUES ('2024-01-01', 1)");
            TestUtils.drainWalQueue(engine);
            final TableToken token = engine.verifyTableName("resume_cover");
            try (TableWriter writer = getWriter(token)) {
                writer.advanceDurableEpoch(1);
            }
            execute("ALTER TABLE resume_cover SUSPEND WAL");
            execute("INSERT INTO resume_cover VALUES ('2024-01-02', 20)");
            execute("INSERT INTO resume_cover VALUES ('2024-01-02', 21)");
            final long skippedThrough = engine.getTableSequencerAPI().lastTxn(token);
            execute("INSERT INTO resume_cover VALUES ('2024-01-03', 3)");
            TestUtils.drainWalQueue(engine);
            isFailing.set(true);
            try {
                Assert.assertThrows(CairoException.class, () -> execute("ALTER TABLE resume_cover RESUME WAL FROM TXN " + (skippedThrough + 1)));
                Assert.assertEquals(2, publications.get());
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
                final io.questdb.cairo.DurableEpochManifest.Candidate[] cuts = operationCompletionCuts(token);
                Assert.assertEquals(2, cuts.length);
                Assert.assertEquals(skippedThrough, cuts[0].epochSeqTxn);
                Assert.assertTrue(cuts[1].epochSeqTxn < skippedThrough);
                // The failed open leaves its generation selectable and must restore its pin. Then explicitly
                // remove that bookkeeping: neither a missing pin nor a cleared owed bit certifies disk coverage.
                final var tracker = engine.getTableSequencerAPI().getTxnTracker(token);
                Assert.assertTrue(tracker.getOtherPinnedEpochTxn() >= 0);
                try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
                    scoreboard.releaseTxn(tracker.isPinnedEpochSlotA() ? TxnScoreboard.EPOCH_ID_B : TxnScoreboard.EPOCH_ID_A,
                            tracker.getOtherPinnedEpochTxn());
                }
                tracker.setPinnedEpoch(tracker.getPinnedEpochTxn(), tracker.isPinnedEpochSlotA(), -1, tracker.getPinnedEpochGeneration());
                tracker.setDurableEpochOwed(false);
                Assert.assertThrows(CairoException.class, () -> execute("ALTER TABLE resume_cover RESUME WAL" + (plainRetry ? "" : " FROM TXN " + (skippedThrough + 1))));
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            } finally {
                isFailing.set(false);
            }
            if (restart) {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.getTxnScoreboardPool().remove(token);
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                Assert.assertFalse("suspension is not persisted", engine.getTableSequencerAPI().isSuspended(token));
                final var recoveredCuts = operationCompletionCuts(token);
                Assert.assertTrue(recoveredCuts[0].epochSeqTxn > recoveredCuts[1].epochSeqTxn);
                if (applyBeforeResume) {
                    isFailing.set(true);
                    try {
                        TestUtils.drainWalQueue(engine);
                        Assert.assertTrue("required completion must fail before replay", engine.getTableSequencerAPI().isSuspended(token));
                        Assert.assertEquals(skippedThrough, readTxnSeqTxn(token));
                    } finally {
                        isFailing.set(false);
                    }
                }
            }
            execute("ALTER TABLE resume_cover RESUME WAL" + (plainRetry ? "" : " FROM TXN " + (skippedThrough + 1)));
            final io.questdb.cairo.DurableEpochManifest.Candidate[] cuts = operationCompletionCuts(token);
            Assert.assertEquals(2, cuts.length);
            for (io.questdb.cairo.DurableEpochManifest.Candidate cut : cuts) {
                Assert.assertEquals(skippedThrough, cut.epochSeqTxn);
            }
            TestUtils.drainWalQueue(engine);
            assertQuery("SELECT id FROM resume_cover ORDER BY id").expectSize().returns("id\n1\n3\n");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            corruptSavePointSection(token, cuts[0].generation);
            engine.getTxnScoreboardPool().remove(token);
            engine.getTableSequencerAPI().resetForReboot(token);
            new RecoveryCoordinator(engine).recover();
            TestUtils.drainWalQueue(engine);
            assertQuery("SELECT id FROM resume_cover ORDER BY id").expectSize().returns("id\n1\n3\n");
            Assert.assertEquals(skippedThrough + 1, readTxnSeqTxn(token));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        });
    }

    private io.questdb.cairo.DurableEpochManifest.Candidate[] operationCompletionCuts(TableToken token) {
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(token);
            return io.questdb.cairo.DurableEpochManifest.loadCandidates(configuration.getFilesFacade(), p, p.size(), token.getTableId());
        }
    }

    private static boolean nextMix(int[] mix, int imageCount) {
        for (int p = 0; p < mix.length; p++) {
            if (++mix[p] < imageCount) {
                return true;
            }
            mix[p] = 0;
        }
        return false;
    }

    private boolean isTornFallbackBelow(TableToken tt, long epochSeqTxn) {
        try (TxReader tx = new TxReader(engine.getConfiguration().getFilesFacade()); Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            tx.ofRO(p.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            return tx.unsafeLoadAll() && tx.unsafeReadVersion() != tx.getVersion() && tx.getSeqTxn() < epochSeqTxn;
        } catch (CairoException e) {
            return false;
        }
    }

    /**
     * Positional 8-byte write of a table's {@code _txn} — corrupts a committed record WITHOUT truncating.
     */
    private void pokeLongTxn(FilesFacade ff, TableToken tt, long offset, long value) {
        pokeLong(ff, tt, TableUtils.TXN_FILE_NAME, offset, value);
    }

    /**
     * Positional 8-byte write into a file of the table's directory.
     */
    private void pokeLong(FilesFacade ff, TableToken tt, CharSequence fileName, long offset, long value) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName).$();
            final long fd = ff.openRW(p.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putLong(buf, value);
                Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, offset));
                ff.fsync(fd);
            } finally {
                Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
        }
    }

    /**
     * Copies every epoch artifact of {@code tt} aside ({@code restore == false}) or back into place
     * ({@code restore == true}), covering both ping-pong generations so the replanted state is exactly the
     * one taken.
     */
    private void copyEpochArtifacts(TableToken tt, boolean restore) {
        final String[] names = {"_epoch.manifest.0", "_epoch.manifest.1"};
        for (int i = 0; i < names.length; i++) {
            final String live = names[i];
            final String saved = live + ".bak";
            final String from = restore ? saved : live;
            if (epochArtifactExists(tt, from, "")) {
                copyTableFile(tt, from, restore ? live : saved);
            }
        }
    }

    private void copyTableFile(TableToken tt, CharSequence from, CharSequence to) {
        final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path src = new Path(); Path dst = new Path()) {
            src.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(from);
            dst.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(to);
            Assert.assertTrue("copy " + from + " -> " + to + " must succeed", ff.copy(src.$(), dst.$()) >= 0);
        }
    }

    /**
     * The shared clear helper used by the ENT restore + demote paths must remove the marker, both save point
     * files and any copy file an older development layout left, leave the LIVE files untouched, and be
     * idempotent.
     */
    @Test
    public void testReadSavePointHeaderIgnoresErrnoOfAbsentFile() throws Exception {
        // The facade reports one errno for every failure, as some test facades do. An absent save point file reads
        // as absent through every reader of the headers; one that exists but cannot be opened is still a failure.
        final AtomicBoolean failOpen = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return 999;
            }

            @Override
            public long openRO(LPSZ name) {
                if (failOpen.get() && Utf8s.containsAscii(name, io.questdb.cairo.DurableEpochManifest.FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            final long scratch = Unsafe.malloc(io.questdb.cairo.DurableEpochManifest.FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path()) {
                Files.createDirectories(Paths.get(root, "probe"));
                final int rootLen = path.of(root).concat("probe").size();
                Assert.assertFalse(io.questdb.cairo.DurableEpochManifest.removeUnpublished(configuration, path, rootLen, 1));
                Assert.assertEquals(0, io.questdb.cairo.DurableEpochManifest.loadCandidates(ff, path, rootLen, 1).length);
                Assert.assertEquals(Long.MAX_VALUE, io.questdb.cairo.DurableEpochManifest.readLowestSeqTxn(ff, path, rootLen, 1, scratch));

                Files.write(Paths.get(root, "probe", io.questdb.cairo.DurableEpochManifest.FILE_NAME + ".0"), new byte[0]);
                failOpen.set(true);
                final String expected = "could not open adaptive save point";
                TestUtils.assertContains(Assert.assertThrows(CairoException.class, () -> io.questdb.cairo.DurableEpochManifest.removeUnpublished(configuration, path, rootLen, 1)).getFlyweightMessage(), expected);
                TestUtils.assertContains(Assert.assertThrows(CairoException.class, () -> io.questdb.cairo.DurableEpochManifest.loadCandidates(ff, path, rootLen, 1)).getFlyweightMessage(), expected);
                // The WAL purge floor reads a failure as seqTxn 0, which keeps every WAL segment.
                Assert.assertEquals(0, io.questdb.cairo.DurableEpochManifest.readLowestSeqTxn(ff, path, rootLen, 1, scratch));
            } finally {
                Unsafe.free(scratch, io.questdb.cairo.DurableEpochManifest.FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRemoveAdaptiveEpochArtifactsRemovesTrioOnly() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            try {
                execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into r values ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("r");

                // Take a bound generational epoch.
                try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // Precondition: both generations and the live files exist.
                for (int generation = 0; generation < 2; generation++) {
                    Assert.assertTrue("manifest generation present", epochArtifactExists(tt, io.questdb.cairo.DurableEpochManifest.FILE_NAME, "." + generation));
                }

                final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path p = new Path()) {
                    final int rootLen = p.of(engine.getConfiguration().getDbRoot()).concat(tt).size();
                    RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, p, rootLen);
                }

                // Both generations are gone; the live _txn/_cv are untouched.
                for (int generation = 0; generation < 2; generation++) {
                    Assert.assertFalse("manifest generation removed", epochArtifactExists(tt, io.questdb.cairo.DurableEpochManifest.FILE_NAME, "." + generation));
                }
                Assert.assertTrue("live _meta untouched", epochArtifactExists(tt, TableUtils.META_FILE_NAME, ""));
                Assert.assertTrue("live _txn untouched", epochArtifactExists(tt, TableUtils.TXN_FILE_NAME, ""));
                Assert.assertTrue("live _cv untouched", epochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME, ""));

                // Idempotent: a second call on the now-absent trio is a no-op (must not throw).
                try (Path p = new Path()) {
                    final int rootLen = p.of(engine.getConfiguration().getDbRoot()).concat(tt).size();
                    RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, p, rootLen);
                }
            } finally {
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    @Test
    public void testRemoveAdaptiveEpochArtifactsFailsClosedWhenArtifactSurvives() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            assertMemoryLeak(() -> {
                execute("create table remove_fail (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken token = engine.verifyTableName("remove_fail");
                final FilesFacade refusingFf = new TestFilesFacadeImpl() {
                    @Override
                    public boolean removeQuiet(LPSZ name) {
                        if (Utf8s.containsAscii(name, io.questdb.cairo.DurableEpochManifest.FILE_NAME)) {
                            return false;
                        }
                        return super.removeQuiet(name);
                    }
                };
                try (Path p = new Path()) {
                    final int rootLen = p.of(configuration.getDbRoot()).concat(token).size();
                    try {
                        RecoveryCoordinator.removeAdaptiveEpochArtifacts(refusingFf, p, rootLen);
                        Assert.fail("surviving save point must abort stale-lineage cleanup");
                    } catch (CairoException expected) {
                        TestUtils.assertContains(expected.getFlyweightMessage(),
                                "could not remove stale adaptive epoch artifact");
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    /**
     * SP-B — per-table failure isolation. A genuine I/O error while restoring ONE adaptive table's
     * durable epoch cut must not strand its healthy siblings or brick boot. {@code recover()} must catch
     * the failure, SUSPEND just that table (idiomatic to WAL-apply error handling — visible in
     * {@code wal_tables()} with an error tag/message), and continue rolling the other adaptive tables
     * forward. Before the fix the unguarded per-table loop let the restore's {@code CairoException}
     * propagate out of {@code recover()}, so every not-yet-visited table was skipped (came up un-rewound)
     * or boot failed outright.
     */
    @Test
    public void testRecoverDirectorySyncFailurePoisonsEngineAndPropagates() throws Exception {
        // The injected fault point does not exist on Windows: RecoveryCoordinator skips the recovery
        // directory fsync there (no directory handles to fsync), so fsyncAndClose is never reached and
        // recover() has nothing to classify. POSIX-only by the shape of the product code.
        org.junit.Assume.assumeFalse("recovery takes no directory fsync on Windows", Os.isWindows());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            final AtomicBoolean isCounting = new AtomicBoolean();
            final AtomicBoolean failNext = new AtomicBoolean();
            final AtomicInteger syncAttempts = new AtomicInteger();
            final FilesFacade failingFf = new TestFilesFacadeImpl() {
                @Override
                public void fsyncAndClose(long fd) {
                    if (isCounting.get()) {
                        syncAttempts.incrementAndGet();
                    }
                    if (failNext.compareAndSet(true, false)) {
                        super.close(fd);
                        throw CairoException.dataSyncFailure(this, 5, "fsyncAndClose", "injected recovery directory sync failure");
                    }
                    super.fsyncAndClose(fd);
                }
            };
            final FilesFacade ffBefore = AbstractCairoTest.ff;
            try {
                buildAdaptiveLazyGapTable("syncfail");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                AbstractCairoTest.ff = failingFf;
                isCounting.set(true);
                failNext.set(true);

                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("classified recovery sync failure must propagate");
                } catch (CairoError expected) {
                    Assert.assertTrue(CairoException.isDataSyncFailure(expected));
                }

                Assert.assertEquals(1, syncAttempts.get());
                Assert.assertTrue(engine.isDurabilityFailed());
                Assert.assertEquals("fsyncAndClose", engine.getDurabilityFailure().getOperation());
            } finally {
                AbstractCairoTest.ff = ffBefore;
                engine.resetDurabilityFailure();
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * A failed flush of the save point headers recovery selected stops the start and poisons the engine, as every
     * failed sync at recovery does, whether one syncfs covers every header or each takes its own fsync. Nothing
     * may trust a header the device may not hold. The next start rewrites and flushes them again.
     */
    @Test
    public void testRecoverHeaderFlushFailurePoisonsEngineAndPropagates() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            final AtomicBoolean isFailing = new AtomicBoolean();
            final FilesFacade failingFf = new TestFilesFacadeImpl() {
                @Override
                public void fsync(long fd) {
                    if (isFailing.get()) {
                        throw CairoException.dataSyncFailure(this, 5, "fsync", "injected header flush failure");
                    }
                    super.fsync(fd);
                }

                @Override
                public void syncfs(long fd) {
                    if (isFailing.get()) {
                        throw CairoException.dataSyncFailure(this, 5, "syncfs", "injected header flush failure");
                    }
                    super.syncfs(fd);
                }
            };
            final FilesFacade ffBefore = AbstractCairoTest.ff;
            try {
                for (String name : new String[]{"flush_a", "flush_b"}) {
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                }
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                AbstractCairoTest.ff = failingFf;
                isFailing.set(true);
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("a failed header flush must stop the start");
                } catch (CairoError expected) {
                    Assert.assertTrue(CairoException.isDataSyncFailure(expected));
                }
                Assert.assertTrue(engine.isDurabilityFailed());
                Assert.assertEquals(failingFf.isSyncfsFileSystemWide() ? "syncfs" : "fsync", engine.getDurabilityFailure().getOperation());

                isFailing.set(false);
                engine.resetDurabilityFailure();
                new RecoveryCoordinator(engine).recover();
                execute("INSERT INTO flush_b VALUES ('2024-09-02T00:00:00.000000Z', 2)");
                drainWalQueue();
                assertQuery("SELECT * FROM flush_b")
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tv
                                2024-09-01T00:00:00.000000Z\t1
                                2024-09-02T00:00:00.000000Z\t2
                                """);
            } finally {
                AbstractCairoTest.ff = ffBefore;
                engine.resetDurabilityFailure();
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            }
        });
    }

    @Test
    public void testRecoverAbortsOnRestoreIoErrorBeforeServingSiblings() throws Exception {
        // The fault is injected on the openRW + copyData in-place transfer -- the POSIX branch of
        // TableUtils.replaceFileContent. Windows takes the removeQuiet + copy route instead, so the
        // seam this facade arms is never exercised there. POSIX-only by the shape of the product code.
        org.junit.Assume.assumeFalse("restore transfer fault seam is POSIX-only", Os.isWindows());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            // A path-targeted transfer fault: fail ONLY the target table's live _txn restore
            // (_txn.epoch -> _txn), reporting ENOSPC (28 -> ErrorTag.DISK_FULL on linux). errno() returns the
            // simulated code exactly once, right after the failed transfer, so no other errno read is poisoned.
            final int simErrno = 28;
            final AtomicReference<String> failDirName = new AtomicReference<>();
            final AtomicBoolean justFailed = new AtomicBoolean(false);
            final FilesFacade failingFf = new RestoreTransferFaultFacade(failDirName, justFailed, simErrno, TableUtils.TXN_FILE_NAME);
            final FilesFacade ffBefore = AbstractCairoTest.ff;
            try {
                // Two adaptive tables, each with a durable epoch + a lazy gap (live _txn ahead of the epoch),
                // so recover() attempts a real restore on both.
                final long epochA = buildAdaptiveLazyGapTable("iso_a");
                final long epochB = buildAdaptiveLazyGapTable("iso_b");
                final TableToken ttA = engine.verifyTableName("iso_a");
                final TableToken ttB = engine.verifyTableName("iso_b");

                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // Fail the FIRST of our two tables in recover()'s actual (hash-based) iteration order, so the
                // other is GUARANTEED to be visited AFTER the failure — proving recover() continues past a
                // failed table, not merely that an earlier table was already recovered. recover() enumerates
                // via the same engine.getTableTokens(), so this ordering matches its own.
                final ObjHashSet<TableToken> order = new ObjHashSet<>();
                engine.getTableTokens(order, false);
                TableToken failTarget = null;
                for (int i = 0, n = order.size(); i < n; i++) {
                    final TableToken t = order.get(i);
                    if (t.equals(ttA) || t.equals(ttB)) {
                        failTarget = t;
                        break;
                    }
                }
                Assert.assertNotNull("expected one of our tables in the iteration order", failTarget);
                final TableToken sibling = failTarget.equals(ttA) ? ttB : ttA;

                // Arm the fault on the fail target's _txn restore and inject the facade for the recover() pass.
                failDirName.set(failTarget.getDirName());
                AbstractCairoTest.ff = failingFf;

                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("startup recovery must abort on a restore I/O error");
                } catch (CairoException expected) {
                    Assert.assertEquals(simErrno, expected.getErrno());
                }
                AbstractCairoTest.ff = ffBefore;

                Assert.assertFalse("failed-closed startup must not substitute sequencer suspension",
                        engine.getTableSequencerAPI().isSuspended(failTarget));
                Assert.assertEquals("a failed table must not bump recoveryIncarnation",
                        0L, engine.getTableSequencerAPI().getTxnTracker(failTarget).getRecoveryIncarnation());
                Assert.assertEquals("a later sibling must not be exposed as recovered after startup abort",
                        0L, engine.getTableSequencerAPI().getTxnTracker(sibling).getRecoveryIncarnation());
            } finally {
                AbstractCairoTest.ff = ffBefore;
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * SP-B / C2 (known limitation, documented in RecoveryCoordinator.recoverTable): the restore truncates
     * its destination before transferring, so a restore that fails mid-transfer leaves the LIVE file torn.
     * Because recover() now SUSPENDS such a table instead of aborting boot (so healthy siblings still
     * recover), a read of the torn table must fail LOUD — it must NEVER silently serve wrong data. The
     * _txn/_cv A/B checksums + mmap bounds guarantee a loud CairoException / SIGBUS-InternalError, not a
     * plausible-but-wrong result. This test proves the suspend + fail-loud contract. (A future temp-copy +
     * atomic-rename restore would remove even the loud window, but is blocked today by the path-keyed fd
     * cache — see the recoverTable NOTE.)
     */
    @Test
    public void testRestoreCvFailureAbortsStartupAndFailsLoud() throws Exception {
        // Same POSIX-only seam as testRecoverAbortsOnRestoreIoErrorBeforeServingSiblings, and the
        // torn-destination window under test (truncate-then-transfer) exists only in that branch of
        // TableUtils.replaceFileContent; Windows replaces the file whole via removeQuiet + copy.
        org.junit.Assume.assumeFalse("restore transfer fault seam is POSIX-only", Os.isWindows());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            final int simErrno = 28;
            final AtomicBoolean justFailed = new AtomicBoolean(false);
            final AtomicReference<String> failDirName = new AtomicReference<>();
            // The restore itself truncates the live _cv to 0 before the transfer this facade fails, so the
            // torn-destination state is produced by the product code rather than staged by the test.
            final FilesFacade failingFf = new RestoreTransferFaultFacade(
                    failDirName, justFailed, simErrno, TableUtils.COLUMN_VERSION_FILE_NAME);
            final FilesFacade ffBefore = AbstractCairoTest.ff;
            try {
                buildAdaptiveLazyGapTable("cvtorn");
                // Recovery restores only the live files that differ from the save point; an UPDATE after it
                // writes column versions, so the live _cv differs and its restore runs.
                execute("update cvtorn set v = v + 1 where v > 4");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("cvtorn");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                failDirName.set(tt.getDirName());
                AbstractCairoTest.ff = failingFf;
                try {
                    new RecoveryCoordinator(engine).recover();
                    Assert.fail("startup recovery must abort after a torn live _cv restore");
                } catch (CairoException expected) {
                    Assert.assertEquals(simErrno, expected.getErrno());
                }
                AbstractCairoTest.ff = ffBefore;

                Assert.assertFalse("failed-closed startup must not substitute sequencer suspension",
                        engine.getTableSequencerAPI().isSuspended(tt));

                // Fail-loud contract: a read off the torn _cv must throw, never silently return wrong data.
                boolean threwLoud = false;
                try {
                    printSql("select v from cvtorn");
                } catch (Throwable t) {
                    threwLoud = true;
                }
                assertTrue("a read off the torn _cv must fail loud, never silently serve wrong data", threwLoud);
            } finally {
                AbstractCairoTest.ff = ffBefore;
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            }
        });
    }

    /**
     * Fails the CONTENT TRANSFER of one named live file's restore, for one table dir.
     * <p>
     * The restore replaces a live file's content in place (open the destination read-write, truncate,
     * transfer its section from the save point file) rather than whole-file copying onto it, because a
     * whole-file copy is refused on Windows once the destination exists. The fault therefore has to be
     * injected on the transfer, and it is keyed by the destination FD learned from the read-write open —
     * {@code copyData} sees fds, not paths. Matching {@code endsWith} keeps the save point file out of it;
     * it is opened read-only anyway.
     */
    private static final class SimulatedCrash extends RuntimeException {
    }

    /**
     * Records, in order, every {@code fsync} and {@code syncfs} with the path its fd was opened by, and every write
     * open of a save point manifest.
     */
    private static final class BaselineBarrierFacade extends TestFilesFacadeImpl {
        private final Map<Long, String> fdPaths = new java.util.concurrent.ConcurrentHashMap<>();
        private final boolean isWide;
        private final List<String> ops = java.util.Collections.synchronizedList(new ArrayList<>());

        private BaselineBarrierFacade(boolean isWide) {
            this.isWide = isWide;
        }

        @Override
        public void fsync(long fd) {
            ops.add("fsync " + fdPaths.get(fd));
            super.fsync(fd);
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            return isWide;
        }

        @Override
        public long openRO(LPSZ name) {
            return track(name, super.openRO(name));
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                ops.add("manifest " + Utf8String.newInstance(name));
            }
            return track(name, super.openRW(name, opts));
        }

        @Override
        public void syncfs(long fd) {
            ops.add("syncfs " + fdPaths.get(fd));
            super.syncfs(fd);
        }

        // The first generation-zero manifest written since the reset must follow a barrier through its own table
        // directory and, file by file, a flush of the partition's files and of the sequencer the baseline names.
        void assertBaselineFlushedFirst(String partitionName) {
            synchronized (ops) {
                final String generationZero = io.questdb.std.Files.SEPARATOR + DurableEpochManifest.FILE_NAME + ".0";
                int manifest = -1;
                for (int i = 0, n = ops.size(); i < n && manifest < 0; i++) {
                    if (ops.get(i).startsWith("manifest ") && ops.get(i).endsWith(generationZero)) {
                        manifest = i;
                    }
                }
                Assert.assertTrue("no generation-zero baseline: " + ops, manifest > -1);
                final String tableDir = ops.get(manifest).substring("manifest ".length(), ops.get(manifest).length() - generationZero.length());
                boolean hasBarrier = false;
                boolean hasPartitionFlush = false;
                boolean hasSequencerFlush = false;
                for (int i = 0; i < manifest; i++) {
                    final String op = ops.get(i);
                    hasBarrier |= op.startsWith("syncfs " + tableDir + io.questdb.std.Files.SEPARATOR);
                    hasPartitionFlush |= op.startsWith("fsync " + tableDir + io.questdb.std.Files.SEPARATOR + partitionName);
                    hasSequencerFlush |= op.startsWith("fsync " + tableDir + io.questdb.std.Files.SEPARATOR + WalUtils.SEQ_DIR
                            + io.questdb.std.Files.SEPARATOR + WalUtils.TXNLOG_FILE_NAME);
                }
                Assert.assertTrue("no barrier through " + tableDir + " before its baseline: " + ops, hasBarrier);
                Assert.assertTrue("no flush of the partition before its baseline: " + ops, isWide || hasPartitionFlush);
                Assert.assertTrue("no flush of the sequencer log before its baseline: " + ops, isWide || hasSequencerFlush);
            }
        }

        void reset() {
            ops.clear();
        }

        private long track(LPSZ name, long fd) {
            if (fd != -1) {
                fdPaths.put(fd, Utf8String.newInstance(name).toString());
            }
            return fd;
        }
    }

    private static final class RestoreTransferFaultFacade extends TestFilesFacadeImpl {
        private final AtomicReference<String> failDirName;
        private final CharSequence fileName;
        private final AtomicBoolean justFailed;
        private final int simErrno;
        private final AtomicLong targetFd = new AtomicLong(-1);

        RestoreTransferFaultFacade(
                AtomicReference<String> failDirName,
                AtomicBoolean justFailed,
                int simErrno,
                CharSequence fileName
        ) {
            this.failDirName = failDirName;
            this.justFailed = justFailed;
            this.simErrno = simErrno;
            this.fileName = fileName;
        }

        @Override
        public boolean close(long fd) {
            targetFd.compareAndSet(fd, -1);
            return super.close(fd);
        }

        @Override
        public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
            if (destFd == targetFd.get()) {
                justFailed.set(true);
                return -1;
            }
            return super.copyData(srcFd, destFd, offsetSrc, length);
        }

        @Override
        public int errno() {
            return justFailed.compareAndSet(true, false) ? simErrno : super.errno();
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            final String dir = failDirName.get();
            // Match on the UTF-8 sequence: an LPSZ's toString is object identity, not the path.
            if (fd > -1 && dir != null && Utf8s.containsAscii(name, dir) && Utf8s.endsWithAscii(name, fileName)) {
                targetFd.set(fd);
            }
            return fd;
        }
    }

    // Fails the n-th mmap of one save point generation's file with a simulated errno, reported once.
    private static final class SavePointMmapFaultFacade extends TestFilesFacadeImpl {
        private final int failAtMmap;
        private final int generation;
        private final AtomicBoolean justFailed = new AtomicBoolean();
        private final LongHashSet manifestFds = new LongHashSet();
        private final int simErrno;
        int failures;
        private int mmaps;

        SavePointMmapFaultFacade(int generation, int failAtMmap, int simErrno) {
            this.generation = generation;
            this.failAtMmap = failAtMmap;
            this.simErrno = simErrno;
        }

        @Override
        public boolean close(long fd) {
            manifestFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public int errno() {
            return justFailed.compareAndSet(true, false) ? simErrno : super.errno();
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (manifestFds.contains(fd) && ++mmaps == failAtMmap) {
                failures++;
                justFailed.set(true);
                return -1;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            if (fd > -1 && Utf8s.endsWithAscii(name, io.questdb.cairo.DurableEpochManifest.FILE_NAME + "." + generation)) {
                manifestFds.add(fd);
            }
            return fd;
        }
    }

    /**
     * Fails the next flush of {@code failingFile} during the replay before a conversion to non-WAL, with a data-sync
     * failure or a plain one. The production callback halts the process inside the call that stops the engine, so
     * the files at that moment are what the next start sees.
     */
    private void assertReplayFlushFailureKeepsEveryRow(String failingFile, long replayIntervalMs, boolean dataSync) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicBoolean armed = new AtomicBoolean();
        final FilesFacade failingFf = new SyncAttributingFilesFacade() {
            @Override
            public void fdatasync(long fd) {
                failIfArmed(pathOfFd(fd), "fdatasync");
                super.fdatasync(fd);
            }

            @Override
            public void fsync(long fd) {
                failIfArmed(pathOfFd(fd), "fsync");
                super.fsync(fd);
            }

            // Where syncfs does not cover the whole filesystem, a save point flushes the sequencer log itself.
            @Override
            public boolean isSyncfsFileSystemWide() {
                return false;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                if (!async) {
                    failIfArmed(pathOfAddress(addr), "msync");
                }
                super.msync(addr, len, async);
            }

            private void failIfArmed(String path, String operation) {
                if (path.endsWith(failingFile) && armed.compareAndSet(true, false)) {
                    if (dataSync) {
                        throw CairoException.dataSyncFailure(this, 5, operation, "injected flush failure [path=" + path + ']');
                    }
                    throw CairoException.critical(5).put("injected plain failure [path=").put(path).put(']');
                }
            }
        };
        final String name = "conv_" + (dataSync ? "" : "plain_") + (replayIntervalMs < 0 ? "converter" : "job") + failingFile.replace('.', '_');
        try {
            assertMemoryLeak(failingFf, () -> {
                execute("create table " + name + " (ts timestamp, v long, s symbol index) timestamp(ts) partition by day wal");
                for (int i = 0; i < 7; i++) {
                    execute("insert into " + name + " values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ", 'a')");
                    if (i == 2) {
                        drainWalQueue();
                        try (TableWriter w = getWriter(name)) {
                            w.advanceDurableEpoch(1L);
                        }
                    }
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName(name);
                execute("ALTER TABLE " + name + " SET TYPE BYPASS WAL");
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, replayIntervalMs);

                final FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path marker = new Path()) {
                    // The replay marker of older builds: left on disk at the halt, it made the next start convert
                    // without a replay.
                    marker.of(engine.getConfiguration().getDbRoot()).concat(tt).concat("_convert.replay");
                    final AtomicInteger callbacks = new AtomicInteger();
                    final AtomicReference<Boolean> markerAtHalt = new AtomicReference<>();
                    engine.setDurabilityFailureHandler(failure -> {
                        if (callbacks.incrementAndGet() == 1) {
                            markerAtHalt.set(ff.exists(marker.$()));
                        }
                    });
                    engine.getTableSequencerAPI().resetForReboot(tt);
                    new RecoveryCoordinator(engine).recover();
                    armed.set(true);
                    boolean stopped = false;
                    try {
                        engine.load();
                    } catch (CairoError e) {
                        stopped = true;
                    }
                    Assert.assertFalse("test setup: the replay's flush must have failed", armed.get());
                    Assert.assertEquals("only a data-sync failure stops the startup", dataSync, stopped);
                    Assert.assertTrue("the table must stay a WAL table", engine.verifyTableName(name).isWal());
                    final String firstStart = "engine stopped: " + engine.isDurabilityFailed() + ", callbacks: " + callbacks.get();
                    // A halt runs no clean-up, so put back a marker that a clean-up after the callback removed.
                    if (Boolean.TRUE.equals(markerAtHalt.get()) && !ff.exists(marker.$())) {
                        Assert.assertTrue(ff.touch(marker.$()));
                    }
                    engine.resetDurabilityFailure();
                    engine.releaseInactive();

                    // The next start, with a healthy disk: a fresh engine runs recovery and the conversion. It
                    // takes the name registry, as a start does.
                    final long rows;
                    final boolean isWal;
                    engine.closeNameRegistry();
                    try (CairoEngine rebooted = new CairoEngine(configuration)) {
                        rebooted.load();
                        final TableToken after = rebooted.verifyTableName(name);
                        isWal = after.isWal();
                        try (TableReader reader = rebooted.getReader(after)) {
                            rows = reader.size();
                        }
                    } finally {
                        engine.reloadTableNames(null);
                    }
                    Assert.assertEquals(
                            (dataSync ? "engine stopped: true, callbacks: 1, replay marker on disk at the halt: false"
                                    : "engine stopped: false, callbacks: 0, replay marker on disk at the halt: null")
                                    + ", rows after the next start: 7 (wal=false)",
                            firstStart + ", replay marker on disk at the halt: " + markerAtHalt.get()
                                    + ", rows after the next start: " + rows + " (wal=" + isWal + ")"
                    );
                }
            });
        } finally {
            engine.resetDurabilityFailure();
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * Builds an adaptive table with a durable epoch taken at seqTxn=K (via {@code fsyncMaterializedState}
     * + an explicit {@code _snapshot} marker, exactly as {@link #testRecoverRestoresTxnToEpochCut()}),
     * then applies M more rows LAZILY so the live {@code _txn} sits ahead of the epoch. Returns the epoch
     * cut's seqTxn. The caller drives recovery.
     */
    private long buildAdaptiveLazyGapTable(String name) throws Exception {
        execute("create table " + name + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < 3; i++) {
            execute("insert into " + name + " values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();

        final TableToken tt = engine.verifyTableName(name);
        final long epochSeqTxn;
        try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
            w.advanceDurableEpoch(1L);
            epochSeqTxn = w.getSeqTxn();
        }

        for (int i = 3; i < 7; i++) {
            execute("insert into " + name + " values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();
        return epochSeqTxn;
    }

    private static List<String> changedFiles(Map<String, byte[]> before, Map<String, byte[]> after) {
        final List<String> changed = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : before.entrySet()) {
            if (!Arrays.equals(e.getValue(), after.get(e.getKey()))) {
                changed.add(e.getKey());
            }
        }
        for (String name : after.keySet()) {
            if (!before.containsKey(name)) {
                changed.add(name);
            }
        }
        return changed;
    }

    // Files whose bytes a power loss would not keep, by the crash model's durable content: a file may come back longer,
    // with the space a writer allocated and a close trimmed, but never without the bytes it holds now.
    private static List<String> bytesLostAtPowerLoss(
            CrashFaultFilesFacade crashFf,
            TableToken tt,
            Map<String, byte[]> files
    ) {
        final Map<String, byte[]> durable = new TreeMap<>();
        for (String name : files.keySet()) {
            // What crash() restores, not the durable prefix: a per-file fsync (macOS, Windows) makes a column durable
            // up to its last written byte and its size as a whole, so the restored file has a zero tail.
            final byte[] content = crashFf.powerLossImageOf(Paths.get(engine.getConfiguration().getDbRoot().toString(), tt.getDirName(), name).toString());
            if (content != null) {
                durable.put(name, content);
            }
        }
        return changedPrefixes(files, durable);
    }

    private static List<String> changedPrefixes(Map<String, byte[]> before, Map<String, byte[]> after) {
        final List<String> changed = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : before.entrySet()) {
            final byte[] kept = after.get(e.getKey());
            if (kept == null || kept.length < e.getValue().length || !Arrays.equals(e.getValue(), Arrays.copyOf(kept, e.getValue().length))) {
                changed.add(e.getKey());
            }
        }
        return changed;
    }

    // The file system calls one recovery pass makes, by facade method name.
    private Map<String, Integer> countRecoveryCalls() {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseAllWalWriters();
        final Map<String, Integer> calls = new TreeMap<>();
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        final FilesFacade delegate = engine.getConfiguration().getFilesFacade();
        AbstractCairoTest.ff = (FilesFacade) Proxy.newProxyInstance(
                FilesFacade.class.getClassLoader(),
                new Class<?>[]{FilesFacade.class},
                (proxy, method, args) -> {
                    calls.merge(method.getName(), 1, Integer::sum);
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
        );
        try {
            new RecoveryCoordinator(engine).recover();
        } finally {
            AbstractCairoTest.ff = ffBefore;
        }
        return calls;
    }

    private boolean epochArtifactExists(TableToken tt, CharSequence base, CharSequence suffix) {
        final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(base);
            if (suffix.length() > 0) {
                p.put(suffix);
            }
            return ff.exists(p.$());
        }
    }

    private void assertCleanRestartRestoresNothing(String tableName) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            assertMemoryLeak(() -> {
                // Siblings make the flush count show whether recovery flushes per table or per start.
                final int siblingCount = 2;
                for (int i = 0; i <= siblingCount; i++) {
                    final String name = i == 0 ? tableName : tableName + "_sibling" + i;
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO " + name + " VALUES ('2024-09-01T00:00:00.000000Z', 1), ('2024-09-02T00:00:00.000000Z', 2)");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName(tableName);
                // The graceful close folds the applied tail into a final epoch at the live cut.
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                final long seqTxn = readTxnSeqTxn(tt);
                final FilesFacade files = engine.getConfiguration().getFilesFacade();
                final int generation;
                try (Path p = new Path()) {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt);
                    final io.questdb.cairo.DurableEpochManifest.Candidate[] saved = io.questdb.cairo.DurableEpochManifest.loadCandidates(files, p, p.size(), tt.getTableId());
                    Assert.assertTrue(saved.length > 0);
                    Assert.assertEquals("precondition: the close epoch names the live cut", seqTxn, saved[0].epochSeqTxn);
                    generation = saved[0].generation;
                }
                engine.getTableSequencerAPI().resetForReboot(tt);
                for (int i = 1; i <= siblingCount; i++) {
                    engine.getTableSequencerAPI().resetForReboot(engine.verifyTableName(tableName + "_sibling" + i));
                }

                final AtomicInteger rewrites = new AtomicInteger();
                final AtomicInteger fsyncs = new AtomicInteger();
                final AtomicInteger syncfsCalls = new AtomicInteger();
                final FilesFacade ffBefore = AbstractCairoTest.ff;
                AbstractCairoTest.ff = new TestFilesFacadeImpl() {
                    @Override
                    public int copy(LPSZ from, LPSZ to) {
                        rewrites.incrementAndGet();
                        return super.copy(from, to);
                    }

                    @Override
                    public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
                        rewrites.incrementAndGet();
                        return super.copyData(srcFd, destFd, offsetSrc, length);
                    }

                    @Override
                    public void fsync(long fd) {
                        fsyncs.incrementAndGet();
                        super.fsync(fd);
                    }

                    @Override
                    public void fsyncAndClose(long fd) {
                        fsyncs.incrementAndGet();
                        super.fsyncAndClose(fd);
                    }

                    @Override
                    public void syncfs(long fd) {
                        syncfsCalls.incrementAndGet();
                        super.syncfs(fd);
                    }
                };
                final long eventsBefore = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events_total");
                try {
                    new RecoveryCoordinator(engine).recover();
                } finally {
                    AbstractCairoTest.ff = ffBefore;
                }

                Assert.assertEquals("live files already at the epoch cut must not be rewritten", 0, rewrites.get());
                // Where syncfs is filesystem-wide, one syncfs per start makes every selected header durable.
                final boolean isOneFlush = files.isSyncfsFileSystemWide();
                Assert.assertEquals("a clean restart fsyncs only the selected save point headers", isOneFlush ? 0 : siblingCount + 1, fsyncs.get());
                Assert.assertEquals("a clean restart flushes the selected save point headers with one syncfs", isOneFlush ? 1 : 0, syncfsCalls.get());
                final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.assertEquals("a clean restart is not a recovery", 0L, tracker.getRecoveryIncarnation());
                Assert.assertEquals("a clean restart is not a recovery",
                        eventsBefore, TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events_total"));
                Assert.assertEquals("the epoch must still bound WAL purge", seqTxn, tracker.getDurableEpochSeqTxn());
                Assert.assertTrue("the epoch must still be pinned", tracker.getPinnedEpochTxn() >= 0);
                Assert.assertEquals(seqTxn, readTxnSeqTxn(tt));
                try (Path p = new Path()) {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt);
                    final io.questdb.cairo.DurableEpochManifest.Candidate[] saved = io.questdb.cairo.DurableEpochManifest.loadCandidates(files, p, p.size(), tt.getTableId());
                    Assert.assertEquals("the live generation stays current", generation, saved[0].generation);
                    Assert.assertEquals(seqTxn, saved[0].epochSeqTxn);
                    Assert.assertEquals(saved[0].epochTxn, tracker.getPinnedEpochTxn());
                    Assert.assertEquals(saved.length > 1 ? saved[1].epochTxn : -1L, tracker.getOtherPinnedEpochTxn());
                    try (TxnScoreboard scoreboard = engine.getTxnScoreboard(tt)) {
                        for (io.questdb.cairo.DurableEpochManifest.Candidate cut : saved) {
                            Assert.assertFalse("every recovered candidate must be pinned before replay",
                                    scoreboard.isRangeAvailable(cut.epochTxn, cut.epochTxn + 1));
                        }
                    }
                }

                execute("INSERT INTO " + tableName + " VALUES ('2024-09-03T00:00:00.000000Z', 3)");
                drainWalQueue();
                assertQuery("SELECT * FROM " + tableName)
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tv
                                2024-09-01T00:00:00.000000Z\t1
                                2024-09-02T00:00:00.000000Z\t2
                                2024-09-03T00:00:00.000000Z\t3
                                """);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    private boolean hasSavePointFile(TableToken tt) {
        return epochArtifactExists(tt, io.questdb.cairo.DurableEpochManifest.FILE_NAME, ".0")
                || epochArtifactExists(tt, io.questdb.cairo.DurableEpochManifest.FILE_NAME, ".1");
    }

    private List<String> listSequencerFiles(TableToken tt) throws IOException {
        final java.nio.file.Path seqDir = Paths.get(configuration.getDbRoot().toString(), tt.getDirName(), WalUtils.SEQ_DIR);
        if (!Files.exists(seqDir)) {
            return new ArrayList<>();
        }
        try (Stream<java.nio.file.Path> files = Files.walk(seqDir)) {
            return files.filter(file -> !file.equals(seqDir)).map(file -> seqDir.relativize(file).toString()).sorted().toList();
        }
    }

    private long newestSavePointSeqTxn(TableToken tt) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt);
            final io.questdb.cairo.DurableEpochManifest.Candidate[] saved = io.questdb.cairo.DurableEpochManifest.loadCandidates(
                    engine.getConfiguration().getFilesFacade(), p, p.size(), tt.getTableId());
            return saved.length > 0 ? saved[0].epochSeqTxn : -1;
        }
    }

    private void assertMetaSwapFiles(TableToken tt, boolean hasMeta, boolean hasMetaPrev) {
        Assert.assertEquals("_meta exists", hasMeta, epochArtifactExists(tt, TableUtils.META_FILE_NAME, ""));
        Assert.assertEquals("_meta.prev exists", hasMetaPrev, epochArtifactExists(tt, TableUtils.META_PREV_FILE_NAME, ""));
    }

    /**
     * Kills an {@code ADD COLUMN} inside TableWriter's metadata swap, as a process death would: on the
     * {@code _meta.swp -> _meta} rename, either before it ({@code isAfterSwapRename == false}: no {@code _meta},
     * only {@code _meta.prev}) or right after it (the uncommitted new {@code _meta}), with {@code _todo_} armed
     * either way. No in-process rollback runs. Then drops the table's cached sequencer state, as a cold start
     * has none.
     */
    private void crashMetaSwap(TableToken tt, boolean isAfterSwapRename) throws Exception {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        AbstractCairoTest.ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isArmed.get() && Utf8s.containsAscii(from, TableUtils.META_SWAP_FILE_NAME)) {
                    if (isAfterSwapRename) {
                        super.rename(from, to);
                    }
                    throw new SimulatedCrash();
                }
                return super.rename(from, to);
            }

            @Override
            public int renameDurable(LPSZ from, LPSZ to) {
                if (isArmed.get() && Utf8s.containsAscii(from, TableUtils.META_SWAP_FILE_NAME)) {
                    if (isAfterSwapRename) {
                        super.renameDurable(from, to);
                    }
                    throw new SimulatedCrash();
                }
                return super.renameDurable(from, to);
            }
        };
        try (TableWriter writer = getWriter(tt)) {
            isArmed.set(true);
            try {
                writer.addColumn("c", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                Assert.fail("the simulated crash must interrupt the metadata swap");
            } catch (SimulatedCrash expected) {
                // the process dies here
            } finally {
                isArmed.set(false);
            }
        } finally {
            AbstractCairoTest.ff = ffBefore;
        }
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.getTableSequencerAPI().resetForReboot(tt);
    }

    private void assertBootLeavesTablesWithUnreadableMetaToFail(String createMode, String bootMode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, createMode);
        try {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE healthy (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE no_meta (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE empty_meta (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO healthy VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                execute("INSERT INTO no_meta VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                execute("INSERT INTO empty_meta VALUES ('2024-09-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final TableToken noMeta = engine.verifyTableName("no_meta");
                final TableToken emptyMeta = engine.verifyTableName("empty_meta");
                engine.releaseInactive();
                // Settle the storage-version migration first. It reads every _meta itself when it runs, which
                // happens only when the storage version changes, and is not what this test is about.
                try (CairoEngine ignored = new CairoEngine(configuration)) {
                    Assert.assertNotNull(ignored.getTableTokenIfExists("healthy"));
                }

                final FilesFacade ff = configuration.getFilesFacade();
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(noMeta).concat(TableUtils.META_FILE_NAME);
                    Assert.assertTrue("remove _meta", ff.removeQuiet(path.$()));
                    path.of(configuration.getDbRoot()).concat(emptyMeta).concat(TableUtils.META_FILE_NAME);
                    final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                    Assert.assertTrue("open _meta", fd > -1);
                    try {
                        Assert.assertTrue("truncate _meta", ff.truncate(fd, 0));
                    } finally {
                        ff.close(fd);
                    }
                }

                setProperty(PropertyKey.CAIRO_COMMIT_MODE, bootMode);
                try (CairoEngine rebooted = new CairoEngine(configuration)) {
                    Assert.assertNotNull("the table stays registered", rebooted.getTableTokenIfExists("no_meta"));
                    Assert.assertNotNull("the table stays registered", rebooted.getTableTokenIfExists("empty_meta"));
                    rebooted.execute("INSERT INTO healthy VALUES ('2024-09-01T01:00:00.000000Z', 2)");
                    TestUtils.drainWalQueue(rebooted);
                    final TableToken healthy = rebooted.verifyTableName("healthy");
                    Assert.assertFalse(rebooted.getTableSequencerAPI().isSuspended(healthy));
                    try (TableReader reader = rebooted.getReader(healthy)) {
                        Assert.assertEquals("the healthy table must keep working", 2L, reader.size());
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    private int readMetaColumnCount(TableToken tt) {
        try (TableReaderMetadata metadata = new TableReaderMetadata(engine.getConfiguration(), tt)) {
            metadata.loadMetadata();
            return metadata.getColumnCount();
        }
    }

    // Runs the two startup steps in the order ServerMain runs them: recovery rewinds the table to its epoch,
    // then load() performs the pending conversion.
    private void rebootAndConvert(TableToken tt) {
        engine.getTableSequencerAPI().resetForReboot(tt);
        new RecoveryCoordinator(engine).recover();
        Assert.assertEquals("precondition: recovery rewound the table to its epoch", 3L, readTxnSeqTxn(tt));
        engine.load();
    }

    // Flips the first byte of the _txn section of one save point file: the header stays valid, the section checksum fails.
    private void corruptSavePointSection(TableToken tt, int generation) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(io.questdb.cairo.DurableEpochManifest.FILE_NAME).put('.').put(generation);
            final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
            Assert.assertTrue("must open the save point file", fd > 0);
            final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(1, ff.read(fd, buf, 1, io.questdb.cairo.DurableEpochManifest.FILE_SIZE));
                Unsafe.getUnsafe().putByte(buf, (byte) ~Unsafe.getUnsafe().getByte(buf));
                Assert.assertEquals(1, ff.write(fd, buf, 1, io.questdb.cairo.DurableEpochManifest.FILE_SIZE));
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
        }
    }

    // Generation `generation` has a valid header naming this cut, and its sections pass their checksums.
    private void assertSavePointHolds(TableToken tt, int generation, long seqTxn, long txn) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt);
            final int rootLen = p.size();
            final io.questdb.cairo.DurableEpochManifest.Candidate[] saved = io.questdb.cairo.DurableEpochManifest.loadCandidates(engine.getConfiguration().getFilesFacade(), p, rootLen, tt.getTableId());
            io.questdb.cairo.DurableEpochManifest.Candidate found = null;
            for (io.questdb.cairo.DurableEpochManifest.Candidate candidate : saved) {
                if (candidate.generation == generation) {
                    found = candidate;
                }
            }
            Assert.assertNotNull("generation " + generation + " must have a valid header", found);
            Assert.assertEquals("seqTxn of generation " + generation, seqTxn, found.epochSeqTxn);
            Assert.assertEquals("txn of generation " + generation, txn, found.epochTxn);
            Assert.assertTrue("sections of generation " + generation + " must pass their checksums",
                    io.questdb.cairo.DurableEpochManifest.validateSections(engine.getConfiguration(), tt, p, rootLen, generation, seqTxn, txn));
        }
    }

    // Writes the section of one live file, as generation `generation` saved it, over the live file.
    private void restoreLiveFileFromSavePoint(TableToken tt, int generation, String name) throws IOException {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            final long[] section = TestUtils.savePointSection(ff, p, engine.getConfiguration().getDbRoot(), tt, generation, name);
            final byte[] manifest = Files.readAllBytes(Paths.get(p.toString()));
            Files.write(
                    Paths.get(engine.getConfiguration().getDbRoot().toString(), tt.getDirName(), name),
                    Arrays.copyOfRange(manifest, (int) section[0], (int) (section[0] + section[1]))
            );
        }
    }

    // The manual step of a refusal: moves one save point file out of the table directory.
    private void moveSavePointFileOut(TableToken tt, int generation) throws IOException {
        final String fileName = io.questdb.cairo.DurableEpochManifest.FILE_NAME + '.' + generation;
        Files.move(
                Paths.get(engine.getConfiguration().getDbRoot().toString(), tt.getDirName(), fileName),
                temp.newFolder().toPath().resolve(fileName)
        );
    }

    private void removeTableFile(TableToken tt, CharSequence name) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(name);
            Assert.assertTrue(engine.getConfiguration().getFilesFacade().removeQuiet(p.$()));
        }
    }

    private long rowCount(TableToken tt) {
        try (TableReader reader = engine.getReader(tt)) {
            return reader.size();
        }
    }

    private long readTxnSeqTxn(TableToken tt) {
        try (TxReader tx = new TxReader(engine.getConfiguration().getFilesFacade());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            tx.ofRO(p.$(), io.questdb.cairo.ColumnType.TIMESTAMP_MICRO, io.questdb.cairo.PartitionBy.DAY);
            tx.unsafeLoadAll();
            return tx.getSeqTxn();
        }
    }

    // Every file of the table by its path relative to the table directory; without the WAL and the sequencer when
    // materializedOnly is set.
    private Map<String, byte[]> readTableFiles(TableToken tt, boolean materializedOnly) {
        final java.nio.file.Path dir = Paths.get(engine.getConfiguration().getDbRoot().toString(), tt.getDirName());
        final Map<String, byte[]> files = new TreeMap<>();
        try (Stream<java.nio.file.Path> stream = Files.walk(dir)) {
            stream.filter(Files::isRegularFile).forEach(p -> {
                final String name = dir.relativize(p).toString();
                if (materializedOnly && (name.startsWith(WalUtils.WAL_NAME_BASE) || name.startsWith(WalUtils.SEQ_DIR))) {
                    return;
                }
                try {
                    files.put(name, Files.readAllBytes(p));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return files;
    }

    private void setCommitMode(String mode) {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
        Assert.assertEquals(mode, CommitMode.toString(engine.getConfiguration().getCommitMode()));
    }

    // Serves a stashed file in place of one live _meta while armed, so a save point taken meanwhile saves it.
    private static final class StaleMetaFacade extends TestFilesFacadeImpl {
        private volatile String livePath;
        private int served;
        private String stalePath;

        @Override
        public long length(LPSZ name) {
            if (isLive(name)) {
                try (Path stale = new Path()) {
                    return super.length(stale.of(stalePath).$());
                }
            }
            return super.length(name);
        }

        @Override
        public long openRO(LPSZ name) {
            if (isLive(name)) {
                served++;
                try (Path stale = new Path()) {
                    return super.openRO(stale.of(stalePath).$());
                }
            }
            return super.openRO(name);
        }

        void arm(String livePath, String stalePath) {
            this.stalePath = stalePath;
            served = 0;
            this.livePath = livePath;
        }

        void disarm() {
            livePath = null;
        }

        private boolean isLive(LPSZ name) {
            final String live = livePath;
            return live != null && Utf8s.equalsAscii(live, name);
        }
    }
}
