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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TxnCorruptionUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Upgrade and mixed-version compatibility for adaptive commit mode. Adaptive ({@link CommitMode#ADAPTIVE}) adds
 * new on-disk artifacts to a table directory. These tests prove in-process that each artifact is inert to a reader
 * that does not know about it, which is the forward-compatibility, rolling-upgrade and downgrade contract. A matrix
 * against old binaries cannot run in-process.
 * <p>
 * Coverage:
 * <ul>
 *   <li>downgrade: {@link #testDowngradeFromAdaptiveDrainsCleanlyAndArtifactsAreInert} and
 *       {@link #testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch};</li>
 *   <li>re-enabling adaptive after a downgrade: {@link #testReEnablingAdaptiveKeepsRowsWrittenUnderNosync};</li>
 *   <li>the {@code _txn} body checksum of an old file (zero sentinel):
 *       {@code TxnTest.testOpenOldFormatTxn_noBodyChecksum};</li>
 *   <li>the {@code _cv} body checksum of an old file (magic-gated):
 *       {@code ColumnVersionWriterTest.testCvChecksumAbsent*}.</li>
 * </ul>
 */
public class AdaptiveUpgradeCompatTest extends AbstractCairoTest {

    /**
     * A table directory as a released version leaves it: {@code _txn} areas without checksum stamps, {@code _meta}
     * at the released format minor version, no save point. Its files read as unverified, never torn: an ADAPTIVE
     * start enrols the table at its live cut with a valid save point, a NOSYNC start opens it as it is, and both
     * read every row and take writes.
     */
    @Test
    public void testReleasedTableDirectoryReadsUnverifiedInEveryMode() throws Exception {
        assertMemoryLeak(() -> {
            for (String mode : new String[]{"adaptive", "nosync"}) {
                node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                final String name = "released_" + mode;
                execute("create table " + name + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into " + name + " values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                execute("insert into " + name + " values ('2024-01-02T00:00:00.000000Z', 2)");
                drainWalQueue();
                final TableToken token = engine.verifyTableName(name);
                releaseHandles();
                TxnCorruptionUtils.makeAreasLookLegacy(engine, name);
                final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
                try (Path path = new Path()) {
                    final int tablePathLen = path.of(configuration.getDbRoot()).concat(token).size();
                    path.concat(TableUtils.META_FILE_NAME);
                    final int minorVersionField = peekInt(ff, path.$(), TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                    pokeInt(ff, path.$(), TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION,
                            Numbers.encodeLowHighShorts(Numbers.decodeLowShort(minorVersionField), TableUtils.META_FORMAT_MINOR_VERSION_TABLE_FORMAT));
                    path.trimTo(tablePathLen).concat(DurableEpochManifest.FILE_NAME).put(".0");
                    Assert.assertFalse("precondition: no save point", ff.exists(path.$()));
                    path.trimTo(tablePathLen).concat(DurableEpochManifest.FILE_NAME).put(".1");
                    Assert.assertFalse("precondition: no save point", ff.exists(path.$()));
                }

                node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
                engine.getTxnScoreboardPool().remove(token);
                engine.getTableSequencerAPI().resetForReboot(token);
                new RecoveryCoordinator(engine).recover();
                try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                    Assert.assertNotNull(writer);
                }
                if ("adaptive".equals(mode)) {
                    final DurableEpochManifest.Candidate savePoint = currentSavePoint(token);
                    Assert.assertNotNull("an adaptive start enrols the released table", savePoint);
                    Assert.assertEquals("at its live cut", readLiveSeqTxn(token), savePoint.epochSeqTxn);
                }
                execute("insert into " + name + " values ('2024-01-03T00:00:00.000000Z', 3)");
                drainWalQueue();
                Assert.assertFalse(mode + ": the table must not be suspended", engine.getTableSequencerAPI().isSuspended(token));
                assertQuery("select * from " + name)
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tv
                                2024-01-01T00:00:00.000000Z\t1
                                2024-01-02T00:00:00.000000Z\t2
                                2024-01-03T00:00:00.000000Z\t3
                                """);
                releaseHandles();
            }
        });
    }

    @Test
    public void testMarkerlessLegacyWalTableEnrollsWhenAdaptiveBecomesDefault() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table legacy_enroll (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into legacy_enroll values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("legacy_enroll");
            engine.releaseInactive();

            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                final int tablePathLen = path.of(configuration.getDbRoot()).concat(token).size();
                RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, path, tablePathLen);

                path.trimTo(tablePathLen).concat(TableUtils.META_FILE_NAME);
                final int current = peekInt(ff, path.$(), TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                final int downgraded = Numbers.encodeLowHighShorts(
                        Numbers.decodeLowShort(current),
                        TableUtils.META_FORMAT_MINOR_VERSION_TABLE_FORMAT
                );
                pokeInt(ff, path.$(), TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION, downgraded);

                path.trimTo(tablePathLen).concat(DurableEpochManifest.FILE_NAME + ".0");
                Assert.assertFalse("legacy precondition: no adaptive marker", ff.exists(path.$()));
            }

            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            new RecoveryCoordinator(engine).recover();

            try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                Assert.assertEquals(CommitMode.ADAPTIVE, writer.getEffectiveCommitMode());
            }
            engine.releaseInactive();

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat(DurableEpochManifest.FILE_NAME + ".0");
                Assert.assertTrue("opening the writer must publish the enrollment baseline", ff.exists(path.$()));

                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                final int minorVersion = peekInt(ff, path.$(), TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                Assert.assertEquals("enrollment must not rewrite metadata",
                        TableUtils.META_FORMAT_MINOR_VERSION_TABLE_FORMAT, Numbers.decodeHighShort(minorVersion));
            }

            new RecoveryCoordinator(engine).recover();
        });
    }

    @Test
    public void testRestoredTableEnrolsAtItsFirstWriter() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("create table checkpoint_enroll (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into checkpoint_enroll values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("checkpoint_enroll");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (Path path = new Path()) {
                final int tableRootLen = path.of(configuration.getDbRoot()).concat(token).size();
                RecoveryCoordinator.removeAdaptiveEpochArtifacts(configuration.getFilesFacade(), path, tableRootLen);
            }

            // A restore leaves no save point: the startup leaves the table as it is, its first writer enrols it, and
            // an ordinary startup after that validates and recovers the baseline.
            new RecoveryCoordinator(engine).recover();
            assertArtifactAbsent(token, DurableEpochManifest.FILE_NAME + ".0");
            try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                Assert.assertNotNull(writer);
            }
            assertArtifactExists(token, DurableEpochManifest.FILE_NAME + ".0");
            new RecoveryCoordinator(engine).recover();
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1L, reader.size());
            }
        });
    }

    /**
     * Downgrade: an adaptive table that has taken a durable epoch is restarted on an instance configured
     * {@code cairo.commit.mode=nosync}. The commit mode is instance-wide and fixed for the life of a
     * process, so a downgrade is exactly this: a restart under the new mode. The start makes the table
     * durable and removes its save point, and the table keeps applying WAL cleanly (no suspend, no corruption).
     */
    @Test
    public void testDowngradeFromAdaptiveDrainsCleanlyAndArtifactsAreInert() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0); // epoch every apply batch
        assertMemoryLeak(() -> {
            execute("create table d (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into d values ('2024-03-01T00:00:00.000000Z', 1)");
            execute("insert into d values ('2024-03-01T01:00:00.000000Z', 2)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("d");

            // Adaptive took a durable epoch: the save point holds copies of _txn and _cv.
            assertArtifactExists(tt, DurableEpochManifest.FILE_NAME + ".0");
            assertCurrentEpochArtifactExists(tt, TableUtils.TXN_FILE_NAME);
            assertCurrentEpochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME);

            // Downgrade the instance and reboot on it.
            releaseHandles();
            try (CairoEngine restarted = new CairoEngine(nosyncConfiguration())) {
                final TableToken rtt = restarted.verifyTableName("d");
                // Keep operating under nosync: applies cleanly, no suspend.
                restarted.execute("insert into d values ('2024-03-01T02:00:00.000000Z', 3)");
                restarted.execute("insert into d values ('2024-03-01T03:00:00.000000Z', 4)");
                TestUtils.drainWalQueue(restarted);
                Assert.assertFalse("downgraded table must not be suspended",
                        restarted.getTableSequencerAPI().isSuspended(rtt));
                try (TableReader r = restarted.getReader(rtt)) {
                    Assert.assertEquals("all rows must survive the downgrade", 4L, r.size());
                }
            }

            Assert.assertFalse("the downgrade start removes the save point", hasSavePoint(tt));
        });
    }

    /**
     * Downgrade + reboot: the hard case. After a downgrade (a restart under {@code nosync}) the WAL purge
     * floor drops from the durable epoch to the applied seqTxn ({@code WalPurgeJob} applies the epoch floor only
     * under ADAPTIVE), so WAL segments above the epoch become purgeable. A save point left
     * behind would rewind the table onto purged WAL at the next start. The downgrade start removes it, so
     * every row survives the purge and the next restart.
     */
    @Test
    public void testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        assertMemoryLeak(() -> {
            execute("create table dr (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into dr values ('2024-04-01T00:00:00.000000Z', 1)");
            execute("insert into dr values ('2024-04-01T01:00:00.000000Z', 2)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("dr");
            assertArtifactExists(tt, DurableEpochManifest.FILE_NAME + ".0");
            Assert.assertTrue("adaptive must have recorded a durable epoch", readMarkerEpochSeqTxn(tt) > 0);

            // Downgrade the instance and reboot on it: the start makes the table durable and removes its save
            // point. Then apply more rows so the live seqTxn advances past the old save point.
            releaseHandles();
            final CairoConfiguration nosyncConfiguration = nosyncConfiguration();
            final long epochSeqTxn = readMarkerEpochSeqTxn(tt);
            try (CairoEngine downgraded = new CairoEngine(nosyncConfiguration)) {
                final TableToken dtt = downgraded.verifyTableName("dr");
                downgraded.execute("insert into dr values ('2024-04-01T02:00:00.000000Z', 3)");
                downgraded.execute("insert into dr values ('2024-04-01T03:00:00.000000Z', 4)");
                downgraded.execute("insert into dr values ('2024-04-01T04:00:00.000000Z', 5)");
                TestUtils.drainWalQueue(downgraded);

                Assert.assertFalse("the downgrade start removes the save point", hasSavePoint(tt));
                final long liveSeqTxn = downgraded.getTableSequencerAPI().getTxnTracker(dtt).getSeqTxn();
                Assert.assertTrue("live seqTxn (" + liveSeqTxn + ") must be ahead of the old save point ("
                        + epochSeqTxn + ")", liveSeqTxn > epochSeqTxn);

                // Force a WAL purge under the nosync floor: segments below the applied txn become purgeable,
                // which would make a rollback to the stale epoch lossy.
                forceWalPurge(downgraded);
            }

            // Reboot again, still nosync: completeInit() -> RecoveryCoordinator.recover() runs as on a real
            // restart.
            try (CairoEngine restarted = new CairoEngine(nosyncConfiguration)) {
                TestUtils.drainWalQueue(restarted);
                final TableToken rtt = restarted.verifyTableName("dr");
                Assert.assertFalse("downgraded table must not be suspended after reboot",
                        restarted.getTableSequencerAPI().isSuspended(rtt));
                try (TableReader r = restarted.getReader(rtt)) {
                    Assert.assertEquals("all 5 rows must survive the downgrade+purge+reboot", 5L, r.size());
                }
            }
        });
    }

    /**
     * Re-enabling adaptive after a nosync period. The nosync start made the table durable and removed its save
     * point, and the WAL purge then dropped the WAL behind it. The restart that turns adaptive back on finds no
     * save point, so there is nothing to roll forward; the first writer to open it enrols it again at its live
     * cut, and every row survives.
     */
    @Test
    public void testReEnablingAdaptiveKeepsRowsWrittenUnderNosync() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        assertMemoryLeak(() -> {
            execute("create table re (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into re values ('2024-04-01T00:00:00.000000Z', 1)");
            execute("insert into re values ('2024-04-01T01:00:00.000000Z', 2)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("re");

            releaseHandles();
            try (CairoEngine downgraded = new CairoEngine(nosyncConfiguration())) {
                downgraded.execute("insert into re values ('2024-04-01T02:00:00.000000Z', 3)");
                downgraded.execute("insert into re values ('2024-04-01T03:00:00.000000Z', 4)");
                downgraded.execute("insert into re values ('2024-04-01T04:00:00.000000Z', 5)");
                TestUtils.drainWalQueue(downgraded);
                forceWalPurge(downgraded);
            }
            Assert.assertFalse("leaving adaptive must remove the save point", hasSavePoint(tt));

            try (CairoEngine reEnabled = new CairoEngine(configuration)) {
                final TableToken rtt = reEnabled.verifyTableName("re");
                reEnabled.execute("insert into re values ('2024-04-01T05:00:00.000000Z', 6)");
                TestUtils.drainWalQueue(reEnabled);
                Assert.assertFalse("re-enabled table must not be suspended",
                        reEnabled.getTableSequencerAPI().isSuspended(rtt));
                try (TableReader reader = reEnabled.getReader(rtt)) {
                    Assert.assertEquals("every row written under nosync must survive", 6L, reader.size());
                }
            }
            Assert.assertTrue("the first writer must enrol the table again", hasSavePoint(tt));

            // An ordinary adaptive restart now rolls forward from the fresh anchor.
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final TableToken rtt = restarted.verifyTableName("re");
                TestUtils.drainWalQueue(restarted);
                Assert.assertFalse("table must not be suspended after the next restart",
                        restarted.getTableSequencerAPI().isSuspended(rtt));
                try (TableReader reader = restarted.getReader(rtt)) {
                    Assert.assertEquals("every row must survive the next restart", 6L, reader.size());
                }
            }
        });
    }

    private boolean hasSavePoint(TableToken tt) {
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt);
            return DurableEpochManifest.exists(configuration.getFilesFacade(), p, p.size());
        }
    }

    private long readLiveSeqTxn(TableToken tt) {
        try (Path path = new Path(); TxReader txReader = new TxReader(configuration.getFilesFacade())) {
            path.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            Assert.assertTrue("_txn must load", txReader.unsafeLoadAll());
            return txReader.getSeqTxn();
        }
    }

    private DurableEpochManifest.Candidate currentSavePoint(TableToken tt) {
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt);
            final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(configuration.getFilesFacade(), p, p.size(), tt.getTableId());
            Assert.assertTrue("a save point must exist", saved.length > 0);
            return saved[0];
        }
    }

    private long readMarkerEpochSeqTxn(TableToken tt) {
        return currentSavePoint(tt).epochSeqTxn;
    }

    private void assertCurrentEpochArtifactExists(TableToken tt, CharSequence baseFileName) {
        try (Path markerPath = new Path()) {
            final int generation = currentSavePoint(tt).generation;
            assertArtifactExists(tt, DurableEpochManifest.FILE_NAME + "." + generation);
            final long[] section = TestUtils.savePointSection(configuration.getFilesFacade(), markerPath, configuration.getDbRoot(), tt, generation, baseFileName.toString());
            Assert.assertTrue("the save point must hold a copy of " + baseFileName, section[1] > 0);
        }
    }

    private void assertArtifactAbsent(TableToken tt, CharSequence fileName) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt).concat(fileName);
            Assert.assertFalse("artifact must not exist: " + p, ff.exists(p.$()));
        }
    }

    private void assertArtifactExists(TableToken tt, CharSequence fileName) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(tt).concat(fileName);
            Assert.assertTrue("artifact must exist: " + p, ff.exists(p.$()));
        }
    }

    /**
     * The same instance configuration with {@code cairo.commit.mode=nosync}. The commit mode is read once
     * at engine construction, so this is how a downgrade is modelled: a restart under the new mode.
     */
    private CairoConfiguration nosyncConfiguration() {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public int getCommitMode() {
                return CommitMode.NOSYNC;
            }
        };
    }

    /**
     * Release every pooled handle so a fresh CairoEngine can open the same db-root (reboot model).
     */
    private void releaseHandles() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseAllWalWriters();
        engine.releaseInactiveTableSequencers();
    }

    /**
     * Run the WAL purge to completion with a strictly-increasing clock (defeats the purge-interval cadence).
     */
    private void forceWalPurge(CairoEngine eng) {
        eng.releaseAllWalWriters();
        final long step = eng.getConfiguration().getWalPurgeInterval() * 1000L + 1_000_000L;
        final long[] tick = {1L};
        final MicrosecondClock incClock = () -> (tick[0] += step);
        try (WalPurgeJob job = new WalPurgeJob(eng, eng.getConfiguration().getFilesFacade(), incClock)) {
            job.run();
            job.run();
        }
    }

    private static int peekInt(FilesFacade ff, LPSZ path, long offset) {
        final long fd = ff.openRO(path);
        Assert.assertTrue("open for read failed", fd > -1);
        final long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Integer.BYTES, ff.read(fd, buf, Integer.BYTES, offset));
            return Unsafe.getInt(buf);
        } finally {
            Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private static void pokeInt(FilesFacade ff, LPSZ path, long offset, int value) {
        final long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue("open for write failed", fd > -1);
        final long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putInt(buf, value);
            Assert.assertEquals(Integer.BYTES, ff.write(fd, buf, Integer.BYTES, offset));
            ff.fsync(fd);
        } finally {
            Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }
}
