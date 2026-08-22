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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * SP-E — Upgrade &amp; mixed-version compatibility for adaptive commit mode.
 * <p>
 * Adaptive ({@link CommitMode#ADAPTIVE}) adds NEW on-disk artifacts to a table dir. These tests prove
 * each new artifact is <b>inert</b> to a reader that does not know about it — the in-process proof of the
 * forward-compat / rolling-upgrade / downgrade contract. See
 * {@code docs/superpowers/specs/2026-07-17-adaptive-sp-e-upgrade-compat-design.md} for the full artifact
 * inventory, the gate behind each, and the external {old-binary} matrix that cannot run in-process.
 * <p>
 * Coverage split (only the GAPS are added here; the rest is cited, not duplicated):
 * <ul>
 *   <li><b>meta {@code commit_mode} field (v2&rarr;v3)</b> &mdash;
 *       {@link #testMetaCommitModeFieldIsInertOnPreV3Meta} (NEW; the gap).</li>
 *   <li><b>{@code _snapshot} marker + {@code .epoch} copies</b> &mdash;
 *       {@link #testStraySnapshotAndEpochArtifactsAreInertOnNormalOpen} (NEW; the gap).</li>
 *   <li><b>downgrade</b> &mdash; {@link #testDowngradeFromAdaptiveDrainsCleanlyAndArtifactsAreInert} +
 *       {@link #testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch} (NEW).</li>
 *   <li><b>{@code _event} CRC trailer</b> &mdash; magic-gated; cited:
 *       {@code WalEventChecksumTest.testLegacyRecordWithoutTrailerStillReads}.</li>
 *   <li><b>{@code _txn} body checksum</b> &mdash; zero-sentinel; cited:
 *       {@code TxnTest.testOpenOldFormatTxn_noBodyChecksum}.</li>
 *   <li><b>{@code _cv} body checksum</b> &mdash; magic-gated; cited:
 *       {@code ColumnVersionWriterTest.testCvChecksumAbsent*}.</li>
 * </ul>
 * These are read-compat assertions only; no behavior is weakened.
 */
public class AdaptiveUpgradeCompatTest extends AbstractCairoTest {

    // ------------------------------------------------------------------------------------------------
    // Artifact 1: the per-table commit_mode field in _meta (meta minor version v2 -> v3).
    // ------------------------------------------------------------------------------------------------

    /**
     * A {@code _meta} written BEFORE the {@code commit_mode} field existed (meta minor version &lt; 3) must
     * be read as {@link CommitMode#UNSET} &rarr; the field is ignored &rarr; the table defers to the global
     * {@code cairo.commit.mode}. This is the gate an OLDER binary relies on (it never reads the field at
     * all) and that a NEW binary applies when it opens a pre-v3 table.
     * <p>
     * Method: create a table {@code WITH commit_mode='adaptive'} (a real v3 {@code _meta} whose field
     * holds {@code ADAPTIVE}). Then flip ONLY the meta minor-version high short 3&rarr;2 on disk, keeping
     * the low-short checksum valid, i.e. turn it into a valid pre-commit-mode (v2 / table-format-era)
     * {@code _meta}. {@link TableUtils#getCommitMode} short-circuits on
     * {@code isMetaFormatAtLeast(mem, 3) == false} and returns {@code UNSET} without reading the field.
     * <p>
     * <b>Non-vacuity A/B on the identical file:</b> before the flip (v3) the field reads {@code ADAPTIVE};
     * after the flip (v2) it reads {@code UNSET}. Only the one version-gate byte changed, so the version
     * gate is provably what makes the field inert. If the field were NOT gated, the v2 read would still
     * return {@code ADAPTIVE} and this test would fail.
     */
    @Test
    public void testMetaCommitModeFieldIsInertOnPreV3Meta() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table m (ts timestamp, v long) timestamp(ts) partition by day wal " +
                    "with commit_mode='adaptive'");
            final TableToken tt = engine.verifyTableName("m");
            // Drop the pooled writer so _meta is unmapped and we can poke it and re-read it from disk.
            engine.releaseInactive();

            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.META_FILE_NAME);
                final LPSZ metaPath = path.$();

                // (control / non-vacuity) v3 meta: the field IS read -> ADAPTIVE.
                try (TableReaderMetadata md = new TableReaderMetadata(configuration, tt)) {
                    md.loadMetadata();
                    Assert.assertEquals("v3 meta must read the stored commit_mode field",
                            CommitMode.ADAPTIVE, md.getCommitMode());
                    Assert.assertEquals("effective(ADAPTIVE, nosync) resolves to the per-table override",
                            CommitMode.ADAPTIVE,
                            CommitMode.effectiveCommitMode(md.getCommitMode(), CommitMode.NOSYNC));
                }

                // Flip meta minor version 3 -> 2 (keep the checksum low short) => a valid pre-v3 meta.
                final int cur = peekInt(ff, metaPath, TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                // The invariant this precondition needs is that the field is LIVE in the file we are about
                // to downgrade, not that the file sits at any particular version: META_FORMAT_MINOR_VERSION
                // _LATEST legitimately moves whenever a tail field is added (it went 3 -> 4 with the enrolled
                // commit mode). Pinning the exact value would fail on every such addition while proving
                // nothing about the gate under test.
                Assert.assertTrue("precondition: fresh table must be written at or above the commit-mode meta"
                                + " version, or the field being downgraded away is not live to begin with",
                        Numbers.decodeHighShort(cur) >= TableUtils.META_FORMAT_MINOR_VERSION_COMMIT_MODE);
                final int downgraded = Numbers.encodeLowHighShorts(
                        Numbers.decodeLowShort(cur),                         // keep the checksum valid
                        TableUtils.META_FORMAT_MINOR_VERSION_TABLE_FORMAT);  // (short) 2 -> pre-commit-mode
                pokeInt(ff, metaPath, TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION, downgraded);

                // (the gate) v2 meta: the field is IGNORED -> UNSET -> defers to the global mode.
                try (TableReaderMetadata md = new TableReaderMetadata(configuration, tt)) {
                    md.loadMetadata();
                    Assert.assertEquals("pre-v3 meta must read commit_mode as UNSET (field gated away)",
                            CommitMode.UNSET, md.getCommitMode());
                    Assert.assertEquals("UNSET must resolve to the global commit mode (nosync)",
                            CommitMode.NOSYNC,
                            CommitMode.effectiveCommitMode(md.getCommitMode(), CommitMode.NOSYNC));
                }
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

                path.trimTo(tablePathLen).concat(TableUtils.SNAPSHOT_FILE_NAME);
                Assert.assertFalse("legacy precondition: no adaptive marker", ff.exists(path.$()));
            }

            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            engine.getTableSequencerAPI().getTxnTracker(token).setCommitMode(CommitMode.UNSET);
            new RecoveryCoordinator(engine).recover();

            try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                Assert.assertEquals("enrolled legacy table must keep inheriting the server default",
                        CommitMode.UNSET, writer.getMetadata().getCommitMode());
                Assert.assertEquals(CommitMode.ADAPTIVE, writer.getEffectiveCommitMode());
            }
            engine.releaseInactive();

            try (Path path = new Path(); TableReaderMetadata metadata = new TableReaderMetadata(configuration, token)) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME);
                Assert.assertTrue("opening the writer must publish the enrollment baseline", ff.exists(path.$()));
                metadata.loadMetadata();
                Assert.assertEquals(CommitMode.UNSET, metadata.getCommitMode());

                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                final int upgraded = peekInt(ff, path.$(), TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
                Assert.assertTrue("enrollment must upgrade metadata to the commit-mode-aware format",
                        Numbers.decodeHighShort(upgraded) >= TableUtils.META_FORMAT_MINOR_VERSION_COMMIT_MODE);
            }

            new RecoveryCoordinator(engine).recover();
        });
    }

    @Test
    public void testCheckpointRestorePublishesBaselineForNextOrdinaryStartup() throws Exception {
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

            // The checkpoint-aware startup must do more than grant a one-process exemption: it publishes a
            // bound baseline that an immediately following ordinary startup can validate and recover.
            new RecoveryCoordinator(engine, true).recover();
            assertArtifactExists(token, TableUtils.SNAPSHOT_FILE_NAME);
            new RecoveryCoordinator(engine, false).recover();
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1L, reader.size());
            }
        });
    }

    // ------------------------------------------------------------------------------------------------
    // Artifacts 2 & 3: the _snapshot marker and the _txn.epoch / _cv.epoch copies.
    // ------------------------------------------------------------------------------------------------

    /**
     * The {@code _snapshot} marker and the {@code _txn.epoch}/{@code _cv.epoch} copies are SEPARATE files
     * that the normal read path never opens (it opens {@code _meta}/{@code _txn}/{@code _cv}/columns by
     * name). Fabricate all three next to a plain (non-adaptive) table and prove a normal query, a fresh
     * {@link TableReader}, and a full engine reboot all read the correct data and never choke — the
     * artifacts remain untouched on disk (inert / ignored).
     * <p>
     * The reboot is the "normal engine open" case: {@code RecoveryCoordinator.recover()} iterates every
     * WAL table but skips roll-forward for a non-adaptive table (effective mode != ADAPTIVE), so the
     * fabricated marker is never even loaded.
     */
    @Test
    public void testStraySnapshotAndEpochArtifactsAreInertOnNormalOpen() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table s (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into s values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into s values ('2024-01-01T01:00:00.000000Z', 2)");
            execute("insert into s values ('2024-01-01T02:00:00.000000Z', 3)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("s");

            // Fabricate the adaptive epoch trio alongside the live files: a loadable _snapshot marker plus
            // _txn.epoch / _cv.epoch copies of the live pointers. A table an adaptive binary wrote and then
            // handed to an unaware reader looks exactly like this.
            fabricateEpochArtifacts(tt);
            assertArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME);
            assertArtifactExists(tt, TableUtils.TXN_FILE_NAME + TableUtils.EPOCH_COPY_SUFFIX);
            assertArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME + TableUtils.EPOCH_COPY_SUFFIX);

            // Normal read path ignores them: query is correct, no choke.
            assertQuery("select count() from s")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
            // A fresh reader opens cleanly with the artifacts present.
            try (TableReader r = engine.getReader(tt)) {
                Assert.assertEquals(3L, r.size());
            }
            // Artifacts untouched by the normal read path => truly inert.
            assertArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME);
            assertArtifactExists(tt, TableUtils.TXN_FILE_NAME + TableUtils.EPOCH_COPY_SUFFIX);

            // Normal ENGINE open (reboot) with the artifacts present: recover() skips the non-adaptive
            // table, so the marker is inert; the table comes up clean with all rows.
            releaseHandles();
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                TestUtils.drainWalQueue(restarted);
                final TableToken rtt = restarted.verifyTableName("s");
                Assert.assertFalse("table must not be suspended after reboot with stray epoch artifacts",
                        restarted.getTableSequencerAPI().isSuspended(rtt));
                try (TableReader r = restarted.getReader(rtt)) {
                    Assert.assertEquals("all rows must survive a reboot with stray artifacts", 3L, r.size());
                }
            }
            // Still present after the reboot => recover() left the non-adaptive table's marker alone.
            assertArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME);
        });
    }

    // ------------------------------------------------------------------------------------------------
    // Downgrade: turning adaptive off must drain cleanly and leave the epoch artifacts inert.
    // ------------------------------------------------------------------------------------------------

    /**
     * Live downgrade: an adaptive table that has taken a durable epoch is switched to nosync via
     * {@code ALTER TABLE ... SET PARAM commit_mode='nosync'}. It keeps applying WAL cleanly (no suspend,
     * no corruption), {@code wal_tables()} reports the new mode, and the leftover {@code _snapshot}/
     * {@code .epoch} artifacts remain on disk, inert (the live read/apply path never opens them).
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

            // Adaptive took a durable epoch: the trio is on disk.
            assertArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME);
            assertCurrentEpochArtifactExists(tt, TableUtils.TXN_FILE_NAME);
            assertCurrentEpochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME);

            // Downgrade this table to nosync.
            execute("alter table d set param commit_mode='nosync'");
            drainWalQueue();
            assertQuery("select name, commitMode from wal_tables() where name = 'd'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("name\tcommitMode\nd\tnosync\n");

            // Keep operating under nosync: applies cleanly, no suspend.
            execute("insert into d values ('2024-03-01T02:00:00.000000Z', 3)");
            execute("insert into d values ('2024-03-01T03:00:00.000000Z', 4)");
            drainWalQueue();
            Assert.assertFalse("downgraded table must not be suspended",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("select count() from d")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n4\n");

            // Leftover artifacts remain, inert (nothing on the live path opens them).
            assertArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME);
            assertCurrentEpochArtifactExists(tt, TableUtils.TXN_FILE_NAME);
        });
    }

    /**
     * Downgrade + reboot: the hard case. After a downgrade the WAL purge floor drops from the durable
     * epoch to the applied seqTxn (the epoch floor applies ONLY under ADAPTIVE — {@code WalPurgeJob}), so
     * WAL segments above the frozen epoch become purgeable. If {@code recover()} still rolled the live
     * {@code _txn}/{@code _cv} back to the stale epoch, the subsequent replay of {@code (epoch, live]}
     * could hit purged WAL &rarr; data loss / suspend. It must NOT: {@code RecoveryCoordinator} skips
     * roll-forward for a non-adaptive table (effective mode != ADAPTIVE), leaving the live {@code _txn}
     * untouched.
     * <p>
     * <b>Non-vacuity:</b> after the reboot the stale {@code _snapshot} marker still exists and its
     * {@code epochSeqTxn} is strictly below the live seqTxn (a stale epoch behind live), and we forced a
     * WAL purge under the nosync floor first — so a roll-forward would have been observably lossy. All
     * rows nonetheless survive and the table is not suspended, which is only possible if recovery ignored
     * the stale epoch.
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
            assertArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME);
            Assert.assertTrue("adaptive must have recorded a durable epoch", readMarkerEpochSeqTxn(tt) > 0);

            // Downgrade to nosync, then apply MORE rows so the live seqTxn advances well past the frozen
            // epoch while NO new epoch is taken (nosync).
            execute("alter table dr set param commit_mode='nosync'");
            drainWalQueue();
            execute("insert into dr values ('2024-04-01T02:00:00.000000Z', 3)");
            execute("insert into dr values ('2024-04-01T03:00:00.000000Z', 4)");
            execute("insert into dr values ('2024-04-01T04:00:00.000000Z', 5)");
            drainWalQueue();

            // The epoch is now FROZEN (nosync takes no new epochs); capture its on-disk value.
            final long frozenEpoch = readMarkerEpochSeqTxn(tt);
            final long liveSeqTxn = engine.getTableSequencerAPI().getTxnTracker(tt).getSeqTxn();
            Assert.assertTrue("live seqTxn (" + liveSeqTxn + ") must be ahead of the frozen epoch ("
                    + frozenEpoch + ")", liveSeqTxn > frozenEpoch);

            // Force a WAL purge under the (now nosync) floor: segments below the applied txn become
            // purgeable — exactly the condition that would make a rollback to the stale epoch lossy.
            forceWalPurge(engine);

            // Reboot on a fresh engine: completeInit() -> RecoveryCoordinator.recover() runs as on a real
            // restart. The stale marker is still on disk and behind live; recovery must ignore it.
            releaseHandles();
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                TestUtils.drainWalQueue(restarted);
                final TableToken rtt = restarted.verifyTableName("dr");
                Assert.assertFalse("downgraded table must not be suspended after reboot",
                        restarted.getTableSequencerAPI().isSuspended(rtt));
                try (TableReader r = restarted.getReader(rtt)) {
                    Assert.assertEquals("all 5 rows must survive the downgrade+purge+reboot", 5L, r.size());
                }
            }

            // Non-vacuity: the stale epoch marker is STILL present, unchanged, and STILL behind the live
            // seqTxn — a roll-forward WOULD have rewound to it (and, with WAL purged, been lossy). Data
            // survived => recovery skipped it.
            assertArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME);
            Assert.assertEquals("the stale epoch marker must be unchanged (inert), not advanced/purged",
                    frozenEpoch, readMarkerEpochSeqTxn(tt));
            Assert.assertTrue("stale epoch must remain behind the live seqTxn",
                    frozenEpoch < liveSeqTxn);
        });
    }

    // ------------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------------

    /**
     * Write a loadable {@code _snapshot} marker and copy the live {@code _txn}/{@code _cv} to {@code .epoch}.
     */
    private void fabricateEpochArtifacts(TableToken tt) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path src = new Path(); Path dst = new Path()) {
            // _txn -> _txn.epoch
            src.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            dst.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX);
            Assert.assertTrue("copy _txn.epoch", ff.copy(src.$(), dst.$()) >= 0);
            // _cv -> _cv.epoch
            src.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            dst.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.COLUMN_VERSION_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX);
            Assert.assertTrue("copy _cv.epoch", ff.copy(src.$(), dst.$()) >= 0);
            // _snapshot marker (a loadable one — the stronger "even a loadable marker is ignored" claim).
            final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(tt).getSeqTxn();
            dst.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                marker.of(dst);
                marker.write(Math.max(1, seqTxn), Math.max(1, seqTxn), 1_000_000L);
            }
        }
    }

    private long readMarkerEpochSeqTxn(TableToken tt) {
        try (Path p = new Path(); SnapshotMarker marker = new SnapshotMarker(configuration)) {
            p.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(p);
            Assert.assertTrue("marker must load", marker.tryLoad());
            return marker.getEpochSeqTxn();
        }
    }

    private void assertCurrentEpochArtifactExists(TableToken tt, CharSequence baseFileName) {
        try (Path markerPath = new Path(); SnapshotMarker marker = new SnapshotMarker(configuration)) {
            markerPath.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(markerPath);
            Assert.assertTrue("marker must load", marker.tryLoad());
            final int generation = marker.getGeneration();
            final String fileName = baseFileName + TableUtils.EPOCH_COPY_SUFFIX
                    + (generation == SnapshotMarker.LEGACY_GENERATION ? "" : "." + generation);
            assertArtifactExists(tt, fileName);
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
