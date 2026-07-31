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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

/**
 * Unit-level oracle for {@link RecoveryCoordinator}: a durable epoch was taken at seqTxn=K, then more
 * rows were applied LAZILY (advancing {@code _txn} to K+M without a new epoch). Recovery must REWIND
 * {@code _txn}/{@code _cv} to the durable epoch cut by copying the immutable {@code _txn.epoch} /
 * {@code _cv.epoch} back over them, so the table opens at exactly {@code epoch.seqTxn}.
 */
public class RecoveryCoordinatorTest extends AbstractCairoTest {

    @Test
    public void testRecoveryDisableSwitchFailsClosedForAdaptiveTable() throws Exception {
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
    }

    @Test
    public void testRuntimeCheckpointBaselineHandsOverEpochPin() throws Exception {
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

            new RecoveryCoordinator(engine, true).recover();
            final long newEpochTxn = tracker.getPinnedEpochTxn();
            Assert.assertTrue(newEpochTxn > priorEpochTxn);
            try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
                Assert.assertTrue("the superseded checkpoint pin must be released",
                        scoreboard.isRangeAvailable(priorEpochTxn, priorEpochTxn + 1));
                Assert.assertFalse("the checkpoint-restored baseline must remain pinned",
                        scoreboard.isRangeAvailable(newEpochTxn, newEpochTxn + 1));
            }
        } finally {
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }

    @Test
    public void testRecoverRestoresTxnToEpochCut() throws Exception {
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
    }

    @Test
    public void testRecoveryRestoresMetadataMatchingEpochTxnAfterStructuralWal() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            execute("create table schema_epoch (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken token = engine.verifyTableName("schema_epoch");
            Assert.assertTrue("creation epoch must bind its metadata payload",
                    io.questdb.cairo.DurableEpochManifest.isMetadataBound(configuration, token, 0));
            try (Path markerPath = new Path(); SnapshotMarker marker = new SnapshotMarker(configuration)) {
                markerPath.of(configuration.getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(markerPath.$());
                Assert.assertEquals(SnapshotMarker.FORMAT_VERSION, marker.loadCandidates()[0].formatVersion);
            }
            execute("alter table schema_epoch add column extra long");
            drainWalQueue();

            try (Path epochMetaPath = new Path(); TableReaderMetadata epochMetadata = new TableReaderMetadata(configuration)) {
                epochMetaPath.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME)
                        .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(0);
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
                epochMetaPath.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME)
                        .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(0);
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
    }

    /**
     * Missing the creation baseline selector is an untrusted startup state and must fail closed.
     */
    @Test
    public void testRecoverWithNoSnapshotFailsClosed() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            execute("create table noepoch (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into noepoch values ('2024-09-01T00:00:00.000000Z', 42)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("noepoch");

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            try (Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                Assert.assertTrue("creation baseline marker must exist", engine.getConfiguration().getFilesFacade().exists(p.$()));
                engine.getConfiguration().getFilesFacade().remove(p.$());
                Assert.assertFalse("marker removal must succeed", engine.getConfiguration().getFilesFacade().exists(p.$()));
            }

            try {
                new RecoveryCoordinator(engine).recover();
                Assert.fail("recovery without a trustworthy baseline must fail");
            } catch (CairoException expected) {
                Assert.assertTrue(expected.getFlyweightMessage().toString().contains("marker is absent"));
            }
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testRecoverLegacyV1AnchorWhenAvailableTupleMatches() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            execute("create table legacy (ts timestamp, v long) timestamp(ts) partition by day wal");
            for (int i = 0; i < 3; i++) {
                execute("insert into legacy values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
            }
            drainWalQueue();
            final TableToken token = engine.verifyTableName("legacy");
            final long epochSeqTxn;
            final long epochTxn;
            try (io.questdb.cairo.TableWriter writer = getWriter(token)) {
                writer.fsyncMaterializedState();
                epochSeqTxn = writer.getSeqTxn();
                epochTxn = writer.getTxn();
            }
            try (SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration()); Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$()).write(epochSeqTxn, epochTxn, 1L);
            }
            execute("insert into legacy values ('2024-09-01T03:00:00.000000Z', 3)");
            drainWalQueue();
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            new RecoveryCoordinator(engine).recover();
            Assert.assertEquals(epochSeqTxn, readTxnSeqTxn(token));
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testCreationBaselineAndPreviousGenerationFallback() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            execute("create table baseline (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken token = engine.verifyTableName("baseline");
            try (Path p = new Path(); SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration())) {
                p.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                SnapshotMarker.Candidate[] candidates = marker.loadCandidates();
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
                p.of(engine.getConfiguration().getDbRoot()).concat(token)
                        .concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(1);
                final long fd = files.openRW(p.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(files.truncate(fd, 0));
                } finally {
                    files.close(fd);
                }
            }

            new RecoveryCoordinator(engine).recover();
            Assert.assertEquals("torn newest generation must fall back to creation baseline", 0, readTxnSeqTxn(token));
            try (Path p = new Path(); SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration())) {
                p.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                Assert.assertTrue(marker.tryLoad());
                Assert.assertTrue("fallback recovery must repair the selector to the restored generation",
                        marker.wasLoadedFromSelector());
                Assert.assertEquals(0L, marker.getEpochSeqTxn());
                Assert.assertEquals(0, marker.getGeneration());
            }
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * Regression: a regular VIEW token is {@code isView()==true} AND {@code isWal()==true} (see
     * {@code CreateViewOperationImpl}), so it slips past the loop's {@code !isWal()} filter. But a view has
     * no {@code _meta}/{@code _txn}/{@code _cv}/data/epoch, and its {@code ViewState} is NOT hydrated when
     * {@code recover()} runs (at {@code CairoEngine.completeInit}, right after the name registry is loaded
     * but BEFORE views are compiled). Before the fix, {@code resolveEffectiveCommitMode -> getTableMetadata
     * -> getViewMetadata} threw {@code view does not exist} on the view token and failed boot. {@code
     * recover()} must skip regular views (mat-views, {@code isView()==false}, are still recovered).
     */
    @Test
    public void testRecoverSkipsRegularViewWithUnhydratedState() throws Exception {
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

            // Reproduce the boot condition precisely. At completeInit, recover() runs against:
            //  (1) an enumerable view token (the name registry is loaded) whose ViewState is NOT yet
            //      hydrated — views compile lazily, after recover() — so getViewMetadata() returns null;
            engine.getViewStateStore().removeViewState(viewToken);
            Assert.assertNull("view state must be absent (pre-hydration boot condition)",
                    engine.getViewStateStore().getViewState(viewToken));

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            //  (2) a fresh in-memory SeqTxnTracker whose commit mode is UNSET (trackers reset on restart),
            //      so resolveEffectiveCommitMode() cannot early-return a cached mode and MUST read the
            //      table metadata — which, for a view, throws. Set UNSET last so nothing re-warms it.
            engine.getTableSequencerAPI().getTxnTracker(viewToken).setCommitMode(CommitMode.UNSET);
            Assert.assertEquals("precondition: tracker commit mode UNSET (fresh-boot state)",
                    CommitMode.UNSET, engine.getTableSequencerAPI().getTxnTracker(viewToken).getCommitMode());

            // ACT: before the fix this threw `view does not exist [view=v]` and failed boot.
            new RecoveryCoordinator(engine).recover();

            // ASSERT: the view was skipped, never treated as a recoverable adaptive table.
            final long incarnation = engine.getTableSequencerAPI().getTxnTracker(viewToken).getRecoveryIncarnation();
            Assert.assertEquals("a regular view must not be recovered", 0L, incarnation);
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
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
    }

    /**
     * Finding C2 (invariant pin) — locks the {@code TxReader.unsafeLoadAll} SLOT-SELECTION property that
     * {@code RecoveryCoordinator.epochIsAheadOfLiveTxn}'s C2 assert relies on: {@code unsafeLoadAll} returns
     * the VERSION-SELECTED (latest) A/B slot when it is intact, and ONLY its IMMEDIATE predecessor
     * (version - 1) when the latest is torn — never an older slot. So {@code unsafeReadVersion() -
     * getVersion()} is exactly 0 on a clean load and exactly 1 on the torn-latest fallback. Together with the
     * version word being durably floored at the epoch cut, that is WHY a single-lineage post-crash {@code
     * _txn} can never load cleanly BELOW the epoch, so the recovery guard's SKIP is only ever the genuine
     * multi-lineage / stale-epoch case (never a slot-selection artifact).
     * <p>
     * The recovery guard's clean-load branch (diff 0) is exercised end-to-end by
     * {@link #testRecoverRestoresTxnToEpochCut} (proceed) and {@link #testRecoverSkipsEpochAheadOfRestoredTxn}
     * (skip). This test pins the tolerated FALLBACK branch (diff 1) directly and deterministically, using the
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

    /**
     * Positional 8-byte write of a table's {@code _txn} — corrupts a committed record WITHOUT truncating.
     */
    private void pokeLongTxn(FilesFacade ff, TableToken tt, long offset, long value) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME).$();
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

    private void copyTableFile(TableToken tt, CharSequence from, CharSequence to) {
        final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path src = new Path(); Path dst = new Path()) {
            src.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(from);
            dst.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(to);
            Assert.assertTrue("copy " + from + " -> " + to + " must succeed", ff.copy(src.$(), dst.$()) >= 0);
        }
    }

    /**
     * The shared clear helper used by the ENT restore + demote paths must remove exactly the epoch trio
     * (_snapshot + _txn.epoch + _cv.epoch), leave the LIVE _txn/_cv untouched, and be idempotent.
     */
    @Test
    public void testRemoveAdaptiveEpochArtifactsRemovesTrioOnly() throws Exception {
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

            // Precondition: both bound generations and the live files exist.
            Assert.assertTrue("marker present", epochArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME, ""));
            for (int generation = 0; generation < 2; generation++) {
                final String suffix = TableUtils.EPOCH_COPY_SUFFIX + "." + generation;
                Assert.assertTrue("meta epoch generation present", epochArtifactExists(tt, TableUtils.META_FILE_NAME, suffix));
                Assert.assertTrue("txn epoch generation present", epochArtifactExists(tt, TableUtils.TXN_FILE_NAME, suffix));
                Assert.assertTrue("cv epoch generation present", epochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME, suffix));
                Assert.assertTrue("manifest generation present", epochArtifactExists(tt, io.questdb.cairo.DurableEpochManifest.FILE_NAME, "." + generation));
            }

            final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path p = new Path()) {
                final int rootLen = p.of(engine.getConfiguration().getDbRoot()).concat(tt).size();
                RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, p, rootLen);
            }

            // All anchors are gone; the live _txn/_cv are untouched.
            Assert.assertFalse("marker removed", epochArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME, ""));
            for (int generation = 0; generation < 2; generation++) {
                final String suffix = TableUtils.EPOCH_COPY_SUFFIX + "." + generation;
                Assert.assertFalse("meta epoch generation removed", epochArtifactExists(tt, TableUtils.META_FILE_NAME, suffix));
                Assert.assertFalse("txn epoch generation removed", epochArtifactExists(tt, TableUtils.TXN_FILE_NAME, suffix));
                Assert.assertFalse("cv epoch generation removed", epochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME, suffix));
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
    }

    @Test
    public void testRemoveAdaptiveEpochArtifactsFailsClosedWhenArtifactSurvives() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        try {
            execute("create table remove_fail (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken token = engine.verifyTableName("remove_fail");
            final FilesFacade refusingFf = new TestFilesFacadeImpl() {
                @Override
                public boolean removeQuiet(LPSZ name) {
                    if (Utf8s.containsAscii(name, TableUtils.SNAPSHOT_FILE_NAME)) {
                        return false;
                    }
                    return super.removeQuiet(name);
                }
            };
            try (Path p = new Path()) {
                final int rootLen = p.of(configuration.getDbRoot()).concat(token).size();
                try {
                    RecoveryCoordinator.removeAdaptiveEpochArtifacts(refusingFf, p, rootLen);
                    Assert.fail("surviving marker must abort stale-lineage cleanup");
                } catch (CairoException expected) {
                    TestUtils.assertContains(expected.getFlyweightMessage(),
                            "could not remove stale adaptive epoch artifact");
                }
            }
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
                    throw CairoException.dataSyncFailure(5, "fsyncAndClose")
                            .put("injected recovery directory sync failure");
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
            Assert.assertTrue(engine.isDurabilityPoisoned());
            Assert.assertEquals("fsyncAndClose", engine.getDurabilityFailure().getOperation());
        } finally {
            AbstractCairoTest.ff = ffBefore;
            resetDurabilityPoisonForTest();
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testRecoverAbortsOnRestoreIoErrorBeforeServingSiblings() throws Exception {
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
    }

    /**
     * Fails the CONTENT TRANSFER of one named live file's restore, for one table dir.
     * <p>
     * The restore replaces a live file's content in place ({@code TableUtils.replaceFileContent}: open the
     * destination read-write, truncate, transfer) rather than whole-file copying onto it, because a
     * whole-file copy is refused on Windows once the destination exists. The fault therefore has to be
     * injected on the transfer, and it is keyed by the destination FD learned from the read-write open —
     * {@code copyData} sees fds, not paths. Matching {@code endsWith} keeps the {@code .epoch.N} sources
     * out of it; those are opened read-only anyway.
     */
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

    private void resetDurabilityPoisonForTest() throws Exception {
        final java.lang.reflect.Field field = CairoEngine.class.getDeclaredField("durabilityFailure");
        field.setAccessible(true);
        ((AtomicReference<?>) field.get(engine)).set(null);
        engine.setDurabilityFailureHandler(failure -> {
        });
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

    private long readTxnSeqTxn(TableToken tt) {
        try (TxReader tx = new TxReader(engine.getConfiguration().getFilesFacade());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            tx.ofRO(p.$(), io.questdb.cairo.ColumnType.TIMESTAMP_MICRO, io.questdb.cairo.PartitionBy.DAY);
            tx.unsafeLoadAll();
            return tx.getSeqTxn();
        }
    }
}
