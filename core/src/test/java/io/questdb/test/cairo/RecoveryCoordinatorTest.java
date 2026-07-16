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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
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
    public void testRecoverRestoresTxnToEpochCut() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Epoch interval -1 => the auto-epoch never fires; we drive the durable cut explicitly so the
        // epoch is recorded at a KNOWN seqTxn (K), then apply more after it without a new epoch.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

            execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
            // K rows, fully applied.
            for (int i = 0; i < 3; i++) {
                execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
            }
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("r");

            // Drive the durable epoch explicitly at the current cut (seqTxn == K): fsyncMaterializedState()
            // writes the immutable _txn.epoch/_cv.epoch copies; then write the _snapshot marker exactly
            // as the apply-worker advance() hook does (this unit test isolates RecoveryCoordinator's
            // restore from the apply-worker cadence — the end-to-end auto-epoch path is covered by the
            // crash oracle).
            final long epochSeqTxn;
            final long epochTxn;
            try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                w.fsyncMaterializedState();
                epochSeqTxn = w.getSeqTxn();
                epochTxn = w.getTxn();
            }
            Assert.assertTrue("epoch must be at seqTxn >= 3", epochSeqTxn >= 3);
            try (io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
                 Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                marker.write(epochSeqTxn, epochTxn, 1L);
            }

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
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
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
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

            execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
            // K rows, fully applied.
            for (int i = 0; i < 3; i++) {
                execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
            }
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("r");

            // Drive the durable epoch explicitly at the current cut (seqTxn == K): fsyncMaterializedState()
            // writes the immutable _txn.epoch/_cv.epoch copies; then write the _snapshot marker exactly
            // as the apply-worker advance() hook does (this unit test isolates RecoveryCoordinator's
            // restore from the apply-worker cadence — the end-to-end auto-epoch path is covered by the
            // crash oracle).
            final long epochSeqTxn;
            final long epochTxn;
            try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                w.fsyncMaterializedState();
                epochSeqTxn = w.getSeqTxn();
                epochTxn = w.getTxn();
            }
            Assert.assertTrue("epoch must be at seqTxn >= 3", epochSeqTxn >= 3);
            try (io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
                 Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                marker.write(epochSeqTxn, epochTxn, 1L);
            }

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
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /**
     * Negative control: a table with NO {@code _snapshot} marker must NOT have its
     * {@code recoveryIncarnation} bumped (the no-op / absent-marker path skips restore).
     */
    @Test
    public void testRecoverWithNoSnapshotLeavesIncarnationZero() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            execute("create table noepoch (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into noepoch values ('2024-09-01T00:00:00.000000Z', 42)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("noepoch");

            // Precondition: no _snapshot marker was ever written for this table.
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // ACT: recovery finds no marker → no restore → incarnation must stay 0.
            new RecoveryCoordinator(engine).recover();

            final long incarnation = engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation();
            Assert.assertEquals("recoveryIncarnation must be 0 when no epoch was restored", 0L, incarnation);
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
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
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
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
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
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
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
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
            final long epochTxn;
            try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                w.fsyncMaterializedState();
                epochSeqTxn = w.getSeqTxn();
                epochTxn = w.getTxn();
            }
            Assert.assertTrue("epoch cut must be ahead of the earlier/backup cut", epochSeqTxn > restoredSeqTxn);
            try (io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
                 Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                marker.write(epochSeqTxn, epochTxn, 1L);
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // Simulate the restore: bring _txn/_cv back to the EARLIER cut, but (the bug) leave the LATER
            // _snapshot/_txn.epoch/_cv.epoch trio in place.
            copyTableFile(tt, "_txn.bak", TableUtils.TXN_FILE_NAME);
            copyTableFile(tt, "_cv.bak", TableUtils.COLUMN_VERSION_FILE_NAME);
            Assert.assertEquals("precondition: live _txn rewound to the earlier (restored) cut",
                    restoredSeqTxn, readTxnSeqTxn(tt));

            // ACT: recovery must skip the stale epoch that post-dates the restored _txn.
            new RecoveryCoordinator(engine).recover();

            // ASSERT: _txn stays at the restored cut (not rolled forward to the ahead-of-live epoch).
            Assert.assertEquals("recovery must skip an epoch ahead of the restored _txn",
                    restoredSeqTxn, readTxnSeqTxn(tt));
            Assert.assertEquals("a skipped (stale) epoch must not bump recoveryIncarnation",
                    0L, engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation());
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
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
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into r values ('2024-09-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("r");

            // Take an epoch so the _snapshot + _txn.epoch + _cv.epoch trio exists on disk.
            final long epochSeqTxn;
            final long epochTxn;
            try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                w.fsyncMaterializedState();
                epochSeqTxn = w.getSeqTxn();
                epochTxn = w.getTxn();
            }
            try (io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
                 Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                marker.write(epochSeqTxn, epochTxn, 1L);
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // Precondition: the trio + the live files all exist.
            Assert.assertTrue("marker present", epochArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME, ""));
            Assert.assertTrue("txn.epoch present", epochArtifactExists(tt, TableUtils.TXN_FILE_NAME, TableUtils.EPOCH_COPY_SUFFIX));
            Assert.assertTrue("cv.epoch present", epochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.EPOCH_COPY_SUFFIX));

            final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path p = new Path()) {
                final int rootLen = p.of(engine.getConfiguration().getDbRoot()).concat(tt).size();
                RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, p, rootLen);
            }

            // The trio is gone; the live _txn/_cv are untouched.
            Assert.assertFalse("marker removed", epochArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME, ""));
            Assert.assertFalse("txn.epoch removed", epochArtifactExists(tt, TableUtils.TXN_FILE_NAME, TableUtils.EPOCH_COPY_SUFFIX));
            Assert.assertFalse("cv.epoch removed", epochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.EPOCH_COPY_SUFFIX));
            Assert.assertTrue("live _txn untouched", epochArtifactExists(tt, TableUtils.TXN_FILE_NAME, ""));
            Assert.assertTrue("live _cv untouched", epochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME, ""));

            // Idempotent: a second call on the now-absent trio is a no-op (must not throw).
            try (Path p = new Path()) {
                final int rootLen = p.of(engine.getConfiguration().getDbRoot()).concat(tt).size();
                RecoveryCoordinator.removeAdaptiveEpochArtifacts(ff, p, rootLen);
            }
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
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
    public void testRecoverSuspendsTableOnRestoreIoErrorAndRecoversSiblings() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        // A path-targeted copy fault: fail ONLY the target table's live _txn restore (_txn.epoch -> _txn),
        // reporting ENOSPC (28 -> ErrorTag.DISK_FULL on linux). errno() returns the simulated code exactly
        // once, right after the failed copy, so no other errno read is poisoned.
        final int simErrno = 28;
        final AtomicReference<String> failDirName = new AtomicReference<>();
        final AtomicBoolean justFailed = new AtomicBoolean(false);
        final FilesFacade failingFf = new TestFilesFacadeImpl() {
            @Override
            public int copy(LPSZ from, LPSZ to) {
                final String dir = failDirName.get();
                // Match the target table's live _txn restore directly on the UTF-8 path (an LPSZ's
                // toString is object identity, so match the sequence, not a decoded String). The _cv
                // restore ("_cv") does not contain "_txn", and the restore dest is never the ".epoch" copy.
                if (dir != null
                        && Utf8s.containsAscii(to, dir)
                        && Utf8s.containsAscii(to, TableUtils.TXN_FILE_NAME)) {
                    justFailed.set(true);
                    return -1;
                }
                return super.copy(from, to);
            }

            @Override
            public int errno() {
                return justFailed.compareAndSet(true, false) ? simErrno : super.errno();
            }
        };
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        try {
            // Two adaptive tables, each with a durable epoch + a lazy gap (live _txn ahead of the epoch),
            // so recover() attempts a real restore on both.
            final long okEpochSeqTxn = buildAdaptiveLazyGapTable("t_ok");
            buildAdaptiveLazyGapTable("t_fail");
            final TableToken ttOk = engine.verifyTableName("t_ok");
            final TableToken ttFail = engine.verifyTableName("t_fail");

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // Arm the fault on t_fail's _txn restore and inject the facade for the recover() pass only.
            failDirName.set(ttFail.getDirName());
            AbstractCairoTest.ff = failingFf;

            // ACT: recover() must NOT throw (pre-fix, the failed copy's CairoException propagates here).
            new RecoveryCoordinator(engine).recover();
            AbstractCairoTest.ff = ffBefore;

            // The failing table is suspended with the errno-derived tag + a message, and was NOT rolled
            // forward (its incarnation stays 0).
            assertTrue("t_fail must be suspended after a restore I/O error",
                    engine.getTableSequencerAPI().isSuspended(ttFail));
            Assert.assertEquals("suspend tag must be derived from the restore errno",
                    ErrorTag.resolveTag(simErrno),
                    engine.getTableSequencerAPI().getTxnTracker(ttFail).getErrorTag());
            Assert.assertFalse("the suspend error message must be populated",
                    engine.getTableSequencerAPI().getTxnTracker(ttFail).getErrorMessage().isEmpty());
            Assert.assertEquals("a table that failed to roll forward must not bump recoveryIncarnation",
                    0L, engine.getTableSequencerAPI().getTxnTracker(ttFail).getRecoveryIncarnation());

            // The healthy sibling was recovered regardless of iteration order: not suspended, rewound to
            // exactly its durable epoch cut, incarnation bumped once.
            Assert.assertFalse("t_ok must not be suspended by a sibling's failure",
                    engine.getTableSequencerAPI().isSuspended(ttOk));
            Assert.assertEquals("t_ok's recoveryIncarnation must be bumped by its successful restore",
                    1L, engine.getTableSequencerAPI().getTxnTracker(ttOk).getRecoveryIncarnation());
            Assert.assertEquals("t_ok must be rewound to exactly its durable epoch cut",
                    okEpochSeqTxn, readTxnSeqTxn(ttOk));
        } finally {
            AbstractCairoTest.ff = ffBefore;
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
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
        final long epochTxn;
        try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
            w.fsyncMaterializedState();
            epochSeqTxn = w.getSeqTxn();
            epochTxn = w.getTxn();
        }
        try (io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(p.$());
            marker.write(epochSeqTxn, epochTxn, 1L);
        }

        for (int i = 3; i < 7; i++) {
            execute("insert into " + name + " values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();
        return epochSeqTxn;
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
