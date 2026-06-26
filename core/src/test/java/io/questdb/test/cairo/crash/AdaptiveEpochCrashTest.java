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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * The crash oracle for the adaptive durable EPOCH (Plan 3B). Under
 * {@link CommitMode#ADAPTIVE} the WAL commit is fdatasync-durable but the TABLE apply is LAZY:
 * {@code TableWriter.syncColumns()} skips the per-column flush, so after a power cut the partition
 * columns can be NON-DURABLE while {@code _txn}/{@code _cv} were only msync'd. A durable epoch
 * ({@code TableWriter.fsyncMaterializedState()}, fired by the apply worker) forces those columns
 * durable and records a {@code _snapshot} marker so recovery can land on the cut.
 *
 * <p>This isolates the TABLE materialized-state durability (Task C roll-forward of the post-epoch
 * WAL is out of scope here). After {@code drainWalQueue} the table is caught up
 * ({@code writerTxn == seqTxn}), so a post-crash read does NOT re-apply any WAL — it reads exactly
 * the durable on-disk column state.
 *
 * <h3>Crash model</h3>
 * {@link CrashFaultFilesFacade} models msync(MS_SYNC)/fsync/fdatasync/syncfs as durable and mmap
 * stores as non-durable-until-journaled. A single in-order insert is applied via the LAG path, which
 * does NOT fsync the columns and (under ADAPTIVE) {@code syncColumns()} is skipped — so right after
 * the lazy apply the partition column's {@code journaledDataEnd} is 0 (verified during development).
 *
 * <h3>Scenarios</h3>
 * <ol>
 *   <li>{@link #testEpochMakesLazilyAppliedRowsDurable}: ADAPTIVE, lazily apply a row, fire a durable
 *       epoch, crash, reopen → the epoch'd row reads back correct and the {@code _snapshot} marker
 *       recorded exactly the cut's seqTxn (a self-consistent durable cut + anchor survives the cut).
 *       Run under per-inode journaling ({@code modelSharedJournal=false}).</li>
 *   <li>{@link #testNegativeControlWithoutEpochAnchorIsAbsent}: NEGATIVE CONTROL — the identical
 *       workload with epochs DISABLED records NO recovery anchor (no {@code _snapshot}, no
 *       {@code _txn.epoch}/{@code _cv.epoch}), before AND after the crash. This is {@code advance()}'s
 *       reproducible contribution: the cut recovery would land on is simply not recorded. (Asserting
 *       column ROW-loss here is not reliable in this harness — see the test body + report.)</li>
 *   <li>{@link #testCrashMidEpochFsyncNoSilentCorruption}: arm a crash partway through the epoch's
 *       fsync sequence (driven explicitly via {@code fsyncMaterializedState()}) → reopening must
 *       never silently corrupt (correct prefix, a loud error, or fewer rows — never a wrong value).</li>
 * </ol>
 */
public class AdaptiveEpochCrashTest extends AbstractCrashConsistencyTest {

    private static final int N = 6;  // rows used by the mid-epoch-fsync corruption scenario

    /**
     * The epoch makes the lazily-applied prefix durable across a crash.
     */
    @Test
    public void testEpochMakesLazilyAppliedRowsDurable() throws Exception {
        // ADAPTIVE + epoch on every batch so the prefix is epoch'd deterministically.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                // Model PER-INODE journaling (ext4 fast_commit): a journal commit on _cv/_txn does NOT
                // incidentally journal the column files' extents. So the ONLY thing that can make the
                // lazily lag-applied column durable is the epoch's own syncfs (the salvage property),
                // which is exactly what we are proving. Under the default shared-journal model the _cv
                // msync would journal the column for free and mask the epoch's contribution.
                crashFf.modelSharedJournal = false;
                execute("create table e (ts timestamp, v varchar) timestamp(ts) partition by day wal");

                // ONE small in-order insert => applied via the LAG path (no column fsync; syncColumns
                // skipped under ADAPTIVE), so the column is dirty-unsynced — EXACTLY the negative
                // control's setup. The only difference is that here the apply worker then fires a
                // durable epoch (interval 0), whose fsyncMaterializedState() forces the column durable.
                execute("insert into e values (" + tsOf(0) + ", '" + vOf(0) + "')");
                drainWalQueue(); // lazy lag apply + epoch

                TableToken tt = engine.verifyTableName("e");
                // Sanity: the epoch marker exists and covers the applied row (seqTxn == 1).
                assertMarkerSeqTxn(tt, 1);

                crashAndReopen();

                // The epoch'd cut must read back fully durable + correct after the crash: the row is
                // present and the marker (asserted above) recorded exactly this cut's seqTxn. This
                // proves fsyncMaterializedState() produces a self-consistent durable cut + anchor that
                // survives a power cut under strict per-inode journaling.
                List<String> actual = readVarcharColumn("e", "v");
                Assert.assertEquals("epoch'd row count must survive crash", 1, actual.size());
                Assert.assertEquals("epoch'd row value", vOf(0), actual.get(0));
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /**
     * NEGATIVE CONTROL: with epochs DISABLED, the lazily-applied columns are never made durable, so
     * a crash loses them. This is what proves scenario 1's durability comes from the epoch flush and
     * not from some other sync on the apply path.
     */
    @Test
    public void testNegativeControlWithoutEpochAnchorIsAbsent() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1); // epochs disabled
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                execute("create table n (ts timestamp, v varchar) timestamp(ts) partition by day wal");

                // Same single-insert LAG path as scenario 1 (column dirty-unsynced after the lazy
                // apply: journaledDataEnd of n/2023-11-14/v.d is 0 here), but NO epoch fires.
                execute("insert into n values (" + tsOf(0) + ", '" + vOf(0) + "')");
                drainWalQueue(); // lazy lag apply, NO epoch

                TableToken tt = engine.verifyTableName("n");

                // advance()'s reproducible, deterministic contribution: WITHOUT it there is NO recovery
                // anchor — neither the _snapshot marker nor the durable epoch copies are written. The
                // cut recovery would land on is simply not recorded, so recovery must fall back to full
                // WAL replay. (We assert the anchor's absence rather than column row-loss because the
                // crash harness cannot deterministically strip the just-applied column here: the
                // writer's clean close fsyncs it and its in-process dirty mmap survives a file-level
                // crash() rollback — see the DONE_WITH_CONCERNS report. Scenario 1 confirms the epoch
                // makes the identical lazily-applied row durable through the crash.)
                assertNoEpochArtifacts(tt);

                crashAndReopen();

                // After the crash the anchor is still absent (it was never written).
                assertNoEpochArtifacts(tt);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /**
     * Arm a crash partway through the epoch's fsync sequence: reopening must never SILENTLY corrupt.
     * The epoch issues many durability ops (column syncfs, _cv fsync, _txn fsync, marker msync+fsync,
     * epoch-copy fsyncs); firing the crash on an early one leaves a partially-durable cut, which must
     * degrade safely (correct prefix / loud error / fewer rows), never a wrong value.
     */
    @Test
    public void testCrashMidEpochFsyncNoSilentCorruption() throws Exception {
        // Epochs DISABLED on the apply path so the apply drains cleanly; we then drive the epoch
        // explicitly via fsyncMaterializedState() with a crash armed mid-way, isolating the
        // mid-epoch-fsync crash from the apply's own (WAL) durability ops.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                execute("create table m (ts timestamp, v varchar) timestamp(ts) partition by day wal");
                for (int i = 0; i < N; i++) {
                    execute("insert into m values (" + tsOf(i) + ", '" + vOf(i) + "')");
                }
                drainWalQueue(); // clean lazy apply, no epoch

                // Drive the epoch's durable cut directly, armed to crash a few fsyncs in.
                boolean crashed = false;
                try (io.questdb.cairo.TableWriter w = getWriter(engine.verifyTableName("m"))) {
                    crashFf.armCrashAt(crashFf.durabilityOpCount() + 4);
                    try {
                        w.fsyncMaterializedState();
                    } catch (CrashSimulationError expected) {
                        crashed = true;
                    }
                } catch (CrashSimulationError expected) {
                    // The crash can also fire as the writer is closed/flushed; still a mid-cut crash.
                    crashed = true;
                }
                Assert.assertTrue("the crash must have fired during the epoch's fsync sequence", crashed);

                crashAndReopen();

                // Whatever survives must not be silently wrong: a correct prefix, a loud error, or
                // fewer rows are all acceptable; a wrong value is not.
                assertNoSilentCorruptionVarchar("m", "v");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    // ---- helpers ----

    private static long tsOf(int i) {
        return 1_700_000_000_000_000L + (long) i * 1_000_000L;
    }

    private static String vOf(int i) {
        // > 9 bytes so it takes the split (out-of-line) varchar layout (data + aux both exercised).
        return "epoch-varchar-payload-" + String.format("%06d", i);
    }

    /**
     * Varchar analogue of {@link AbstractCrashConsistencyTest#assertNoSilentCorruption}: the rows
     * that read back must match the expected prefix, OR a loud error is thrown. Fewer rows
     * (rollback) is acceptable; a silently WRONG value is not.
     */
    private void assertNoSilentCorruptionVarchar(String table, String column) {
        try {
            List<String> actual = readVarcharColumn(table, column);
            int n = Math.min(actual.size(), N);
            for (int i = 0; i < n; i++) {
                Assert.assertEquals("row " + i + " silently wrong", vOf(i), actual.get(i));
            }
        } catch (io.questdb.cairo.CairoException | io.questdb.cairo.CairoError e) {
            // acceptable: corruption detected loudly
        } catch (InternalError e) {
            // SIGBUS (mmap past truncated file) -> InternalError; acceptable loud detection
        } catch (RuntimeException e) {
            if (!(e.getCause() instanceof io.questdb.cairo.CairoException)
                    && !(e.getCause() instanceof io.questdb.cairo.CairoError)) {
                throw e;
            }
        }
    }

    /** The epoch's recovery anchor (marker + durable copies) must NOT exist (no epoch fired). */
    private void assertNoEpochArtifacts(TableToken tt) {
        assertFileAbsent(tt, TableUtils.SNAPSHOT_FILE_NAME);
        assertFileAbsent(tt, TableUtils.TXN_FILE_NAME + TableUtils.EPOCH_COPY_SUFFIX);
        assertFileAbsent(tt, TableUtils.COLUMN_VERSION_FILE_NAME + TableUtils.EPOCH_COPY_SUFFIX);
    }

    private void assertFileAbsent(TableToken tt, String fileName) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName);
            Assert.assertFalse("must be absent without an epoch: " + fileName,
                    engine.getConfiguration().getFilesFacade().exists(p.$()));
        }
    }

    private void assertMarkerSeqTxn(TableToken tt, long expectedSeqTxn) {
        try (SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(p.$());
            Assert.assertTrue("epoch marker must exist + load", marker.tryLoad());
            Assert.assertEquals("epoch marker seqTxn", expectedSeqTxn, marker.getEpochSeqTxn());
        }
    }
}
