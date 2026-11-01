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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * End-to-end crash check of the adaptive save point roll-forward:
 * <ol>
 *   <li>ADAPTIVE table; ingest and apply K txns; a durable epoch fires at seqTxn=K (columns and
 *       {@code _txn} made durable, a save point written to {@code _epoch.manifest.0/1}).</li>
 *   <li>Ingest and apply M more txns lazily. The applied columns are not durable (column sync is skipped
 *       under ADAPTIVE), and neither are the {@code _txn}/{@code _cv} pointers that expose them (their
 *       commit sites are gated on {@code CommitMode.appliesColumnSync}). Only the WAL is fdatasync-durable.
 *       The test makes {@code _txn}/{@code _cv} durable explicitly, modelling a kernel writeback; see the
 *       comment at that call site.</li>
 *   <li>Record the rows a reader sees before the crash (all K+M).</li>
 *   <li>{@code crash()} drops the non-durable post-epoch column data and keeps the fsync/msync'd state
 *       (including the save point) and the durable WAL.</li>
 *   <li>Restart: {@link io.questdb.cairo.RecoveryCoordinator} restores the save point and the boot path
 *       re-applies {@code (K, frontier]} from the durable WAL, so all K+M rows are present and correct and
 *       the table is not suspended.</li>
 *   <li>Control: the same crash with recovery disabled loses the post-epoch rows or opens torn, which shows
 *       that recovery does real work and that the applied columns are lazy.</li>
 * </ol>
 *
 * <p>Runs under per-inode journaling ({@code modelSharedJournal=false}) so a journal commit on
 * {@code _txn} does not journal the column files as a side effect: the post-epoch columns are durable only
 * if something explicitly flushed them, which under lazy adaptive apply nothing does.
 */
public class AdaptiveRecoveryRollForwardCrashTest extends AbstractCrashConsistencyTest {

    private static final int K = 4; // txns before the epoch
    private static final int M = 5; // txns after the epoch (lazily applied)

    /**
     * Restart with recovery enabled: all K+M rows are rebuilt from the durable WAL.
     */
    @Test
    public void testRollForwardRebuildsPostEpochRowsAfterCrash() throws Exception {
        assertRollForward(true);
    }

    /**
     * Control: restart with recovery disabled. The post-epoch columns are not durable, so the crash drops
     * them; without recovery rewinding the cut and re-applying the WAL, the reopened table is missing the
     * post-epoch rows (fewer than K+M) or opens torn or suspended. This shows that recovery does real work
     * and that the columns are lazy.
     */
    @Test
    public void testNegativeControlWithoutRecoveryLosesPostEpochRows() throws Exception {
        assertRollForward(false);
    }

    private void assertRollForward(boolean recoveryEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Epoch on the first applied batch (interval 0); the test disables it before the post-epoch
        // batch so those M rows are applied with no further epoch.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");

                // K rows -> apply -> the apply worker fires a durable epoch (interval 0) at seqTxn=K.
                for (int i = 0; i < K; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");

                // Disable the epoch so the next M rows are applied lazily (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
                for (int i = K; i < K + M; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Before the crash a reader sees all K+M rows.
                final List<Long> pre = readVs(engine);
                Assert.assertEquals("pre-crash must see all N+M rows", K + M, pre.size());

                // Model the kernel writing back the dirty mmap pages of _txn and _cv before the crash, with no
                // msync from QuestDB. This builds the state recovery exists to repair: the commit pointer
                // survives at the post-epoch frontier while the column data it exposes does not.
                //
                // TxWriter.commit and ColumnVersionWriter.commit do not msync under ADAPTIVE (gated on
                // appliesColumnSync), so without this a crash rolls the pointer back with the data and plain
                // WAL replay repairs the table. The kernel may write back a mapped page at any moment, though,
                // so the skew is reachable in production and RecoveryCoordinator is what guarantees the
                // rewind; forcing it here keeps the control testing recovery.
                final String tableDir = engine.getConfiguration().getDbRoot() + java.io.File.separator + tt.getDirName();
                crashFf.markFileDurable(tableDir + java.io.File.separator + TableUtils.TXN_FILE_NAME);
                crashFf.markFileDurable(tableDir + java.io.File.separator + TableUtils.COLUMN_VERSION_FILE_NAME);

                // Crash: drop non-durable column data; keep the fsync/msync'd state and the durable WAL.
                // crashAndReopen() releases all readers and writers, so the next open re-reads from disk.
                crashAndReopen();

                // Simulate a restart on the same engine (a second CairoEngine on the same db root spins the
                // registry reload), in the order a real boot uses:
                //   1) RecoveryCoordinator.recover(), the startup hook (CairoEngine.completeInit). The
                //      recovery path runs it (rewinds _txn/_cv to the epoch cut). The control omits it,
                //      modelling the kill switch or "recovery did not run".
                //   2) notifyWalTxnRepublisher(tt) uninitialises the in-memory SeqTxnTracker and triggers
                //      a rescan, so the boot WAL apply (CheckWalTransactionsJob) re-initialises writerTxn
                //      from the on-disk _txn and re-checks the table, as a fresh engine does (the live
                //      tracker otherwise caches the pre-crash frontier and nothing would re-apply).
                if (recoveryEnabled) {
                    new io.questdb.cairo.RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(tt);

                // Boot WAL apply. With recovery the tracker re-initialises at the rewound epoch seqTxn=K,
                // sees the sequencer frontier K+M and re-derives (K, K+M] from the durable WAL. In the control
                // it re-initialises at the surviving frontier (_txn is not rewound), so nothing is re-applied
                // and the read hits the columns the crash truncated to the epoch (torn).
                drainWalQueue();

                final boolean suspended = engine.getTableSequencerAPI().isSuspended(tt);
                final List<Long> post = readVsAllowTorn(engine);

                if (recoveryEnabled) {
                    Assert.assertFalse("table must NOT be suspended after recovery", suspended);
                    Assert.assertEquals("recovery must rebuild ALL N+M rows from the WAL", K + M, post.size());
                    for (int i = 0; i < K + M; i++) {
                        Assert.assertEquals("row " + i + " value", Long.valueOf(i), post.get(i));
                    }
                } else {
                    // Control: without recovery, the post-epoch column data the crash dropped is never
                    // re-applied, so the full identity result {0,1,..,K+M-1} must not come back. Acceptable
                    // torn outcomes: the K epoch'd rows correct but the M post-epoch rows missing or wrong
                    // (for example read back as zeros), a loud read error, fewer rows, or a suspended table.
                    final boolean fullCorrect = !suspended
                            && post.size() == (K + M)
                            && post.indexOf(TORN_SENTINEL) < 0
                            && isIdentityPrefix(post);
                    Assert.assertFalse(
                            "NEGATIVE CONTROL must reproduce post-epoch row loss, but the full correct result "
                                    + "came back without recovery (rows=" + post + ", suspended=" + suspended
                                    + ") -> recovery did no real work / columns not lazy",
                            fullCorrect
                    );
                    // The K epoch'd rows survive (durable cut), so the loss is in the M post-epoch rows: at
                    // least one of indices [K, K+M) must be missing or wrong. This check is skipped on a loud
                    // torn read, where no rows are readable at all.
                    final boolean tornRead = post.size() == 1 && post.get(0) == TORN_SENTINEL;
                    if (!suspended && !tornRead) {
                        boolean postEpochLost = post.size() < (K + M);
                        for (int i = K; !postEpochLost && i < Math.min(post.size(), K + M); i++) {
                            if (post.get(i) == null || post.get(i) != (long) i) {
                                postEpochLost = true;
                            }
                        }
                        Assert.assertTrue(
                                "NEGATIVE CONTROL: the M post-epoch rows must be lost/wrong without recovery "
                                        + "(rows=" + post + ")",
                                postEpochLost
                        );
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    // Returned as the single element when a read throws a loud Cairo or JVM error; a torn read is an
    // acceptable control outcome. Distinct from any real row value (0..K+M-1).
    private static final long TORN_SENTINEL = Long.MIN_VALUE;

    /**
     * Strict read: all rows in order, throwing on any error (for the recovery-enabled assertions).
     */
    private List<Long> readVs(CairoEngine eng) {
        final List<Long> out = new ArrayList<>();
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(eng);
                RecordCursorFactory f = eng.select("select v from t order by ts", ctx)
        ) {
            try (RecordCursor c = f.getCursor(ctx)) {
                io.questdb.cairo.sql.Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * Tolerant read for the control: a torn post-crash table may throw a loud Cairo or JVM error (for
     * example SIGBUS reading a column truncated below {@code _txn}'s row count). That is an acceptable
     * outcome, returned as a single {@link #TORN_SENTINEL}.
     */
    private List<Long> readVsAllowTorn(CairoEngine eng) {
        try {
            return readVs(eng);
        } catch (RuntimeException | io.questdb.cairo.CairoError | InternalError e) {
            // RuntimeException covers CairoException + the readVs SqlException wrapper; CairoError and
            // InternalError (SIGBUS on a truncated mmap) are the other loud torn-read signals.
            final List<Long> torn = new ArrayList<>();
            torn.add(TORN_SENTINEL);
            return torn;
        }
    }

    /**
     * True iff {@code rows} is exactly the identity sequence 0,1,2,... (the uncorrupted full result).
     */
    private boolean isIdentityPrefix(List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i) == null || rows.get(i) != (long) i) {
                return false;
            }
        }
        return true;
    }
}
