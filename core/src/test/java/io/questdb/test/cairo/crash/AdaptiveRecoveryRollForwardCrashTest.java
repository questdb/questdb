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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The HEADLINE end-to-end crash oracle for the adaptive durable-epoch ROLL-FORWARD (Plan 3 Task C).
 *
 * <p>This is the keystone proof that ADAPTIVE is crash-safe end to end:
 * <ol>
 *   <li>ADAPTIVE table; ingest+apply N txns; fire a durable epoch at seqTxn=K (columns + {@code _txn}
 *       forced durable, {@code _snapshot} + {@code _txn.epoch}/{@code _cv.epoch} recorded).</li>
 *   <li>Ingest+apply M MORE txns LAZILY. After Plan 3 Task C Part 1 the O3-applied columns are
 *       GENUINELY non-durable ({@code O3CopyJob}/PMAR-release column sync skipped under ADAPTIVE);
 *       only {@code _txn} is msync'd and the WAL is fdatasync-durable.</li>
 *   <li>Record the rows a reader sees pre-crash (all N+M).</li>
 *   <li>{@code crash()} — drops the non-durable post-epoch column data; keeps the fsync/msync'd state
 *       (incl. the rewound-able epoch cut) and the durable WAL.</li>
 *   <li>RESTART the engine -> {@link io.questdb.cairo.RecoveryCoordinator} restores the epoch cut, the
 *       boot path re-applies {@code (K, frontier]} from the durable WAL -> ALL N+M rows present and
 *       correct, table NOT suspended.</li>
 *   <li>NEGATIVE CONTROL: the identical crash with recovery DISABLED loses the post-epoch rows (or the
 *       table opens torn) — proving recovery does real work and that Part 1 made the columns truly
 *       lazy (the row loss the Plan-3B test could not reproduce).</li>
 * </ol>
 *
 * <p>Runs under per-inode journaling ({@code modelSharedJournal=false}) so a journal commit on
 * {@code _txn} does NOT incidentally journal the column files: the post-epoch columns are durable
 * ONLY if something explicitly flushed them, which under truly-lazy adaptive nothing does.
 */
public class AdaptiveRecoveryRollForwardCrashTest extends AbstractCrashConsistencyTest {

    private static final int K = 4; // txns before the epoch
    private static final int M = 5; // txns after the epoch (lazily applied)

    /**
     * GREEN path: restart with recovery ENABLED -> all N+M rows rebuilt from the durable WAL.
     */
    @Test
    public void testRollForwardRebuildsPostEpochRowsAfterCrash() throws Exception {
        assertRollForward(true);
    }

    /**
     * NEGATIVE CONTROL (RED without the fix): restart with recovery DISABLED. The post-epoch columns
     * were never made durable (Part 1), so the crash drops them; without recovery rewinding the cut
     * and re-applying the WAL, the reopened table is MISSING the post-epoch rows (fewer than N+M) or
     * opens torn/suspended. This proves recovery does real work AND that the columns are truly lazy.
     */
    @Test
    public void testNegativeControlWithoutRecoveryLosesPostEpochRows() throws Exception {
        assertRollForward(false);
    }

    private void assertRollForward(boolean recoveryEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Epoch on the FIRST applied batch (interval 0), then we disable it before the post-epoch
        // batch so those M rows are applied with NO further epoch.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
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

                // Now DISABLE the epoch so the next M rows are applied LAZILY (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
                for (int i = K; i < K + M; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Pre-crash: a reader sees ALL N+M rows.
                final List<Long> pre = readVs(engine);
                Assert.assertEquals("pre-crash must see all N+M rows", K + M, pre.size());

                // CRASH: drop non-durable column data; keep fsync/msync'd state + the durable WAL.
                // crashAndReopen() releases all readers + writers (so the next open re-reads from disk).
                crashAndReopen();

                // SIMULATE RESTART on the same engine (a second CairoEngine on the same db root spins the
                // registry reload), in the SAME ORDER a real boot does it:
                //   1) RecoveryCoordinator.recover() — the startup hook (CairoEngine.completeInit). The
                //      GREEN path runs it (rewinds _txn/_cv to the epoch cut). The NEGATIVE CONTROL omits
                //      it (recovery disabled) — modelling the kill-switch / "recovery did not run".
                //   2) notifyWalTxnRepublisher(tt) — UNINITIALISES the in-memory SeqTxnTracker and triggers
                //      a rescan, so the boot WAL apply (CheckWalTransactionsJob) RE-INITIALISES writerTxn
                //      from the on-disk _txn and re-checks the table, exactly as a fresh engine does (the
                //      live tracker otherwise caches the pre-crash frontier and nothing would re-apply).
                if (recoveryEnabled) {
                    new io.questdb.cairo.RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(tt);

                // Boot WAL apply: GREEN -> tracker re-inits at the rewound epoch seqTxn=K, sees the
                // sequencer frontier=K+M, re-derives (K, K+M] from the durable WAL. NEGATIVE CONTROL ->
                // tracker re-inits at the SURVIVING frontier (_txn was never rewound), so nothing is
                // re-applied and the read hits the columns the crash truncated to the epoch (torn).
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
                    // NEGATIVE CONTROL: without recovery, the post-epoch column data the crash dropped is
                    // never re-applied. The table MUST therefore be torn — the full, correct identity
                    // result {0,1,..,N+M-1} must NOT come back. Acceptable torn outcomes: the K epoch'd
                    // rows correct but the M post-epoch rows MISSING/WRONG (e.g. read back as zeros), a
                    // loud read error, fewer rows, or a suspended table. Anything but the full correct
                    // result proves recovery does real work AND that Part 1 made the columns truly lazy.
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
                    // Stronger: the K epoch'd rows survived (durable cut), so the loss is specifically in
                    // the M POST-epoch rows — at least one of indices [K, K+M) must be missing or wrong.
                    // (Skip this sharper check on a loud torn read, where no rows are readable at all.)
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
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    // Returned (as the single element) when a read throws a loud Cairo/JVM error — a torn read is an
    // acceptable negative-control outcome. Distinct from any real row value (which are 0..K+M-1).
    private static final long TORN_SENTINEL = Long.MIN_VALUE;

    /** Strict read: all rows in order, throwing on any error (used to ASSERT the GREEN recovery path). */
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
     * Tolerant read for the NEGATIVE CONTROL: a torn post-crash table may throw a loud Cairo/JVM error
     * (e.g. SIGBUS reading a column truncated below {@code _txn}'s row count) — that is an acceptable
     * "recovery did real work" outcome, returned as a single {@link #TORN_SENTINEL}.
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

    /** True iff {@code rows} is exactly the identity sequence 0,1,2,... (the uncorrupted full result). */
    private boolean isIdentityPrefix(List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i) == null || rows.get(i) != (long) i) {
                return false;
            }
        }
        return true;
    }
}
