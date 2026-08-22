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
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The MIRROR of {@link AdaptiveRecoveryRollForwardCrashTest}: what happens to a table that crashed while
 * ADAPTIVE when the operator turns adaptive OFF and restarts.
 *
 * <p>{@code RecoveryCoordinator.recover()} decides whether to roll a table forward from its
 * {@code effective} commit mode — the table's {@code _meta} override resolved against the CURRENT global
 * {@code cairo.commit.mode}. That is a statement about how the table will be written NEXT, not about how
 * its materialized state was left. A table whose columns were applied lazily under adaptive and whose
 * process then died is torn ahead of its durable epoch regardless of what the config file says on the way
 * back up; skipping the roll-forward serves that torn state.
 *
 * <p>Both arms below crash identically. They differ ONLY in the global mode the engine restarts under:
 * <ul>
 *   <li>{@link #testCrashedAdaptiveTableRollsForwardWhenGlobalStaysAdaptive} — restarts adaptive. This is
 *       the arm {@link AdaptiveRecoveryRollForwardCrashTest} already covers; it is repeated here as the
 *       CONTROL that proves this harness really does construct a lazily-torn table.</li>
 *   <li>{@link #testCrashedAdaptiveTableRollsForwardAfterGlobalFlipToNosync} — restarts nosync. The rows
 *       must survive here too: the durable WAL still holds them and the durable epoch still names the cut
 *       to replay from. The table's FUTURE durability is nosync's business; its PAST is not.</li>
 * </ul>
 *
 * <p>Do not confuse this with a CLEAN downgrade, which
 * {@code AdaptiveUpgradeCompatTest#testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch} pins in the
 * opposite direction: there the running writer reconciled the table before the mode changed, the WAL purge
 * floor then dropped to the applied seqTxn, and rolling back to the frozen epoch would replay purged WAL.
 * The two cases are only distinguishable by a DURABLE record of the mode the table was last written under
 * — which is what the fix adds. A clean downgrade records the new mode after making the state durable; a
 * crash leaves the record saying ADAPTIVE.
 */
public class AdaptiveCommitModeFlipCrashTest extends AbstractCrashConsistencyTest {

    // Returned (as the single element) when a read throws a loud Cairo/JVM error.
    private static final long TORN_SENTINEL = Long.MIN_VALUE;
    private static final int K = 4; // txns covered by the durable epoch
    private static final int M = 5; // txns applied lazily after it

    @Test
    public void testCrashedAdaptiveTableRollsForwardAfterGlobalFlipToNosync() throws Exception {
        assertRollForwardSurvivesRebootMode("nosync");
    }

    @Test
    public void testCrashedAdaptiveTableRollsForwardWhenGlobalStaysAdaptive() throws Exception {
        assertRollForwardSurvivesRebootMode("adaptive");
    }

    /**
     * The repair must also CONVERGE. Rolling forward a table that no longer resolves to adaptive is right
     * once; leaving it enrolled afterwards would make every subsequent startup rewind to an epoch that
     * nosync never advances — and once the WAL purge floor drops to the applied seqTxn (the epoch floor
     * applies only under ADAPTIVE), the WAL that rewind needs is gone. That is the loss
     * {@code AdaptiveUpgradeCompatTest#testDowngradeThenRebootPreservesDataAndIgnoresStaleEpoch} guards.
     *
     * <p>So the writer that opens after the roll-forward reconciles the table and clears the enrolment. This
     * test proves the whole sequence: crash adaptive, flip to nosync, recover, keep writing under nosync,
     * purge the WAL under the nosync floor, restart again — everything survives, and the second restart is
     * not relying on the epoch at all.
     */
    @Test
    public void testDowngradedTableStopsRewindingOnceReconciled() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                for (int i = 0; i < K; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("t");

                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
                for (int i = K; i < K + M; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                final String tableDir = engine.getConfiguration().getDbRoot() + java.io.File.separator + tt.getDirName();
                crashFf.markFileDurable(tableDir + java.io.File.separator + TableUtils.TXN_FILE_NAME);
                crashFf.markFileDurable(tableDir + java.io.File.separator + TableUtils.COLUMN_VERSION_FILE_NAME);
                crashAndReopen();

                setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();
                Assert.assertEquals("roll-forward must rebuild every row", K + M, readVs(engine).size());

                // The table is now reconciled: _meta no longer claims the state may be ahead of an epoch.
                try (TableReaderMetadata md = new TableReaderMetadata(engine.getConfiguration(), tt)) {
                    md.loadMetadata();
                    Assert.assertNotEquals(
                            "a downgraded table must not stay enrolled, or every restart rewinds to an epoch"
                                    + " nosync never advances",
                            CommitMode.ADAPTIVE,
                            md.getEnrolledCommitMode()
                    );
                }

                // Keep writing under nosync, then purge the WAL under the nosync floor: the epoch is frozen
                // and now strictly below the live cut, so a rewind to it would be observably lossy.
                for (int i = K + M; i < K + M + 3; i++) {
                    execute("insert into t values ('2024-10-02T0" + (i - K - M) + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                engine.releaseAllWalWriters();
                final long step = engine.getConfiguration().getWalPurgeInterval() * 1000L + 1_000_000L;
                final long[] tick = {1L};
                try (WalPurgeJob job = new WalPurgeJob(engine, engine.getConfiguration().getFilesFacade(),
                        () -> (tick[0] += step))) {
                    job.run();
                    job.run();
                }

                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                Assert.assertFalse("reconciled table must not be suspended",
                        engine.getTableSequencerAPI().isSuspended(tt));
                final List<Long> post = readVsAllowTorn(engine);
                Assert.assertEquals("every row must survive the purge and the second restart [rows=" + post + ']',
                        K + M + 3, post.size());
                for (int i = 0; i < K + M + 3; i++) {
                    Assert.assertEquals("row " + i + " value", Long.valueOf(i), post.get(i));
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private void assertRollForwardSurvivesRebootMode(String rebootMode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                // Per-inode journaling: a journal commit on _txn must not incidentally journal the columns.
                crashFf.modelSharedJournal = false;
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");

                for (int i = 0; i < K; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("t");

                // Disable the epoch so the next M rows are applied LAZILY — non-durable columns above the cut.
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
                for (int i = K; i < K + M; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                Assert.assertEquals("pre-crash must see all K+M rows", K + M, readVs(engine).size());

                // Model the kernel writing back _txn/_cv without an msync from QuestDB: the commit POINTER
                // survives at the post-epoch frontier while the column DATA it exposes does not. Same
                // construction as AdaptiveRecoveryRollForwardCrashTest — see the rationale there.
                final String tableDir = engine.getConfiguration().getDbRoot() + java.io.File.separator + tt.getDirName();
                crashFf.markFileDurable(tableDir + java.io.File.separator + TableUtils.TXN_FILE_NAME);
                crashFf.markFileDurable(tableDir + java.io.File.separator + TableUtils.COLUMN_VERSION_FILE_NAME);

                crashAndReopen();

                // The operator edits cairo.commit.mode and restarts. Drop the cached tracker so the mode is
                // resolved from _meta + the (new) server default, as it is on a cold start; otherwise the
                // live tracker answers ADAPTIVE from memory and the flip is not modelled at all.
                setProperty(PropertyKey.CAIRO_COMMIT_MODE, rebootMode);
                engine.getTableSequencerAPI().resetForReboot(tt);

                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                final boolean suspended = engine.getTableSequencerAPI().isSuspended(tt);
                final List<Long> post = readVsAllowTorn(engine);
                Assert.assertFalse("table must NOT be suspended after recovery [rebootMode=" + rebootMode + ']', suspended);
                Assert.assertEquals(
                        "recovery must rebuild ALL K+M rows from the durable WAL [rebootMode=" + rebootMode
                                + ", rows=" + post + ']',
                        K + M,
                        post.size()
                );
                for (int i = 0; i < K + M; i++) {
                    Assert.assertEquals("row " + i + " value [rebootMode=" + rebootMode + ']',
                            Long.valueOf(i), post.get(i));
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

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

    private List<Long> readVsAllowTorn(CairoEngine eng) {
        try {
            return readVs(eng);
        } catch (RuntimeException | io.questdb.cairo.CairoError | InternalError e) {
            final List<Long> torn = new ArrayList<>();
            torn.add(TORN_SENTINEL);
            return torn;
        }
    }
}
