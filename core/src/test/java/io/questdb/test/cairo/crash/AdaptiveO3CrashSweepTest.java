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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The ADAPTIVE out-of-order (O3) crash sweep: drives an adaptive WAL table through
 * {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint} with a commit phase whose row
 * timestamps are out of order, so every apply after the first goes through the O3 merge path rather than
 * a tail append, and checks that adaptive recovery is crash-safe under O3.
 *
 * <p>The workload matches {@link AdaptiveCrashSweepSelfCheckTest}, except:
 * <ul>
 *   <li>{@code v} (commit order, 0..ROWS-1) and {@code ts} (designated timestamp order) are decoupled:
 *       row {@code v=0} carries the latest timestamp of the batch, a ceiling within the single
 *       {@code partition by day} bucket, and {@code v=1..5} each land at a distinct timestamp below that
 *       ceiling in zig-zag order. Every commit from {@code v=1} onward therefore has a minimum timestamp
 *       below the table's maximum timestamp, which routes WAL apply through the O3 merge path (see
 *       {@code TableWriter}'s {@code o3TimestampMin}/{@code getMaxTimestamp} handling and {@code
 *       WalEventCursor#isOutOfOrder}).</li>
 *   <li>The oracle orders by {@code v}, not {@code ts}: the surviving rows, sorted by {@code v}, must be
 *       an exact identity prefix {@code {0..m-1}}, whatever their physical order on disk.</li>
 * </ul>
 *
 * <p>At every crash point k the oracle asserts: (1) no silent corruption (surviving rows ordered by v are
 * an exact prefix, loud torn reads tolerated); (2) the table is not suspended and a follow-up insert and
 * read work; (3) full restore at k=N; (4) the recovered count does not decrease as k grows.
 */
public class AdaptiveO3CrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    private static final int ROWS = 6; // v = 0,1,2,3,4,5 (commit order)

    /**
     * Hour of day (single partition day 2024-10-01), indexed by {@code v}. Index 0 (v=0) is the ceiling:
     * the latest timestamp in the batch, inserted first. Every later element is strictly below the ceiling
     * and moves through the day in zig-zag order (down, up, down, up, down relative to the previous
     * element), so each of v=1..5 forces an O3 merge at a different position in the existing column data
     * rather than always at the head.
     *
     * <pre>
     *   v:      0   1   2   3   4   5
     *   hour:  20   8  14   2  11   5
     * </pre>
     * In ascending timestamp order the v labels are {@code [3,5,1,4,2,0]}, unrelated to the commit order
     * {@code [0,1,2,3,4,5]}.
     */
    private static final int[] TS_HOUR = {20, 8, 14, 2, 11, 5};

    /**
     * O3 rows applied before the durable epoch in the lazy-gap scenario.
     */
    private static final int LAZY_K = 4;
    /**
     * O3 rows applied lazily after the epoch, with epochs disabled: the rows at risk without recovery.
     */
    private static final int LAZY_M = 5;
    // Hours for the K+M=9 lazy-gap rows; index 0 is the ceiling (23), all others below it, in a
    // non-monotonic order (independent scenario from TS_HOUR, same O3-forcing technique).
    private static final int[] LAZY_TS_HOUR = {23, 9, 14, 4, 11, 2, 17, 7, 20};
    /**
     * The crash point of the lazy-gap comparison, set by phase rather than by an op count: this many lazy
     * rows are applied, each as its own txn, and the crash fires on the first durability op of the next lazy
     * row's commit, before that row's WAL is durable. Recovery restores the {@code LAZY_K} epoch rows plus
     * the applied rows, {@code [0, 1, 2, 3, 4]}, a partial result whatever the number of ops per commit.
     */
    private static final int LAZY_GAP_APPLIED_ROWS = 1;

    /**
     * The O3 sweep: every commit-phase durability op is crashed at least once; recovery must leave an
     * identity prefix (ordered by v) at every k, never suspended, and restore everything at k=N.
     */
    @Test
    public void testO3SweepRecoversCleanlyAtEveryCrashPoint() throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final SweepResult r = forEachAdaptiveCrashPoint(new O3Workload());

            LOG.info().$("[O3 sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("default cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);

            // Oracle clause 4: recovered counts never decrease as k grows.
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // A genuine rise, not a degenerate all-full sweep: the earliest crash point recovers
            // strictly fewer than the full set.
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );

            // Oracle clause 3: at the last crash point, k=N, recovery restores every committed row.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * Runs the lazy-gap scenario at the same crash point with {@code RecoveryCoordinator.recover()} enabled
     * and disabled: {@code LAZY_K} O3 rows, a durable epoch ({@code interval=0}), then {@code LAZY_M} more
     * O3 rows applied lazily with epochs disabled ({@code interval=-1}), then a crash after
     * {@link #LAZY_GAP_APPLIED_ROWS} lazy applies, which leaves a partial result. Both arms must recover a
     * valid identity prefix, never suspended, and the two recovered row sets are identical: for O3 data at
     * this crash point, the {@code _txn} and {@code _cv} rewind to the epoch does not change the outcome.
     * For in-order data the same scenario loses the lazily applied rows when recovery is disabled.
     */
    @Test
    public void testRecoveryVsNoRecoveryIdenticalForO3LazyGapCrash() throws Exception {
        final List<Long> withRecovery = runLazyGapCrashScenario(LAZY_GAP_APPLIED_ROWS, true);
        final List<Long> withoutRecovery = runLazyGapCrashScenario(LAZY_GAP_APPLIED_ROWS, false);

        LOG.info().$("[O3 lazy-gap finding] appliedLazyRows=").$(LAZY_GAP_APPLIED_ROWS)
                .$(" withRecovery=").$(withRecovery.toString())
                .$(" withoutRecovery=").$(withoutRecovery.toString()).$();

        // Base oracle (both arms independently): no silent corruption -- an exact identity prefix.
        Assert.assertTrue("withRecovery must be a valid identity prefix: " + withRecovery,
                isIdentityPrefix(withRecovery));
        Assert.assertTrue("withoutRecovery must be a valid identity prefix: " + withoutRecovery,
                isIdentityPrefix(withoutRecovery));
        // Non-degenerate: a genuine partial result (not 0, not the full LAZY_K+LAZY_M), so the
        // comparison below is meaningful rather than vacuously true regardless of recovery.
        Assert.assertTrue(
                "the crash must land on a non-degenerate PARTIAL result for this "
                        + "comparison to be meaningful (withRecovery=" + withRecovery + ")",
                withRecovery.size() > LAZY_K && withRecovery.size() < (LAZY_K + LAZY_M)
        );
        Assert.assertEquals(
                "the crash must land after " + LAZY_GAP_APPLIED_ROWS + " lazy applies and before the next "
                        + "lazy row's WAL is durable (withRecovery=" + withRecovery + ")",
                LAZY_K + LAZY_GAP_APPLIED_ROWS, withRecovery.size()
        );

        // Recovery does not change the outcome at this crash point. If this assertion fails, recovery
        // matters for O3 here: invert the test to assert that the recovery-disabled arm loses rows, and
        // update the javadoc, rather than restoring this assertion.
        Assert.assertEquals(
                "recovery-enabled and recovery-disabled currently produce IDENTICAL results after "
                        + LAZY_GAP_APPLIED_ROWS + " lazy applies for this O3 lazy-gap scenario (the SP-D1 W1 finding)",
                withRecovery, withoutRecovery
        );
    }

    /**
     * Save point taken at the saved lag, then the consuming commit finishes and no newer save point
     * follows; the process is killed after that. Recovery restores the save point, whose {@code _txn}
     * counts the lag rows, and the consuming commit has rewritten their bytes in place. NOSYNC has no save
     * point and is safe in this case; ADAPTIVE is pinned.
     */
    @Test
    public void testSavedLagSavePointSurvivesKillAfterConsumingCommit() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        runWithCrashFacade(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final SavedLagWorkload workload = SavedLagWorkload.inPlaceAppend(false);
            workload.savePointsInCommitPhase = false;
            final Set<Long> nonCacheFdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
            final TableToken[] tokens = workload.setup(1);
            workload.commit();
            Assert.assertEquals("the consuming commit left lag in _txn", 0, workload.savedLagRows());
            crashFf.takeKillImage(engine.getConfiguration().getDbRoot());
            recoverAfterCrash(tokens);
            workload.oracle(1, 1);
            workload.teardown();
            releaseEngineHandles();
            reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        });
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointAcrossPartitionBoundary() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.acrossPartitionBoundary());
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointDedup() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.inPlaceAppend(true));
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointEmptyTable() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.emptyTable());
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointInPlaceAppend() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.inPlaceAppend(false));
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointSplitLastPartition() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.splitLastPartition());
    }

    /**
     * A WAL apply pass that stops early saves the WAL lag in {@code _txn}; the lag rows sit in the last
     * partition's files after its committed rows, and the next commit consumes them. Kill the process at
     * every kill point of that consuming commit, in the suite's commit mode: after recovery the table must
     * hold every row of both WAL transactions, by value, because a kill keeps the WAL.
     */
    private void assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload workload) throws Exception {
        runWithCrashFacade(() -> {
            final SweepResult r = forEachKillPoint(workload);
            LOG.info().$("[saved lag kill sweep] ").$(workload.name).$(" N=").$(r.n)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();
            Assert.assertEquals(r.n + 1, r.sweptPoints);
        });
    }

    /**
     * Runs the LAZY_K/LAZY_M lazy-gap scenario once: LAZY_K O3 rows -> durable epoch -> LAZY_M more O3
     * rows applied lazily (epoch disabled) -> crash on the first durability op of the commit of lazy row
     * {@code appliedLazyRows} (counted from 0), after that many lazy applies ->
     * the recovery triple with {@code RecoveryCoordinator.recover()} included or skipped per
     * {@code recoveryOn} -> returns the recovered rows ordered by v (tolerant of a loud torn read).
     */
    private List<Long> runLazyGapCrashScenario(int appliedLazyRows, boolean recoveryOn) throws Exception {
        final List<Long>[] resultBox = new List[1];
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, recoveryOn ? "true" : "false");
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
                final String table = "o3_lazy_gap";
                execute("drop table if exists " + table);
                execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName(table);

                // LAZY_K rows, out-of-order among themselves -> apply -> durable epoch at seqTxn=LAZY_K
                // (interval=0 fires an epoch on this first applied batch).
                for (int i = 0; i < LAZY_K; i++) {
                    insertO3Row(table, LAZY_TS_HOUR[i], i);
                }
                drainWalQueue();

                // Disable further epochs: the next LAZY_M rows are applied lazily (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                // Baseline of non-cached engine fds after a clean release (as in the sweep driver): a
                // simulated crash unwinds an fsync mid-operation on the live JVM, so a fault-injection fd
                // left open by the interrupted operation lingers where a real power loss's process death
                // would have reclaimed it. Reclaim the per-cycle delta at the end.
                final Set<Long> nonCacheFdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());

                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                int crashOp = -1;
                try {
                    for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                        if (i == LAZY_K + appliedLazyRows) {
                            // the first durability op of this row's commit
                            crashOp = crashFf.durabilityOpCount() + 1;
                            crashFf.armCrashAt(crashOp);
                        }
                        insertO3Row(table, LAZY_TS_HOUR[i], i);
                        drainWalQueue();
                        if (anyTableSuspended(tt)) {
                            break;
                        }
                    }
                } catch (CrashSimulationError propagated) {
                    // expected: the armed crash fired during the WAL-commit fsync path
                }
                Assert.assertTrue(
                        "the lazy batch must reach row " + (LAZY_K + appliedLazyRows) + " to arm the crash",
                        crashOp > 0
                );
                Assert.assertFalse("the armed crash must fire", crashFf.isCrashArmed());
                final String crashedOp = crashFf.durabilityOpLog().get(crashOp - 1);
                LOG.info().$("[O3 lazy-gap finding] recovery=").$(recoveryOn)
                        .$(" crash at k=").$(crashOp - base).$(" of the lazy batch: ").$(crashedOp).$();

                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                crashFf.crash(engine.getConfiguration().getDbRoot());
                // Model a fresh restart: clear the transient in-memory suspend a live-engine apply-crash
                // left (a real power loss never ran that catch) -- recovery-neutral, mirrors the driver.
                if (engine.getTableSequencerAPI().isSuspended(tt)) {
                    engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
                }
                if (recoveryOn) {
                    new RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                resultBox[0] = readVOrderedByVAllowTorn(table);

                execute("drop table if exists " + table);
                drainWalQueue();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                engine.releaseInactiveTableSequencers();
                for (long fd : crashFf.noCacheOpenFdsSnapshot()) {
                    if (!nonCacheFdBaseline.contains(fd)) {
                        crashFf.forceClose(fd);
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, "true");
        }
        return resultBox[0];
    }

    private static void insertO3Row(String table, int hour, int v) throws Exception {
        execute("insert into " + table + " values ('" + String.format("2024-10-01T%02d:00:00.000000Z", hour)
                + "', " + v + ")");
    }

    private void withAdaptiveW0(RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0"); // W = 0 (synchronous)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);        // durable epoch every batch
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            body.run();
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private interface RunnableEx {
        void run() throws Exception;
    }

    /**
     * count(*): the committed row count from table metadata, reliable even if a column read would tear.
     */
    private long rowCount(String table) {
        try (RecordCursorFactory f = select("select count() from " + table)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                return c.hasNext() ? r.getLong(0) : 0L;
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads {@code select v ... order by v}, returning the identity values gathered so far. A loud torn
     * read (CairoException, CairoError, or a SIGBUS InternalError on a truncated column) is an acceptable
     * crash outcome: the possibly partial result read before the tear is returned rather than rethrown.
     */
    private List<Long> readVOrderedByVAllowTorn(String table) {
        final List<Long> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select v from " + table + " order by v")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // acceptable: corruption detected loudly; return the prefix read before the tear
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * True iff {@code rows}, in order, is exactly the identity sequence 0,1,2,...
     */
    private boolean isIdentityPrefix(List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i) == null || rows.get(i) != (long) i) {
                return false;
            }
        }
        return true;
    }

    /**
     * The O3 workload: an ADAPTIVE {@code (ts timestamp, v long)} WAL table, single {@code
     * partition by day} bucket, {@code v=0..ROWS-1} identity in commit order, timestamps out of order
     * (see {@link #TS_HOUR}) so every apply after v=0 goes through the O3 merge path.
     */
    private final class O3Workload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "sweep_o3";
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            tt = engine.verifyTableName(table);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = 0; i < ROWS; i++) {
                // v=0 sets the ceiling (latest ts in the batch); v=1..5 each land below it at a
                // non-monotonic position -> every apply after the first is an O3 merge, not an append.
                // W=0: the WAL commit fdatasyncs synchronously here -> an armed crash on that op
                // propagates a CrashSimulationError out of execute().
                insertO3Row(table, TS_HOUR[i], i);
                // ADAPTIVE apply + durable epoch -> more durability ops. An armed crash here is
                // swallowed by ApplyWal2TableJob's catch(Throwable) into a table suspend (no throw).
                drainWalQueue();
                // Stop as soon as the crash has fired (as a real power loss would): if it manifested as
                // a suspend, further inserts would durably extend the WAL and mask this injection point.
                if (anyTableSuspended(tt)) {
                    return;
                }
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // Clean reopen: recovery must not leave the table suspended.
            Assert.assertFalse(
                    "k=" + k + ": table must NOT be suspended after recovery",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );

            // No silent corruption: surviving rows, ordered by v (ts is scrambled on purpose), are an
            // exact identity prefix {0..m-1}. A loud torn read is tolerated (returns the prefix read so
            // far); a wrong or absent value inside the prefix fails the test.
            final List<Long> rows = readVOrderedByVAllowTorn(table);
            for (int i = 0; i < rows.size(); i++) {
                Assert.assertNotNull("k=" + k + " row " + i + " read back NULL (corruption)", rows.get(i));
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG (not an identity prefix ordered by v)",
                        (long) i, (long) rows.get(i)
                );
            }

            // Recovered committed-row count from the metadata (reliable even if a column read tore).
            final int recovered = (int) rowCount(table);
            Assert.assertTrue(
                    "k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                    rows.size() <= recovered
            );

            if (k == n) {
                Assert.assertEquals("k=N: recovery must restore ALL committed rows", ROWS, recovered);
                Assert.assertEquals("k=N: the full identity set must read back clean", ROWS, rows.size());
            }

            // Clean reopen: a follow-up write and read must succeed on the recovered table. A later day
            // (a fresh partition, plain append) keeps the follow-up check independent of the O3 path.
            execute("insert into " + table + " values ('2024-10-09T00:00:00.000000Z', 999)");
            drainWalQueue();
            Assert.assertEquals(
                    "k=" + k + ": follow-up insert must land on the recovered table",
                    recovered + 1, rowCount(table)
            );
            return recovered;
        }

        @Override
        public void teardown() throws Exception {
            // The next setup drops the table first, so a single reused table name keeps exactly one table
            // alive at a time, bounding on-disk state and engine-registry churn across the whole sweep.
            // Best-effort final drop so the last iteration leaves nothing.
            try {
                execute("drop table if exists " + table);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[O3 sweep] teardown drop skipped for ").$(table).$(": ").$(e.getMessage()).$();
            }
        }
    }

    /**
     * The saved-lag workload. Setup: the table's committed rows, then two WAL transactions: the first
     * holds exactly the table's lag limit of rows ({@code maxUncommittedRows} times the lag multiplier),
     * the second one row out of order below them, written through a second WAL so that no later
     * transaction shares the first one's WAL segment. One apply pass with a zero time quota applies the
     * first transaction on its own, keeps its rows in WAL lag (the second sorts below them), stops on the
     * quota and saves the lag in {@code _txn}; under ADAPTIVE a save point lands on it. Setup asserts
     * {@code _txn} holds that lag, so the test cannot pass on a workload that saved none, and
     * then runs the WAL purge, which must not take the lag's WAL segment. Commit phase: the drain that
     * consumes the lag. Oracle: every row of the table and of both transactions, by value.
     */
    private static final class SavedLagWorkload implements AdaptiveCrashWorkload {
        private final boolean dedup;
        private final List<String> expectedRows = new ArrayList<>();
        private final String[] initialInserts;
        private final int lagRows;
        private final int maxUncommittedRows;
        private final String name;
        private final String outOfOrderTs;
        private final String table;
        private final String[] lagInsert;
        private final boolean splitLastPartition;
        boolean savePointsInCommitPhase = true;
        private TableToken tt;

        private SavedLagWorkload(
                String name,
                int maxUncommittedRows,
                boolean dedup,
                boolean splitLastPartition,
                String[] initialInserts,
                String[] lagInsert,
                String outOfOrderTs
        ) {
            this.name = name;
            this.maxUncommittedRows = maxUncommittedRows;
            this.dedup = dedup;
            this.splitLastPartition = splitLastPartition;
            this.initialInserts = initialInserts;
            this.lagInsert = lagInsert;
            this.lagRows = Integer.parseInt(lagInsert[1]);
            this.outOfOrderTs = outOfOrderTs;
            // a table per workload: a failing workload must not leave its table to the next one
            this.table = "sweep_lag_" + name;
            for (int i = 0; i < initialInserts.length; i += 3) {
                addExpectedRows(initialInserts[i], Integer.parseInt(initialInserts[i + 1]), Long.parseLong(initialInserts[i + 2]));
            }
            addExpectedRows(lagInsert[0], lagRows, Long.parseLong(lagInsert[2]));
            expectedRows.add(row(micros(outOfOrderTs), -1, "neg", "neg"));
            expectedRows.sort(null);
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.fail(name + " kill point k=" + k + " of " + n + ": the table is suspended after recovery [error="
                        + tracker.getErrorMessage() + "]");
            }
            final List<String> actual = new ArrayList<>();
            try (RecordCursorFactory f = select("select ts, v, s, vc from " + table)) {
                try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                    final Record r = c.getRecord();
                    while (c.hasNext()) {
                        final CharSequence sym = r.getSymA(2);
                        final io.questdb.std.str.Utf8Sequence vc = r.getVarcharA(3);
                        actual.add(row(r.getTimestamp(0), r.getLong(1), sym == null ? null : sym.toString(), vc == null ? null : vc.toString()));
                    }
                }
            }
            actual.sort(null);
            if (!expectedRows.equals(actual)) {
                int firstDiff = 0;
                while (firstDiff < Math.min(actual.size(), expectedRows.size()) && actual.get(firstDiff).equals(expectedRows.get(firstDiff))) {
                    firstDiff++;
                }
                Assert.fail(name + " kill point k=" + k + " of " + n + ": the table does not hold the rows of both WAL transactions"
                        + " [rows=" + actual.size() + ", sum(v)=" + sumV(actual)
                        + ", expected rows=" + expectedRows.size() + ", sum(v)=" + sumV(expectedRows)
                        + ", first difference at " + firstDiff + ": "
                        + (firstDiff < actual.size() ? actual.get(firstDiff) : "<none>") + " instead of "
                        + (firstDiff < expectedRows.size() ? expectedRows.get(firstDiff) : "<none>") + "]");
            }
            return actual.size();
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            execute("drop table if exists " + table);
            drainWalQueue();
            // set inside the assertMemoryLeak body: a quota set before it does not reach the engine
            setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            if (splitLastPartition) {
                setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            }
            execute("create table " + table + " (ts timestamp, v long, s symbol, vc varchar) timestamp(ts) partition by day wal"
                    + " with maxUncommittedRows=" + maxUncommittedRows);
            if (dedup) {
                execute("alter table " + table + " dedup enable upsert keys(ts)");
            }
            tt = engine.verifyTableName(table);
            for (int i = 0; i < initialInserts.length; i += 3) {
                execute(insertSql(initialInserts[i], initialInserts[i + 1], initialInserts[i + 2]));
            }
            drainWalQueue();

            execute(insertSql(lagInsert[0], lagInsert[1], lagInsert[2]));
            // hold the first WAL so the out-of-order row goes to a second one
            try (WalWriter ignore = engine.getWalWriter(tt)) {
                execute("insert into " + table + " values ('" + outOfOrderTs + "', -1, 'neg', 'neg')");
            }
            tickWalQueue(1);
            Assert.assertEquals(name + ": _txn does not hold the saved WAL lag", lagRows, savedLagRows());
            engine.releaseAllWalWriters();
            drainPurgeJob();
            if (!savePointsInCommitPhase) {
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            }
            return new TableToken[]{tt};
        }

        @Override
        public void commit() {
            drainWalQueue();
        }

        @Override
        public void teardown() throws Exception {
            try {
                execute("drop table if exists " + table);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[saved lag sweep] teardown drop skipped: ").$(e.getMessage()).$();
            }
        }

        static SavedLagWorkload acrossPartitionBoundary() {
            // lag limit 2,000 rows: 600 lag rows on day 2, 1,400 on day 3; the out-of-order row lands on
            // day 1, the lag's day-2 rows append in place, and day 2 is trimmed when day 3 becomes last
            return new SavedLagWorkload(
                    "acrossPartitionBoundary", 100, false, false,
                    new String[]{"2022-02-24T00:00:00.000000Z", "1", "1000000", "2022-02-25T00:00:00.000000Z", "1", "2000000"},
                    new String[]{"2022-02-25T23:50:00.000000Z", "2000", "0"},
                    "2022-02-24T12:00:00.000000Z"
            );
        }

        static SavedLagWorkload emptyTable() {
            // no committed rows: the lag lives in an artificial partition the consuming commit removes
            return new SavedLagWorkload(
                    "emptyTable", 50, false, false,
                    new String[0],
                    new String[]{"2022-02-25T01:00:00.000000Z", "1000", "0"},
                    "2022-02-25T00:30:00.000000Z"
            );
        }

        static SavedLagWorkload inPlaceAppend(boolean dedup) {
            // one committed row; the out-of-order row sorts between it and the lag, so the consuming commit
            // appends the merged rows in place over the lag
            return new SavedLagWorkload(
                    dedup ? "inPlaceAppendDedup" : "inPlaceAppend", 50, dedup, false,
                    new String[]{"2022-02-25T00:00:00.000000Z", "1", "1000000"},
                    new String[]{"2022-02-25T01:00:00.000000Z", "1000", "0"},
                    "2022-02-25T00:30:00.000000Z"
            );
        }

        static SavedLagWorkload splitLastPartition() {
            // the out-of-order row lands inside the committed tail; with a 1-byte split threshold the
            // consuming commit splits the last partition and cuts the old directory to its committed rows
            return new SavedLagWorkload(
                    "splitLastPartition", 50, false, true,
                    new String[]{"2022-02-25T00:00:00.000000Z", "5000", "1000000"},
                    new String[]{"2022-02-25T02:00:00.000000Z", "1000", "0"},
                    "2022-02-25T01:23:00.500000Z"
            );
        }

        private static long micros(String ts) {
            final Instant instant = Instant.parse(ts);
            return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        }

        private static String row(long ts, long v, String s, String vc) {
            // zero-padded, so the natural string order is the timestamp order
            return String.format("%020d|%d|%s|%s", ts, v, s, vc);
        }

        private static long sumV(List<String> rows) {
            long sum = 0;
            for (String r : rows) {
                final int from = r.indexOf('|') + 1;
                sum += Long.parseLong(r.substring(from, r.indexOf('|', from)));
            }
            return sum;
        }

        private void addExpectedRows(String fromTs, int rows, long vBase) {
            final long from = micros(fromTs);
            for (int x = 1; x <= rows; x++) {
                final long v = vBase + x;
                expectedRows.add(row(from + (x - 1) * 1_000_000L, v, "k" + (x % 7), "r" + v));
            }
        }

        private String insertSql(String fromTs, String rows, String vBase) {
            return "insert into " + table + " select timestamp_sequence('" + fromTs + "', 1_000_000L), " + vBase + " + x,"
                    + " 'k' || (x % 7), 'r' || (" + vBase + " + x) from long_sequence(" + rows + ")";
        }

        int savedLagRows() {
            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade()); Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
                txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                txReader.unsafeLoadAll();
                return txReader.getLagRowCount();
            }
        }
    }
}
