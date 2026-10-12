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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Adaptive O3 crash sweep over a sustained lazy gap.
 *
 * <p>{@link AdaptiveO3CrashSweepTest} runs with {@code EPOCH_INTERVAL_MS=0}, a durable epoch every batch, so it
 * never builds a sustained lazy gap: the path adaptive recovery exists for (columns non-durable between epochs,
 * {@code _txn}/{@code _cv} rewound to the last durable cut and re-derived from the durable WAL). This sweep drives
 * the lazy-gap scenario of {@link AdaptiveRecoveryRollForwardCrashTest} (K O3 rows, durable epoch, epoch
 * disabled, M more O3 rows applied lazily, crash) through
 * {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint}, with its per-cycle isolation
 * ({@code releaseEngineHandles}, {@code markDurableBaseline}, non-cache fd reclamation, transient-suspend clear).
 *
 * <ul>
 *   <li>{@link #testO3LazyGapSweepRecoversCleanlyAtEveryCrashPoint}: the full oracle holds at every crash point
 *       (never suspended, identity prefix by {@code v}, monotonic floor, full restore at k=N).</li>
 *   <li>{@link #testStaleScoreboardArtifactPresentOnlyWithoutEviction}: the same scenario with and without the
 *       driver's eviction of the pooled {@code TxnScoreboard}. Without it, a live engine reports a spurious
 *       read timeout or suspension that a real restart cannot see; this keeps that eviction honest.</li>
 * </ul>
 */
public class AdaptiveO3LazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    /**
     * Pre-epoch O3 rows: a durable epoch is taken after these (interval 0 on the first applied batch).
     */
    private static final int LAZY_K = 4;
    /**
     * Post-epoch O3 rows applied lazily (epoch disabled): the sustained lazy gap this sweep crashes into.
     */
    private static final int LAZY_M = 5;
    /**
     * Total committed rows once the whole gap is applied.
     */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * Hour of day within the single {@code partition by day} bucket 2024-10-01, indexed by {@code v} (commit order
     * 0..8). Index 0 (v=0) is the ceiling (hour 23, inserted first); every later element is below it and visits the
     * timeline non-monotonically, so every apply after v=0 takes the O3 merge path (min ts &lt; current max ts), in
     * both the K pre-epoch rows and the M lazily applied post-epoch rows.
     */
    private static final int[] LAZY_TS_HOUR = {23, 9, 14, 4, 11, 2, 17, 7, 20};

    /**
     * The isolated lazy-gap sweep. Builds a durable epoch at seqTxn=K, disables further epochs, then sweeps a crash
     * across every durability op of the M lazily applied O3 rows, running the recovery triple and the oracle at each.
     * Asserts, at every crash point k:
     * <ol>
     *   <li>the table is not suspended after recovery;</li>
     *   <li>surviving rows ordered by {@code v} are an exact identity prefix {@code {0..m-1}} (a visible torn read
     *       is tolerated; a wrong or absent value inside the prefix, or a {@code [0,0,..]} zero fill, fails);</li>
     *   <li>a follow-up write and read succeed on the recovered table;</li>
     *   <li>the recovered count is non-decreasing in k, rises from a short prefix, and equals {@code ROWS} at
     *       k=N.</li>
     * </ol>
     */
    @Test
    public void testO3LazyGapSweepRecoversCleanlyAtEveryCrashPoint() throws Exception {
        withAdaptiveLazyGap(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final SweepResult r = forEachAdaptiveCrashPoint(new LazyGapO3Workload());

            LOG.info().$("[O3 lazy-gap sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("default cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);

            // Oracle clause 4 (durable-survival floor): recovered counts non-decreasing in k.
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // A real rise: the earliest crash point recovers fewer than the full set (at least the K durable epoch rows
            // survive, but not all M).
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );
            // The durable epoch floor: even the earliest crash point keeps at least the K epoch rows.
            Assert.assertTrue(
                    "every crash point must recover at least the K durable epoch rows (floor)",
                    r.recoveredByK()[1] >= LAZY_K
            );

            // Oracle clause 3: at the last crash point k=N, recovery restores all committed rows; the roll-forward
            // re-derives the whole (epoch, frontier] range from the WAL.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * The rewinding crash point, pinned by phase rather than by an op count from the batch start: this many lazy
     * rows are applied, each as its own txn, and the crash fires on the first durability op of the next lazy row's
     * commit, before that row is published. Each pre-crash O3 apply pushes the pooled {@code TxnScoreboard}'s
     * {@code max} up; recovery rewinds {@code _txn} to the durable epoch cut and the WAL apply replays the applied
     * rows as one block, so with two or more of them the post-recovery data txn sits below the stale scoreboard
     * {@code max}. Recovery restores {@code LAZY_K} plus this many rows. A change to the number of durability ops
     * per commit cannot move this point.
     */
    private static final int SCOREBOARD_APPLIED_LAZY_ROWS = 2;

    /**
     * A single-variable A/B on the fresh-process eviction of the pooled {@code TxnScoreboard}. Both arms run the
     * same lazy-gap scenario at the same rewinding crash point and the same recovery, including the
     * transient-suspend clear, and differ only in whether the pooled scoreboard is evicted before the post-recovery
     * read, as a booted engine (fresh, empty scoreboard) would have it.
     * <ul>
     *   <li>With eviction: the read returns a clean identity prefix and the table is not suspended.</li>
     *   <li>Without eviction: the scoreboard's stale pre-crash {@code max} (anonymous native memory {@code crash()}
     *       cannot roll back) sits above the rewound {@code _txn}, so a reader can never satisfy
     *       {@code TxnScoreboardV2.acquireTxn} ({@code updateMax} fails on {@code txn < max}): a spurious
     *       {@code Transaction read timeout}, or a lingering suspension.</li>
     * </ul>
     * A real restart always starts with a fresh scoreboard, so the second arm is a live-engine artifact the sweep's
     * eviction removes.
     * <p>
     * The crash point must be one where the scoreboard is stale: see {@link #SCOREBOARD_APPLIED_LAZY_ROWS}. A crash
     * before the second lazy apply leaves the recovered txn equal to the pre-crash {@code max}, and the arms then
     * agree.
     */
    @Test
    public void testStaleScoreboardArtifactPresentOnlyWithoutEviction() throws Exception {
        final String withoutEviction = runScoreboardArm(false);
        final String withEviction = runScoreboardArm(true);

        LOG.info().$("[O3 lazy-gap scoreboard A/B] appliedLazyRows=").$(SCOREBOARD_APPLIED_LAZY_ROWS)
                .$(" withoutEviction=").$(withoutEviction)
                .$(" withEviction=").$(withEviction).$();

        // With the driver's scoreboard eviction: clean identity prefix. This is the deterministic side and matches what
        // the isolated sweep does at every point.
        Assert.assertTrue(
                "WITH scoreboard eviction (fresh-restart model) the O3 lazy-gap table must recover to a "
                        + "clean identity prefix, got: " + withEviction,
                withEviction.startsWith("IDENTITY")
        );
        // The crash point itself: the epoch rows plus the applied lazy rows survive, the row whose commit
        // crashed does not. Any other count means the crash no longer lands where the scoreboard is stale.
        final List<Long> expectedRows = new ArrayList<>();
        for (long v = 0; v < LAZY_K + SCOREBOARD_APPLIED_LAZY_ROWS; v++) {
            expectedRows.add(v);
        }
        Assert.assertEquals(
                "the crash must land after " + SCOREBOARD_APPLIED_LAZY_ROWS + " lazy applies and before the "
                        + "next lazy row is published",
                "IDENTITY:" + expectedRows, withEviction
        );

        // Without eviction: the stale pooled scoreboard brings a spurious post-recovery artifact the isolated sweep
        // never shows. A stale scoreboard can only make a reader wait or the table suspend; other rows here would be a
        // real defect.
        Assert.assertTrue(
                "WITHOUT scoreboard eviction the stale live scoreboard must reintroduce a spurious "
                        + "post-recovery artifact (read timeout or suspend) absent from the isolated sweep, got: "
                        + withoutEviction,
                withoutEviction.startsWith("READ_TIMEOUT") || withoutEviction.equals("SUSPENDED")
        );
        // The two arms differ: same scenario, same crash point, isolation the only variable.
        Assert.assertNotEquals(
                "the scoreboard-eviction isolation must be the discriminating variable (arms must differ)",
                withEviction, withoutEviction
        );
    }

    /**
     * Runs the lazy-gap scenario once, crashing at the point {@link #SCOREBOARD_APPLIED_LAZY_ROWS} names, with the
     * driver's recovery, then evicts the pooled {@code TxnScoreboard} before the observing read only if
     * {@code evictScoreboard}. Returns an outcome tag: {@code IDENTITY:[..]}, {@code NON_IDENTITY:[..]},
     * {@code SUSPENDED}, or {@code READ_TIMEOUT:<msg>}. Drops the table and reclaims per-cycle fds before returning.
     */
    private String runScoreboardArm(boolean evictScoreboard) throws Exception {
        final String[] outcome = new String[1];
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                final String table = "o3_lazygap_ab";
                execute("drop table if exists " + table);
                drainWalQueue();
                execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName(table);

                // K rows and a durable epoch, then disable the epoch: the sustained lazy gap begins.
                for (int i = 0; i < LAZY_K; i++) {
                    insertO3Row(table, LAZY_TS_HOUR[i], i);
                }
                drainWalQueue();
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                int crashOp = -1;
                try {
                    for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                        if (i == LAZY_K + SCOREBOARD_APPLIED_LAZY_ROWS) {
                            // The first durability op of this row's commit.
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
                    // Expected: the crash fired on the WAL-commit fsync path.
                }
                Assert.assertTrue(
                        "the lazy batch must reach row " + (LAZY_K + SCOREBOARD_APPLIED_LAZY_ROWS) + " to arm the crash",
                        crashOp > 0
                );
                Assert.assertFalse("the armed crash must fire", crashFf.isCrashArmed());
                final String crashedOp = crashFf.durabilityOpLog().get(crashOp - 1);
                LOG.info().$("[O3 lazy-gap scoreboard A/B] evict=").$(evictScoreboard)
                        .$(" crash at k=").$(crashOp - base).$(" of the lazy batch: ").$(crashedOp).$();

                // The driver's recovery, except that the scoreboard eviction is the toggled variable.
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                crashFf.crash(engine.getConfiguration().getDbRoot());
                if (engine.getTableSequencerAPI().isSuspended(tt)) {
                    engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
                }
                if (evictScoreboard) {
                    engine.getTxnScoreboardPool().remove(tt); // the fresh-restart model
                }
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                // Observe.
                if (engine.getTableSequencerAPI().isSuspended(tt)) {
                    outcome[0] = "SUSPENDED";
                } else {
                    try {
                        final List<Long> rows = readVOrderedByVAllowTorn(table);
                        outcome[0] = (isIdentityPrefix(rows) ? "IDENTITY:" : "NON_IDENTITY:") + rows;
                    } catch (RuntimeException readFailed) {
                        final String msg = readFailed.getCause() != null
                                ? readFailed.getCause().getMessage() : readFailed.getMessage();
                        outcome[0] = (msg != null && msg.contains("Transaction read timeout"))
                                ? "READ_TIMEOUT:" + msg : "READ_ERROR:" + msg;
                    }
                }

                // Cleanup: always evict the scoreboard and drop, regardless of the arm.
                if (engine.getTableSequencerAPI().isSuspended(tt)) {
                    engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
                }
                engine.getTxnScoreboardPool().remove(tt);
                execute("drop table if exists " + table);
                drainWalQueue();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                engine.releaseInactiveTableSequencers();
                for (long fd : crashFf.noCacheOpenFdsSnapshot()) {
                    if (!fdBaseline.contains(fd)) {
                        crashFf.forceClose(fd);
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
        return outcome[0];
    }

    private void withAdaptiveLazyGap(RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0"); // W = 0 (synchronous)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);        // setup re-affirms/flips per phase
        // Recovery roll-forward stays at its default (enabled): this sweep checks that it recovers.
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

    private static void insertO3Row(String table, int hour, int v) throws Exception {
        execute("insert into " + table + " values ('" + String.format("2024-10-01T%02d:00:00.000000Z", hour)
                + "', " + v + ")");
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
     * Reads {@code select v ... order by v}, returning the identity values gathered so far. A visible torn read
     * (CairoException, CairoError, or a SIGBUS InternalError on a truncated column) is an acceptable crash outcome:
     * the partial result read before the tear is returned rather than rethrown.
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
            // Acceptable: corruption detected visibly; return the prefix read before the tear.
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
     * The lazy-gap O3 workload. {@code setup} builds the durable epoch prefix (K O3 rows with the epoch enabled,
     * then the epoch disabled), so the swept {@code commit} phase is the M lazily applied O3 rows, a sustained lazy
     * gap. It reuses one table name (drop and recreate at the head of each setup); the driver supplies the
     * per-cycle isolation.
     */
    private final class LazyGapO3Workload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "o3_lazygap";
            // Epoch enabled for the K-row prefix, so a durable cut is taken at seqTxn=K.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            tt = engine.verifyTableName(table);

            // K rows, out of order among themselves, then apply: durable epoch at seqTxn=K.
            for (int i = 0; i < LAZY_K; i++) {
                insertO3Row(table, LAZY_TS_HOUR[i], i);
            }
            drainWalQueue();

            // Disable further epochs: the swept commit() phase (the M rows) is applied lazily, building the sustained
            // gap between the durable cut and the frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                // v=LAZY_K..ROWS-1 each land below the ceiling at a non-monotonic position (O3 merge), applied lazily,
                // so the columns are non-durable until recovery re-derives them. W=0: the WAL commit fdatasyncs
                // synchronously, so an armed crash propagates here.
                insertO3Row(table, LAZY_TS_HOUR[i], i);
                // Adaptive lazy apply (no epoch): _txn msync only. ApplyWal2TableJob's catch(Throwable) turns an armed
                // crash here into a table suspend.
                drainWalQueue();
                if (anyTableSuspended(tt)) {
                    return; // stop as a real power loss would, so later commits do not mask the injection point
                }
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // (1) Clean reopen: recovery does not leave the table suspended, at any crash point.
            Assert.assertFalse(
                    "k=" + k + ": table must NOT be suspended after recovery (W1 report §5.2 observation)",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );

            // (2) No silent corruption: surviving rows ordered by v are an exact identity prefix {0..m-1}. A visible
            // torn read is tolerated (prefix read so far); a wrong or absent value, including a [0,0,..] zero fill (row
            // i reading back 0 instead of i), fails.
            final List<Long> rows = readVOrderedByVAllowTorn(table);
            for (int i = 0; i < rows.size(); i++) {
                Assert.assertNotNull("k=" + k + " row " + i + " read back NULL (corruption)", rows.get(i));
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG (not an identity prefix ordered by v) — "
                                + "a zero-fill/gap here is the suspected O3 lazy-gap recovery bug",
                        (long) i, (long) rows.get(i)
                );
            }

            // Recovered committed-row count from the metadata (reliable even if a column read tore).
            final int recovered = (int) rowCount(table);
            Assert.assertTrue(
                    "k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                    rows.size() <= recovered
            );
            // The durable epoch floor: the K epoch rows are always durable, so recovery never drops below them wherever
            // in the lazy M batch the crash landed.
            Assert.assertTrue(
                    "k=" + k + ": recovery must never drop below the K durable epoch rows (recovered="
                            + recovered + ")",
                    recovered >= LAZY_K
            );

            if (k == n) {
                Assert.assertEquals("k=N: recovery must restore ALL committed rows", ROWS, recovered);
                Assert.assertEquals("k=N: the full identity set must read back clean", ROWS, rows.size());
            }

            // (3) Clean reopen: a follow-up write and read succeed on the recovered table. A later day (a fresh
            // partition, plain append) keeps the check independent of the O3 path.
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
            try {
                execute("drop table if exists " + table);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[O3 lazy-gap sweep] teardown drop skipped for ").$(table).$(": ")
                        .$(e.getMessage()).$();
            }
        }
    }
}
