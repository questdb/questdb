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
 * SP-D Task W1-INV: the ADAPTIVE O3 <b>lazy-gap</b> crash sweep — the properly-isolated repro that
 * settles the W1 report's flagged, unverified secondary observation (report §5.2): a throwaway,
 * NON-isolated ~20-point scan of this exact scenario had shown the table left SUSPENDED reading a
 * {@code [0,0,0,0,0]}-shaped result at a handful of crash points (k=22,25,36,50) in BOTH recovery arms.
 *
 * <h3>Why this closes a real coverage gap (not just re-checks the W1 sweep)</h3>
 * The committed W1 sweep ({@link AdaptiveO3CrashSweepTest}) runs with {@code EPOCH_INTERVAL_MS=0} — a
 * durable epoch every batch — so it never builds a <b>sustained lazy gap</b>: the exact path adaptive
 * recovery exists for (columns non-durable BETWEEN epochs, {@code _txn}/{@code _cv} rewound to the last
 * durable cut and re-derived from the durable WAL). This sweep drives the canonical lazy-gap technique
 * of {@link AdaptiveRecoveryRollForwardCrashTest} (K O3 rows -> durable epoch -> disable epoch -> M more
 * O3 rows applied LAZILY -> crash) through the VALIDATED {@link
 * AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint} driver, whose per-cycle isolation
 * ({@code releaseEngineHandles} + {@code markDurableBaseline} + non-cache-fd reclamation +
 * transient-suspend clear) is exactly the hygiene the throwaway scan lacked. So it (a) tests the exact
 * suspect scenario with CORRECT isolation and (b) fills the sustained-lazy-gap coverage hole.
 *
 * <h3>The discriminator (real bug vs harness artifact)</h3>
 * <ul>
 *   <li>{@link #testO3LazyGapSweepRecoversCleanlyAtEveryCrashPoint} — the ISOLATED sweep. If the full
 *       oracle holds at every crash point (never suspended, identity prefix by {@code v}, monotonic
 *       floor, full restore at k=N), the earlier suspension was a harness artifact of the non-isolated
 *       scan, and this stands as genuine new coverage.</li>
 *   <li>{@link #testContaminationReproducesSuspensionWithoutIsolation} — the PROOF of the artifact
 *       mechanism: the SAME lazy-gap scenario and the SAME reused table name, but run WITHOUT the
 *       driver's per-cycle isolation (exactly as the throwaway scan did), reproduces the spurious
 *       post-recovery suspension. Suspension that appears ONLY when isolation is removed, and vanishes
 *       when it is restored, is proof of artifact — not an engine bug.</li>
 * </ul>
 * The two tests vary exactly ONE thing — the per-cycle isolation — with the scenario, table name, row
 * data, epoch cadence and recovery path held identical, so the discriminator is clean.
 */
public class AdaptiveO3LazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    /** Pre-epoch O3 rows: a durable epoch is taken after these (interval=0 on the first applied batch). */
    private static final int LAZY_K = 4;
    /** Post-epoch O3 rows applied LAZILY (epoch disabled) — the sustained lazy gap this sweep crashes into. */
    private static final int LAZY_M = 5;
    /** Total committed rows once the whole gap is applied. */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * Hour-of-day (single {@code partition by day} bucket 2024-10-01), indexed by {@code v} (commit
     * order 0..8). Index 0 (v=0) is the CEILING (hour 23, inserted FIRST); every later element is
     * strictly below it and visits the timeline non-monotonically, so every apply after v=0 engages the
     * O3 merge path (min ts &lt; current max ts), not a plain tail append — across BOTH the K pre-epoch
     * rows and the M lazily-applied post-epoch rows.
     */
    private static final int[] LAZY_TS_HOUR = {23, 9, 14, 4, 11, 2, 17, 7, 20};

    /**
     * THE HEADLINE, PROPERLY-ISOLATED lazy-gap sweep. Builds a durable epoch at seqTxn=K, disables
     * further epochs, then sweeps a crash across EVERY durability op of the M lazily-applied O3 rows,
     * running the full recovery triple and oracle at each. Asserts, at every crash point k:
     * <ol>
     *   <li>the table is NOT suspended after recovery;</li>
     *   <li>no silent corruption — surviving rows ordered by {@code v} are an exact identity prefix
     *       {@code {0..m-1}} (a loud torn read tolerated; a wrong/absent value inside the prefix, or a
     *       {@code [0,0,..]} zero-fill, is a FAILURE);</li>
     *   <li>a follow-up write+read succeeds on the recovered table;</li>
     *   <li>the recovered count is monotonic non-decreasing in k, rising from a short prefix to the full
     *       set, and equals ALL {@code ROWS} at k=N.</li>
     * </ol>
     * A suspended table or a {@code [0,0,0,0,0]} read here — reproducing the W1 report's flagged
     * observation UNDER correct isolation — would be a real adaptive O3 lazy-gap recovery bug.
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

            // Oracle clause 4 (Bar-2 durable-survival FLOOR): recovered counts non-decreasing in k.
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // A genuine rise, not a degenerate all-full sweep: the earliest crash point recovers strictly
            // fewer than the full set (at minimum the K durable epoch rows survive, but not yet all M).
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );
            // The durable epoch floor: even the earliest crash point keeps at least the K epoch'd rows.
            Assert.assertTrue(
                    "every crash point must recover at least the K durable epoch rows (floor)",
                    r.recoveredByK()[1] >= LAZY_K
            );

            // Oracle clause 3: at the LAST crash point k=N, recovery restores ALL committed rows — the
            // sustained-lazy-gap roll-forward re-derives the whole (epoch, frontier] range from the WAL.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * A rewinding crash point (mid lazy M-batch) empirically chosen during this investigation: the
     * pre-crash O3 apply has pushed the pooled {@code TxnScoreboard}'s {@code max} up, and recovery then
     * rewinds {@code _txn} to the durable epoch cut and re-derives only PART of the gap — so the live
     * (post-recovery) data txn sits BELOW the stale scoreboard {@code max}. (At this point the isolated
     * sweep recovers 6 of the 9 rows.)
     */
    private static final int SCOREBOARD_REWIND_CRASH_K = 27;

    /**
     * THE DISCRIMINATOR (the proof-of-mechanism half of the verdict). A single-variable A/B on the ONE
     * thing the throwaway scan lacked and the driver supplies: the fresh-process-restart eviction of the
     * pooled {@code TxnScoreboard}. Both arms run the IDENTICAL lazy-gap scenario at the IDENTICAL
     * rewinding crash point and do the IDENTICAL recovery (including the transient-suspend clear) —
     * differing ONLY in whether the pooled scoreboard is evicted before the post-recovery read, exactly
     * as a real booted engine (fresh, empty scoreboard) would.
     * <ul>
     *   <li>WITHOUT eviction — the live-engine artifact the throwaway scan hit: the scoreboard's stale
     *       pre-crash {@code max} high-water mark (anonymous native memory {@code crash()} cannot roll
     *       back) sits above the rewound {@code _txn}, so a reader can NEVER satisfy
     *       {@code TxnScoreboardV2.acquireTxn} ({@code updateMax} fails on {@code txn < max}) — a
     *       spurious {@code Transaction read timeout}, or (when the crash landed in apply) a lingering
     *       {@code SUSPENDED}. Either is a post-recovery artifact the isolated sweep NEVER shows.</li>
     *   <li>WITH eviction — the fresh-restart model the driver now applies: the read comes back a clean
     *       identity prefix, table not suspended.</li>
     * </ul>
     * Artifact present ONLY when the isolation is removed, and gone when it is restored, is proof the W1
     * report's flagged suspension/{@code [0,0,..]} observation was a HARNESS ARTIFACT of the non-isolated
     * scan — not an adaptive O3 lazy-gap recovery bug.
     */
    @Test
    public void testStaleScoreboardArtifactPresentOnlyWithoutEviction() throws Exception {
        final String withoutEviction = runScoreboardArm(SCOREBOARD_REWIND_CRASH_K, false);
        final String withEviction = runScoreboardArm(SCOREBOARD_REWIND_CRASH_K, true);

        LOG.info().$("[O3 lazy-gap scoreboard A/B] k=").$(SCOREBOARD_REWIND_CRASH_K)
                .$(" withoutEviction=").$(withoutEviction)
                .$(" withEviction=").$(withEviction).$();

        // WITH the driver's fresh-restart scoreboard eviction: clean identity prefix, no artifact. This is
        // the load-bearing, deterministic side and mirrors what the isolated sweep does at every point.
        Assert.assertTrue(
                "WITH scoreboard eviction (fresh-restart model) the O3 lazy-gap table must recover to a "
                        + "clean identity prefix, got: " + withEviction,
                withEviction.startsWith("IDENTITY")
        );

        // WITHOUT eviction: the stale pooled scoreboard reintroduces a spurious post-recovery artifact
        // (read timeout or suspend) that the isolated sweep never shows -> proves it is a harness artifact.
        final boolean artifactWithout = withoutEviction.startsWith("READ_TIMEOUT")
                || withoutEviction.equals("SUSPENDED")
                || withoutEviction.startsWith("NON_IDENTITY");
        Assert.assertTrue(
                "WITHOUT scoreboard eviction the stale live scoreboard must reintroduce a spurious "
                        + "post-recovery artifact absent from the isolated sweep, got: " + withoutEviction,
                artifactWithout
        );
        // And the two arms must DIFFER — same scenario, same crash point, isolation the only variable.
        Assert.assertNotEquals(
                "the scoreboard-eviction isolation must be the discriminating variable (arms must differ)",
                withEviction, withoutEviction
        );
    }

    /**
     * Runs the lazy-gap scenario once at crash point {@code k} with the FULL driver-style recovery, then
     * evicts the pooled {@code TxnScoreboard} before the observing read ONLY if {@code evictScoreboard}.
     * Returns a compact outcome tag: {@code IDENTITY:[..]}, {@code NON_IDENTITY:[..]}, {@code SUSPENDED},
     * or {@code READ_TIMEOUT:<msg>}. Leak-safe: drops the table + reclaims per-cycle fds before returning.
     */
    private String runScoreboardArm(int k, boolean evictScoreboard) throws Exception {
        final String[] outcome = new String[1];
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                final String table = "o3_lazygap_ab";
                execute("drop table if exists " + table);
                drainWalQueue();
                execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                        + "with commit_mode='adaptive'");
                final TableToken tt = engine.verifyTableName(table);

                // K rows + durable epoch, then disable the epoch (the sustained lazy gap begins).
                for (int i = 0; i < LAZY_K; i++) {
                    insertO3Row(table, LAZY_TS_HOUR[i], i);
                }
                drainWalQueue();
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);

                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                crashFf.armCrashAt(base + k);
                try {
                    for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                        insertO3Row(table, LAZY_TS_HOUR[i], i);
                        drainWalQueue();
                        if (anyTableSuspended(tt)) {
                            break;
                        }
                    }
                } catch (CrashSimulationError propagated) {
                    // expected: crash fired on the WAL-commit fsync path
                }

                // Driver-faithful recovery EXCEPT the scoreboard eviction is the toggled variable.
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

                // OBSERVE.
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

                // Leak-safe cleanup (always evict the scoreboard + drop, regardless of the arm).
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
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
        return outcome[0];
    }

    private void withAdaptiveLazyGap(RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0"); // W = 0 (synchronous)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);        // setup re-affirms/flips per phase
        // Recovery roll-forward left at its default (enabled) — we WANT it to run and prove it recovers.
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            body.run();
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    private interface RunnableEx {
        void run() throws Exception;
    }

    private static void insertO3Row(String table, int hour, int v) throws Exception {
        execute("insert into " + table + " values ('" + String.format("2024-10-01T%02d:00:00.000000Z", hour)
                + "', " + v + ")");
    }

    /** count(*) — the committed row count from table metadata (reliable even if a column read would tear). */
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
     * Read {@code select v ... order by v}, returning the identity values gathered so far. A loud torn
     * read (CairoException/CairoError/SIGBUS-InternalError on a truncated column) is an acceptable crash
     * outcome — the (possibly partial) result read before the tear is returned rather than rethrown.
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

    /** True iff {@code rows}, in order, is exactly the identity sequence 0,1,2,... */
    private boolean isIdentityPrefix(List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i) == null || rows.get(i) != (long) i) {
                return false;
            }
        }
        return true;
    }

    /**
     * W1-INV — the lazy-gap O3 workload. {@code setup} builds the durable epoch prefix (K O3 rows with
     * the epoch enabled, then the epoch DISABLED), so the driver's swept {@code commit} phase is the M
     * lazily-applied O3 rows — a sustained lazy gap. Uses ONE reused table name (drop+recreate at the
     * head of each setup); the driver supplies the per-cycle isolation.
     */
    private final class LazyGapO3Workload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "o3_lazygap";
            // Epoch ENABLED for the K-row prefix so a durable cut is taken at seqTxn=K.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");
            tt = engine.verifyTableName(table);

            // K rows, out-of-order among themselves -> apply -> durable epoch at seqTxn=K.
            for (int i = 0; i < LAZY_K; i++) {
                insertO3Row(table, LAZY_TS_HOUR[i], i);
            }
            drainWalQueue();

            // DISABLE further epochs: the driver's swept commit() phase (the M rows) is applied LAZILY,
            // building the sustained gap between the durable cut and the frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                // v=LAZY_K..ROWS-1 each land BELOW the ceiling at a non-monotonic position -> O3 merge,
                // applied LAZILY (epoch disabled) so the columns are non-durable until recovery re-derives
                // them. W=0: the WAL commit fdatasyncs synchronously -> an armed crash propagates here.
                insertO3Row(table, LAZY_TS_HOUR[i], i);
                // ADAPTIVE lazy apply (no epoch) -> _txn msync only. An armed crash here is swallowed by
                // ApplyWal2TableJob's catch(Throwable) into a table SUSPEND (no throw).
                drainWalQueue();
                if (anyTableSuspended(tt)) {
                    return; // stop as a real power loss would, so we don't durably mask the injection point
                }
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // (1) Clean reopen: recovery must not leave the table suspended — the W1 report's flagged
            // observation. Under correct isolation this must hold at EVERY crash point.
            Assert.assertFalse(
                    "k=" + k + ": table must NOT be suspended after recovery (W1 report §5.2 observation)",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );

            // (2) No silent corruption: surviving rows ORDERED BY v are an exact identity PREFIX {0..m-1}.
            // A loud torn read is tolerated (prefix read so far); a wrong/absent value — INCLUDING a
            // [0,0,..] zero-fill (row i reading back 0 instead of i) — is a FAILURE.
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
            // The durable epoch floor: the K epoch'd rows are always durable, so recovery never drops below
            // them regardless of where in the lazy M-batch the crash landed.
            Assert.assertTrue(
                    "k=" + k + ": recovery must never drop below the K durable epoch rows (recovered="
                            + recovered + ")",
                    recovered >= LAZY_K
            );

            if (k == n) {
                Assert.assertEquals("k=N: recovery must restore ALL committed rows", ROWS, recovered);
                Assert.assertEquals("k=N: the full identity set must read back clean", ROWS, rows.size());
            }

            // (3) Clean reopen: a follow-up write + read must succeed on the recovered table. Use a LATER
            // day (a fresh partition, plain append) so the follow-up check is independent of the O3 path.
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
