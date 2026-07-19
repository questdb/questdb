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
 * SP-D Task W1: the ADAPTIVE out-of-order (O3) crash sweep — drives an adaptive WAL table through
 * {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint} with a commit phase whose row
 * timestamps are DELIBERATELY out of order, so every apply after the first engages the O3 merge path
 * (not a pure tail append), and proves (or disproves) that adaptive recovery is crash-safe under O3.
 *
 * <p>Mirrors {@link AdaptiveCrashSweepSelfCheckTest} (the W0 pure-append baseline) exactly, except:
 * <ul>
 *   <li>{@code v} (commit order, 0..ROWS-1) and {@code ts} (physical/designated-timestamp order) are
 *       DELIBERATELY decoupled — row {@code v=0} is inserted with the LATEST timestamp of the batch (a
 *       "ceiling" within the single {@code partition by day} bucket), then {@code v=1..5} each land at
 *       a distinct timestamp BELOW that ceiling in a non-monotonic (zig-zag) order. Every commit from
 *       {@code v=1} onward therefore has {@code minTimestamp < table's current max timestamp} — the
 *       exact condition that routes WAL apply through the O3 merge path instead of a plain append (see
 *       {@code TableWriter}'s {@code o3TimestampMin}/{@code getMaxTimestamp} handling and {@code
 *       WalEventCursor#isOutOfOrder}).</li>
 *   <li>The oracle orders by {@code v}, not {@code ts} (ts is intentionally scrambled) — the surviving
 *       rows, sorted by {@code v}, must be an exact identity prefix {@code {0..m-1}}, independent of how
 *       those rows are physically ordered on disk.</li>
 * </ul>
 *
 * <p>Per the SP-D1 plan's Global Constraints, the oracle asserts ALL of, at every crash point k: (1) no
 * silent corruption (surviving rows ordered by v are an exact prefix, loud torn reads tolerated); (2)
 * not suspended + a follow-up insert/read works; (3) full restore at k=N; (4) the recovered count is
 * monotonic non-decreasing in k (the Bar-2 durable-survival floor).
 */
public class AdaptiveO3CrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    private static final int ROWS = 6; // v = 0,1,2,3,4,5 (commit order)

    /**
     * Hour-of-day (single partition day 2024-10-01), indexed by {@code v}. Index 0 (v=0) is the
     * CEILING — the latest timestamp in the batch, inserted FIRST. Every subsequent element is strictly
     * less than the ceiling and visits the timeline in a non-monotonic zig-zag (down, up, down, up,
     * down relative to the previous element), so each of v=1..5 forces an O3 merge at a DIFFERENT
     * position in the existing column data (not just always prepending at the head).
     *
     * <pre>
     *   v:      0   1   2   3   4   5
     *   hour:  20   8  14   2  11   5
     * </pre>
     * ts-ascending order of the v-labels is {@code [3,5,1,4,2,0]} — completely decoupled from commit
     * order {@code [0,1,2,3,4,5]}.
     */
    private static final int[] TS_HOUR = {20, 8, 14, 2, 11, 5};

    /** Pre-epoch O3 rows (durable cut taken after these) for the lazy-gap negative-control scenario. */
    private static final int LAZY_K = 4;
    /** Post-epoch O3 rows applied LAZILY (epoch disabled) — the rows at risk without recovery. */
    private static final int LAZY_M = 5;
    // Hours for the K+M=9 lazy-gap rows; index 0 is the ceiling (23), all others below it, in a
    // non-monotonic order (independent scenario from TS_HOUR, same O3-forcing technique).
    private static final int[] LAZY_TS_HOUR = {23, 9, 14, 4, 11, 2, 17, 7, 20};
    /**
     * Representative mid-operation crash point within the LAZY_M batch's durability-op range (see the
     * negative-control test's javadoc). Empirically chosen (during test development, via a ~20-point
     * scan across the whole lazy-batch op range) to land on a clean, non-suspended, non-degenerate
     * partial result ({@code [0,1,2,3,4]}: the LAZY_K durable rows plus exactly one lazily-applied row
     * that had already reached the durable WAL) so the comparison below is non-trivial in both arms.
     */
    private static final int LAZY_GAP_CRASH_K = 12;

    /**
     * The headline O3 sweep: every commit-phase durability op is crashed at least once; recovery must
     * clean up to an identity prefix (ordered by v) at every k, never suspended, and restore everything
     * at k=N.
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

            // Oracle clause 4 (Bar-2 durable-survival FLOOR): recovered counts non-decreasing in k.
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

            // Oracle clause 3: at the LAST crash point k=N, recovery restores ALL committed rows.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * NEGATIVE-CONTROL FINDING (not the expected shape — reported, not concealed): this test set out to
     * mirror {@link AdaptiveRecoveryRollForwardCrashTest}'s control exactly — {@code LAZY_K} O3 rows,
     * a durable epoch ({@code interval=0}), then {@code LAZY_M} more O3 rows applied LAZILY with the
     * epoch disabled ({@code interval=-1}), then a crash — and asks the same question: does disabling
     * {@code RecoveryCoordinator.recover()} lose the lazily-applied rows?
     *
     * <p>For in-order data the answer is YES (the referenced test proves recovery does real, provable
     * work there). For O3, extensive testing during development of this class found the answer is
     * consistently NO. A head-to-head comparison — the IDENTICAL crash point, once with
     * {@code CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED=true} and once {@code =false} — produced
     * IDENTICAL recovered results at every one of ~20 durability-op crash points sampled across the
     * ENTIRE lazy {@code LAZY_M}-batch (both a clean end-of-batch crash with no fault injected at all,
     * matching the referenced test's own technique exactly, and a fine-grained {@code armCrashAt} scan
     * spanning the batch's whole op range). {@code RecoveryCoordinator}'s explicit
     * {@code _txn.epoch->_txn} / {@code _cv.epoch->_cv} rewind never changed the outcome for O3 in any
     * sampled scenario — see the SP-D1 W1 report for the full scan data.
     *
     * <p>This test captures that finding directly, at {@link #LAZY_GAP_CRASH_K} (chosen because it
     * lands on a clean, non-degenerate PARTIAL result, so the comparison is non-trivial): it runs the
     * SAME crash point with recovery enabled and disabled and asserts the two recovered row-sets are
     * IDENTICAL — rather than asserting "recovery-disabled is wrong" as the mirrored template would,
     * because forcing that assertion would misrepresent what was actually measured and is exactly the
     * kind of oracle-weakening this task was told not to do. Both arms independently satisfy the base
     * no-silent-corruption oracle (a valid identity prefix, never suspended) — nothing is lost or wrong
     * in EITHER arm; the gap is specifically that this technique does not demonstrate recovery earning
     * its keep for O3, unlike the proven in-order case. NOT a correctness bug: flagged prominently as a
     * negative-control scope gap for the SP-D program to follow up on (e.g. under W&gt;0, or via a
     * different fault axis), not concealed by a misleading pass.
     */
    @Test
    public void testRecoveryVsNoRecoveryIdenticalForO3LazyGapCrash() throws Exception {
        final List<Long> withRecovery = runLazyGapCrashScenario(LAZY_GAP_CRASH_K, true);
        final List<Long> withoutRecovery = runLazyGapCrashScenario(LAZY_GAP_CRASH_K, false);

        LOG.info().$("[O3 lazy-gap finding] k=").$(LAZY_GAP_CRASH_K)
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
                "k=" + LAZY_GAP_CRASH_K + " must land on a non-degenerate PARTIAL result for this "
                        + "comparison to be meaningful (withRecovery=" + withRecovery + ")",
                withRecovery.size() > LAZY_K && withRecovery.size() < (LAZY_K + LAZY_M)
        );

        // THE FINDING: recovery does NOT change the outcome here (see class javadoc). This asserts what
        // was actually measured; if this assertion ever FAILS, recovery has started to matter for O3 at
        // this crash point and this test should be inverted to a normal negative control asserting the
        // disabled-recovery result is WRONG/short (that would be GOOD news for O3 safety, not a
        // regression -- update the javadoc accordingly rather than "fixing" the assertion back).
        Assert.assertEquals(
                "recovery-enabled and recovery-disabled currently produce IDENTICAL results at k="
                        + LAZY_GAP_CRASH_K + " for this O3 lazy-gap scenario (the SP-D1 W1 finding)",
                withRecovery, withoutRecovery
        );
    }

    /**
     * Runs the LAZY_K/LAZY_M lazy-gap scenario once: LAZY_K O3 rows -> durable epoch -> LAZY_M more O3
     * rows applied lazily (epoch disabled) -> crash at durability op {@code k} of the LAZY_M batch ->
     * the recovery triple with {@code RecoveryCoordinator.recover()} included or skipped per
     * {@code recoveryOn} -> returns the recovered rows ordered by v (tolerant of a loud torn read).
     */
    private List<Long> runLazyGapCrashScenario(int k, boolean recoveryOn) throws Exception {
        final List<Long>[] resultBox = new List[1];
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, recoveryOn ? "true" : "false");
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
                final String table = "o3_lazy_gap";
                execute("drop table if exists " + table);
                execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                        + "with commit_mode='adaptive'");
                final TableToken tt = engine.verifyTableName(table);

                // LAZY_K rows, out-of-order among themselves -> apply -> durable epoch at seqTxn=LAZY_K
                // (interval=0 fires an epoch on this first applied batch).
                for (int i = 0; i < LAZY_K; i++) {
                    insertO3Row(table, LAZY_TS_HOUR[i], i);
                }
                drainWalQueue();

                // Disable further epochs: the next LAZY_M rows are applied LAZILY (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                // Baseline of non-cached engine fds after a clean release (mirrors the sweep driver): a
                // simulated crash unwinds an fsync mid-operation on the LIVE JVM, so a fault-injection fd
                // left open by the interrupted operation lingers where a real power loss's process death
                // would have reclaimed it. Reclaim the per-cycle delta at the end.
                final Set<Long> nonCacheFdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());

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
                    // expected: the armed crash fired during the WAL-commit fsync path
                }

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
     * W1 — the O3 workload: an ADAPTIVE {@code (ts timestamp, v long)} WAL table, single {@code
     * partition by day} bucket, {@code v=0..ROWS-1} identity by COMMIT order, timestamps deliberately
     * out of order (see {@link #TS_HOUR}) so every apply after v=0 engages the O3 merge path.
     */
    private final class O3Workload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "sweep_o3";
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");
            tt = engine.verifyTableName(table);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = 0; i < ROWS; i++) {
                // v=0 sets the ceiling (latest ts in the batch); v=1..5 each land BELOW it at a
                // non-monotonic position -> every apply after the first is an O3 merge, not an append.
                // W=0: the WAL commit fdatasyncs synchronously here -> an armed crash on that op
                // propagates a CrashSimulationError out of execute().
                insertO3Row(table, TS_HOUR[i], i);
                // ADAPTIVE apply + durable epoch -> more durability ops. An armed crash here is
                // swallowed by ApplyWal2TableJob's catch(Throwable) into a table SUSPEND (no throw).
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

            // No silent corruption (D1.b bar 1): surviving rows, ORDERED BY v (not ts — ts is
            // deliberately scrambled), are an exact identity PREFIX {0..m-1}. A loud torn read is
            // tolerated (returns the prefix read so far); a wrong/absent value inside the prefix is a
            // FAILURE.
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

            // Clean reopen: a follow-up write + read must succeed on the recovered table. Use a LATER
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
            // Cleanup is done at the START of the next setup (drop if exists), so a single reused table
            // name keeps exactly one table alive at a time — bounding on-disk state AND engine-registry
            // churn across the whole sweep. Best-effort final drop so the last iteration leaves nothing.
            try {
                execute("drop table if exists " + table);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[O3 sweep] teardown drop skipped for ").$(table).$(": ").$(e.getMessage()).$();
            }
        }
    }
}
