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
 * SP-D Task W2: the ADAPTIVE <b>indexed-symbol lazy-gap</b> crash sweep — drives an adaptive WAL table
 * with a {@code symbol index} column with a sustained lazy gap (K durable-epoch rows -&gt; disable epoch
 * -&gt; M more rows applied LAZILY) through {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint},
 * and asks whether adaptive recovery is crash-safe not just for row DATA but for the SYMBOL DICTIONARY and
 * its INDEX after a rewind + partial WAL roll-forward.
 *
 * <p>Mirrors {@link AdaptiveO3LazyGapCrashSweepTest} (the proven W1-INV lazy-gap template) structurally —
 * same K/M lazy-gap technique, same driver, same base oracle (identity prefix by {@code v}, not suspended,
 * full restore at k=N, monotonic floor) — but the schema is {@code (ts timestamp, s symbol index, v long)}
 * with {@code s} cycling a small rotating set ({@code {"a","b","c"}}, by {@code v % 3}) so BOTH the symbol
 * dictionary and its bitmap index grow across the lazy gap, and adds the schema-specific oracle clause this
 * task exists to check: INDEX/DATA CONSISTENCY (SP-D1 plan Task W2, review watch-item i).
 *
 * <h3>Why plain (non-O3) timestamps here, unlike the W1/W1-INV template</h3>
 * W1 already isolates the O3-merge-path variable for adaptive recovery. Conflating O3 forcing with the
 * symbol/index variable this task targets would make a future RED result ambiguous (an O3-merge bug vs a
 * symbol/index recovery bug). This workload therefore uses plain ascending per-hour timestamps (a pure
 * tail append every commit, single {@code partition by day} bucket) so a bug found here is unambiguously
 * about indexed-symbol lazy-gap recovery, not O3.
 *
 * <h3>The index/data-consistency oracle (this task's raison d'être)</h3>
 * At every crash point, beyond the base identity-prefix-by-{@code v} check (which here ALSO asserts each
 * surviving row's {@code s} equals the exact symbol it was committed with — a wrong symbol string is
 * dictionary/aux corruption), the oracle additionally reads, for EACH of the three symbol values, {@code
 * select v from t where s = '<val>' order by v} (the INDEXED path) and compares it against the expected
 * set computed from the SAME non-indexed full-table scan already used for the base identity check. Never
 * MORE than expected (an index pointing at a rewound-away/phantom row); never SILENTLY fewer than expected
 * absent a loud torn-read exception (a missed surviving row); never a wrong value at any position (index/data
 * skew). The three per-symbol counts must also sum to the non-indexed scan's total surviving count.
 */
public class AdaptiveIndexedSymbolLazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    /**
     * The rotating symbol set; row v's symbol is {@code SYMBOLS[v % SYMBOLS.length]}.
     */
    private static final String[] SYMBOLS = {"a", "b", "c"};

    /**
     * Pre-epoch rows: a durable epoch is taken after these (interval=0 on the first applied batch).
     */
    private static final int LAZY_K = 4;
    /**
     * Post-epoch rows applied LAZILY (epoch disabled) — the sustained lazy gap this sweep crashes into.
     */
    private static final int LAZY_M = 5;
    /**
     * Total committed rows once the whole gap is applied. v=0..ROWS-1 is also the commit-order identity.
     */
    private static final int ROWS = LAZY_K + LAZY_M; // 9 -> 3 rows per symbol (evenly distributed by v%3)

    /**
     * THE HEADLINE, ISOLATED indexed-symbol lazy-gap sweep. Builds a durable epoch at seqTxn=LAZY_K,
     * disables further epochs, then sweeps a crash across EVERY durability op of the LAZY_M lazily-applied
     * rows, running the full recovery triple and oracle at each. Asserts, at every crash point k:
     * <ol>
     *   <li>the table is NOT suspended after recovery;</li>
     *   <li>no silent corruption — surviving rows ordered by {@code v} are an exact identity prefix
     *       {@code {0..m-1}}, AND each surviving row's symbol matches {@code SYMBOLS[v % 3]} exactly (a
     *       loud torn read tolerated; a wrong/absent {@code v} OR a wrong symbol string is a FAILURE);</li>
     *   <li><b>index/data consistency</b> — see the class javadoc;</li>
     *   <li>a follow-up write+read succeeds on the recovered table (including extending the symbol
     *       dictionary with a brand-new distinct value);</li>
     *   <li>the recovered count is monotonic non-decreasing in k, rising from a short prefix to the full
     *       set, and equals ALL {@code ROWS} at k=N.</li>
     * </ol>
     * A suspended table, a zero-fill/gap, a wrong symbol, or an index/data skew here would be a real
     * adaptive indexed-symbol lazy-gap recovery bug.
     */
    @Test
    public void testIndexedSymbolLazyGapSweepRecoversCleanlyAtEveryCrashPoint() throws Exception {
        withAdaptiveLazyGap(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final SweepResult r = forEachAdaptiveCrashPoint(new LazyGapIndexedSymbolWorkload());

            LOG.info().$("[indexed-symbol lazy-gap sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("default cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);

            // Oracle clause (Bar-2 durable-survival FLOOR): recovered counts non-decreasing in k.
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // A genuine rise, not a degenerate all-full sweep.
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );
            // The durable epoch floor: even the earliest crash point keeps at least the LAZY_K epoch'd rows.
            Assert.assertTrue(
                    "every crash point must recover at least the LAZY_K durable epoch rows (floor)",
                    r.recoveredByK()[1] >= LAZY_K
            );

            // At the LAST crash point k=N, recovery restores ALL committed rows.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * A representative REWINDING crash point within the LAZY_M batch's durability-op range: recovery must
     * rewind {@code _txn}/{@code _cv} to the LAZY_K durable-epoch cut and re-derive only PART of the gap
     * from the durable WAL, landing on a genuine, non-degenerate PARTIAL result (more than LAZY_K, fewer
     * than ROWS) — chosen from this class's own sweep staircase (see {@link
     * #testIndexedSymbolLazyGapSweepRecoversCleanlyAtEveryCrashPoint}'s logged {@code recoveredByK}) the
     * same way {@code AdaptiveO3LazyGapCrashSweepTest.SCOREBOARD_REWIND_CRASH_K} and {@code
     * AdaptiveO3CrashSweepTest.LAZY_GAP_CRASH_K} were.
     */
    private static final int REPRESENTATIVE_REWIND_CRASH_K = 30;

    /**
     * NEGATIVE CONTROL: does {@code RecoveryCoordinator.recover()} (the {@code _txn}/{@code _cv}
     * epoch-rewind + WAL roll-forward) do real work for an indexed-symbol lazy gap, or — as W1 found for
     * O3's copy-on-write merge path — is the outcome identical with recovery disabled? Symbol columns are
     * IN-PLACE appends (no copy-on-write partition versioning), so the a-priori expectation is that
     * disabling recovery genuinely loses/torns the lazily-applied rows here, unlike O3.
     *
     * <p>Mirrors {@link AdaptiveRecoveryRollForwardCrashTest}'s tolerance model exactly (its javadoc:
     * "Acceptable torn outcomes: ... MISSING/WRONG (e.g. read back as zeros), a loud read error, fewer
     * rows, or a suspended table" for the recovery-DISABLED arm — the strict identity+symbol-prefix bar
     * applies ONLY to the recovery-ENABLED arm, the supported/default configuration). The ONLY unacceptable
     * outcome for the disabled arm is the FULL correct result (which would mean recovery did not matter
     * here, mirroring the O3 finding instead).
     */
    @Test
    public void testRecoveryVsNoRecoveryForIndexedSymbolLazyGapCrash() throws Exception {
        final VsAndSyms withRecovery = runLazyGapCrashScenario(REPRESENTATIVE_REWIND_CRASH_K, true);
        final VsAndSyms withoutRecovery = runLazyGapCrashScenario(REPRESENTATIVE_REWIND_CRASH_K, false);

        LOG.info().$("[indexed-symbol lazy-gap negative control] k=").$(REPRESENTATIVE_REWIND_CRASH_K)
                .$(" withRecovery.vs=").$(withRecovery.vs.toString())
                .$(" withRecovery.syms=").$(withRecovery.syms.toString())
                .$(" withoutRecovery.vs=").$(withoutRecovery.vs.toString())
                .$(" withoutRecovery.syms=").$(withoutRecovery.syms.toString()).$();

        // WITH recovery (the supported/default configuration): must be a fully valid, correct
        // identity+symbol prefix — a failure here would be a real bug in the ENABLED-recovery path itself,
        // not merely a negative-control finding.
        assertValidIdentityAndSymbolPrefix("withRecovery", withRecovery);
        Assert.assertTrue(
                "k=" + REPRESENTATIVE_REWIND_CRASH_K + " (recovery enabled) must land on a non-degenerate "
                        + "PARTIAL result for this comparison to be meaningful (withRecovery.vs="
                        + withRecovery.vs + ")",
                withRecovery.vs.size() > LAZY_K && withRecovery.vs.size() < ROWS
        );

        // WITHOUT recovery: disabled-recovery must not show MORE surviving rows than recovery-enabled at
        // the identical crash point (a sanity bound; more rows without recovery than with it would be a
        // different, stranger anomaly, not ordinary data loss).
        Assert.assertTrue(
                "recovery-disabled must not show MORE surviving rows than recovery-enabled at the same "
                        + "crash point (withoutRecovery.vs=" + withoutRecovery.vs
                        + ", withRecovery.vs=" + withRecovery.vs + ")",
                withoutRecovery.vs.size() <= withRecovery.vs.size()
        );

        // THE FINDING: measured directly (see the report) — disabling recovery does NOT reproduce the full
        // correct result; the lazily-applied rows are lost/WRONG (a zero-fill shape: rows read back with a
        // v of 0 instead of their real committed value), exactly the "expected negative-control shape" the
        // task anticipated for in-place symbol appends (unlike O3's copy-on-write self-heal). If this
        // assertion ever starts FAILING (the two arms become identical), that would mean recovery has
        // stopped demonstrably mattering here — mirroring the O3 finding instead — and this test should be
        // re-documented accordingly (good news, not a regression) rather than "fixed" back.
        final boolean fullCorrect = withoutRecovery.vs.equals(withRecovery.vs)
                && withoutRecovery.syms.equals(withRecovery.syms);
        Assert.assertFalse(
                "NEGATIVE CONTROL: recovery-disabled must NOT reproduce the FULL correct recovery-enabled "
                        + "result at a rewinding crash point (withoutRecovery.vs=" + withoutRecovery.vs
                        + ", syms=" + withoutRecovery.syms + ") — if this fails, recovery is not doing "
                        + "demonstrable work for indexed-symbol lazy-gap at this crash point (would mirror "
                        + "the O3 finding; re-classify as a scope gap, not a bug, per spd-w1-report.md §3)",
                fullCorrect
        );
    }

    /**
     * Runs the LAZY_K/LAZY_M lazy-gap scenario once: LAZY_K rows -&gt; durable epoch -&gt; LAZY_M more rows
     * applied lazily (epoch disabled) -&gt; crash at durability op {@code k} of the LAZY_M batch -&gt; the
     * recovery triple with {@code RecoveryCoordinator.recover()} included or skipped per {@code
     * recoveryOn} -&gt; returns the recovered (v, s) pairs ordered by v (tolerant of a loud torn read).
     * Leak-safe: drops the table + reclaims per-cycle fds before returning, mirroring the sweep driver.
     */
    private VsAndSyms runLazyGapCrashScenario(int k, boolean recoveryOn) throws Exception {
        final VsAndSyms[] resultBox = new VsAndSyms[1];
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, recoveryOn ? "true" : "false");
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
                final String table = "sym_lazygap_nc";
                execute("drop table if exists " + table);
                drainWalQueue();
                execute("create table " + table + " (ts timestamp, s symbol index, v long) timestamp(ts) "
                        + "partition by day wal with commit_mode='adaptive'");
                final TableToken tt = engine.verifyTableName(table);

                // LAZY_K rows -> apply -> durable epoch at seqTxn=LAZY_K (interval=0 fires on this batch).
                for (int i = 0; i < LAZY_K; i++) {
                    insertRow(table, i);
                }
                drainWalQueue();

                // Disable further epochs: the next LAZY_M rows are applied LAZILY (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                crashFf.armCrashAt(base + k);
                try {
                    for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                        insertRow(table, i);
                        drainWalQueue();
                        if (anyTableSuspended(tt)) {
                            break;
                        }
                    }
                } catch (CrashSimulationError propagated) {
                    // expected: the armed crash fired during the WAL-commit fsync path
                }

                // Driver-faithful recovery EXCEPT the recovery-enabled flag is the toggled variable.
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                crashFf.crash(engine.getConfiguration().getDbRoot());
                if (engine.getTableSequencerAPI().isSuspended(tt)) {
                    engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
                }
                engine.getTxnScoreboardPool().remove(tt); // fresh-restart model (see the driver's javadoc)
                if (recoveryOn) {
                    new RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                resultBox[0] = readVAndSOrderedByVAllowTorn(table);

                // Leak-safe cleanup.
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
            setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, "true");
        }
        return resultBox[0];
    }

    /**
     * Asserts {@code vas} is internally consistent: an identity prefix by v, each row's symbol correct.
     */
    private void assertValidIdentityAndSymbolPrefix(String label, VsAndSyms vas) {
        for (int i = 0; i < vas.vs.size(); i++) {
            Assert.assertNotNull(label + " row " + i + " v read back NULL (corruption)", vas.vs.get(i));
            Assert.assertEquals(
                    label + " row " + i + " silently WRONG v (not an identity prefix)",
                    (long) i, (long) vas.vs.get(i)
            );
            Assert.assertEquals(
                    label + " row " + i + " silently WRONG symbol (dictionary/aux corruption)",
                    SYMBOLS[i % SYMBOLS.length], vas.syms.get(i)
            );
        }
    }

    private void withAdaptiveLazyGap(RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0"); // W = 0 (synchronous)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);        // setup re-affirms/flips per phase
        // Recovery roll-forward left at its default (enabled) — we WANT it to run and prove it recovers.
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

    private static void insertRow(String table, int v) throws Exception {
        execute("insert into " + table + " values ('" + String.format("2024-10-01T%02d:00:00.000000Z", v)
                + "', '" + SYMBOLS[v % SYMBOLS.length] + "', " + v + ")");
    }

    /**
     * count(*) — the committed row count from table metadata (reliable even if a column read would tear).
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
     * Paired (v, s) columns read together so the symbol-fidelity and index cross-checks share one scan.
     */
    private static final class VsAndSyms {
        final List<Long> vs;
        final List<String> syms;

        VsAndSyms(List<Long> vs, List<String> syms) {
            this.vs = vs;
            this.syms = syms;
        }
    }

    /**
     * Torn-tolerant read result: the values read before any tear, and whether a loud tear occurred.
     */
    private static final class TornRead {
        final List<Long> values;
        final boolean torn;

        TornRead(List<Long> values, boolean torn) {
            this.values = values;
            this.torn = torn;
        }
    }

    /**
     * Read {@code select v, s ... order by v} (the NON-indexed full-table scan — no predicate on {@code
     * s}), returning the (v, s) pairs gathered so far. A loud torn read (CairoException/CairoError/SIGBUS
     * -InternalError on a truncated column) is an acceptable crash outcome — the (possibly partial) result
     * read before the tear is returned rather than rethrown.
     */
    private VsAndSyms readVAndSOrderedByVAllowTorn(String table) {
        final List<Long> vs = new ArrayList<>();
        final List<String> syms = new ArrayList<>();
        try (RecordCursorFactory f = select("select v, s from " + table + " order by v")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    vs.add(r.getLong(0));
                    final CharSequence s = r.getSymA(1);
                    syms.add(s == null ? null : s.toString());
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // acceptable: corruption detected loudly; return the prefix read before the tear
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return new VsAndSyms(vs, syms);
    }

    /**
     * Read {@code select v from t where s = '<sym>' order by v} — the INDEXED path — returning the values
     * gathered so far and whether a loud torn-read exception occurred (distinguished from a legitimate,
     * fully-completed short read, so the oracle can tell "torn" apart from a SILENT under/over-count).
     */
    private TornRead readVForSymbolOrderedByVAllowTorn(String table, String sym) {
        final List<Long> out = new ArrayList<>();
        boolean torn = false;
        try (RecordCursorFactory f = select("select v from " + table + " where s = '" + sym + "' order by v")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (CairoException | CairoError | InternalError e) {
            torn = true; // acceptable: corruption detected loudly; return the prefix read before the tear
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return new TornRead(out, torn);
    }

    /**
     * W2 — the indexed-symbol lazy-gap workload. {@code setup} builds the durable epoch prefix (LAZY_K
     * rows with the epoch enabled, then the epoch DISABLED), so the driver's swept {@code commit} phase is
     * the LAZY_M lazily-applied rows — a sustained lazy gap over a growing symbol dictionary + index.
     */
    private final class LazyGapIndexedSymbolWorkload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "sym_lazygap";
            // Epoch ENABLED for the LAZY_K-row prefix so a durable cut is taken at seqTxn=LAZY_K.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, s symbol index, v long) timestamp(ts) "
                    + "partition by day wal with commit_mode='adaptive'");
            tt = engine.verifyTableName(table);

            // LAZY_K rows -> apply -> durable epoch at seqTxn=LAZY_K. Plain ascending timestamps (pure
            // tail append) -- see the class javadoc for why this workload does NOT force O3.
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(table, i);
            }
            drainWalQueue();

            // DISABLE further epochs: the driver's swept commit() phase (the LAZY_M rows) is applied
            // LAZILY, building the sustained gap between the durable cut and the frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                // v=LAZY_K..ROWS-1, applied LAZILY (epoch disabled) so the columns (data, symbol dict,
                // index) are non-durable until recovery re-derives them. W=0: the WAL commit fdatasyncs
                // synchronously -> an armed crash propagates here.
                insertRow(table, i);
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
            // (1) Clean reopen: recovery must not leave the table suspended.
            Assert.assertFalse(
                    "k=" + k + ": table must NOT be suspended after recovery",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );

            // (2) No silent corruption: surviving rows ORDERED BY v are an exact identity PREFIX {0..m-1},
            // AND each surviving row's SYMBOL matches SYMBOLS[v % 3] exactly. A loud torn read is tolerated
            // (prefix read so far); a wrong/absent v OR a wrong symbol string is a FAILURE.
            final VsAndSyms base = readVAndSOrderedByVAllowTorn(table);
            final int m = base.vs.size();
            for (int i = 0; i < m; i++) {
                Assert.assertNotNull("k=" + k + " row " + i + " v read back NULL (corruption)", base.vs.get(i));
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG v (not an identity prefix ordered by v) — "
                                + "a zero-fill/gap here is the suspected recovery bug",
                        (long) i, (long) base.vs.get(i)
                );
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG symbol (dictionary/aux corruption)",
                        SYMBOLS[i % SYMBOLS.length], base.syms.get(i)
                );
            }

            // Recovered committed-row count from the metadata (reliable even if a column read tore).
            final int recovered = (int) rowCount(table);
            Assert.assertTrue(
                    "k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                    m <= recovered
            );
            // The durable epoch floor: the LAZY_K epoch'd rows are always durable, so recovery never drops
            // below them regardless of where in the lazy M-batch the crash landed.
            Assert.assertTrue(
                    "k=" + k + ": recovery must never drop below the LAZY_K durable epoch rows (recovered="
                            + recovered + ")",
                    recovered >= LAZY_K
            );

            // (3) INDEX/DATA CONSISTENCY — for each symbol value, the indexed lookup must return EXACTLY
            // the v's committed with that symbol among the m surviving (base-scan-confirmed) rows: never
            // MORE (an index pointing at a rewound-away/phantom row -> CORRUPTION); absent a loud torn-read
            // exception on the indexed query itself, never fewer either (a SILENTLY missed surviving row ->
            // CORRUPTION); and never a wrong value at any position (index/data skew -> CORRUPTION).
            int sumIndexed = 0;
            boolean anySymbolTorn = false;
            for (String sym : SYMBOLS) {
                final List<Long> expected = new ArrayList<>();
                for (int i = 0; i < m; i++) {
                    if (sym.equals(SYMBOLS[i % SYMBOLS.length])) {
                        expected.add((long) i);
                    }
                }
                final TornRead actual = readVForSymbolOrderedByVAllowTorn(table, sym);
                anySymbolTorn |= actual.torn;
                Assert.assertTrue(
                        "k=" + k + " symbol '" + sym + "': index returned MORE rows (" + actual.values
                                + ") than committed/surviving for this symbol (expected " + expected
                                + ") — index points at a rewound-away or phantom row (INDEX CORRUPTION)",
                        actual.values.size() <= expected.size()
                );
                for (int j = 0; j < actual.values.size(); j++) {
                    Assert.assertEquals(
                            "k=" + k + " symbol '" + sym + "' position " + j + ": index returned WRONG v "
                                    + "(index/data skew — INDEX CORRUPTION). expected=" + expected
                                    + " actual=" + actual.values,
                            expected.get(j), actual.values.get(j)
                    );
                }
                if (!actual.torn) {
                    Assert.assertEquals(
                            "k=" + k + " symbol '" + sym + "': index SILENTLY missed/over-reported surviving "
                                    + "rows (no torn-read exception thrown) — index/data skew (INDEX "
                                    + "CORRUPTION). expected=" + expected + " actual=" + actual.values,
                            expected.size(), actual.values.size()
                    );
                    sumIndexed += actual.values.size();
                }
            }
            // Cross-check the total via the NON-indexed scan: if no per-symbol query tore, the three
            // indexed per-symbol counts must sum to exactly the non-indexed scan's surviving row count (m).
            if (!anySymbolTorn) {
                Assert.assertEquals(
                        "k=" + k + ": sum of indexed per-symbol counts (" + sumIndexed + ") must equal the "
                                + "non-indexed scan's surviving row count (" + m + ") — index/data skew",
                        m, sumIndexed
                );
            }

            if (k == n) {
                Assert.assertEquals("k=N: recovery must restore ALL committed rows", ROWS, recovered);
                Assert.assertEquals("k=N: the full identity set must read back clean", ROWS, m);
            }

            // (4) Clean reopen: a follow-up write + read must succeed on the recovered table, INCLUDING
            // extending the (possibly recovered) symbol dictionary with a brand-new distinct value. Use a
            // LATER day (a fresh partition, plain append) so the follow-up check is independent of the
            // lazy-gap path.
            execute("insert into " + table + " values ('2024-10-09T00:00:00.000000Z', 'z', 999)");
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
                LOG.info().$("[indexed-symbol lazy-gap sweep] teardown drop skipped for ").$(table).$(": ")
                        .$(e.getMessage()).$();
            }
        }
    }
}
