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
import io.questdb.cairo.PartitionBy;
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
 * ADAPTIVE lazy-gap crash sweep over an indexed symbol column. A durable epoch is taken over LAZY_K rows,
 * further epochs are disabled, and LAZY_M more rows are applied lazily, so the symbol dictionary and its
 * bitmap index grow across the lazy gap. {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint}
 * crashes at every durability op of the lazy phase. At each point the oracle checks the identity prefix by
 * {@code v}, each surviving row's symbol, and that the indexed lookup for every symbol agrees with a
 * non-indexed scan after the rewind and partial WAL roll-forward.
 * <p>
 * The schema is {@code (ts timestamp, s symbol index, v long)}, with {@code s} cycling {@code {"a","b","c"}}
 * by {@code v % 3} and plain ascending timestamps in one day partition, so every apply is a tail append and
 * a failure points at indexed-symbol recovery, not O3. {@link AdaptiveO3LazyGapCrashSweepTest} covers the
 * O3 variable with the same structure.
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
     * Post-epoch rows applied lazily (epochs disabled): the lazy gap this sweep crashes into.
     */
    private static final int LAZY_M = 5;
    /**
     * Total committed rows once the whole gap is applied. v=0..ROWS-1 is also the commit-order identity.
     */
    private static final int ROWS = LAZY_K + LAZY_M; // 9 -> 3 rows per symbol (evenly distributed by v%3)

    /**
     * Builds a durable epoch at seqTxn=LAZY_K, disables further epochs, then sweeps a crash across every
     * durability op of the LAZY_M lazy rows, running the recovery triple and the oracle at each. Asserts, at
     * every crash point k:
     * <ol>
     *   <li>the table is not suspended after recovery;</li>
     *   <li>no silent corruption: surviving rows ordered by {@code v} are an exact identity prefix
     *       {@code {0..m-1}} and each row's symbol is {@code SYMBOLS[v % 3]} (a loud torn read is tolerated,
     *       a wrong or absent {@code v} or a wrong symbol fails);</li>
     *   <li>index/data consistency: the indexed lookup for each symbol returns exactly the surviving rows
     *       with that symbol (see {@link LazyGapIndexedSymbolWorkload#oracle});</li>
     *   <li>a follow-up write and read succeed on the recovered table, adding a new value to the symbol
     *       dictionary;</li>
     *   <li>the recovered count never decreases with k, rises from a short prefix and reaches all
     *       {@code ROWS} at k=N.</li>
     * </ol>
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

            // Recovered counts never decrease with k.
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

            // At the last crash point k=N, recovery restores all committed rows.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * The rewinding crash point, chosen by phase rather than by op count: this many lazy rows are applied,
     * each as its own txn, and the crash fires on the first durability op of the next lazy row's commit,
     * before that row's WAL is durable. Recovery rewinds {@code _txn}/{@code _cv} to the LAZY_K durable-epoch
     * cut and replays the applied rows from the durable WAL, a partial result ({@code LAZY_K} plus this many
     * rows) whatever the number of ops per commit.
     */
    private static final int REWIND_APPLIED_LAZY_ROWS = 2;

    /**
     * Data before pointer, for an indexed-symbol lazy gap. {@code _txn}, {@code _cv} and the index files make
     * their per-commit flush decision on the table's effective commit mode, so under ADAPTIVE the commit
     * pointer is as lazily durable as the column data it describes. After a crash in the lazy phase, and
     * before any recovery runs, {@code _txn} must therefore not claim a seqTxn beyond the durable epoch cut.
     * A {@code _txn} flushed on the instance-wide mode would claim seqTxn 6 at this crash point, past the
     * LAZY_K=4 cut, and the arm without recovery would read the lazy rows back zero-filled. The same crash
     * runs with and without {@code RecoveryCoordinator.recover()}; both arms must keep the pointer at or
     * below the cut and read back the same valid prefix. {@link AdaptiveRecoveryRollForwardCrashTest} covers
     * roll-forward itself.
     */
    @Test
    public void testCommitPointerNeverPublishedAheadOfDataForIndexedSymbolLazyGap() throws Exception {
        final VsAndSyms withRecovery = runLazyGapCrashScenario(REWIND_APPLIED_LAZY_ROWS, true);
        final VsAndSyms withoutRecovery = runLazyGapCrashScenario(REWIND_APPLIED_LAZY_ROWS, false);

        LOG.info().$("[indexed-symbol lazy-gap negative control] appliedLazyRows=").$(REWIND_APPLIED_LAZY_ROWS)
                .$(" withRecovery.vs=").$(withRecovery.vs.toString())
                .$(" withRecovery.syms=").$(withRecovery.syms.toString())
                .$(" withoutRecovery.vs=").$(withoutRecovery.vs.toString())
                .$(" withoutRecovery.syms=").$(withoutRecovery.syms.toString()).$();

        // With recovery (the default configuration) the result must be a valid identity and symbol prefix;
        // a failure here is a bug in the recovery path itself.
        assertValidIdentityAndSymbolPrefix("withRecovery", withRecovery);
        Assert.assertTrue(
                "the crash (recovery enabled) must land on a non-degenerate "
                        + "PARTIAL result for this comparison to be meaningful (withRecovery.vs="
                        + withRecovery.vs + ")",
                withRecovery.vs.size() > LAZY_K && withRecovery.vs.size() < ROWS
        );
        Assert.assertEquals(
                "the crash must land after " + REWIND_APPLIED_LAZY_ROWS + " lazy applies and before the next "
                        + "lazy row's WAL is durable (withRecovery.vs=" + withRecovery.vs + ")",
                LAZY_K + REWIND_APPLIED_LAZY_ROWS, withRecovery.vs.size()
        );

        // Sanity bound: without recovery, no more rows survive than with recovery at the same crash point.
        Assert.assertTrue(
                "recovery-disabled must not show MORE surviving rows than recovery-enabled at the same "
                        + "crash point (withoutRecovery.vs=" + withoutRecovery.vs
                        + ", withRecovery.vs=" + withRecovery.vs + ")",
                withoutRecovery.vs.size() <= withRecovery.vs.size()
        );

        // Data before pointer, asserted before the row check below so a regression reports its cause rather
        // than its symptom: with _txn at or below the durable epoch cut in both arms, no reader sees a row
        // whose column data is not durable, recovery or not.
        for (VsAndSyms arm : new VsAndSyms[]{withRecovery, withoutRecovery}) {
            Assert.assertTrue(
                    "the crash must not leave _txn claiming past the durable epoch cut: the commit pointer "
                            + "is lazily durable like the data it exposes (preRecoverySeqTxn="
                            + arm.preRecoverySeqTxn + ", durable cut=" + LAZY_K + ")",
                    arm.preRecoverySeqTxn >= 0 && arm.preRecoverySeqTxn <= LAZY_K
            );
        }

        // As a consequence the arm without recovery is correct too. A pointer ahead of its data that the
        // check above misses shows up here as a zero-filled v.
        assertValidIdentityAndSymbolPrefix("withoutRecovery", withoutRecovery);
    }

    /**
     * Runs the lazy-gap scenario once, outside the sweep driver: LAZY_K rows -&gt; durable epoch -&gt; LAZY_M
     * rows applied lazily (epochs disabled) -&gt; a crash on the first durability op of the commit of lazy row
     * {@code appliedLazyRows} (counted from 0) -&gt; the recovery triple, with
     * {@code RecoveryCoordinator.recover()} run or skipped per {@code recoveryOn}. Returns the recovered
     * (v, s) pairs ordered by v (tolerating a loud torn read) and the pre-recovery {@code _txn} seqTxn.
     * Drops the table and closes per-cycle fds before returning, as the sweep driver does.
     */
    private VsAndSyms runLazyGapCrashScenario(int appliedLazyRows, boolean recoveryOn) throws Exception {
        final VsAndSyms[] resultBox = new VsAndSyms[1];
        final long[] preRecoverySeqTxn = {-1};
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
                        + "partition by day wal");
                final TableToken tt = engine.verifyTableName(table);

                // LAZY_K rows -> apply -> durable epoch at seqTxn=LAZY_K (interval=0 fires on this batch).
                for (int i = 0; i < LAZY_K; i++) {
                    insertRow(table, i);
                }
                drainWalQueue();

                // Disable further epochs: the next LAZY_M rows are applied lazily (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
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
                        insertRow(table, i);
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
                LOG.info().$("[indexed-symbol lazy-gap negative control] recovery=").$(recoveryOn)
                        .$(" crash at k=").$(crashOp - base).$(" of the lazy batch: ").$(crashedOp).$();

                // The sweep driver's restart and recovery steps, except that recover() runs only when recoveryOn is
                // set.
                simulateRestart(new TableToken[]{tt});
                // The commit pointer as the crash left it, before anything gets a chance to repair it.
                preRecoverySeqTxn[0] = readOnDiskTxnSeqTxn(tt, PartitionBy.DAY);
                if (recoveryOn) {
                    new RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                resultBox[0] = readVAndSOrderedByVAllowTorn(table);
                resultBox[0].preRecoverySeqTxn = preRecoverySeqTxn[0];

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
        // Recovery roll-forward stays at its default (enabled), so the sweep exercises it.
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
     * count(*): the committed row count from table metadata (reliable even if a column read would tear).
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
        /**
         * The on-disk {@code _txn} seqTxn as the crash left it, sampled before any recovery ran.
         */
        long preRecoverySeqTxn = -1;

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
     * Reads {@code select v, s ... order by v} (the non-indexed full-table scan, no predicate on {@code s}) and
     * returns the (v, s) pairs gathered. A loud torn read (CairoException/CairoError/SIGBUS-InternalError on a
     * truncated column) is an acceptable crash outcome: the partial result read before the tear is returned.
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
     * Reads {@code select v from t where s = '<sym>' order by v} through the index and returns the values
     * gathered and whether a loud torn-read exception occurred, so the oracle can tell a torn read from a
     * silent under- or over-count.
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
     * The indexed-symbol lazy-gap workload. {@code setup} builds the durable epoch prefix (LAZY_K rows with
     * epochs enabled, then disables epochs), so the driver's swept {@code commit} phase is the LAZY_M rows,
     * applied lazily over a growing symbol dictionary and index.
     */
    private final class LazyGapIndexedSymbolWorkload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "sym_lazygap";
            // Epochs enabled for the LAZY_K-row prefix, so a durable cut is taken at seqTxn=LAZY_K.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, s symbol index, v long) timestamp(ts) "
                    + "partition by day wal");
            tt = engine.verifyTableName(table);

            // LAZY_K rows -> apply -> durable epoch at seqTxn=LAZY_K. Plain ascending timestamps (tail
            // append), so this workload never goes through O3.
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(table, i);
            }
            drainWalQueue();

            // Disable further epochs: the driver's swept commit() phase (the LAZY_M rows) is applied
            // lazily, building the gap between the durable cut and the frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                // v=LAZY_K..ROWS-1, applied lazily (epochs disabled), so the columns (data, symbol dictionary,
                // index) are not durable until recovery re-derives them. W=0: the WAL commit fdatasyncs
                // synchronously, so an armed crash propagates here.
                insertRow(table, i);
                // ADAPTIVE lazy apply with no epoch: the table files are not synced. An armed crash here is
                // swallowed by ApplyWal2TableJob's catch(Throwable) and suspends the table (no throw).
                drainWalQueue();
                if (anyTableSuspended(tt)) {
                    return; // stop as a real power loss would, so the injection point is not durably masked
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

            // (2) No silent corruption: surviving rows ordered by v are an exact identity prefix {0..m-1},
            // and each surviving row's symbol is SYMBOLS[v % 3]. A loud torn read is tolerated (prefix read
            // so far); a wrong or absent v or a wrong symbol string fails.
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

            // (3) Index/data consistency: for each symbol, the indexed lookup returns exactly the v values
            // committed with that symbol among the m surviving rows of the base scan. More rows means an index
            // entry for a rewound or phantom row; fewer rows without a loud torn read on the indexed query
            // means a silently missed surviving row; a wrong value at any position means index/data skew.
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
            // Cross-check the total via the non-indexed scan: if no per-symbol query tore, the three
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

            // (4) A follow-up write and read succeed on the recovered table and add a new value to the
            // recovered symbol dictionary. The row goes on a later day (a new partition, plain append), so the
            // check does not depend on the lazy-gap path.
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
