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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * SP-D Task W5: the ADAPTIVE <b>multi-partition</b> lazy-gap crash sweep — closes the one Important gap
 * the SP-D D1 whole-branch review found: W0-W4 (base O3, the O3/indexed-symbol/multi-table/mat-view
 * lazy-gap sweeps) all accidentally confined every row to a SINGLE {@code partition by day} bucket, so
 * partition-rollover-under-crash and cross-partition epoch consistency were never exercised. This sweep
 * drives rows across MULTIPLE day partitions — including brand-new partition DIRECTORIES created by
 * LAZY (non-durable) applies — through the same proven {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint}
 * driver.
 *
 * <h3>Why plain schema, forward-only appends (no O3, no symbols, no mat-view)</h3>
 * W1/W1-INV already isolate the O3-merge-path variable, W2 the indexed-symbol variable, W3 the
 * simultaneous-multi-table variable and W4 the mat-view variable — each against a SINGLE partition.
 * Conflating any of those with the multi-partition variable this task targets would make a future RED
 * result ambiguous. This workload therefore uses the simplest possible schema —
 * {@code (ts timestamp, v long)} with {@code v} = commit order and {@code ts} STRICTLY increasing (a pure
 * tail append every commit, never revisiting an earlier partition) — so any bug found here is
 * unambiguously about PARTITION ROLLOVER, not O3 merge, symbol dictionaries, or view refresh.
 *
 * <h3>The layout (mirrors the task's own worked example exactly)</h3>
 * {@code v} is commit order 0..{@link #ROWS}-1. {@link #DAY}{@code [v]} is the day-of-month (October
 * 2024) each row lands on:
 * <ul>
 *   <li><b>K = {@value #LAZY_K} durable-epoch prefix rows</b> (v=0..3): 2 rows on 2024-10-01, 2 rows on
 *       2024-10-02 — the durable epoch (interval=0 fires on this first applied batch) is therefore taken
 *       at a cut that SPANS 2 partitions, not 1.</li>
 *   <li><b>M = {@value #LAZY_M} lazily-applied rows</b> (v=4..8), epoch DISABLED: EACH lands on its OWN
 *       brand-new day (2024-10-03, -04, -05, -06, -07) — every one of the 5 lazy applies creates a
 *       brand-new partition DIRECTORY that is non-durable until recovery re-derives it.</li>
 * </ul>
 * The full committed set therefore spans <b>7 distinct day partitions</b> (days 1..7) — confirmed,
 * fault-free, by {@link #testWorkloadLayoutSpansSevenDistinctPartitions()}.
 *
 * <h3>What this answers that no single-partition sweep could</h3>
 * Recovery must rewind the TABLE-level {@code _txn}/{@code _cv} to an epoch cut spanning 2 partitions,
 * then re-derive the NEW per-partition directories (2024-10-03.. as far as the frontier reached) from
 * the durable WAL. Two distinct multi-partition-specific failure modes are in scope:
 * <ol>
 *   <li>an ORPHANED partition directory left behind after the rewind — physically present on disk
 *       (e.g. a lazily mkdir'd-but-never-synced new-partition directory the crash model does not delete,
 *       only truncates its files to zero — see {@code CrashFaultFilesFacade#crash}) but referencing data
 *       the epoch cut rewound away;</li>
 *   <li>a MISSING partition for a surviving row, or a table-level row count that silently disagrees with
 *       what a clean per-partition scan actually finds (a whole partition quietly dropped without a loud
 *       error) — the specific loophole {@link #oracle} closes beyond the base single-table identity-prefix
 *       check (see its javadoc).</li>
 * </ol>
 *
 * <h3>Relationship to the sibling lazy-gap sweeps</h3>
 * Structurally mirrors {@link AdaptiveO3LazyGapCrashSweepTest} (W1-INV), {@link
 * AdaptiveIndexedSymbolLazyGapCrashSweepTest} (W2) and {@link AdaptiveMultiTableLazyGapCrashSweepTest}
 * (W3): K durable-epoch rows -&gt; disable epoch -&gt; M more rows applied LAZILY -&gt; sweep a crash
 * across every durability op of the M-phase via the validated driver. The ONLY new variable is that the
 * K/M rows are spread across 7 partitions instead of confined to 1.
 */
public class AdaptiveMultiPartitionLazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    /** Pre-epoch rows: a durable epoch is taken after these (interval=0 on the first applied batch). */
    private static final int LAZY_K = 4;
    /** Post-epoch rows applied LAZILY (epoch disabled) — the sustained lazy gap this sweep crashes into. */
    private static final int LAZY_M = 5;
    /** Total committed rows once the whole gap is applied. */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * Day-of-month (October 2024), indexed by {@code v} (commit order 0..8). v=0..3 (the durable K-prefix)
     * span EXACTLY 2 partitions (days 1,1,2,2); v=4..8 (the lazy M-batch) each land on their OWN brand-new
     * day (3,4,5,6,7) — 5 more partitions, one created per lazy apply. Strictly non-decreasing: {@code ts}
     * only ever moves FORWARD, so every apply is a plain tail append/new-partition-create, never an O3
     * merge — the multi-partition variable is isolated from the O3 variable W1/W1-INV already cover.
     */
    private static final int[] DAY = {1, 1, 2, 2, 3, 4, 5, 6, 7};
    /** Hour-of-day paired with {@link #DAY}; only the K-prefix needs 2 distinct hours per shared day. */
    private static final int[] HOUR = {0, 12, 0, 12, 0, 0, 0, 0, 0};
    /** The full set of distinct day-of-month values the committed set spans (days 1..7). */
    private static final Set<Integer> ALL_DAYS = new TreeSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
    /** A day never used by any K/M row — the follow-up write's "yet another new partition". */
    private static final int FOLLOWUP_DAY = 20;

    /**
     * Dedicated, CRASH-FREE confirmation that the fixed {@link #DAY}/{@link #HOUR} mapping genuinely
     * produces the claimed layout before any fault-injection runs against it: the durable K-prefix spans
     * EXACTLY 2 day-partitions and each of the 5 lazily-applied M rows lands on its OWN brand-new day —
     * 7 distinct partitions in total, with per-partition row counts 2,2,1,1,1,1,1 summing to {@link #ROWS}.
     * Logged verbatim (distinct-partition count + the day set + per-day row counts) for the SP-D W5 report.
     */
    @Test
    public void testWorkloadLayoutSpansSevenDistinctPartitions() throws Exception {
        withAdaptiveLazyGap(() -> {
            final String table = "mp_layout_confirm";
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");

            for (int i = 0; i < LAZY_K; i++) {
                insertRow(table, i);
            }
            drainWalQueue();
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
            for (int i = LAZY_K; i < ROWS; i++) {
                insertRow(table, i);
                drainWalQueue();
            }

            Assert.assertEquals("total committed rows", ROWS, (int) rowCount(table));

            final PartitionScan scan = readPartitionDays(table);
            Assert.assertFalse("partition metadata read must not be torn in this fault-free confirmation", scan.torn);
            LOG.info().$("[multi-partition lazy-gap layout] distinct partitions=").$(scan.days.size())
                    .$(" days=").$(scan.days.toString()).$(" rowsByDay=").$(scan.rowsByDay.toString()).$();

            Assert.assertEquals(
                    "the durable K-prefix + M lazy rows must together span exactly 7 distinct day "
                            + "partitions (days 1..7), confirming rows genuinely land in distinct partitions",
                    ALL_DAYS, scan.days
            );
            Assert.assertEquals("day 1 (K-prefix) numRows", Long.valueOf(2L), scan.rowsByDay.get(1));
            Assert.assertEquals("day 2 (K-prefix) numRows", Long.valueOf(2L), scan.rowsByDay.get(2));
            for (int day = 3; day <= 7; day++) {
                Assert.assertEquals("day " + day + " (lazy M-row) numRows", Long.valueOf(1L), scan.rowsByDay.get(day));
            }

            execute("drop table if exists " + table);
            drainWalQueue();
        });
    }

    /**
     * THE HEADLINE multi-partition lazy-gap sweep. Builds a durable epoch at seqTxn=LAZY_K spanning 2
     * partitions, disables further epochs, then sweeps a crash across EVERY durability op of the 5
     * lazily-applied M rows — each of which creates a brand-new partition directory — running the full
     * recovery triple and oracle at each. Asserts, at every crash point k:
     * <ol>
     *   <li>the table is NOT suspended after recovery;</li>
     *   <li>no silent corruption — surviving rows ordered by {@code v} are an exact identity prefix
     *       {@code {0..m-1}} (a loud torn read tolerated; a wrong/absent value is a FAILURE);</li>
     *   <li>[multi-partition-specific — see {@link #oracle}] the set of partitions physically present
     *       corresponds EXACTLY to the surviving rows, and a CLEAN read shows EXACTLY the committed count
     *       (no silently-dropped trailing partition);</li>
     *   <li>a follow-up write+read succeeds on the recovered table, landing in YET ANOTHER brand-new
     *       partition;</li>
     *   <li>the recovered count is monotonic non-decreasing in k, reaching ALL {@link #ROWS} rows across
     *       ALL 7 partitions at k=N.</li>
     * </ol>
     * A suspended table, a zero-fill/gap, an orphaned partition directory, or a silently-dropped partition
     * would be a real adaptive multi-partition rollover recovery bug — a candidate GA-blocker.
     */
    @Test
    public void testMultiPartitionLazyGapSweepRecoversCleanlyAtEveryCrashPoint() throws Exception {
        withAdaptiveLazyGap(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final SweepResult r = forEachAdaptiveCrashPoint(new LazyGapMultiPartitionWorkload());

            LOG.info().$("[multi-partition lazy-gap sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
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
            // A genuine rise, not a degenerate all-full sweep.
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );
            // The durable epoch floor spans 2 partitions: even the earliest crash point keeps them both.
            Assert.assertTrue(
                    "every crash point must recover at least the LAZY_K durable epoch rows spanning 2 "
                            + "partitions (floor)",
                    r.recoveredByK()[1] >= LAZY_K
            );

            // Oracle clause 3: at the LAST crash point k=N, recovery restores ALL committed rows across
            // ALL 7 partitions.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows across all 7 partitions",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * A representative REWINDING crash point within the lazily-applied M-batch, picked from this class's
     * own sweep staircase (see the logged {@code recoveredByK} in
     * {@link #testMultiPartitionLazyGapSweepRecoversCleanlyAtEveryCrashPoint}) so the recovery-enabled arm
     * lands on a genuine, non-degenerate PARTIAL result (strictly more than {@link #LAZY_K}, strictly
     * fewer than {@link #ROWS}) — chosen the same way the sibling lazy-gap sweeps chose theirs ({@code
     * AdaptiveO3LazyGapCrashSweepTest.SCOREBOARD_REWIND_CRASH_K}, {@code
     * AdaptiveIndexedSymbolLazyGapCrashSweepTest.REPRESENTATIVE_REWIND_CRASH_K}, {@code
     * AdaptiveMultiTableLazyGapCrashSweepTest.REPRESENTATIVE_REWIND_CRASH_K}).
     */
    private static final int REPRESENTATIVE_REWIND_CRASH_K = 40;

    /**
     * NEGATIVE CONTROL: does {@code RecoveryCoordinator.recover()} (the {@code _txn}/{@code _cv}
     * epoch-rewind spanning 2 partitions + WAL roll-forward re-deriving the new per-partition directories)
     * do real work for a multi-partition lazy gap, or is the outcome identical with recovery disabled?
     * Rows here are PLAIN in-place/new-partition appends (no copy-on-write, unlike O3), so — mirroring
     * W2/W3's finding — the a-priori expectation is that disabling recovery genuinely loses/torns the
     * rolled-over-partition rows.
     *
     * <p>Mirrors the sibling lazy-gap negative controls' tolerance model exactly: the strict
     * identity-prefix + partition-layout bar applies ONLY to the recovery-ENABLED arm (the
     * supported/default configuration). The ONLY unacceptable outcome for the recovery-DISABLED arm is
     * reproducing the FULL correct result (rows AND partition layout both) — which would mean recovery did
     * not matter here.
     */
    @Test
    public void testNegativeControlRecoveryDisabledLosesRolledOverPartitionRows() throws Exception {
        final PartitionRows withRecovery = runLazyGapCrashScenario(REPRESENTATIVE_REWIND_CRASH_K, true);
        final PartitionRows withoutRecovery = runLazyGapCrashScenario(REPRESENTATIVE_REWIND_CRASH_K, false);

        LOG.info().$("[multi-partition lazy-gap negative control] k=").$(REPRESENTATIVE_REWIND_CRASH_K)
                .$(" withRecovery=").$(withRecovery.toString())
                .$(" withoutRecovery=").$(withoutRecovery.toString()).$();

        // WITH recovery (the supported/default configuration): must be a fully valid, correct identity
        // prefix — a failure here would be a bug in the ENABLED-recovery path itself, not merely a
        // negative-control finding.
        assertValidIdentityPrefix("withRecovery", withRecovery.rows);
        Assert.assertTrue(
                "k=" + REPRESENTATIVE_REWIND_CRASH_K + " (recovery enabled) must land on a non-degenerate "
                        + "PARTIAL result for this comparison to be meaningful (withRecovery=" + withRecovery.rows + ")",
                withRecovery.rows.size() > LAZY_K && withRecovery.rows.size() < ROWS
        );

        // WITHOUT recovery: disabled-recovery must not show MORE surviving rows than recovery-enabled at
        // the identical crash point (a sanity bound; more rows without recovery would be a different,
        // stranger anomaly, not ordinary data loss).
        Assert.assertTrue(
                "recovery-disabled must not show MORE surviving rows than recovery-enabled at the same "
                        + "crash point (withoutRecovery=" + withoutRecovery.rows
                        + ", withRecovery=" + withRecovery.rows + ")",
                withoutRecovery.rows.size() <= withRecovery.rows.size()
        );

        // THE FINDING: with in-place/new-partition appends (unlike O3's copy-on-write self-heal), recovery
        // should earn its keep for a multi-partition lazy gap — disabling it must NOT reproduce the full
        // correct result (rows AND partition layout together) at a rewinding crash point.
        final boolean fullCorrect = withoutRecovery.rows.equals(withRecovery.rows)
                && withoutRecovery.days.equals(withRecovery.days);
        Assert.assertFalse(
                "NEGATIVE CONTROL: recovery-disabled must NOT reproduce the FULL correct recovery-enabled "
                        + "result (rows AND partition layout) at a rewinding crash point (withoutRecovery="
                        + withoutRecovery + ") — if this fails, recovery is not doing demonstrable work for "
                        + "the multi-partition lazy gap here",
                fullCorrect
        );
    }

    private void assertValidIdentityPrefix(String label, List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            Assert.assertNotNull(label + " row " + i + " read back NULL (corruption)", rows.get(i));
            Assert.assertEquals(
                    label + " row " + i + " silently WRONG (not an identity prefix)",
                    (long) i, (long) rows.get(i)
            );
        }
    }

    /** Paired recovered-rows + partition-day-set snapshot for the negative-control comparison. */
    private static final class PartitionRows {
        final List<Long> rows;
        final Set<Integer> days;

        PartitionRows(List<Long> rows, Set<Integer> days) {
            this.rows = rows;
            this.days = days;
        }

        @Override
        public String toString() {
            return "rows=" + rows + ", days=" + days;
        }
    }

    /**
     * Runs the LAZY_K/LAZY_M multi-partition lazy-gap scenario once, standalone (not through the sweep
     * driver): LAZY_K rows spanning 2 partitions -&gt; durable epoch -&gt; epoch disabled -&gt; LAZY_M more
     * rows, each on its own brand-new partition, applied LAZILY -&gt; a crash armed at durability op
     * {@code k} -&gt; the recovery triple with {@code RecoveryCoordinator.recover()} included or skipped
     * per {@code recoveryOn}. Returns the recovered {@code v} list (tolerant of a loud torn read) paired
     * with the physically-present partition day set. Leak-safe: drops the table + reclaims per-cycle fds
     * before returning, mirroring the sweep driver.
     */
    private PartitionRows runLazyGapCrashScenario(int k, boolean recoveryOn) throws Exception {
        final PartitionRows[] resultBox = new PartitionRows[1];
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, recoveryOn ? "true" : "false");
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
                final String table = "mp_lazygap_nc";
                execute("drop table if exists " + table);
                drainWalQueue();
                execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                        + "with commit_mode='adaptive'");
                final TableToken tt = engine.verifyTableName(table);

                for (int i = 0; i < LAZY_K; i++) {
                    insertRow(table, i);
                }
                drainWalQueue();
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);

                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                crashFf.armCrashAt(base + k);
                try {
                    for (int i = LAZY_K; i < ROWS; i++) {
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

                final ReadResult rr = readVOrderedByVTracked(table);
                final PartitionScan scan = readPartitionDays(table);
                resultBox[0] = new PartitionRows(rr.rows, scan.torn ? new TreeSet<>() : scan.days);

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
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, "true");
        }
        return resultBox[0];
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

    /** Inserts row {@code v} at its fixed ({@link #DAY}, {@link #HOUR}) timestamp (October 2024). */
    private static void insertRow(String table, int v) throws Exception {
        execute("insert into " + table + " values ('"
                + String.format("2024-10-%02dT%02d:00:00.000000Z", DAY[v], HOUR[v]) + "', " + v + ")");
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

    /** The result of a torn-tolerant identity-column read: the rows gathered, and whether it teared. */
    private static final class ReadResult {
        final List<Long> rows;
        final boolean torn;

        ReadResult(List<Long> rows, boolean torn) {
            this.rows = rows;
            this.torn = torn;
        }
    }

    /**
     * Read {@code select v ... order by v}, returning the identity values gathered so far AND whether the
     * read teared. A loud torn read (CairoException/CairoError/SIGBUS-InternalError on a truncated column)
     * is an acceptable crash outcome — the (possibly partial) result read before the tear is returned
     * rather than rethrown, with {@code torn=true} so the caller can apply the stricter CLEAN-read-only
     * exact-count check (see {@link #oracle}).
     */
    private ReadResult readVOrderedByVTracked(String table) {
        final List<Long> out = new ArrayList<>();
        boolean torn = false;
        try (RecordCursorFactory f = select("select v from " + table + " order by v")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (CairoException | CairoError | InternalError e) {
            // acceptable: corruption detected loudly; return the prefix read before the tear
            torn = true;
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return new ReadResult(out, torn);
    }

    /** Distinct partition day-of-month values present, plus per-day numRows, and whether the read teared. */
    private static final class PartitionScan {
        final Set<Integer> days;
        final Map<Integer, Long> rowsByDay;
        final boolean torn;

        PartitionScan(Set<Integer> days, Map<Integer, Long> rowsByDay, boolean torn) {
            this.days = days;
            this.rowsByDay = rowsByDay;
            this.torn = torn;
        }
    }

    /**
     * The SQL-level partition-metadata view ({@code table_partitions('t')} — {@code name}, e.g.
     * "2024-10-03", and {@code numRows}), reduced to the set of distinct day-of-month values physically
     * present and each day's total row count. A loud error reading partition metadata is tolerated the same
     * way a torn column read is (returns an empty/partial result with {@code torn=true}); this reflects
     * exactly what {@code _txn}'s CURRENT (post-recovery) partition list contains, so an orphaned partition
     * still referenced by a stale {@code _txn} entry whose directory recovery actually removed would show
     * up as a loud error here rather than silently vanishing.
     */
    private PartitionScan readPartitionDays(String table) {
        final Set<Integer> days = new TreeSet<>();
        final Map<Integer, Long> rowsByDay = new TreeMap<>();
        try (RecordCursorFactory f = select(
                "select name, numRows from table_partitions('" + table + "') order by name")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    final String name = r.getStrA(0).toString();
                    final int day = Integer.parseInt(name.substring(8, 10));
                    days.add(day);
                    rowsByDay.merge(day, r.getLong(1), Long::sum);
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            return new PartitionScan(days, rowsByDay, true);
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return new PartitionScan(days, rowsByDay, false);
    }

    /** The set of distinct days spanned by the identity prefix {@code v=0..recovered-1} (per {@link #DAY}). */
    private static Set<Integer> expectedDaysForPrefix(int recovered) {
        final Set<Integer> days = new TreeSet<>();
        for (int i = 0; i < recovered && i < ROWS; i++) {
            days.add(DAY[i]);
        }
        return days;
    }

    /**
     * W5 — the multi-partition lazy-gap workload. {@code setup} builds the durable epoch prefix (LAZY_K
     * rows spanning 2 partitions with the epoch enabled, then the epoch DISABLED), so the driver's swept
     * {@code commit} phase is the LAZY_M rows — each landing on its own brand-new partition, applied
     * LAZILY. Uses ONE reused table name (drop+recreate at the head of each setup); the driver supplies
     * the per-cycle isolation.
     */
    private final class LazyGapMultiPartitionWorkload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "mp_lazygap";
            // Epoch ENABLED for the LAZY_K-row prefix so a durable cut is taken at seqTxn=LAZY_K.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");
            tt = engine.verifyTableName(table);

            // LAZY_K rows spanning 2 partitions (days 1,1,2,2) -> apply -> durable epoch at seqTxn=LAZY_K,
            // a cut that SPANS both partitions.
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(table, i);
            }
            drainWalQueue();

            // DISABLE further epochs: the driver's swept commit() phase (the LAZY_M rows, each its own new
            // partition) is applied LAZILY, building the sustained gap between the durable cut and the
            // frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < ROWS; i++) {
                // v=i lands on DAY[i], a BRAND-NEW day never used by any earlier row -> this apply creates
                // a brand-new partition DIRECTORY, non-durable until recovery re-derives it from the WAL.
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

            // (2) No silent corruption: surviving rows ORDERED BY v are an exact identity PREFIX {0..m-1}.
            // A loud torn read is tolerated; a wrong/absent value — including a zero-fill — is a FAILURE.
            final ReadResult rr = readVOrderedByVTracked(table);
            final List<Long> rows = rr.rows;
            for (int i = 0; i < rows.size(); i++) {
                Assert.assertNotNull("k=" + k + " row " + i + " read back NULL (corruption)", rows.get(i));
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG (not an identity prefix ordered by v) — "
                                + "a zero-fill/gap/cross-partition mix-up here is the suspected "
                                + "partition-rollover recovery bug",
                        (long) i, (long) rows.get(i)
                );
            }

            // Recovered committed-row count from the metadata (reliable even if a column read tore).
            final int recovered = (int) rowCount(table);
            Assert.assertTrue(
                    "k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                    rows.size() <= recovered
            );
            // The durable epoch floor spans 2 partitions: recovery never drops below the LAZY_K rows
            // regardless of where in the lazy M-batch the crash landed.
            Assert.assertTrue(
                    "k=" + k + ": recovery must never drop below the LAZY_K durable epoch rows spanning 2 "
                            + "partitions (recovered=" + recovered + ")",
                    recovered >= LAZY_K
            );
            // MULTI-PARTITION HOLE #1: a CLEAN (non-torn) read must show EXACTLY the committed row count.
            // The base "<=" tolerance above exists for a genuinely torn (loud-exception) read; it would
            // ALSO silently accept a read that returns FEWER rows than committed with NO exception at all —
            // e.g. a reader that quietly skips a silently-dropped/orphaned trailing partition without
            // erroring. That specific silent-shortfall shape is exactly what a whole-partition rollover bug
            // could look like, and it is what this closes.
            if (!rr.torn) {
                Assert.assertEquals(
                        "k=" + k + ": a CLEAN (non-torn) read must return EXACTLY the committed row count — "
                                + "a silent shortfall here (no loud error) means a partition was silently "
                                + "dropped/skipped without detection",
                        recovered, rows.size()
                );
            }

            // MULTI-PARTITION HOLE #2: the set of partitions PHYSICALLY present (table_partitions()) must
            // correspond EXACTLY to what the committed row count implies (expectedDaysForPrefix(recovered)).
            // An EXTRA day is an ORPHANED partition directory referencing rewound-away data; a MISSING day
            // is a partition silently dropped for a surviving row. A loud error reading partition metadata
            // is tolerated (same philosophy as the identity-prefix read); silent disagreement is not.
            final PartitionScan scan = readPartitionDays(table);
            if (!scan.torn) {
                final Set<Integer> expectedDays = expectedDaysForPrefix(recovered);
                Assert.assertEquals(
                        "k=" + k + ": the set of partitions present after recovery must correspond EXACTLY "
                                + "to the surviving rows (recovered=" + recovered + ") — an extra entry is "
                                + "an ORPHANED partition dir referencing rewound-away data, a missing entry "
                                + "is a DROPPED partition for a surviving row",
                        expectedDays, scan.days
                );
                // Per-partition scan consistency: numRows summed across table_partitions() must equal the
                // table-level committed count (catches a partition whose OWN row count silently disagrees
                // with the table total even when its mere presence/absence looks right).
                long summedRows = 0;
                for (long v : scan.rowsByDay.values()) {
                    summedRows += v;
                }
                Assert.assertEquals(
                        "k=" + k + ": summed per-partition numRows must equal the table-level committed count",
                        recovered, (int) summedRows
                );
            }

            if (k == n) {
                Assert.assertEquals("k=N: recovery must restore ALL committed rows", ROWS, recovered);
                Assert.assertEquals("k=N: the full identity set must read back clean", ROWS, rows.size());
                Assert.assertFalse("k=N: partition metadata read must not be torn at full restore", scan.torn);
                Assert.assertEquals(
                        "k=N: all 7 distinct partitions (days 1..7) must be present", ALL_DAYS, scan.days
                );
            }

            // (3) Clean reopen: a follow-up write + read must succeed on the recovered table, landing in
            // YET ANOTHER brand-new partition (FOLLOWUP_DAY=20 — never used by any K/M row).
            execute("insert into " + table + " values ('2024-10-" + FOLLOWUP_DAY + "T00:00:00.000000Z', 999)");
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
                LOG.info().$("[multi-partition lazy-gap sweep] teardown drop skipped for ").$(table).$(": ")
                        .$(e.getMessage()).$();
            }
        }
    }
}
