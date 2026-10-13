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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * ADAPTIVE lazy-gap crash sweep across partition rollover. A durable epoch is taken over rows that span two
 * day partitions, further epochs are disabled, and five more rows are applied lazily, each creating a new
 * day partition directory. {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint} crashes at every
 * durability op of the lazy phase. At each point recovery must rewind {@code _txn}/{@code _cv} to the
 * two-partition cut and re-derive the new partitions from the durable WAL, leaving no orphaned partition
 * directory (the crash model truncates the files of an unsynced new directory but does not delete it, see
 * {@code CrashFaultFilesFacade#crash}) and no missing partition for a surviving row.
 * <p>
 * The schema is plain {@code (ts timestamp, v long)} with {@code ts} strictly increasing, so every apply is
 * a tail append or a new partition, never an O3 merge, and a failure points at partition rollover alone.
 * {@link AdaptiveO3LazyGapCrashSweepTest}, {@link AdaptiveIndexedSymbolLazyGapCrashSweepTest} and
 * {@link AdaptiveMultiTableLazyGapCrashSweepTest} cover the O3, indexed-symbol and multi-table variables with
 * the same structure against a single partition.
 */
public class AdaptiveMultiPartitionLazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    /**
     * Pre-epoch rows: a durable epoch is taken after these (interval=0 on the first applied batch).
     */
    private static final int LAZY_K = 4;
    /**
     * Post-epoch rows applied lazily (epochs disabled): the lazy gap this sweep crashes into.
     */
    private static final int LAZY_M = 5;
    /**
     * Total committed rows once the whole gap is applied.
     */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * Day of month (October 2024), indexed by {@code v} (commit order 0..8). The durable K-prefix v=0..3 spans
     * two partitions (days 1, 1, 2, 2); each lazy M-row v=4..8 lands on a new day (3 to 7), so every lazy
     * apply creates a partition. The days never decrease, so every apply is a tail append or a new partition,
     * never an O3 merge.
     */
    private static final int[] DAY = {1, 1, 2, 2, 3, 4, 5, 6, 7};
    /**
     * Hour-of-day paired with {@link #DAY}; only the K-prefix needs 2 distinct hours per shared day.
     */
    private static final int[] HOUR = {0, 12, 0, 12, 0, 0, 0, 0, 0};
    /**
     * The full set of distinct day-of-month values the committed set spans (days 1..7).
     */
    private static final Set<Integer> ALL_DAYS = new TreeSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
    /**
     * A day no K/M row uses: the follow-up write creates one more new partition.
     */
    private static final int FOLLOWUP_DAY = 20;

    /**
     * Fault-free check that the {@link #DAY}/{@link #HOUR} mapping produces the intended layout: the durable
     * K-prefix spans two day partitions and each lazy M-row lands on its own new day, seven partitions in
     * total with row counts 2, 2, 1, 1, 1, 1, 1 summing to {@link #ROWS}.
     */
    @Test
    public void testWorkloadLayoutSpansSevenDistinctPartitions() throws Exception {
        withAdaptiveLazyGap(() -> {
            final String table = "mp_layout_confirm";
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");

            for (int i = 0; i < LAZY_K; i++) {
                insertRow(table, i);
            }
            drainWalQueue();
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
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
     * Builds a durable epoch at seqTxn=LAZY_K spanning two partitions, disables further epochs, then sweeps
     * a crash across every durability op of the five lazy M-rows, each of which creates a new partition
     * directory, running the recovery triple and the oracle at each. Asserts, at every crash point k:
     * <ol>
     *   <li>the table is not suspended after recovery;</li>
     *   <li>no silent corruption: surviving rows ordered by {@code v} are an exact identity prefix
     *       {@code {0..m-1}} (a loud torn read is tolerated, a wrong or absent value fails);</li>
     *   <li>the partitions present match the surviving rows exactly, and a clean read returns exactly the
     *       committed count (see {@link LazyGapMultiPartitionWorkload#oracle});</li>
     *   <li>a follow-up write and read succeed on the recovered table, landing in another new
     *       partition;</li>
     *   <li>the recovered count never decreases with k and reaches all {@link #ROWS} rows across all seven
     *       partitions at k=N.</li>
     * </ol>
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
            // The durable epoch floor spans 2 partitions: even the earliest crash point keeps them both.
            Assert.assertTrue(
                    "every crash point must recover at least the LAZY_K durable epoch rows spanning 2 "
                            + "partitions (floor)",
                    r.recoveredByK()[1] >= LAZY_K
            );

            // At the last crash point k=N, recovery restores all committed rows across all seven partitions.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows across all 7 partitions",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * The rewinding crash point, chosen by phase rather than by op count: this many lazy rows are applied,
     * each as its own txn on a new partition, and the crash fires on the first durability op of the next
     * lazy row's commit, before that row's WAL is durable. Recovery rewinds to the {@link #LAZY_K}
     * durable-epoch cut and replays the applied rows, re-creating their partitions: a partial result
     * ({@code [0..6]}, days 1 to 5) whatever the number of ops per commit.
     */
    private static final int REWIND_APPLIED_LAZY_ROWS = 3;

    /**
     * Data before pointer, across a partition rollover. {@code _txn}, {@code _cv} and the index files make
     * their per-commit flush decision on the table's effective commit mode, so under ADAPTIVE the commit
     * pointer is as lazily durable as the column data it describes. After a crash in the lazy phase, and
     * before any recovery runs, {@code _txn} must therefore not claim a seqTxn beyond the durable epoch cut.
     * The same crash runs with and without {@code RecoveryCoordinator.recover()}; both arms must keep the
     * pointer at or below the cut and land on the same valid identity prefix and partition layout.
     * {@link AdaptiveRecoveryRollForwardCrashTest} covers roll-forward itself.
     */
    @Test
    public void testCommitPointerNeverPublishedAheadOfDataAcrossPartitionRollover() throws Exception {
        final PartitionRows withRecovery = runLazyGapCrashScenario(REWIND_APPLIED_LAZY_ROWS, true);
        final PartitionRows withoutRecovery = runLazyGapCrashScenario(REWIND_APPLIED_LAZY_ROWS, false);

        LOG.info().$("[multi-partition lazy-gap negative control] appliedLazyRows=").$(REWIND_APPLIED_LAZY_ROWS)
                .$(" withRecovery=").$(withRecovery.toString())
                .$(" withoutRecovery=").$(withoutRecovery.toString()).$();

        // With recovery (the default configuration) the result must be a valid identity prefix; a failure
        // here is a bug in the recovery path itself.
        assertValidIdentityPrefix("withRecovery", withRecovery.rows);
        Assert.assertTrue(
                "the crash (recovery enabled) must land on a non-degenerate "
                        + "PARTIAL result for this comparison to be meaningful (withRecovery=" + withRecovery.rows + ")",
                withRecovery.rows.size() > LAZY_K && withRecovery.rows.size() < ROWS
        );
        Assert.assertEquals(
                "the crash must land after " + REWIND_APPLIED_LAZY_ROWS + " lazy applies and before the next "
                        + "lazy row's WAL is durable (withRecovery=" + withRecovery.rows + ")",
                LAZY_K + REWIND_APPLIED_LAZY_ROWS, withRecovery.rows.size()
        );

        // Sanity bound: without recovery, no more rows survive than with recovery at the same crash point.
        Assert.assertTrue(
                "recovery-disabled must not show MORE surviving rows than recovery-enabled at the same "
                        + "crash point (withoutRecovery=" + withoutRecovery.rows
                        + ", withRecovery=" + withRecovery.rows + ")",
                withoutRecovery.rows.size() <= withRecovery.rows.size()
        );

        // Data before pointer, asserted before the row check below so a regression reports its cause rather
        // than its symptom: with _txn at or below the durable epoch cut in both arms, no reader sees a row or
        // a partition directory whose data is not durable.
        for (PartitionRows arm : new PartitionRows[]{withRecovery, withoutRecovery}) {
            Assert.assertTrue(
                    "the crash must not leave _txn claiming past the durable epoch cut: the commit pointer "
                            + "is lazily durable like the data it exposes (preRecoverySeqTxn="
                            + arm.preRecoverySeqTxn + ", durable cut=" + LAZY_K + ")",
                    arm.preRecoverySeqTxn >= 0 && arm.preRecoverySeqTxn <= LAZY_K
            );
        }

        // As a consequence the arm without recovery is correct too, in rows and in partition layout.
        assertValidIdentityPrefix("withoutRecovery", withoutRecovery.rows);
        Assert.assertEquals(
                "recovery-disabled must land on the same partition layout as recovery-enabled",
                withRecovery.days, withoutRecovery.days
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

    /**
     * Recovered rows and partition day set of one arm of the with/without-recovery comparison.
     */
    private static final class PartitionRows {
        final List<Long> rows;
        final Set<Integer> days;
        /**
         * The on-disk {@code _txn} seqTxn as the crash left it, sampled before any recovery ran.
         */
        long preRecoverySeqTxn = -1;

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
     * Runs the multi-partition lazy-gap scenario once, outside the sweep driver: LAZY_K rows spanning two
     * partitions -&gt; durable epoch -&gt; epochs disabled -&gt; LAZY_M rows, each on a new partition, applied
     * lazily -&gt; a crash on the first durability op of the commit of lazy row {@code appliedLazyRows}
     * (counted from 0) -&gt; the recovery triple, with {@code RecoveryCoordinator.recover()} run or skipped
     * per {@code recoveryOn}. Returns the recovered {@code v} list (tolerating a loud torn read), the
     * partition day set and the pre-recovery {@code _txn} seqTxn. Drops the table and closes per-cycle fds
     * before returning, as the sweep driver does.
     */
    private PartitionRows runLazyGapCrashScenario(int appliedLazyRows, boolean recoveryOn) throws Exception {
        final PartitionRows[] resultBox = new PartitionRows[1];
        final long[] preRecoverySeqTxn = {-1};
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED, recoveryOn ? "true" : "false");
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
                final String table = "mp_lazygap_nc";
                execute("drop table if exists " + table);
                drainWalQueue();
                execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName(table);

                for (int i = 0; i < LAZY_K; i++) {
                    insertRow(table, i);
                }
                drainWalQueue();
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                int crashOp = -1;
                try {
                    for (int i = LAZY_K; i < ROWS; i++) {
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
                LOG.info().$("[multi-partition lazy-gap negative control] recovery=").$(recoveryOn)
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

                final ReadResult rr = readVOrderedByVTracked(table);
                final PartitionScan scan = readPartitionDays(table);
                resultBox[0] = new PartitionRows(rr.rows, scan.torn ? new TreeSet<>() : scan.days);
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

    /**
     * Inserts row {@code v} at its fixed ({@link #DAY}, {@link #HOUR}) timestamp (October 2024).
     */
    private static void insertRow(String table, int v) throws Exception {
        execute("insert into " + table + " values ('"
                + String.format("2024-10-%02dT%02d:00:00.000000Z", DAY[v], HOUR[v]) + "', " + v + ")");
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
     * The result of a torn-tolerant identity-column read: the rows gathered, and whether it teared.
     */
    private static final class ReadResult {
        final List<Long> rows;
        final boolean torn;

        ReadResult(List<Long> rows, boolean torn) {
            this.rows = rows;
            this.torn = torn;
        }
    }

    /**
     * Reads {@code select v ... order by v} and returns the identity values gathered and whether the read
     * tore. A loud torn read (CairoException/CairoError/SIGBUS-InternalError on a truncated column) is an
     * acceptable crash outcome: the partial result read before the tear is returned with {@code torn=true},
     * so the caller applies the exact-count check to clean reads only.
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

    /**
     * Distinct partition day-of-month values present, plus per-day numRows, and whether the read teared.
     */
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
     * Reads {@code table_partitions('t')} ({@code name}, e.g. "2024-10-03", and {@code numRows}) and reduces it
     * to the set of day-of-month values present and each day's row count. A loud error reading partition
     * metadata is tolerated like a torn column read and returns a partial result with {@code torn=true}. The
     * result reflects the post-recovery {@code _txn} partition list, so a stale {@code _txn} entry whose
     * directory recovery removed shows up as a loud error here rather than silently vanishing.
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

    /**
     * The set of distinct days spanned by the identity prefix {@code v=0..recovered-1} (per {@link #DAY}).
     */
    private static Set<Integer> expectedDaysForPrefix(int recovered) {
        final Set<Integer> days = new TreeSet<>();
        for (int i = 0; i < recovered && i < ROWS; i++) {
            days.add(DAY[i]);
        }
        return days;
    }

    /**
     * The multi-partition lazy-gap workload. {@code setup} builds the durable epoch prefix (LAZY_K rows
     * spanning two partitions with epochs enabled, then disables epochs), so the driver's swept
     * {@code commit} phase is the LAZY_M rows, each on its own new partition, applied lazily. Each setup
     * drops and recreates the same table name; the driver supplies the per-cycle isolation.
     */
    private final class LazyGapMultiPartitionWorkload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "mp_lazygap";
            // Epochs enabled for the LAZY_K-row prefix, so a durable cut is taken at seqTxn=LAZY_K.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            tt = engine.verifyTableName(table);

            // LAZY_K rows spanning two partitions (days 1, 1, 2, 2) -> apply -> durable epoch at seqTxn=LAZY_K,
            // a cut that spans both partitions.
            for (int i = 0; i < LAZY_K; i++) {
                insertRow(table, i);
            }
            drainWalQueue();

            // Disable further epochs: the driver's swept commit() phase (the LAZY_M rows, each its own new
            // partition) is applied lazily, building the gap between the durable cut and the frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < ROWS; i++) {
                // v=i lands on DAY[i], a day no earlier row uses, so this apply creates a new partition
                // directory, not durable until recovery re-derives it from the WAL.
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

            // (2) No silent corruption: surviving rows ordered by v are an exact identity prefix {0..m-1}.
            // A loud torn read is tolerated; a wrong or absent value, including a zero-fill, fails.
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
            // A clean (non-torn) read must return exactly the committed row count. The "<=" bound above
            // allows for a loud torn read, but would also accept a read that silently skips a dropped or
            // orphaned trailing partition, which is how a partition rollover bug would show.
            if (!rr.torn) {
                Assert.assertEquals(
                        "k=" + k + ": a CLEAN (non-torn) read must return EXACTLY the committed row count — "
                                + "a silent shortfall here (no loud error) means a partition was silently "
                                + "dropped/skipped without detection",
                        recovered, rows.size()
                );
            }

            // The partitions present in table_partitions() must match what the committed row count implies
            // (expectedDaysForPrefix(recovered)). An extra day is an orphaned partition directory referencing
            // rewound data; a missing day is a partition dropped for a surviving row. A loud error reading
            // partition metadata is tolerated, as for the identity-prefix read; silent disagreement is not.
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
                // table-level committed count (catches a partition whose own row count disagrees with the
                // table total even when its presence or absence looks right).
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

            // (3) A follow-up write and read succeed on the recovered table, landing in another new partition
            // (FOLLOWUP_DAY, which no K/M row uses).
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
