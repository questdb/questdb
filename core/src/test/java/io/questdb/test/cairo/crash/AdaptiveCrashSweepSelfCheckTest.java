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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Self-check of the adaptive crash-point sweep driver
 * ({@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint}), as {@code CrashModelSelfCheckTest}
 * is for the fault model.
 *
 * <p>It drives the W0 workload (adaptive {@code (ts, v long)}, identity {@code v = 0..ROWS-1}, one row per
 * commit, {@code W = 0} synchronous, epoch every batch) through the sweep and checks that the driver:
 * <ol>
 *   <li>injects at N distinct durability ops (the per-k recovered-row counts are non-decreasing in k and
 *       rise from a short prefix to the full set), with a fresh, count-stable baseline per k;</li>
 *   <li>recovers cleanly at every crash point: the surviving rows are an exact identity prefix
 *       {@code {0..m-1}} (never a gap, never a silently wrong value; a loud torn read is tolerated), the
 *       table is not left suspended, and a follow-up write and read succeed;</li>
 *   <li>restores all committed rows at the last crash point k=N (W=0 full durability);</li>
 *   <li>caps and logs truncation when cap &lt; N, never capping silently.</li>
 * </ol>
 * A recovered state that violates the identity-prefix oracle is an adaptive durability bug.
 */
public class AdaptiveCrashSweepSelfCheckTest extends AbstractAdaptiveCrashSweepTest {

    private static final long FIRST_TS = 1_727_740_800_000_000L; // 2024-10-01T00:00:00Z
    // Rows per commit of the paged sweep: 6,400 bytes of WAL timestamp (16 bytes a row) and 3,200 of v, so
    // with one-OS-page column pages every commit releases pages before it syncs.
    private static final int PAGED_ROWS_PER_COMMIT = 400;
    private static final int ROWS = 6; // identity sequence v = 0,1,2,3,4,5
    private static final int TIMESTAMP_COLUMN_ROW_SIZE = 16;

    /**
     * Sweeps W0 across every commit-phase durability op and asserts the driver hit N distinct injection
     * points, each recovering cleanly to an identity prefix, reaching the full set by k=N.
     */
    @Test
    public void testSweepInjectsAtEveryDurabilityOpAndRecoversCleanly() throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final SweepResult r = forEachAdaptiveCrashPoint(new IdentityWorkload());

            LOG.info().$("[self-check] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();

            // The sweep actually ran over every declared atomic durability op with N > 0.
            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals(
                    "default cap must sweep every declared atomic durability op",
                    r.atomicCommitDurabilityOpCount,
                    r.sweptPoints
            );
            Assert.assertTrue("post-commit apply tail must use a declared boundary", r.stoppedAtDeclaredBoundary);
            Assert.assertFalse("small workload must not be truncated", r.truncated);

            // Distinct injection points: the per-k recovered committed-row count is non-decreasing in k
            // (arming at op k keeps ops 1..k durable, so durable state only grows) ...
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // ... and it is a real rise, not a degenerate all-full sweep: the earliest crash point
            // recovers strictly fewer than the full set, so the injection points do distinct work.
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );

            // Upper bound: at the last declared atomic point, every W=0 WAL commit has returned durably.
            Assert.assertEquals(
                    "last atomic point must recover ALL committed rows",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    @Test
    public void testNeuteredSweepRejectsMissingFlushClass() throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            for (int commits : new int[]{0, ROWS}) {
                final AssertionError error = Assert.assertThrows(AssertionError.class, () ->
                        assertSweepGoesRedWithNeuteredFlushes(
                                new IdentityWorkload(commits, 1, false),
                                "a class that performs no flush",
                                frame -> false
                        )
                );
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("nothing to control"));
            }
        }));
    }

    /**
     * The negative control of the control: with the WAL writer's own flushes removed, the W0 sweep must go red.
     * The sweeps crash at every durability op, so a removed flush shrinks N instead of failing anything;
     * {@link #assertSweepGoesRedWithNeuteredFlushes} is what makes a removed flush visible, and this is the one
     * case that proves it can. Reverting the facade hook makes the sweep pass vacuously, which the inverted verdict
     * reports.
     */
    @Test
    public void testNeuteredWalWriterFlushTurnsTheSweepRed() throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
            assertSweepGoesRedWithNeuteredFlushes(
                    new IdentityWorkload(),
                    "the WAL writer's flushes",
                    frame -> frame.getClassName().startsWith("io.questdb.cairo.wal.WalWriter")
            );
        }));
    }

    /**
     * The same sweep over WAL ingest that releases column pages before its commit and rolls a segment:
     * one-OS-page WAL column pages, three commits of {@link #PAGED_ROWS_PER_COMMIT} rows, a segment roll after
     * the second. At every crash point every row the durable frontier covered when the power failed must read
     * back, with its value; at window 0 the frontier moves inside the commit.
     */
    @Test
    public void testSweepAcrossWalPagesAndSegmentRollW0() throws Exception {
        assertSweepAcrossWalPagesAndSegmentRoll(0);
    }

    /**
     * {@link #testSweepAcrossWalPagesAndSegmentRollW0()} at the default group-commit window, 50 ms: the commit
     * defers the sequencer flush, which runs when the insert hands its writer back.
     */
    @Test
    public void testSweepAcrossWalPagesAndSegmentRollDefaultWindow() throws Exception {
        assertSweepAcrossWalPagesAndSegmentRoll(50_000);
    }

    /**
     * A cap below N must truncate the sweep to exactly cap points and flag it (the driver also logs it),
     * never capping silently.
     */
    @Test
    public void testCapBelowNTruncatesAndLogs() throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false;
            final int cap = 3;
            final SweepResult r = forEachAdaptiveCrashPoint(new IdentityWorkload(), cap);
            Assert.assertTrue("workload must have N > cap for a meaningful truncation check", r.n > cap);
            Assert.assertEquals("cap must truncate the sweep to exactly cap points", cap, r.sweptPoints);
            Assert.assertTrue("truncation past N must be flagged (and logged)", r.truncated);
        }));
    }

    @Test
    public void testDeclaredPostCommitBoundaryExcludesOnlyDeclaredTail() throws Exception {
        runWithCrashFacade(() -> {
            final SweepResult r = forEachAdaptiveCrashPoint(new SwallowedFaultWorkload(1));
            Assert.assertEquals(3, r.n);
            Assert.assertEquals(1, r.atomicCommitDurabilityOpCount);
            Assert.assertEquals(1, r.sweptPoints);
            Assert.assertTrue(r.stoppedAtDeclaredBoundary);
            Assert.assertFalse(r.truncated);
        });
    }

    @Test
    public void testInvalidAtomicDurabilityBoundariesFail() throws Exception {
        runWithCrashFacade(() -> {
            assertInvalidBoundary(0);
            assertInvalidBoundary(4);
        });
    }

    @Test
    public void testUndeclaredSwallowedFaultFails() throws Exception {
        runWithCrashFacade(() -> {
            final AssertionError error = Assert.assertThrows(
                    AssertionError.class,
                    () -> forEachAdaptiveCrashPoint(new SwallowedFaultWorkload(-1))
            );
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("undeclared swallowed durability fault"));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("k=2"));
        });
    }

    private void assertSweepAcrossWalPagesAndSegmentRoll(long windowUs) throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 2 * PAGED_ROWS_PER_COMMIT);
        withAdaptive(windowUs, () -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
            Assert.assertEquals(Files.PAGE_SIZE, engine.getConfiguration().getWalDataAppendPageSize());
            Assert.assertEquals(windowUs, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());

            final IdentityWorkload workload = new IdentityWorkload(3, PAGED_ROWS_PER_COMMIT, false);
            final SweepResult r = forEachAdaptiveCrashPoint(workload);

            LOG.info().$("[self-check] paged W=").$(windowUs).$("us, N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();
            Assert.assertTrue("the ingest must release column pages before a commit and roll a segment",
                    workload.isPagedAndRolled);
            Assert.assertEquals(
                    "default cap must sweep every durability op of the ingest",
                    r.atomicCommitDurabilityOpCount,
                    r.sweptPoints
            );
            Assert.assertFalse("the ingest sweep must not be truncated", r.truncated);
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < workload.totalRows()
            );
            Assert.assertEquals("last point must recover ALL committed rows", workload.totalRows(), r.recoveredByK()[r.sweptPoints]);
        }));
    }

    private void assertInvalidBoundary(int declaredOps) {
        final AssertionError error = Assert.assertThrows(
                AssertionError.class,
                () -> forEachAdaptiveCrashPoint(new SwallowedFaultWorkload(declaredOps))
        );
        Assert.assertTrue(
                error.getMessage(),
                error.getMessage().contains("declared atomic durability-op count must be in [1, N]")
        );
    }

    private void withAdaptive(long windowUs, RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(windowUs));
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

    private void withAdaptiveW0(RunnableEx body) throws Exception {
        withAdaptive(0, body); // W = 0 (synchronous)
    }

    private interface RunnableEx {
        void run() throws Exception;
    }

    /**
     * Three deterministic file fsyncs. The second consumes and swallows the injected fault, modelling a
     * post-commit best-effort operation. A negative declaration uses the interface default, so the
     * self-check can show that an undeclared swallowed fault fails closed.
     */
    private final class SwallowedFaultWorkload implements AdaptiveCrashWorkload {
        private final int declaredOps;
        private long fd = -1;

        private SwallowedFaultWorkload(int declaredOps) {
            this.declaredOps = declaredOps;
        }

        @Override
        public int atomicCommitDurabilityOpCount(int countedOps) {
            return declaredOps < 0 ? AdaptiveCrashWorkload.super.atomicCommitDurabilityOpCount(countedOps) : declaredOps;
        }

        @Override
        public void commit() {
            try {
                crashFf.fsync(fd);
                try {
                    crashFf.fsync(fd);
                } catch (CrashSimulationError deliberatelySwallowed) {
                    // Synthetic post-commit best-effort operation for validating the driver's boundary contract.
                }
                crashFf.fsync(fd);
            } finally {
                crashFf.close(fd);
                fd = -1;
            }
        }

        @Override
        public int oracle(int k, int n) {
            return k;
        }

        @Override
        public TableToken[] setup(int iteration) {
            try (Path path = new Path().of(engine.getConfiguration().getDbRoot())
                    .concat("boundary-self-check-").put(declaredOps).put('-').put(iteration).put(".d")) {
                fd = crashFf.openRW(path.$(), CairoConfiguration.O_NONE);
            }
            Assert.assertTrue("self-check file must open", fd > -1);
            return new TableToken[0];
        }

        @Override
        public void teardown() {
            if (fd > -1) {
                crashFf.close(fd);
                fd = -1;
            }
        }
    }

    /**
     * The W0 workload: an ADAPTIVE {@code (ts timestamp, v long)} WAL table, identity
     * {@code v = 0..ROWS-1}, one row per commit followed by a synchronous {@code drainWalQueue()}.
     */
    private final class IdentityWorkload implements AdaptiveCrashWorkload {
        private final int commits;
        private final boolean isApplyingEachCommit;
        private final int rowsPerCommit;
        private int atomicCommitOps;
        // Rows the durable frontier (localDurableSeqTxn) covered when the commit phase stopped: at a crash,
        // the rows acknowledged durable when the power failed.
        private long durableRowsAtCrash;
        private boolean isPagedAndRolled;
        private String table;
        private TableToken tt;

        private IdentityWorkload() {
            this(ROWS, 1, true);
        }

        /**
         * @param isApplyingEachCommit drain the WAL queue (apply and durable epoch) after every commit; when
         *                             false the phase is WAL ingest only and recovery applies it
         */
        private IdentityWorkload(int commits, int rowsPerCommit, boolean isApplyingEachCommit) {
            this.commits = commits;
            this.rowsPerCommit = rowsPerCommit;
            this.isApplyingEachCommit = isApplyingEachCommit;
        }

        @Override
        public int atomicCommitDurabilityOpCount(int countedOps) {
            Assert.assertTrue("identity workload must record at least one atomic commit op", atomicCommitOps > 0);
            Assert.assertTrue(
                    "identity workload must retain a post-commit apply tail",
                    !isApplyingEachCommit || atomicCommitOps < countedOps
            );
            return atomicCommitOps;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "sweep_w0";
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            tt = engine.verifyTableName(table);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            final int opsBeforeAtomicCommits = crashFf.durabilityOpCount();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            try {
                for (int i = 0; i < commits; i++) {
                    // W=0: the WAL commit fdatasyncs synchronously here -> an armed crash on that op
                    // propagates a CrashSimulationError out of execute().
                    if (rowsPerCommit == 1) {
                        execute("insert into " + table + " values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                    } else {
                        final long first = (long) i * rowsPerCommit;
                        execute("insert into " + table + " select timestamp_sequence(" + (FIRST_TS + first * 1_000_000L)
                                + ", 1_000_000), x - 1 + " + first + " from long_sequence(" + rowsPerCommit + ")");
                    }
                    if (i == commits - 1) {
                        // Every WAL commit has returned durably at this point (W=0 in the commit, W>0 when the
                        // insert handed its writer back). Only the final materialized apply/epoch tail remains, so
                        // no later atomic commit is excluded by this explicit boundary.
                        atomicCommitOps = crashFf.durabilityOpCount() - opsBeforeAtomicCommits;
                        isPagedAndRolled = rowsPerCommit * TIMESTAMP_COLUMN_ROW_SIZE > Files.PAGE_SIZE
                                && new File(engine.getConfiguration().getDbRoot() + File.separator + tt.getDirName()
                                + File.separator + WalUtils.WAL_NAME_BASE + 1 + File.separator + 1).isDirectory();
                    }
                    if (isApplyingEachCommit) {
                        // ADAPTIVE apply + durable epoch -> more durability ops. An armed crash here is swallowed
                        // by ApplyWal2TableJob's catch(Throwable) into a table suspend (no throw).
                        drainWalQueue();
                        // Stop as soon as the crash has fired (as a real power loss would): if it manifested as a
                        // suspend, further inserts would durably extend the WAL and mask this injection point.
                        if (anyTableSuspended(tt)) {
                            return;
                        }
                    }
                }
            } finally {
                durableRowsAtCrash = tracker.getLocalDurableSeqTxn() * rowsPerCommit;
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // Clean reopen: recovery must not leave the table suspended.
            Assert.assertFalse(
                    "k=" + k + ": table must NOT be suspended after recovery",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );

            // No silent corruption: the rows that read back are an exact identity prefix {0..m-1}, each
            // with its inserted timestamp. A loud torn read is tolerated (returns the prefix read so far);
            // a wrong or absent value inside the prefix fails the test.
            final List<long[]> rows = readIdentityPrefixAllowTorn(table);
            final long tsStep = rowsPerCommit == 1 ? 3_600_000_000L : 1_000_000L;
            for (int i = 0; i < rows.size(); i++) {
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG (not an identity prefix)",
                        i, rows.get(i)[0]
                );
                Assert.assertEquals(
                        "k=" + k + " row " + i + " has a silently WRONG timestamp",
                        FIRST_TS + i * tsStep, rows.get(i)[1]
                );
            }

            // Recovered committed-row count from the metadata (reliable even if a column read tore).
            final int recovered = (int) rowCount(table);
            Assert.assertTrue(
                    "k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                    rows.size() <= recovered
            );

            // Nothing acknowledged is lost: every row the durable frontier covered at the crash reads back,
            // with its value.
            Assert.assertTrue(
                    "k=" + k + " (" + phaseDurabilityOp(k) + "): a row the durable frontier covered was lost [frontierRows="
                            + durableRowsAtCrash + ", readBack=" + rows.size() + ", recovered=" + recovered + ']',
                    rows.size() >= durableRowsAtCrash
            );

            if (k == atomicCommitOps) {
                Assert.assertEquals("last atomic op: recovery must restore ALL committed rows", totalRows(), recovered);
                Assert.assertEquals("last atomic op: the full identity set must read back clean", totalRows(), rows.size());
            }

            // Clean reopen: a follow-up write and read must succeed on the recovered table.
            execute("insert into " + table + " values ('2024-10-09T00:00:00.000000Z', 999)");
            drainWalQueue();
            Assert.assertEquals(
                    "k=" + k + ": follow-up insert must land on the recovered table",
                    recovered + 1, rowCount(table)
            );
            return recovered;
        }

        private int totalRows() {
            return commits * rowsPerCommit;
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
                LOG.info().$("[self-check] teardown drop skipped for ").$(table).$(": ").$(e.getMessage()).$();
            }
        }
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
     * Reads {@code select v, ts ... order by ts}, returning the identity-prefix rows {v, ts} gathered so far.
     * A loud torn read (CairoException, CairoError, or a SIGBUS InternalError on a truncated column) is an
     * acceptable crash outcome: the prefix read before it is returned rather than rethrown.
     */
    private List<long[]> readIdentityPrefixAllowTorn(String table) {
        final List<long[]> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select v, ts from " + table + " order by ts")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(new long[]{r.getLong(0), r.getTimestamp(1)});
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // acceptable: corruption detected loudly; return the prefix read before the tear
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
}
