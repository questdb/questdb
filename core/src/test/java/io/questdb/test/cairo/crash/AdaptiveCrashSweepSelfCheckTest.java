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
import io.questdb.std.str.Path;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The SELF-CHECK for the adaptive exhaustive crash-point sweep driver
 * ({@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint}, SP-D increment D1.a) — the
 * driver's own gate, mirroring {@code CrashModelSelfCheckTest}'s role for the fault model.
 *
 * <p>It drives the anchor W0 workload (adaptive {@code (ts, v long)}, identity {@code v = 0..ROWS-1},
 * one row per commit, {@code W = 0} synchronous, epoch every batch) through the sweep and PROVES the
 * driver:
 * <ol>
 *   <li>injects at N DISTINCT durability ops (the per-k recovered-row counts are non-decreasing in k and
 *       rise from a short prefix to the full set) — a fresh, count-stable baseline per k;</li>
 *   <li>recovers cleanly at EVERY crash point (D1.b): the surviving rows are an exact identity PREFIX
 *       {@code {0..m-1}} (never a gap, never a silently-wrong value; a loud torn read is tolerated), the
 *       table is NOT left suspended, and a follow-up write+read succeeds;</li>
 *   <li>restores ALL committed rows at the last crash point k=N (W=0 full durability);</li>
 *   <li>caps + LOGS truncation when cap &lt; N (never a silent cap).</li>
 * </ol>
 * A recovered state that violated the identity-prefix oracle would be a REAL adaptive durability bug.
 */
public class AdaptiveCrashSweepSelfCheckTest extends AbstractAdaptiveCrashSweepTest {

    private static final int ROWS = 6; // identity sequence v = 0,1,2,3,4,5

    /**
     * The headline self-check: sweep W0 across every commit-phase durability op and assert the driver
     * hit N distinct injection points, each recovering cleanly to an identity prefix, reaching the full
     * set by k=N.
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
            // (arming at op k keeps ops 1..k durable — durable state only grows) ...
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // ... and it is a GENUINE rise, not a degenerate all-full sweep: the earliest crash point
            // recovers strictly fewer than the full set, proving the injection points do distinct work.
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

    /**
     * A cap below N must TRUNCATE the sweep to exactly cap points and flag it (the driver also LOGs it) —
     * never a silent cap.
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
     * Three deterministic file fsyncs. The second deliberately consumes and swallows the injected fault,
     * modelling a genuine post-commit best-effort operation. A negative declaration uses the interface
     * default, so the self-check can prove that an undeclared swallowed fault fails closed.
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
     * W0 — the anchor workload: an ADAPTIVE {@code (ts timestamp, v long)} WAL table, identity
     * {@code v = 0..ROWS-1}, one row per commit followed by a synchronous {@code drainWalQueue()}.
     */
    private final class IdentityWorkload implements AdaptiveCrashWorkload {
        private int atomicCommitOps;
        private String table;
        private TableToken tt;

        @Override
        public int atomicCommitDurabilityOpCount(int countedOps) {
            Assert.assertTrue("identity workload must record at least one atomic commit op", atomicCommitOps > 0);
            Assert.assertTrue("identity workload must retain a post-commit apply tail", atomicCommitOps < countedOps);
            return atomicCommitOps;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "sweep_w0";
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");
            tt = engine.verifyTableName(table);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            final int opsBeforeAtomicCommits = crashFf.durabilityOpCount();
            for (int i = 0; i < ROWS; i++) {
                // W=0: the WAL commit fdatasyncs synchronously here -> an armed crash on that op
                // propagates a CrashSimulationError out of execute().
                execute("insert into " + table + " values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                if (i == ROWS - 1) {
                    // Every W=0 WAL commit has now returned durably. Only the final materialized apply/epoch
                    // tail remains, so no later atomic commit is excluded by this explicit boundary.
                    atomicCommitOps = crashFf.durabilityOpCount() - opsBeforeAtomicCommits;
                }
                // ADAPTIVE apply + durable epoch -> more durability ops. An armed crash here is swallowed
                // by ApplyWal2TableJob's catch(Throwable) into a table SUSPEND (no throw).
                drainWalQueue();
                // Stop as soon as the crash has fired (as a real power loss would): if it manifested as a
                // suspend, further inserts would durably extend the WAL and mask this injection point.
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

            // No silent corruption (D1.b bar 1): the rows that read back are an exact identity PREFIX
            // {0..m-1}. A loud torn read is tolerated (returns the prefix read so far); a wrong/absent
            // value inside the prefix is a FAILURE.
            final List<Long> rows = readIdentityPrefixAllowTorn(table);
            for (int i = 0; i < rows.size(); i++) {
                Assert.assertNotNull("k=" + k + " row " + i + " read back NULL (corruption)", rows.get(i));
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG (not an identity prefix)",
                        (long) i, (long) rows.get(i)
                );
            }

            // Recovered committed-row count from the metadata (reliable even if a column read tore).
            final int recovered = (int) rowCount(table);
            Assert.assertTrue(
                    "k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                    rows.size() <= recovered
            );

            if (k == atomicCommitOps) {
                Assert.assertEquals("last atomic op: recovery must restore ALL committed rows", ROWS, recovered);
                Assert.assertEquals("last atomic op: the full identity set must read back clean", ROWS, rows.size());
            }

            // Clean reopen: a follow-up write + read must succeed on the recovered table.
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
                LOG.info().$("[self-check] teardown drop skipped for ").$(table).$(": ").$(e.getMessage()).$();
            }
        }
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
     * Read {@code select v ... order by ts}, returning the identity-prefix values gathered so far. A loud
     * torn read (CairoException/CairoError/SIGBUS-InternalError on a truncated column) is an acceptable
     * crash outcome — the prefix read before it is returned rather than rethrown.
     */
    private List<Long> readIdentityPrefixAllowTorn(String table) {
        final List<Long> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select v from " + table + " order by ts")) {
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
}
