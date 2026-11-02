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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalReader;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Does a materialized view recover to a state its row-expiry cleanup actually committed, after a crash at every
 * durability op of a cleanup sweep and the lazy apply that follows it?
 *
 * <h3>The workload</h3>
 * A base WAL table and a passthrough mat view {@code mv} with {@code EXPIRE ROWS WHEN v < 0}, partitioned by day:
 * 2024-01-01 holds one expired row, 2024-01-02 one expired row and one kept row, 2024-01-03 one expired row, and
 * 2024-01-04 the active partition, which cleanup never touches. Setup applies everything with the durable epoch on,
 * then turns the epoch off and releases the pooled WAL writers, so the swept phase runs on a WAL writer the pool
 * creates after the durable baseline. The swept phase is one {@link RowExpiryCleanupJob#cleanupTable} sweep, which
 * sequences three conditional REPLACE_RANGE commits on that writer (a wipe of 2024-01-01, a compaction of
 * 2024-01-02 to its kept row, a wipe of 2024-01-03), followed by the lazy apply of all three.
 *
 * <h3>A crash the sweep swallows</h3>
 * {@code cleanupTable} catches a failure per partition, logs it and goes on with the next partition, so a
 * {@link CrashSimulationError} fired inside a cleanup commit neither propagates nor suspends a table. The workload
 * therefore checks {@link CrashFaultFilesFacade#isPowerLossMarked()} after the sweep and rethrows. Commits the sweep
 * makes after the mark carry no durable state, as after a real power loss.
 *
 * <h3>The oracle</h3>
 * At every crash point k, after recovery:
 * <ol>
 *   <li>neither table is suspended and the view is valid;</li>
 *   <li>every WAL segment a data txn after the baseline names opens through {@link WalReader}, which reads the
 *       segment {@code _meta} the way the live-view drain does;</li>
 *   <li>after the post-boot apply, the view's partitions and row counts equal one of the four states the sweep
 *       passes through, in commit order, and that state index is non-decreasing in k;</li>
 *   <li>the kept rows read back, so cleanup never loses a survivor;</li>
 *   <li>at k=N the view is in the final state;</li>
 *   <li>a follow-up sweep brings the view to the final state from wherever recovery left it.</li>
 * </ol>
 */
public class AdaptiveRowExpiryCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    private static final int CRASH_POINT_CAP = 256;
    // Index of the last entry in STATES.
    private static final int FINAL_STATE = 3;
    // 2024-01-10T00:00:00Z: a pinned clock keeps the durability-op sequence equal across the count and sweep passes.
    private static final long PINNED_MICROS = 1_704_844_800_000_000L;
    // The view's partitions and row counts before the sweep and after each of its three commits, in commit order.
    private static final String[] STATES = {
            "2024-01-01:1,2024-01-02:2,2024-01-03:1,2024-01-04:1",
            "2024-01-02:2,2024-01-03:1,2024-01-04:1",
            "2024-01-02:1,2024-01-03:1,2024-01-04:1",
            "2024-01-02:1,2024-01-04:1",
    };

    @Test
    public void testCleanupSweepRecoversToCommittedStateAtCrashPoints1To30() throws Exception {
        assertSweepPartRecoversToCommittedState(1, 30);
    }

    @Test
    public void testCleanupSweepRecoversToCommittedStateAtCrashPoints31ToLast() throws Exception {
        assertSweepPartRecoversToCommittedState(31, Integer.MAX_VALUE);
    }

    private static String readPartitions() throws SqlException {
        final StringBuilder sb = new StringBuilder();
        try (
                RecordCursorFactory f = select("SELECT name::STRING, numRows FROM table_partitions('mv') ORDER BY name");
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            final Record r = c.getRecord();
            while (c.hasNext()) {
                if (!sb.isEmpty()) {
                    sb.append(',');
                }
                sb.append(r.getStrA(0)).append(':').append(r.getLong(1));
            }
        }
        return sb.toString();
    }

    private static int stateIndex(int k, String partitions) {
        for (int i = 0; i < STATES.length; i++) {
            if (STATES[i].equals(partitions)) {
                return i;
            }
        }
        Assert.fail("k=" + k + ": the view's partitions [" + partitions + "] match no state the sweep committed "
                + Arrays.toString(STATES));
        return -1;
    }

    private static boolean viewInvalid(TableToken mvTt) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(mvTt);
        return state != null && state.isInvalid();
    }

    /**
     * The sweep over crash points {@code firstK..min(lastK, N)}. Every point starts from a fresh identical baseline,
     * so a part sweeps exactly the points the whole sweep would in its range.
     */
    private void assertSweepPartRecoversToCommittedState(int firstK, int lastK) throws Exception {
        Assert.assertEquals(STATES.length - 1, FINAL_STATE);
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
        runWithCrashFacade(() -> {
            // Per-inode strictness: one file's fsync does not make another file's size durable.
            crashFf.modelSharedJournal = false;

            final CleanupWorkload workload = new CleanupWorkload();
            final SweepResult r = forEachAdaptiveCrashPoint(workload, CRASH_POINT_CAP, firstK, lastK);
            final int first = r.firstSweptK;
            final int last = r.lastSweptK;

            LOG.info().$("[row-expiry cleanup sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", part=").$(first).$("..").$(last)
                    .$(", stateByK=").$(Arrays.toString(r.recoveredByK())).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertFalse("the cap must not truncate this workload", r.truncated);
            Assert.assertEquals("the cap must not truncate this workload", r.n, r.sweptPoints);
            for (int k = first + 1; k <= last; k++) {
                Assert.assertTrue(
                        "the recovered state must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            if (first == 1) {
                Assert.assertTrue(
                        "the earliest crash point must recover a state before the final one",
                        r.recoveredByK()[1] < FINAL_STATE
                );
            }
            if (last == r.sweptPoints) {
                Assert.assertEquals(
                        "k=N must recover the final state (W=0: every returned cleanup commit is durable)",
                        FINAL_STATE, r.recoveredByK()[last]
                );
            }
        });
    }

    private final class CleanupWorkload implements AdaptiveCrashWorkload {
        private TableToken baseTt;
        private long baselineSeqTxn;
        private TableToken mvTt;

        @Override
        public void commit() throws Exception {
            AdaptiveRowExpiryCleanupCrashTest.cleanup(mvTt);
            if (crashFf.isPowerLossMarked()) {
                throw new CrashSimulationError(-1);
            }
            drainWalAndMatViewQueues();
            if (anyTableSuspended(baseTt, mvTt)) {
                return;
            }
            if (crashFf.isPowerLossMarked()) {
                throw new CrashSimulationError(-1);
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // A real boot re-reads the recovered _mv.s; the live engine kept the pre-crash state in memory.
            engine.hydrateMatViewStateStore();

            Assert.assertFalse("k=" + k + ": base must not be suspended", engine.getTableSequencerAPI().isSuspended(baseTt));
            Assert.assertFalse("k=" + k + ": mv must not be suspended", engine.getTableSequencerAPI().isSuspended(mvTt));
            assertWalSegmentsReadable(mvTt, baselineSeqTxn, "k=" + k + ": ");

            drainWalAndMatViewQueues();
            Assert.assertFalse("k=" + k + ": mv must stay valid", viewInvalid(mvTt));
            Assert.assertFalse("k=" + k + ": mv must not be suspended after apply", engine.getTableSequencerAPI().isSuspended(mvTt));
            final int state = stateIndex(k, readPartitions());
            assertQuery("SELECT v FROM mv ORDER BY v").noLeakCheck().returns("v\n4.0\n9.0\n");
            if (k == n) {
                Assert.assertEquals("k=N: the view must be in the final state", FINAL_STATE, state);
            }

            AdaptiveRowExpiryCleanupCrashTest.cleanup(mvTt);
            drainWalAndMatViewQueues();
            Assert.assertEquals("k=" + k + ": a follow-up sweep must reach the final state",
                    FINAL_STATE, stateIndex(k, readPartitions()));
            assertQuery("SELECT v FROM mv ORDER BY v").noLeakCheck().returns("v\n4.0\n9.0\n");
            return state;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            setCurrentMicros(PINNED_MICROS);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("DROP MATERIALIZED VIEW IF EXISTS mv");
            execute("DROP TABLE IF EXISTS base");
            drainWalAndMatViewQueues();

            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    (-1.0, '2024-01-01T00:00:00.000000Z'),
                    (-2.0, '2024-01-02T00:00:00.000000Z'),
                    (9.0, '2024-01-02T12:00:00.000000Z'),
                    (-3.0, '2024-01-03T00:00:00.000000Z'),
                    (4.0, '2024-01-04T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 0");
            drainWalAndMatViewQueues();
            baseTt = engine.verifyTableName("base");
            mvTt = engine.verifyTableName("mv");
            Assert.assertEquals("setup must leave the view in the initial state", STATES[0], readPartitions());

            // The swept phase applies lazily, and the pool creates the sweep's WAL writer after the driver's
            // durable baseline, so the cleanup commits are first in a segment whose _meta nothing has flushed.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            engine.releaseAllWalWriters();
            baselineSeqTxn = engine.getTableSequencerAPI().getTxnTracker(mvTt).getSeqTxn();
            return new TableToken[]{baseTt, mvTt};
        }

        @Override
        public void teardown() throws Exception {
            try {
                execute("DROP MATERIALIZED VIEW IF EXISTS mv");
                execute("DROP TABLE IF EXISTS base");
                drainWalAndMatViewQueues();
            } catch (Exception e) {
                LOG.info().$("[row-expiry cleanup sweep] teardown drop skipped: ").$(e.getMessage()).$();
            }
        }
    }
}
