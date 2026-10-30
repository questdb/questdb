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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.WindowsBarrierContractFilesFacade;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Drives a broad slice of the storage surface and fails if any durability barrier goes through a handle
 * opened without write access, which fails with {@code ERROR_ACCESS_DENIED} on Windows.
 * {@link WindowsBarrierContractFilesFacade} models that contract on POSIX. The sweep sees only the paths its
 * workloads reach, so a new barrier site needs its own workload. Both commit modes run: {@code sync} takes
 * the eager barrier at every apply-path site, and {@code adaptive} reaches the durable epoch
 * ({@code DurableEpochManifest} and {@code fsyncMaterializedState}), which {@code sync} never executes.
 */
public class WriteAccessBarrierSweepTest extends AbstractCairoTest {

    /**
     * Runs on POSIX only, and the skip loses no coverage: on real Windows {@code FlushFileBuffers} itself
     * returns {@code ERROR_ACCESS_DENIED} through a handle opened without {@code GENERIC_WRITE}; the sites a
     * Windows run adds are directory fsyncs, which the facade exempts ({@code isDirOrSoftLinkDir}) on every
     * platform; and {@code runStorageWorkloads()} issues {@code CHECKPOINT CREATE}, which the engine refuses
     * on Windows. The skip is an {@code Assume}, so a skipped run does not report a pass.
     */
    @Before
    public void setUp() {
        Assume.assumeFalse(
                "sweep models the Windows barrier contract on POSIX; on Windows it is redundant and"
                        + " CHECKPOINT CREATE is refused by design -- see this class's setUp javadoc",
                Os.isWindows()
        );
        super.setUp();
    }

    @Test
    public void testAdaptiveEpochNeverBarriersThroughAReadOnlyHandle() throws Exception {
        final WindowsBarrierContractFilesFacade ff = new WindowsBarrierContractFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            // Every applied batch cuts an epoch, so the ordinary inserts below reach the epoch barriers
            // without waiting for the 60s cadence.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "0");

            runStorageWorkloads();

            // Writer close fires the graceful-close epoch flush, a distinct site from the cadence-driven cut.
            engine.releaseInactive();

            ff.assertNoReadOnlyFileBarrier();
            assertBarriersWereActuallyIssued(ff);
            // The epoch itself must be reached; otherwise this case covers only the apply-path sites the sync
            // case already covers.
            Assert.assertTrue(
                    "no durable-epoch barrier was issued, so this case adds nothing over the sync one"
                            + ff.debugDump(),
                    ff.barrierCount("_epoch.manifest") > 0
            );
        });
    }

    @Test
    public void testSyncStorageWorkloadsNeverBarrierThroughAReadOnlyHandle() throws Exception {
        final WindowsBarrierContractFilesFacade ff = new WindowsBarrierContractFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");

            runStorageWorkloads();

            ff.assertNoReadOnlyFileBarrier();
            assertBarriersWereActuallyIssued(ff);
        });
    }

    /**
     * Guards against a vacuous sweep: a run that issues no barriers reports no violations, and would pass
     * with every barrier in the engine deleted.
     */
    private static void assertBarriersWereActuallyIssued(WindowsBarrierContractFilesFacade ff) {
        Assert.assertTrue(
                "the workloads issued no durability barriers at all, so this sweep proves nothing"
                        + ff.debugDump(),
                ff.barrierCount("") > 0
        );
    }

    /**
     * The workloads. Each operation is small: the point is to reach barrier sites, not volume, so the test
     * stays fast.
     */
    private void runStorageWorkloads() throws Exception {
        // Ordinary create + append, the plain commit path.
        execute("CREATE TABLE t (id INT, s SYMBOL, v DOUBLE, txt VARCHAR, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES (1, 'a', 1.5, 'one', '2024-06-10T01:00:00.000000Z')");
        execute("INSERT INTO t VALUES (2, 'b', 2.5, 'two', '2024-06-11T01:00:00.000000Z')");
        execute("INSERT INTO t VALUES (3, 'c', 3.5, 'three', '2024-06-12T01:00:00.000000Z')");

        // O3: lands before the last row of an existing partition.
        execute("INSERT INTO t VALUES (4, 'a', 4.5, 'four', '2024-06-10T00:30:00.000000Z')");

        // Structural DDL. These are one-shot writes that no epoch covers, so they take the eager grade even
        // under adaptive; the _meta.swp barrier sits on this path.
        execute("ALTER TABLE t ADD COLUMN extra LONG");
        execute("ALTER TABLE t ALTER COLUMN s ADD INDEX");
        execute("INSERT INTO t VALUES (5, 'd', 5.5, 'five', '2024-06-13T01:00:00.000000Z', 50)");
        // Widening, because a narrowing conversion is rejected before it reaches any barrier.
        execute("ALTER TABLE t ALTER COLUMN id TYPE LONG");

        // Native -> parquet -> native, both conversion directions.
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");
        execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-06-10'");

        // In-place column rewrite, then partition removal.
        execute("UPDATE t SET v = 9.5 WHERE id = 2");
        execute("ALTER TABLE t DROP PARTITION LIST '2024-06-11'");

        // Detach moves a partition directory, which is the rename-versus-publish family. The matching
        // ATTACH is not covered: it needs the .detached directory renamed to .attachable out of band. A
        // change to an attach-path barrier needs its own coverage.
        execute("ALTER TABLE t DETACH PARTITION LIST '2024-06-12'");

        // Checkpoint copies _txn/_cv and is the path a restore later lands on.
        execute("CHECKPOINT CREATE");
        execute("CHECKPOINT RELEASE");

        // A WAL table exercises the segment, event and sequencer barriers, a different family from the
        // table-writer ones above, and is the only path that reaches the durable epoch.
        execute("CREATE TABLE w (id INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO w VALUES (1, 'a', '2024-06-10T00:00:00.000000Z')");
        execute("INSERT INTO w VALUES (2, 'b', '2024-06-10T00:00:01.000000Z')");
        drainWalQueue();
        execute("ALTER TABLE w ADD COLUMN c2 SYMBOL");
        execute("INSERT INTO w VALUES (3, 'c', '2024-06-11T00:00:00.000000Z', 'x')");
        // O3 into the WAL table, so the apply path takes the merge branch and not just the append one.
        execute("INSERT INTO w VALUES (4, 'd', '2024-06-10T00:00:00.500000Z', 'y')");
        drainWalQueue();
        execute("TRUNCATE TABLE w");
        drainWalQueue();
    }
}
