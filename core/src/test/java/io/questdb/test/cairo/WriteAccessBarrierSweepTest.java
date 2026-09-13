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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.WindowsBarrierContractFilesFacade;
import org.junit.Assert;
import org.junit.Test;

/**
 * A tripwire for the whole class of defect, not one instance of it: drive a broad slice of the storage
 * surface and fail if ANY durability barrier is issued through a handle opened without write access.
 * <p>
 * Such a barrier is fine on POSIX and {@code ERROR_ACCESS_DENIED}s on Windows, and OSS PR CI is Linux-only
 * (all 27 jobs), so nothing here notices on its own -- see {@link WindowsBarrierContractFilesFacade}. Four
 * sites shipped written that way. The first was found only because an Enterprise cold-storage test happened
 * to exercise it on the Windows leg of a different pipeline, two months later; the other three
 * ({@code _meta.swp}, {@code _todo_}, and the O3 rewrite) were found by this sweep, in about a second,
 * having been missed by a careful reading of the same code.
 * <p>
 * <b>Linux is the platform that has to be right</b>, and Linux cannot see this bug by construction, so the
 * guard has to live here rather than in a Windows CI leg that does not exist. That makes this test the only
 * thing standing between the next barrier and a platform outage, which is why it sweeps rather than pins.
 * <p>
 * Coverage is honestly bounded by the workloads below: a barrier on a path none of them reach is still
 * invisible. Adding a workload is cheap -- please do, rather than assuming the sweep already covers what you
 * are about to write.
 *
 * <h2>Why both commit modes</h2>
 * They take DIFFERENT barrier sites, so neither subsumes the other:
 * <ul>
 *   <li>{@code sync} takes the eager grade at every apply-path site, including the ones {@code adaptive}
 *       defers to the durable epoch.</li>
 *   <li>{@code adaptive} reaches sites {@code sync} never executes at all -- the epoch itself:
 *       {@code DurableEpochManifest}, the {@code _txn.epoch}/{@code _cv.epoch} payload copies, and
 *       {@code fsyncMaterializedState}. Running only {@code sync} would leave the entire epoch machinery,
 *       which is the whole point of this branch, unswept.</li>
 * </ul>
 */
public class WriteAccessBarrierSweepTest extends AbstractCairoTest {

    @Test
    public void testAdaptiveEpochNeverBarriersThroughAReadOnlyHandle() throws Exception {
        final WindowsBarrierContractFilesFacade ff = new WindowsBarrierContractFilesFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            // Every applied batch cuts an epoch, so the epoch barriers are reached by the ordinary inserts
            // below instead of depending on a 60s cadence this test will never wait for.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "0");

            runStorageWorkloads();

            // Writer close fires the graceful-close epoch flush, which is a distinct site from the
            // cadence-driven cut taken above.
            engine.releaseInactive();

            ff.assertNoReadOnlyFileBarrier();
            assertBarriersWereActuallyIssued(ff);
            // The epoch machinery specifically: without this the test could pass having exercised only the
            // same apply-path sites the sync case already covers, which is precisely the gap that let the
            // epoch go unswept in the first place.
            Assert.assertTrue(
                    "no durable-epoch barrier was issued, so this case adds nothing over the sync one"
                            + ff.debugDump(),
                    ff.barrierCount(".epoch") > 0 || ff.barrierCount("_epoch.manifest") > 0
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
     * Without this the sweep is vacuous: a run that issued no barriers reports no violations, and would stay
     * green with every barrier in the engine deleted.
     */
    private static void assertBarriersWereActuallyIssued(WindowsBarrierContractFilesFacade ff) {
        Assert.assertTrue(
                "the workloads issued no durability barriers at all, so this sweep proves nothing"
                        + ff.debugDump(),
                ff.barrierCount("") > 0
        );
    }

    /**
     * The workload battery. Deliberately small per operation -- this is about reaching barrier SITES, not
     * about volume, so it stays fast enough to sit in PR CI.
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

        // Structural DDL. These are one-shot writes that no epoch covers, so they take the eager grade
        // even under adaptive -- and _meta.swp, one of the four broken sites, lives here.
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
        // ATTACH is NOT covered: it needs the .detached directory renamed to .attachable out of band, and
        // the file juggling that takes is more fragility than the extra site is worth here. If you touch
        // an attach-path barrier, cover it yourself.
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
