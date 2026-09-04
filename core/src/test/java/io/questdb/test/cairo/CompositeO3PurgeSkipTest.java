/*+*****************************************************************************
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

import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Written to test whether {@code O3PartitionPurgeJob}'s composite skip is harmless — the project's
 * invariant permits a skip only with such a test. It was not harmless: 20 rounds of out-of-order
 * writes took a composite table from 1 to 4 day-level directories while its plain twin stayed at 1.
 * <p>
 * <b>The cause was NOT the purge job.</b> Running the same churn with the purge job ON and OFF leaves
 * a byte-identical set of directories, so the writer produces them and the purge merely fails to
 * reclaim them. {@code TableWriter.openLastPartitionAndSetAppendPosition} opened a day-level "last
 * partition" for a routed composite table; {@code openPartition} resolved that path with the
 * cell-blind {@code setStateForTimestamp} — yielding {@code <day>.<cellKey-0 nameTxn>} — and then
 * called {@code ff.mkdirs}, creating a directory nothing ever reads. A composite table's partitions
 * are the CELLS of the day, and the fast-append path opens those itself.
 * <p>
 * Sub-project 1A fixed the producer. The skip itself has since been replaced by a composite arm,
 * {@code O3PartitionPurgeJob#purgeCompositeSupersededCellVersions}, which honours the constraint this
 * class recorded: the LIVE container is the UNVERSIONED day directory, so a naive "keep the newest
 * {@code <day>.<txn>}" walk would delete every live cell. That arm therefore refuses any root
 * directory whose name carries a dot, and only ever removes a version that a strictly newer, LIVE
 * version of the SAME cell supersedes. This test is unaffected either way -- it counts day-level
 * directories, which that arm never creates and never removes.
 * <p>
 * See {@link CompositeO3LayoutTest} for the on-disk layout this rests on.
 */
public class CompositeO3PurgeSkipTest extends AbstractCairoTest {
    private static O3PartitionPurgeJob purgeJob;

    @AfterClass
    public static void tearDownPurgeJob() {
        purgeJob = Misc.free(purgeJob);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        purgeJob = new O3PartitionPurgeJob(engine, 1);
    }

    @Test
    public void testCompositeReclaimsObsoletePartitionVersions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            seed("c");
            seed("p");
            drainWalQueue();

            final int compositeBefore = countPartitionDirs("c");
            final int plainBefore = countPartitionDirs("p");

            // 20 rounds of out-of-order writes into an already-written day: each round rewrites the
            // partition, producing a new version directory and orphaning the previous one.
            for (int round = 1; round <= 20; round++) {
                churn("c", round);
                churn("p", round);
                drainWalQueue();
                engine.releaseInactive();
                runPartitionPurgeJobs();
            }
            drainWalQueue();
            engine.releaseInactive();
            runPartitionPurgeJobs();

            final int compositeAfter = countPartitionDirs("c");
            final int plainAfter = countPartitionDirs("p");

            // The plain twin is the control: it proves the workload really does create obsolete
            // versions and that purge really does reclaim them on this build.
            Assert.assertTrue(
                    "control failed: the plain twin did not accumulate-then-reclaim, so this workload"
                            + " does not exercise purge at all (plainBefore=" + plainBefore
                            + ", plainAfter=" + plainAfter + ")",
                    plainAfter <= plainBefore + 1);

            Assert.assertTrue(
                    "composite leaked day-level partition version directories: before=" + compositeBefore
                            + " after=" + compositeAfter + " (plain: " + plainBefore + " -> " + plainAfter
                            + "). Sub-project 1A: TableWriter.openLastPartitionAndSetAppendPosition must not"
                            + " open a day-level last partition for a routed composite table -- openPartition"
                            + " resolves the path cell-blind and then ff.mkdirs CREATES it.",
                    compositeAfter <= compositeBefore + 1);
        });
    }

    private void churn(String table, int round) throws Exception {
        // Lands inside the already-written day. MOST rounds are genuinely out-of-order; the ones
        // where (round % 6) == 5 land at 05:30, after the seeded max of 05:00, and are in-order
        // appends. That is fine for this measurement -- ~17 of 20 rounds still rewrite the partition,
        // and the growth it produces is unambiguous -- but the distinction is stated rather than
        // glossed, because a reader who assumed all 20 were O3 would mis-size the leak.
        execute("INSERT INTO " + table + " VALUES ('2023-01-02T0" + (round % 6) + ":30:00.000000Z','E"
                + (round % 3) + "'," + round + ".0)");
    }

    /**
     * Counts directories under the table root that look like a partition version (a dated directory,
     * with or without a .<txn> suffix). An obsolete version left behind by O3 shows up here.
     */
    private int countPartitionDirs(String table) throws IOException {
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 2)) {
            return (int) walk
                    .filter(Files::isDirectory)
                    .filter(p -> {
                        final Path parent = p.getParent();
                        return parent != null
                                && parent.getFileName().toString().startsWith(table + "~")
                                && p.getFileName().toString().startsWith("2023-");
                    })
                    .count();
        }
    }

    private void runPartitionPurgeJobs() {
        // when the reader is returned to pool, it remains in open state
        // holding files such that purge fails with access violation
        engine.releaseInactive();
        purgeJob.drain(0);
    }

    private void seed(String table) throws Exception {
        execute("INSERT INTO " + table + " VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-02T05:00:00.000000Z','E1',5.0)");
    }
}
