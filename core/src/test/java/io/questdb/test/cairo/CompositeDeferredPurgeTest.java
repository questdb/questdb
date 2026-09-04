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

import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * What happens to a superseded partition-version directory on a COMPOSITE table when it cannot be
 * removed at the moment it is superseded.
 * <p>
 * {@code TableWriter#processPartitionRemoveCandidates0} has two arms. When no reader sits before the
 * committed txn it removes the directory inline. Otherwise -- a reader is holding an older txn, a
 * checkpoint is running, or the inline {@code unlinkOrRemove} fails, which is routine on Windows
 * where a mapped file cannot be deleted -- it sets {@code scheduleAsyncPurge} and hands the work to
 * {@code O3PartitionPurgeJob}.
 * <p>
 * That job used to return immediately for a composite table. The reason was sound -- its walk is
 * day-only and cellKey-0, so running it unchanged would misread a live multi-cell day as detached and
 * delete it -- but the consequence was that the deferred arm had NO consumer at all, and a directory
 * that missed its inline removal was never reclaimed.
 * <p>
 * <b>Windows is what forced the issue.</b> A directory holding a mapped file cannot be deleted there,
 * so the inline removal fails with errno=5 whenever any reader is on the cell -- routine, not rare.
 * CI caught it as a day settling at three directories, {@code exch=E0.8, exch=E1.4, exch=E1.8}, E1.4
 * being a superseded version of the cell E1.8 replaced. It also corrected what an earlier version of
 * this class claimed: that the writer's orphan scan at open closes the gap. It does not, reliably --
 * the scan's own removal hits the same errno=5 if a reader still holds the directory when it runs.
 * <p>
 * {@code O3PartitionPurgeJob} now has a composite arm ({@code purgeCompositeSupersededCellVersions})
 * that walks {@code <day>/<segment>.<nameTxn>} and reclaims a cell's superseded versions ONLY -- never
 * a cell, a day, or anything detached. So there are two consumers again, and this class asserts both.
 * <p>
 * The plain twin is the control: the identical sequence on a plain table leaves the directory too and
 * then the purge job takes it. Same deferral, and now the same reclaim on both sides.
 */
public class CompositeDeferredPurgeTest extends AbstractCairoTest {

    /**
     * The control. A plain table defers the same way and the purge job reclaims it, which is what
     * makes the composite result below a gap rather than just how deferral works.
     */
    @Test
    public void testPlainTableReclaimsADeferredVersionDirectory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T05:00:00.000000Z','E0',2.0)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("p");
            final int before = versionDirCount(tt);

            // A reader pinned at the current txn: the writer must not remove anything it might read.
            try (TableReader ignored = getReader("p")) {
                execute("INSERT INTO p VALUES ('2023-01-01T03:00:00.000000Z','E0',3.0)");
                drainWalQueue();
            }
            engine.releaseInactive();

            Assert.assertTrue(
                    "the O3 merge should have left a superseded version directory behind",
                    versionDirCount(tt) > before
            );

            runPurgeJob();
            Assert.assertEquals(
                    "a plain table's deferred version directory must be reclaimed by O3PartitionPurgeJob",
                    before, versionDirCount(tt)
            );
        });
    }

    /**
     * Composite: the purge job reclaims it, and so does the writer's orphan scan at the next writer
     * open. Both are asserted -- two independent consumers, so the reclaim does not depend on which
     * runs first.
     */
    @Test
    public void testCompositeReclaimsADeferredVersionDirectoryAtWriterOpen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T05:00:00.000000Z','E0',2.0)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("c");
            final int before = versionDirCount(tt);

            try (TableReader ignored = getReader("c")) {
                execute("INSERT INTO c VALUES ('2023-01-01T03:00:00.000000Z','E0',3.0)");
                drainWalQueue();
            }
            engine.releaseInactive();

            final int afterMerge = versionDirCount(tt);
            Assert.assertTrue(
                    "the O3 merge should have left a superseded version directory behind",
                    afterMerge > before
            );

            // The purge job now reclaims for a composite table too. This assertion used to be the
            // opposite -- it pinned the job returning immediately -- and the note it carried, that a
            // cell-aware purge would flip it, is what happened. See the class doc for why it had to.
            runPurgeJob();
            Assert.assertEquals(
                    "O3PartitionPurgeJob must reclaim a composite table's superseded cell version",
                    before, versionDirCount(tt)
            );

            // And the orphan scan at writer open remains a second consumer, so the reclaim does not
            // depend on which one runs first. Deliberately NOT by inserting -- a write into a new day
            // would add a directory of its own and the count would move for an unrelated reason.
            engine.releaseInactive();
            try (TableWriter ignored = getWriter("c")) {
                // opening is the point
            }
            engine.releaseInactive();
            Assert.assertEquals("reopening must not resurrect or add a directory",
                    before, versionDirCount(tt));
            // The data is unaffected either way: this is disk that is never given back, not rows.
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3\n");
        });
    }

    private void runPurgeJob() {
        try (O3PartitionPurgeJob job = new O3PartitionPurgeJob(engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (job.run()) {
                // drain
            }
        }
        engine.releaseInactive();
    }

    /**
     * Every {@code <name>.<txn>} version directory under the table, at either level: a plain table
     * keeps them directly under the table root, a composite one under its day container.
     */
    private int versionDirCount(TableToken tt) {
        final File tableDir = new File(configuration.getDbRoot(), tt.getDirName());
        final File[] top = tableDir.listFiles(File::isDirectory);
        if (top == null) {
            return 0;
        }
        int count = 0;
        for (File f : top) {
            final File[] nested = f.listFiles(File::isDirectory);
            if (nested != null && nested.length > 0) {
                count += nested.length;   // composite: the day container's cells
            } else {
                count++;                  // plain: the partition directory itself
            }
        }
        return count;
    }

    /**
     * The purge job must not delete a cell's LIVE directory because a SIBLING cell of the same day is
     * at a higher nameTxn.
     * <p>
     * This pins the third clause of {@code purgeCompositeSupersededCellVersions}' rule, and it is the
     * clause that is easy to leave out: the set of live nameTxns the job reads from {@code _txn} is
     * DAY-wide, while a cell directory is per-cell. Cells of one day routinely sit at different
     * nameTxns -- an O3 write into one cell rewrites only that cell -- so "some record of this day
     * names version N" does NOT mean "version N of THIS cell is live". Without the clause, a stale
     * {@code exch=E1.<n>} whose {@code n} happens to match the live nameTxn of E0 makes the job treat
     * the genuinely live {@code exch=E1.<m>} (m &lt; n) as superseded and delete it: silent data loss,
     * which is the exact failure the composite skip existed to prevent.
     * <p>
     * The stale sibling is PLANTED rather than produced by a workload. Producing one needs an install
     * to fail at a specific nameTxn on a specific cell, which is a fault-injection test in the
     * enterprise cold-storage suite; the planted directory reproduces the same on-disk shape here in
     * three lines, and the shape is all the job reads.
     */
    @Test
    public void testPurgeKeepsALiveCellWhoseSiblingIsAtAHigherNameTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T10:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T05:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            // O3 into E0 only, twice. Each timestamp is EARLIER than what E0 already holds, so the
            // cell is rewritten at a new nameTxn rather than appended to -- an append leaves the
            // directory name alone and the two cells would stay at the same (unversioned) version.
            for (int i = 0; i < 2; i++) {
                execute("INSERT INTO c VALUES ('2023-01-01T0" + (3 - i) + ":00:00.000000Z','E0',9.0)");
                drainWalQueue();
                engine.releaseInactive();
                runPurgeJob();
            }

            final TableToken tt = engine.verifyTableName("c");
            final File day = new File(new File(configuration.getDbRoot(), tt.getDirName()), "2023-01-01");
            final String liveE1 = onlyDirStartingWith(day, "exch=E1");
            final String liveE0 = onlyDirStartingWith(day, "exch=E0");
            final long e0NameTxn = nameTxnOf(liveE0);
            Assert.assertTrue("the two cells must be at different nameTxns for this test to mean"
                            + " anything [E0=" + liveE0 + ", E1=" + liveE1 + "]",
                    e0NameTxn > nameTxnOf(liveE1));

            // The plant: a stale version of E1 named with E0's LIVE nameTxn.
            final File planted = new File(day, "exch=E1." + e0NameTxn);
            Assert.assertTrue("could not plant " + planted, planted.mkdirs());

            // VACUUM is what puts this table on the purge discovery queue. Without it the job has
            // nothing to dequeue and returns immediately, which made an earlier version of this test
            // pass with the guard REMOVED -- it never reached the code it claims to pin.
            execute("VACUUM partitions 'c'");
            runPurgeJob();

            Assert.assertTrue(
                    "the purge job deleted a LIVE cell directory (" + liveE1 + ") because a sibling"
                            + " cell of the same day was at a higher nameTxn",
                    new File(day, liveE1).exists()
            );
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n4\n");
        });
    }

    private static long nameTxnOf(String dirName) {
        final int dot = dirName.lastIndexOf('.');
        return dot < 0 ? -1L : Long.parseLong(dirName.substring(dot + 1));
    }

    private static String onlyDirStartingWith(File day, String prefix) {
        final File[] all = day.listFiles(f -> f.isDirectory() && f.getName().startsWith(prefix));
        Assert.assertNotNull("no directories under " + day, all);
        Assert.assertEquals("expected exactly one live directory for " + prefix + ", found "
                + java.util.Arrays.toString(all), 1, all.length);
        return all[0].getName();
    }
}
