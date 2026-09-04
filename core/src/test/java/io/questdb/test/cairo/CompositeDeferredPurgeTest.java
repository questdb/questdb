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
 * That job returns immediately for a composite table, by design: its walk is day-only and cellKey-0,
 * so running it would misread a live multi-cell day as detached and delete it. Skipping is correct.
 * <p>
 * The consequence is what this class pins: for a composite table the deferred arm has NO consumer, so
 * a directory that misses its inline removal is never reclaimed. The writer's own orphan scan does not
 * close the gap either -- it runs at writer OPEN, and the same reader that forced the deferral is
 * still what stops the removal.
 * <p>
 * The plain twin is the control, and it is the point: the identical sequence on a plain table leaves
 * the directory too, and then the purge job takes it. Same deferral, one reclaim path, one not.
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
     * Composite: the purge job does NOT reclaim it (that job skips composite tables outright), and the
     * writer's orphan scan does, at the next writer open. Both halves are asserted, because the first
     * is what makes the second load-bearing.
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

            // The purge job is the plain table's answer and is a NO-OP here: it returns immediately
            // for a composite table. If this ever reclaims, a cell-aware purge has landed and the
            // orphan scan is no longer the only thing standing between this and a leak.
            runPurgeJob();
            Assert.assertEquals(
                    "O3PartitionPurgeJob must not reclaim for a composite table -- it skips them",
                    afterMerge, versionDirCount(tt)
            );

            // Reopen the writer: purgeUnusedPartitions() runs in its constructor and the orphan scan
            // takes the superseded directory. Deliberately NOT by inserting -- a write into a new day
            // would add a directory of its own and the count would move for an unrelated reason.
            engine.releaseInactive();
            try (TableWriter ignored = getWriter("c")) {
                // opening is the point
            }
            engine.releaseInactive();

            Assert.assertEquals(
                    "a composite table's deferred version directory must be reclaimed by the orphan"
                            + " scan at writer open -- it is the only path that does, since the purge"
                            + " job above skips composite tables",
                    before, versionDirCount(tt)
            );
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
}
