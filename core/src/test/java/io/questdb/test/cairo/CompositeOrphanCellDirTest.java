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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Orphaned CELL directories, and the scan that has to reclaim them without touching a live one.
 * <p>
 * A partition install that dies part way through -- an enterprise cold-switch or parquet commit
 * failing between the file moves and the day's single {@code _txn} commit -- leaves
 * {@code <day>/<cell>.<nameTxn>} directories that {@code _txn} never recorded. The writer's
 * open-time scan walks the table root's immediate children, which for a composite table are DAY
 * containers, so a cell's directory one level deeper was never discovered: measured as five
 * directories surviving for a two-cell day, through a writer reopen.
 * <p>
 * The DANGEROUS direction is the second test. This is the code that deletes partition directories,
 * and the day-level comment in {@code removePartitionDirsNotAttached} warns exactly why: answer
 * "is this live?" wrong and a day's live cells are queued for removal. So reclaiming an orphan is
 * asserted alongside a live day surviving the same scan with all its rows.
 */
public class CompositeOrphanCellDirTest extends AbstractCairoTest {

    @Test
    public void testALiveCellSurvivesTheScanThatReclaimsItsOrphan() throws Exception {
        assertMemoryLeak(() -> {
            createTwoCellDay();
            final TableToken tt = engine.verifyTableName("c");
            final File day = dayDir(tt);

            // An orphan for cell E0 alongside its live directory: same cell, a name txn `_txn` does
            // not point at, which is exactly the shape a failed install leaves.
            final String liveName = liveCellDirName(day, "exch=E0");
            final File orphan = new File(day, "exch=E0.99999");
            Assert.assertTrue("could not plant the orphan", orphan.mkdirs());
            Assert.assertTrue("the orphan must sit beside a LIVE directory for this to prove anything",
                    liveName != null && new File(day, liveName).isDirectory());

            engine.releaseInactive();
            try (TableWriter ignored = getWriter("c")) {
                // Opening the writer runs purgeUnusedPartitions.
            }
            engine.releaseInactive();

            Assert.assertFalse("the orphaned cell directory must be reclaimed [" + orphan + "]", orphan.exists());
            Assert.assertTrue("the LIVE cell directory must survive the same scan [" + liveName + "]",
                    new File(day, liveName).isDirectory());

            // The rows are the real oracle: a scan that removed a live cell would lose them.
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("SELECT ts, exch, px FROM c WHERE ts IN '2023-01-01' ORDER BY ts")
                    .noLeakCheck().timestamp("ts")
                    .returns("ts\texch\tpx\n"
                            + "2023-01-01T01:00:00.000000Z\tE0\t1.0\n"
                            + "2023-01-01T02:00:00.000000Z\tE1\t2.0\n");
        });
    }

    /**
     * A directory whose cell is not attached AT ALL is left alone, deliberately. The scan identifies an
     * orphan by rendering each ATTACHED cell's segment and comparing name txns; a segment belonging to
     * no attached record cannot be resolved to a cellKey, and guessing is how a live day gets deleted.
     * Conservative, and stated rather than discovered.
     */
    @Test
    public void testADirectoryForAnUnknownCellIsLeftAlone() throws Exception {
        assertMemoryLeak(() -> {
            createTwoCellDay();
            final TableToken tt = engine.verifyTableName("c");
            final File day = dayDir(tt);
            final File unknown = new File(day, "exch=NEVER_INGESTED.7");
            Assert.assertTrue("could not plant the unknown-cell directory", unknown.mkdirs());

            engine.releaseInactive();
            try (TableWriter ignored = getWriter("c")) {
                // purgeUnusedPartitions
            }
            engine.releaseInactive();

            Assert.assertTrue("a directory for an unattached cell must be left alone", unknown.exists());
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
        });
    }

    private void createTwoCellDay() throws Exception {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY, exch WAL");
        execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private File dayDir(TableToken tt) {
        return new File(new File(configuration.getDbRoot(), tt.getDirName()), "2023-01-01");
    }

    /**
     * The name of the day's live directory for {@code segment}, whatever name txn it currently carries.
     */
    private String liveCellDirName(File day, String segment) {
        final String[] names = day.list();
        if (names == null) {
            return null;
        }
        for (String name : names) {
            if (name.equals(segment) || (name.startsWith(segment + ".") && !name.endsWith(".99999"))) {
                return name;
            }
        }
        return null;
    }
}
