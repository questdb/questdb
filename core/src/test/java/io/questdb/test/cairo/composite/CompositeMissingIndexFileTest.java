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


package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * A COMPOSITE active partition leaves {@code columns[]} closed, so
 * {@code TableWriter#openLastPartitionAndSetAppendPosition} configures its BITMAP indexers through
 * {@code configureIndexersForClosedActivePartition} instead of through {@code openPartition}. That
 * path must tolerate an indexed column whose key file is absent, the way the plain-partition path
 * and the POSTING path already do - a restored table reaches the first writer open in exactly that
 * state, because backup carries no column files for a column with no rows in the partition and the
 * restore's index rebuild skips it on the same test. Without the tolerance the writer throws
 * "index does not exist" while it is being constructed, and WAL apply suspends the table.
 */
public class CompositeMissingIndexFileTest extends AbstractCairoTest {

    @Test
    public void testCompositeActivePartitionOpensWithoutRowLessColumnIndex() throws Exception {
        assertWriterOpens(true);
    }

    /**
     * The same table with merge-append off: the active partition stays plain, {@code openPartition}
     * runs, and the writer has always opened. The control that says the arm above is the one under
     * test.
     */
    @Test
    public void testPlainActivePartitionOpensWithoutRowLessColumnIndex() throws Exception {
        assertWriterOpens(false);
    }

    private void assertWriterOpens(boolean mergeAppend) throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, String.valueOf(mergeAppend));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::INT, timestamp_sequence('2022-02-24', 60*1000000L) ts" +
                    " FROM long_sequence(500)");
            drainWalQueue();
            // Backdated, into the middle of the day the table is still writing: merge-append folds it
            // into the partition's own geometry and leaves the ACTIVE partition composite.
            execute("INSERT INTO t SELECT x::INT + 5000, timestamp_sequence('2022-02-24T03:00:07', 1000000L) ts" +
                    " FROM long_sequence(5)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("t");
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertEquals("merge-append decides whether the active partition is composite",
                        mergeAppend,
                        reader.getTxFile().isPartitionComposite(reader.getTxFile().getPartitionCount() - 1));
            }

            // An indexed column with no rows in any partition it now spans - ADD COLUMN builds its
            // key file on the active partition, which is what the removal below takes away.
            execute("ALTER TABLE t ADD COLUMN sym_top SYMBOL INDEX");
            drainWalQueue();
            Assert.assertTrue("fixture must remove a sym_top index file", removeSymTopIndexFiles(tt));

            // The cold open: the writer is constructed from _txn alone, with no partition open.
            engine.releaseAllWriters();
            try (TableWriter writer = engine.getWriter(tt, "testing")) {
                Assert.assertNotNull(writer);
            }

            // And the column still takes rows and reads back through its index afterwards - the
            // write path rebuilds what the missing key file left behind.
            execute("INSERT INTO t VALUES (7, '2022-02-24T09:00:00.000000Z', 'AA')");
            drainWalQueue();
            assertQuery("SELECT i FROM t WHERE sym_top = 'AA'").returns("i\n7\n");
        });
    }

    private boolean removeSymTopIndexFiles(TableToken tableToken) {
        final File tableDir = new File(engine.getConfiguration().getDbRoot(), tableToken.getDirName());
        final File[] partitionDirs = tableDir.listFiles(File::isDirectory);
        Assert.assertNotNull(partitionDirs);
        boolean removed = false;
        for (File partitionDir : partitionDirs) {
            final File[] indexFiles = partitionDir.listFiles(
                    (ignore, name) -> name.startsWith("sym_top.k") || name.startsWith("sym_top.v"));
            if (indexFiles != null) {
                for (File indexFile : indexFiles) {
                    Assert.assertTrue(indexFile.delete());
                    removed = true;
                }
            }
        }
        return removed;
    }
}
