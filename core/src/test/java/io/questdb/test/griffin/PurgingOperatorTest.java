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

package io.questdb.test.griffin;

import com.sun.management.ThreadMXBean;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.griffin.PurgingOperator;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PurgingOperatorTest extends AbstractCairoTest {

    @Test
    public void testClearReleasesColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            // TableWriter keeps one operator for its whole lifetime and clears it
            // after every purge, so clear() must not leave column names behind.
            final PurgingOperator operator = new PurgingOperator(LOG, configuration, engine.getMessageBus());
            for (int i = 0; i < 3; i++) {
                operator.add(0, "sym", ColumnType.SYMBOL, IndexType.BITMAP, i, 0, -1);
                operator.add(1, "sym2", ColumnType.SYMBOL, IndexType.POSTING, i, 0, -1);
                Assert.assertEquals(2, operator.getColumnNameCount());
                operator.clear();
                Assert.assertEquals(0, operator.getColumnNameCount());
            }
        });
    }

    @Test
    public void testPurgePostingIndexDoesNotAllocate() throws Exception {
        assertMemoryLeak(() -> {
            // TableWriter runs a synchronous purge of superseded POSTING index files
            // from housekeep() on the commit path, so the purge must not allocate.
            execute("CREATE TABLE t (s SYMBOL INDEX TYPE POSTING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // The partition directory must exist: purge() scans it for sealed files.
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00.000000Z')");
            final TableToken tableToken = engine.verifyTableName("t");
            final PurgingOperator operator = new PurgingOperator(LOG, configuration, engine.getMessageBus());
            try (
                    Path path = new Path().of(configuration.getDbRoot()).concat(tableToken);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                Assert.assertTrue(configuration.getFilesFacade().exists(path.concat("1970-01-01").$()));
                path.of(configuration.getDbRoot()).concat(tableToken);
                runPostingPurgeRounds(operator, path, tableToken, 20_000);

                final ThreadMXBean threadMXBean = scope.getBean();
                final long threadId = Thread.currentThread().threadId();
                final long before = threadMXBean.getThreadAllocatedBytes(threadId);
                runPostingPurgeRounds(operator, path, tableToken, 10_000);
                final long allocated = threadMXBean.getThreadAllocatedBytes(threadId) - before;
                Assert.assertTrue("10,000 posting index purges allocated " + allocated + " bytes", allocated < 1024);
            }
        });
    }

    @Test
    public void testSealedFileRemoverReuseResetsState() throws Exception {
        final boolean[] isPcRemovalFailing = {true};
        final FilesFacade failingFf = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isPcRemovalFailing[0] && Utf8s.containsAscii(name, ".pc")) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(failingFf, () -> {
            final String columnName = "sym";
            final long postingTxn = 3;
            final long otherPostingTxn = 4;
            final long sealTxn = 7;
            final FilesFacade ff = configuration.getFilesFacade();
            try (
                    Path path = new Path().of(configuration.getDbRoot());
                    Path pci = new Path().of(configuration.getDbRoot());
                    Path pc0 = new Path().of(configuration.getDbRoot());
                    Path pv = new Path().of(configuration.getDbRoot());
                    Path otherPv = new Path().of(configuration.getDbRoot())
            ) {
                final int pathLen = path.size();
                touchFile(ff, PostingIndexUtils.coverInfoFileName(pci, columnName, postingTxn));
                touchFile(ff, PostingIndexUtils.coverDataFileName(pc0, columnName, 0, postingTxn, 2, sealTxn));
                touchFile(ff, PostingIndexUtils.valueFileName(pv, columnName, postingTxn, sealTxn));
                touchFile(ff, PostingIndexUtils.valueFileName(otherPv, columnName, otherPostingTxn, sealTxn));

                final PostingIndexUtils.SealedFileRemover remover = new PostingIndexUtils.SealedFileRemover();
                Assert.assertTrue(remover.remove(ff, path, pathLen, columnName, postingTxn));
                Assert.assertEquals(pathLen, path.size());
                Assert.assertTrue(ff.exists(pci.$()));
                Assert.assertTrue(ff.exists(pc0.$()));
                Assert.assertFalse(ff.exists(pv.$()));

                // The same instance must not carry the failure into the next call.
                isPcRemovalFailing[0] = false;
                Assert.assertFalse(remover.remove(ff, path, pathLen, columnName, postingTxn));
                Assert.assertEquals(pathLen, path.size());
                Assert.assertFalse(ff.exists(pci.$()));
                Assert.assertFalse(ff.exists(pc0.$()));
                Assert.assertFalse(ff.exists(pv.$()));
                Assert.assertTrue(ff.exists(otherPv.$()));
            }
        });
    }

    private static void runPostingPurgeRounds(PurgingOperator operator, Path path, TableToken tableToken, int rounds) {
        for (int i = 0; i < rounds; i++) {
            operator.add(0, "s", ColumnType.SYMBOL, IndexType.POSTING, 5, 0, -1);
            operator.purge(path, tableToken, ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY, false, 0, 10);
            operator.clear();
        }
    }

    private void touchFile(FilesFacade ff, LPSZ file) {
        final long fd = ff.openRW(file, configuration.getWriterFileOpenOpts());
        Assert.assertTrue("could not create " + file, fd > -1);
        ff.close(fd);
    }
}
