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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * An O3 append into an existing partition reads that partition's last aux entry to find where the
 * data vector ends. It trusts the aux file to already hold an entry for every row {@code _txn}/{@code _cv}
 * claim; the read is taken through a mapping that has just been extended to the NEW row count, so a
 * short file is not a read error — the missing tail simply reads back as zeros.
 *
 * <p>Those zeros used to travel: under {@code -ea} a zero varchar header trips an assertion deep in the
 * type driver, with no column, partition or size in the message; with assertions off it yields a data
 * vector size of 0, and the damage is only noticed later and elsewhere, when some reader opens the
 * partition and reports {@code Invalid column size}. This asserts the shortfall is named where it is
 * detectable — at the append, with the file length, the length required, and the column top.
 *
 * <p>Seen in the wild on PR #7411 CI: {@code WalWriterFuzzTest#testCreateTableAsParquet} died on the
 * assertion during an O3 apply, and the follow-up query then reported {@code Invalid column size} for a
 * different column of the same partition — two unrelated-looking failures, one short aux vector.
 */
public class O3AuxVectorShortfallTest extends AbstractCairoTest {
    private static final LogCapture capture = new LogCapture();

    @Before
    @Override
    public void setUp() {
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testO3AppendNamesShortAuxVectorInsteadOfReadingZeros() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Two rows in 2024-06-10, then a later partition so 2024-06-10 is no longer the active
            // one: its column files are closed, and an O3 row landing in it takes the append-mid-
            // partition path that maps the aux file fresh.
            execute("INSERT INTO t VALUES" +
                    "('aaaaaaaaaaaaaaaaaaaaaaaa','2024-06-10T01:00:00.000000Z')," +
                    "('bbbbbbbbbbbbbbbbbbbbbbbb','2024-06-10T02:00:00.000000Z')," +
                    "('cccccccccccccccccccccccc','2024-06-11T01:00:00.000000Z')");

            // Drop the last row's aux entry, leaving the file one entry short of what _txn says.
            final long keptEntries = 1;
            truncateAuxVector("t", 0, ColumnType.getDriver(ColumnType.VARCHAR).getAuxVectorOffset(keptEntries));

            try {
                // O3: before the table's max timestamp, after 2024-06-10's own max, so it appends to
                // the tail of that partition and reads the entry that is no longer there.
                execute("INSERT INTO t VALUES('dddddddddddddddddddddddd','2024-06-10T03:00:00.000000Z')");
                Assert.fail("expected the short aux vector to be reported");
            } catch (CairoException e) {
                // The caller only ever sees the O3 wrapper -- processO3Block rethrows a generic message
                // once a worker has set o3ErrorCount, which is exactly why the detail has to reach the
                // LOG at the origin. This is the shape the CI failure had, with a bare AssertionError
                // in place of the message asserted below.
                TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
            }

            // The diagnostic itself: what was short, by how much, and for which column and partition.
            capture.assertLogged("aux vector is shorter than the partition row count");
            capture.assertLogged("columnType=VARCHAR");
            capture.assertLogged("auxFileLen=16");
            capture.assertLogged("requiredLen=32");
            capture.assertLogged("srcDataMax=2");
            capture.assertLogged("srcDataTop=0");
        });
    }

    private static void truncateAuxVector(String tableName, int partitionIndex, long newLen) {
        final FilesFacade ff = configuration.getFilesFacade();
        final TableToken token = engine.verifyTableName(tableName);
        final long partitionTs;
        final long partitionNameTxn;
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertTrue("partition not found: " + partitionIndex, partitionIndex < reader.getPartitionCount());
            partitionTs = reader.getTxFile().getPartitionTimestampByIndex(partitionIndex);
            partitionNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }

        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForNativePartition(
                    path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            TableUtils.iFile(path, "v", -1L);
            final long fd = ff.openRW(path.$(), 0);
            Assert.assertTrue("could not open aux file: " + path, fd >= 0);
            try {
                Assert.assertTrue("truncate failed", ff.truncate(fd, newLen));
            } finally {
                ff.close(fd);
            }
        }
    }
}
