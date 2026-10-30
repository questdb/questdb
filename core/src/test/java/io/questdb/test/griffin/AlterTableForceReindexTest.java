/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class AlterTableForceReindexTest extends AbstractCairoTest {
    private static final String K1_AFTER_INSERT = """
            ts\tx\tv
            2024-01-01T08:00:00.000000Z\t1\t1
            2024-01-02T08:00:00.000000Z\t4\t4
            2024-01-03T08:00:00.000000Z\t7\t7
            2024-01-04T10:00:00.000000Z\t10\t10
            """;
    private static final String K1_ROWS = """
            ts\tx\tv
            2024-01-01T08:00:00.000000Z\t1\t1
            2024-01-02T08:00:00.000000Z\t4\t4
            2024-01-03T08:00:00.000000Z\t7\t7
            """;

    @Test
    public void testColdPartitionsAreSkipped() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false);
            execute("alter table t convert partition to parquet list '2024-01-01'");
            // Cold: parquet served from the object store, no local data
            try (TableWriter writer = getWriter("t")) {
                writer.getTxWriter().setPartitionParquetGenerated(0, false);
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final String[] before = partitionDirs();

            final String coldOnly = "alter table t force reindex partition list '2024-01-01'";
            assertExceptionNoLeakCheck(coldOnly, coldOnly.indexOf("'2024-01-01'"), "cannot reindex cold partitions, they have no local data");
            Assert.assertArrayEquals(before, partitionDirs());

            // a cold partition next to others is skipped
            execute("alter table t force reindex partition list '2024-01-01', '2024-01-02'");
            String[] after = partitionDirs();
            Assert.assertEquals(before[0], after[0]);
            Assert.assertNotEquals(before[1], after[1]);
            Assert.assertEquals(before[2], after[2]);

            execute("alter table t force reindex");
            after = partitionDirs();
            Assert.assertEquals(before[0], after[0]);
            Assert.assertNotEquals(before[2], after[2]);
            Assert.assertNotEquals(before[3], after[3]);
        });
    }

    @Test
    public void testColumnAndPartitionList() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false);
            final String[] before = partitionDirs();
            deleteIndexKeyFile(before[1], "b");
            final String query = "select ts, x, v from t where b = 'k1'";
            assertExceptionNoLeakCheck(query, -1, "could not open");

            execute("alter table t force reindex column b partition list '2024-01-02', '2024-01-02'");
            final String[] after = partitionDirs();
            Assert.assertEquals(before[0], after[0]);
            Assert.assertNotEquals(before[1], after[1]);
            Assert.assertEquals(before[2], after[2]);
            Assert.assertEquals(before[3], after[3]);
            assertIndexQueries(K1_ROWS);
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table e (ts timestamp, s symbol index) timestamp(ts) partition by day wal");
            execute("alter table e force reindex");
            execute("create table n (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            execute("insert into n values ('2024-01-01', 'a')");
            drainWalQueue();
            // no index to rebuild
            execute("alter table n force reindex");
            assertQuery("select ts, s from n").timestamp("ts").expectSize().returns("ts\ts\n2024-01-01T00:00:00.000000Z\ta\n");
        });
    }

    @Test
    public void testErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false);
            String sql = "alter table t force reindex column zz";
            assertExceptionNoLeakCheck(sql, sql.indexOf("zz"), "Invalid column: zz");
            sql = "alter table t force reindex column x";
            assertExceptionNoLeakCheck(sql, sql.lastIndexOf('x'), "column is not indexed");
            sql = "alter table t force reindex partition list '2025-01-01'";
            assertExceptionNoLeakCheck(sql, sql.indexOf("'2025"), "partition does not exist");
            sql = "alter table t force reindex partition";
            assertExceptionNoLeakCheck(sql, sql.length(), "'list' expected");
            sql = "alter table t force reindex now";
            assertExceptionNoLeakCheck(sql, sql.indexOf("now"), "'partition' expected");
            sql = "alter table t force rebuild";
            assertExceptionNoLeakCheck(sql, sql.indexOf("rebuild"), "'drop' or 'reindex' expected");

            execute("create table np (ts timestamp, s symbol index) timestamp(ts)");
            sql = "alter table np force reindex";
            assertExceptionNoLeakCheck(sql, -1, "table is not partitioned, use REINDEX TABLE ... LOCK EXCLUSIVE");
        });
    }

    @Test
    public void testOpenReaderKeepsReplacedVersion() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false);
            final File day1 = new File(tableDir(), partitionDirs()[0]);
            try (RecordCursorFactory factory = select("select ts, x, v from t where b = 'k1'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    execute("alter table t force reindex");
                    Assert.assertTrue(day1.exists());
                    println(factory, cursor);
                    TestUtils.assertEquals(K1_ROWS, sink);
                }
            }
            engine.releaseAllReaders();
            releaseSavePointHolds("t");
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                //noinspection StatementWithEmptyBody
                while (purgeJob.run()) {
                }
            }
            Assert.assertFalse(day1.exists());
            Assert.assertEquals(4, partitionDirs().length);
            assertIndexQueries(K1_ROWS);
        });
    }

    @Test
    public void testRebuildsNativeAndParquetPartitions() throws Exception {
        assertRebuildsAllPartitions(false);
    }

    @Test
    public void testRebuildsNativeAndParquetPartitionsWal() throws Exception {
        assertRebuildsAllPartitions(true);
    }

    private static void deleteIndexKeyFile(String partitionDir, String column) {
        engine.releaseAllReaders();
        final File keyFile = new File(new File(tableDir(), partitionDir), column + ".k");
        Assert.assertTrue(keyFile.getPath(), keyFile.delete());
    }

    private static String[] partitionDirs() {
        final String[] dirs = tableDir().list((dir, name) -> name.startsWith("2024-"));
        Assert.assertNotNull(dirs);
        Arrays.sort(dirs);
        return dirs;
    }

    private static File tableDir() {
        return new File(configuration.getDbRoot().toString(), engine.verifyTableName("t").getDirName());
    }

    private void assertIndexQueries(String expected) throws Exception {
        assertQuery("select ts, x, v from t where b = 'k1'")
                .timestamp("ts")
                .withPlanContaining("Index forward scan on: b")
                .returns(expected);
        assertQuery("select ts, x, v from t where p = 'k1'")
                .timestamp("ts")
                .withPlanContaining("Index forward scan on: p")
                .returns(expected);
        assertQuery("select ts, x, v from t where c = 'k1'")
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("CoveringIndex on: c")
                .returns(expected);
    }

    private void assertRebuildsAllPartitions(boolean isWal) throws Exception {
        assertMemoryLeak(() -> {
            createTable(isWal);
            execute("alter table t convert partition to parquet list '2024-01-02'");
            drainWalQueue();
            releaseSavePointHolds("t");
            final String[] before = partitionDirs();
            Assert.assertEquals(4, before.length);
            // a broken native index, the active partition's included
            deleteIndexKeyFile(before[0], "b");
            deleteIndexKeyFile(before[3], "b");

            execute("alter table t force reindex");
            releaseSavePointHolds("t");
            final String[] after = partitionDirs();
            Assert.assertEquals(4, after.length);
            for (int i = 0; i < 4; i++) {
                Assert.assertNotEquals(before[i], after[i]);
            }
            assertIndexQueries(K1_ROWS);

            // the writer appends into the new version of the active partition
            execute("insert into t values ('2024-01-04T10:00:00.000000Z', 10, 10, 'k1', 'k1', 'k1')");
            drainWalQueue();
            assertIndexQueries(K1_AFTER_INSERT);
        });
    }

    private void createTable(boolean isWal) throws Exception {
        execute("create table t (ts timestamp, x long, v long, b symbol index, p symbol index type posting, c symbol) timestamp(ts) partition by day "
                + (isWal ? "wal" : "bypass wal"));
        execute("alter table t alter column c add index type posting include (x, v)");
        drainWalQueue();
        // days 1-4: x 1-2, 3-5, 6-8, 9
        execute("""
                insert into t select dateadd('h', x::int * 8, '2024-01-01T00:00:00.000000Z'), x, x,
                'k' || (x % 3), 'k' || (x % 3), 'k' || (x % 3)
                from long_sequence(9)
                """);
        drainWalQueue();
    }
}
