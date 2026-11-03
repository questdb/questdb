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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
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
    public void testCancelStopsAfterCurrentPartition() throws Exception {
        assertInterruptedAfterFirstPartition(SqlExecutionCircuitBreaker.STATE_CANCELLED, "cancelled by user");
    }

    @Test
    public void testColdPartitionsAreSkipped() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false);
            // Cold: parquet served from the object store, no local data
            try (TableWriter writer = getWriter("t")) {
                writer.getTxWriter().setPartitionParquetGenerated(0, false);
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final String[] before = partitionDirs();

            final String coldOnly = "alter table t force reindex parquet partition list '2024-01-01'";
            assertExceptionNoLeakCheck(coldOnly, coldOnly.indexOf("'2024-01-01'"), "cannot reindex cold partitions, they have no local data");
            Assert.assertArrayEquals(before, partitionDirs());

            // a cold partition next to others is skipped
            execute("alter table t force reindex parquet partition list '2024-01-01', '2024-01-02'");
            releaseSavePointHolds("t");
            String[] after = partitionDirs();
            Assert.assertEquals(before[0], after[0]);
            Assert.assertNotEquals(before[1], after[1]);
            Assert.assertEquals(before[2], after[2]);

            execute("alter table t force reindex parquet");
            releaseSavePointHolds("t");
            after = partitionDirs();
            Assert.assertEquals(before[0], after[0]);
            Assert.assertNotEquals(before[1], after[1]);
            Assert.assertNotEquals(before[2], after[2]);
            // native
            Assert.assertEquals(before[3], after[3]);
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

            execute("alter table t force reindex parquet column b partition list '2024-01-02', '2024-01-02'");
            releaseSavePointHolds("t");
            final String[] after = partitionDirs();
            Assert.assertEquals(before[0], after[0]);
            Assert.assertNotEquals(before[1], after[1]);
            Assert.assertEquals(before[2], after[2]);
            Assert.assertEquals(before[3], after[3]);
            assertIndexQueries(K1_ROWS);
        });
    }

    @Test
    public void testColumnIndexDroppedWal() throws Exception {
        assertMemoryLeak(() -> {
            // b's index comes from CREATE TABLE, which the WAL sequencer metadata keeps after DROP INDEX
            createTable(true);
            execute("alter table t alter column b drop index");
            drainWalQueue();
            final String[] before = partitionDirs();
            String sql = "alter table t force reindex parquet column b";
            assertExceptionNoLeakCheck(sql, sql.lastIndexOf('b'), "column is not indexed [column=b]");
            // an indexed column the WAL apply job has not added to the table yet
            execute("alter table t add column z symbol index");
            sql = "alter table t force reindex parquet column z";
            assertExceptionNoLeakCheck(sql, sql.lastIndexOf('z'), "column is not indexed [column=z]");
            Assert.assertArrayEquals(before, partitionDirs());
        });
    }

    @Test
    public void testColumnIndexedByAlterWal() throws Exception {
        assertMemoryLeak(() -> {
            // c's index comes from ALTER TABLE, which the WAL sequencer metadata does not record
            createTable(true);
            final String[] before = partitionDirs();
            execute("alter table t force reindex parquet column c partition list '2024-01-02'");
            releaseSavePointHolds("t");
            final String[] after = partitionDirs();
            Assert.assertEquals(before[0], after[0]);
            Assert.assertNotEquals(before[1], after[1]);
            Assert.assertEquals(before[2], after[2]);
            Assert.assertEquals(before[3], after[3]);
            assertIndexQueries(K1_ROWS);

            execute("alter table t force reindex parquet column c");
            releaseSavePointHolds("t");
            final String[] afterAll = partitionDirs();
            for (int i = 0; i < 3; i++) {
                Assert.assertNotEquals(after[i], afterAll[i]);
            }
            Assert.assertEquals(after[3], afterAll[3]);
            assertIndexQueries(K1_ROWS);
        });
    }

    @Test
    public void testDroppedConnectionStopsAfterCurrentPartition() throws Exception {
        assertInterruptedAfterFirstPartition(SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION, "remote disconnected, query aborted");
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table e (ts timestamp, s symbol index) timestamp(ts) partition by day wal");
            execute("alter table e force reindex parquet");
            execute("create table n (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            execute("insert into n values ('2024-01-01', 'a')");
            drainWalQueue();
            // no index to rebuild
            execute("alter table n force reindex parquet");
            // the writer is left fit for the next commit: a new partition and an O3 row
            execute("insert into n values ('2024-01-03', 'c'), ('2024-01-01T05', 'b')");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("n")));
            assertQuery("select ts, s from n").timestamp("ts").expectSize().returns("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\ta
                    2024-01-01T05:00:00.000000Z\tb
                    2024-01-03T00:00:00.000000Z\tc
                    """);

            execute("create table nn (ts timestamp, s symbol) timestamp(ts) partition by day");
            execute("insert into nn values ('2024-01-01', 'a')");
            execute("alter table nn force reindex parquet");
            execute("insert into nn values ('2024-01-03', 'c'), ('2024-01-01T05', 'b')");
            assertQuery("select ts, s from nn").timestamp("ts").expectSize().returns("""
                    ts\ts
                    2024-01-01T00:00:00.000000Z\ta
                    2024-01-01T05:00:00.000000Z\tb
                    2024-01-03T00:00:00.000000Z\tc
                    """);
        });
    }

    @Test
    public void testErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTable(false);
            String sql = "alter table t force reindex";
            assertExceptionNoLeakCheck(sql, sql.length(), "'parquet' expected");
            sql = "alter table t force reindex column b";
            assertExceptionNoLeakCheck(sql, sql.indexOf("column"), "'parquet' expected");
            sql = "alter table t force reindex parquet column zz";
            assertExceptionNoLeakCheck(sql, sql.indexOf("zz"), "Invalid column: zz");
            sql = "alter table t force reindex parquet column x";
            assertExceptionNoLeakCheck(sql, sql.lastIndexOf('x'), "column is not indexed");
            sql = "alter table t force reindex parquet partition list '2025-01-01'";
            assertExceptionNoLeakCheck(sql, sql.indexOf("'2025"), "partition does not exist");
            final String[] before = partitionDirs();
            sql = "alter table t force reindex parquet partition list '2024-01-01', '2024-01-04'";
            assertExceptionNoLeakCheck(sql, sql.indexOf("'2024-01-04'"), "partition is not parquet, use REINDEX TABLE ... LOCK EXCLUSIVE for native partitions");
            Assert.assertArrayEquals(before, partitionDirs());
            sql = "alter table t force reindex parquet partition";
            assertExceptionNoLeakCheck(sql, sql.length(), "'list' expected");
            sql = "alter table t force reindex parquet now";
            assertExceptionNoLeakCheck(sql, sql.indexOf("now"), "'partition' expected");
            sql = "alter table t force rebuild";
            assertExceptionNoLeakCheck(sql, sql.indexOf("rebuild"), "'drop' or 'reindex' expected");

            execute("create table np (ts timestamp, s symbol index) timestamp(ts)");
            sql = "alter table np force reindex parquet";
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
                    execute("alter table t force reindex parquet");
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
    public void testRebuildsParquetPartitions() throws Exception {
        assertRebuildsParquetPartitions(false);
    }

    @Test
    public void testRebuildsParquetPartitionsWal() throws Exception {
        assertRebuildsParquetPartitions(true);
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

    private void assertInterruptedAfterFirstPartition(int state, String message) throws Exception {
        assertMemoryLeak(() -> {
            createTable(false);
            final String[] before = partitionDirs();
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            final SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
            // a timeout never stops it, a cancel or a dropped connection does, once the partition at hand is done
            try (InterruptingCircuitBreaker breaker = new InterruptingCircuitBreaker(state)) {
                context.with(breaker);
                try {
                    execute("alter table t force reindex parquet", context);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), message);
                } finally {
                    context.with(original);
                }
            }
            releaseSavePointHolds("t");
            final String[] after = partitionDirs();
            Assert.assertEquals(4, after.length);
            Assert.assertNotEquals("the first partition must be committed", before[0], after[0]);
            for (int i = 1; i < 4; i++) {
                Assert.assertEquals(before[i], after[i]);
            }
            assertIndexQueries(K1_ROWS);
        });
    }

    // The WAL table's active partition is parquet too, the non-WAL table's stays native and keeps its version
    private void assertRebuildsParquetPartitions(boolean isWal) throws Exception {
        assertMemoryLeak(() -> {
            createTable(isWal);
            if (isWal) {
                execute("alter table t convert partition to parquet list '2024-01-04'");
                drainWalQueue();
                releaseSavePointHolds("t");
            }
            final String[] before = partitionDirs();
            Assert.assertEquals(4, before.length);
            // a broken parquet index, the active partition's included on the WAL table
            deleteIndexKeyFile(before[0], "b");
            if (isWal) {
                deleteIndexKeyFile(before[3], "b");
            }

            execute("alter table t force reindex parquet");
            releaseSavePointHolds("t");
            final String[] after = partitionDirs();
            Assert.assertEquals(4, after.length);
            for (int i = 0; i < 3; i++) {
                Assert.assertNotEquals(before[i], after[i]);
            }
            if (isWal) {
                Assert.assertNotEquals(before[3], after[3]);
            } else {
                Assert.assertEquals(before[3], after[3]);
            }
            assertIndexQueries(K1_ROWS);

            // the writer appends into the active partition
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
        execute("alter table t convert partition to parquet list '2024-01-01', '2024-01-02', '2024-01-03'");
        drainWalQueue();
        releaseSavePointHolds("t");
    }

    // Reports state from the second check on: the first one runs before the first partition, the second after it
    private static class InterruptingCircuitBreaker extends NetworkSqlExecutionCircuitBreaker {
        private final int state;
        private int stateCheckCount;

        InterruptingCircuitBreaker(int state) {
            super(engine, configuration.getCircuitBreakerConfiguration());
            this.state = state;
        }

        @Override
        public int getState(long millis, long fd) {
            Assert.assertEquals("a reindex must not time out", Long.MAX_VALUE, millis);
            return ++stateCheckCount > 1 ? state : STATE_OK;
        }
    }
}
