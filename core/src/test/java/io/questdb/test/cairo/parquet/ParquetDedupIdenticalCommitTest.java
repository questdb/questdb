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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * A DEDUP UPSERT KEYS commit whose every row duplicates an existing row with equal
 * non-key values must leave a parquet partition untouched, as the native merge does:
 * same name txn, same file size, no dead bytes, no leftover rewrite directory. A
 * commit that changes any value (including NULL vs non-NULL) must still merge.
 * Every test compares pq against its native twin nat.
 */
public class ParquetDedupIdenticalCommitTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // 12 rows per day at 2h spacing -> 3 row groups of 4 per parquet partition.
        // Disable the dead-bytes rewrite triggers so only the tests that ask for a
        // rewrite get one.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testGapInsertAfterDeferredRowGroupsInRewriteFlushesThem() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 0);
            createTwins();
            // row group 0 is a deferred copy, row group 1 an identical MERGE (deferred
            // too), then one new row at 15:00 lands in the gap before row group 2 as a
            // COPY_O3: it must write both deferred row groups before its own
            assertChangedCommitMerges(
                    """
                            SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'
                            UNION ALL
                            SELECT 100, ts + 3_600_000_000L, v, sym, d, s, bin FROM nat WHERE id = 8
                            """,
                    true,
                    "SELECT id FROM pq WHERE ts BETWEEN '2024-01-01T14:00' AND '2024-01-01T16:00'",
                    """
                            id
                            8
                            100
                            9
                            """
            );
            Assert.assertEquals(4, parquetRowGroupCount());
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'").noLeakCheck().expectSize().noRandomAccess().returns("count\n13\n");
        });
    }

    @Test
    public void testIdenticalAcrossAllRowGroupsLeavesPartitionUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // every row of day 1: three MERGE actions, all identical
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts IN '2024-01-01'", false);
        });
    }

    @Test
    public void testIdenticalAfterAddColumnLeavesPartitionUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("ALTER TABLE nat ADD COLUMN extra INT");
            execute("ALTER TABLE pq ADD COLUMN extra INT");
            drainWalQueue();
            // the parquet file lacks extra (schema change: rewrite, NULL-column copies),
            // and the commit's extra is NULL, so nothing changes
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'", true);
            // a value in the missing column is a change
            assertChangedCommitMerges(
                    "SELECT id, ts, v, sym, d, s, bin, 42 FROM nat WHERE id = 6",
                    true,
                    "SELECT extra FROM pq WHERE id = 6",
                    "extra\n42\n"
            );
        });
    }

    @Test
    public void testIdenticalAfterAlterColumnTypeLeavesPartitionUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("ALTER TABLE nat ALTER COLUMN d TYPE FLOAT");
            execute("ALTER TABLE pq ALTER COLUMN d TYPE FLOAT");
            drainWalQueue();
            // d stays DOUBLE in the parquet file: the merge compares the converted
            // FLOAT buffer, and the rewrite copies eagerly (materialized), then the
            // identical commit abandons it
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T16:00' AND '2024-01-01T22:00'", true);
        });
    }

    @Test
    public void testIdenticalInOneRowGroupLeavesPartitionUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // rows 5..7 of day 1 (08:00..12:00) sit in row group 1; row 7 has NULL s
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'", false);
        });
    }

    @Test
    public void testIdenticalInRewriteModeLeavesPartitionUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            // any merge now crosses the dead-bytes gate, so the job opens a rewrite;
            // the identical commit must abandon it: no new directory, no txn change
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 0);
            createTwins();
            // row group 2 only: row groups 0 and 1 are leading copies the rewrite defers
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T16:00' AND '2024-01-01T22:00'", true);
        });
    }

    @Test
    public void testIdenticalSingleRowGroupLeavesPartitionUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            // one row group per partition always takes the rewrite path
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
            createTwins();
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T02:00' AND '2024-01-01T06:00'", true);
        });
    }

    @Test
    public void testNewRowStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            final long txnBefore = partitionNameTxn();
            final long sizeBefore = parquetFileSize();
            // identical rows plus one new key (id 999 at an existing timestamp)
            insertBoth("""
                    SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'
                    UNION ALL
                    SELECT 999, ts, v, sym, d, s, bin FROM nat WHERE id = 6
                    """);
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn());
            Assert.assertNotEquals(sizeBefore, parquetFileSize());
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'").noLeakCheck().expectSize().noRandomAccess().returns("count\n13\n");
        });
    }

    @Test
    public void testNullToValueStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // row 7 (12:00) has NULL s; the commit sets it
            assertChangedCommitMerges(
                    "SELECT id, ts, v, sym, d, CASE WHEN id = 7 THEN 'set' ELSE s END, bin FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'",
                    false,
                    "SELECT s FROM pq WHERE id = 7",
                    "s\nset\n"
            );
        });
    }

    @Test
    public void testOneValueDiffersInRewriteModeStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 0);
            createTwins();
            // row groups 0 and 2 identical, row group 1 changes one double: the leading
            // identical row group is a deferred copy that must be flushed before the
            // merge writes, and the trailing one is copied as is
            assertChangedCommitMerges(
                    "SELECT id, ts, v, sym, CASE WHEN id = 6 THEN -1.0 ELSE d END, s, bin FROM nat WHERE ts IN '2024-01-01'",
                    true,
                    "SELECT d FROM pq WHERE id = 6",
                    "d\n-1.0\n"
            );
            Assert.assertEquals(3, parquetRowGroupCount());
        });
    }

    @Test
    public void testOneValueDiffersStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // one VARCHAR in the last row of the commit differs
            assertChangedCommitMerges(
                    "SELECT id, ts, CASE WHEN id = 8 THEN 'changed' ELSE v END, sym, d, s, bin FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'",
                    false,
                    "SELECT v FROM pq WHERE id = 8",
                    "v\nchanged\n"
            );
        });
    }

    @Test
    public void testTailInsertSingleRowGroupInRewriteFlushesDeferredCopy() throws Exception {
        assertMemoryLeak(() -> {
            // 12 rows per day -> one row group, which always takes the rewrite path;
            // 6 new rows fold into it only below 16 / 4 rows, so they form a COPY_O3
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
            createTwins();
            // after the only row group's max (22:00): its deferred copy must be written
            // before the new row group, or the rewrite publishes the new rows alone
            assertChangedCommitMerges(
                    "SELECT (100 + id)::INT, '2024-01-01T22:30'::TIMESTAMP + id * 60_000_000L, v, sym, d, s, bin FROM nat WHERE id <= 6",
                    true,
                    "SELECT id FROM pq WHERE ts BETWEEN '2024-01-01T20:00' AND '2024-01-01T22:33'",
                    """
                            id
                            11
                            12
                            101
                            102
                            103
                            """
            );
            Assert.assertEquals(2, parquetRowGroupCount());
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'").noLeakCheck().expectSize().noRandomAccess().returns("count\n18\n");
        });
    }

    @Test
    public void testValueToNullStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // row 6 (10:00) has a non-NULL s; the commit clears it
            assertChangedCommitMerges(
                    "SELECT id, ts, v, sym, d, CASE WHEN id = 6 THEN NULL ELSE s END, bin FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'",
                    false,
                    "SELECT s FROM pq WHERE id = 6",
                    "s\n\n"
            );
        });
    }

    private static void insertBoth(String select) throws Exception {
        // nat's rows are read before either insert is applied, so both tables get
        // the same commit
        execute("INSERT INTO pq " + select);
        execute("INSERT INTO nat " + select);
        drainWalQueue();
    }

    private static long parquetFileSize() {
        try (TableReader reader = engine.getReader("pq")) {
            return reader.getTxFile().getPartitionParquetFileSize(0);
        }
    }

    private static int parquetRowGroupCount() {
        try (TableReader reader = engine.getReader("pq")) {
            reader.openPartition(0);
            return reader.getAndInitParquetPartitionDecoder(0).metadata().getRowGroupCount();
        }
    }

    private static long parquetUnusedBytes() {
        try (TableReader reader = engine.getReader("pq")) {
            reader.openPartition(0);
            return reader.getAndInitParquetPartitionDecoder(0).metadata().getUnusedBytes();
        }
    }

    private static long partitionNameTxn() {
        try (TableReader reader = engine.getReader("pq")) {
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    /**
     * Counts pq's directories for day 1 (2024-01-01 and 2024-01-01.N): an abandoned
     * rewrite must not leave its txn-named directory behind.
     */
    private static int partitionDirCount() {
        final String[] names = new File(configuration.getDbRoot().toString(), engine.verifyTableName("pq").getDirName()).list();
        Assert.assertNotNull(names);
        int count = 0;
        for (String name : names) {
            if (name.startsWith("2024-01-01")) {
                count++;
            }
        }
        return count;
    }

    private void assertChangedCommitMerges(String select, boolean expectRewrite, String probe, String expected) throws Exception {
        final long txnBefore = partitionNameTxn();
        final long sizeBefore = parquetFileSize();
        insertBoth(select);
        assertTwinsEqual();
        if (expectRewrite) {
            Assert.assertNotEquals(txnBefore, partitionNameTxn());
        } else {
            Assert.assertEquals(txnBefore, partitionNameTxn());
            Assert.assertNotEquals(sizeBefore, parquetFileSize());
            Assert.assertTrue(parquetUnusedBytes() > 0);
        }
        assertQuery(probe).noLeakCheck().returns(expected);
    }

    private void assertIdenticalCommitIsNoop(String select, boolean rewriteMode) throws Exception {
        final long txnBefore = partitionNameTxn();
        final long sizeBefore = parquetFileSize();
        final long unusedBefore = parquetUnusedBytes();
        final int rowGroupsBefore = parquetRowGroupCount();
        final int dirsBefore = partitionDirCount();
        insertBoth(select);
        assertTwinsEqual();
        Assert.assertEquals("name txn", txnBefore, partitionNameTxn());
        Assert.assertEquals("parquet file size", sizeBefore, parquetFileSize());
        Assert.assertEquals("unused bytes", unusedBefore, parquetUnusedBytes());
        Assert.assertEquals("row groups", rowGroupsBefore, parquetRowGroupCount());
        Assert.assertEquals("partition dirs", dirsBefore, partitionDirCount());
        if (rewriteMode) {
            Assert.assertEquals(0, unusedBefore);
        }
    }

    private void assertTwinsEqual() throws Exception {
        Assert.assertFalse("nat suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("nat")));
        Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
        assertSqlCursors("nat", "pq");
        assertSqlCursors("SELECT count(), min(ts), max(ts) FROM nat", "SELECT count(), min(ts), max(ts) FROM pq");
    }

    /**
     * Creates nat (native) and pq, both DEDUP UPSERT KEYS(ts, id), with 3 daily
     * partitions of 12 rows at 2h spacing, and converts pq's first two days to parquet.
     * Every 4th s is NULL.
     */
    private void createTwins() throws Exception {
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, v VARCHAR, sym SYMBOL, d DOUBLE, s STRING, bin BINARY)" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, id)";
        execute(String.format(ddl, "nat"));
        execute(String.format(ddl, "pq"));
        execute("""
                INSERT INTO nat
                SELECT
                    x::INT,
                    '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L,
                    'v' || x,
                    's' || (x % 3),
                    x * 1.5,
                    CASE WHEN x % 4 = 3 THEN NULL ELSE 'str' || x END,
                    rnd_bin(2, 2, 2)
                FROM long_sequence(36)
                """);
        drainWalQueue();
        execute("INSERT INTO pq SELECT * FROM nat");
        drainWalQueue();
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        drainWalQueue();
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet").noLeakCheck().expectSize().noRandomAccess().returns("count\n2\n");
    }
}
