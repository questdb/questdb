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
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.O3ParquetPathCounts;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.assertTwinsEqual;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.indexFiles;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.isDeadBytesRewriteExpected;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetFileSize;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetRowGroupCompressedSize;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetRowGroupCount;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetUnusedBytes;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.partitionDirCount;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.partitionNameTxn;

/**
 * A DEDUP UPSERT KEYS commit whose every row duplicates an existing row with equal
 * non-key values must leave a parquet partition untouched, as the native merge does:
 * same name txn, same file size, no dead bytes, no leftover rewrite directory. A
 * commit that changes any value (including NULL vs non-NULL) must still merge.
 * Every test compares pq against its native twin nat, or against the rows its
 * inserts put there.
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
    public void testDeferredCopyFaultInRewriteRemovesNewDirectory() throws Exception {
        // As testOneValueDiffersInRewriteModeStillMerges: row group 0 stays as stored until
        // the merge of row group 1 opens the rewrite, which copies it before its own rows.
        // The rewrite's new data.parquet opens read-only, so that first write, the copy of
        // row group 0, fails. The job must remove the new directory and publish nothing, and
        // the retry after RESUME WAL must converge.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // only the rewrite opens a data.parquet read-write: it reads the committed one
                if (Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME) && armed.compareAndSet(true, false)) {
                    faults.incrementAndGet();
                    final long rwFd = super.openRW(name, opts);
                    super.close(rwFd);
                    // the job detaches the fd for the Rust writer, so it must bypass the fd cache
                    return super.openRONoCache(name);
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 0);
            createTwins();
            final long txnBefore = partitionNameTxn(engine, "pq", 0);
            final long sizeBefore = parquetFileSize(engine, "pq", 0);
            final int dirsBefore = partitionDirCount(engine, "pq", "2024-01-01");
            armed.set(true);
            insertBoth("SELECT id, ts, v, sym, CASE WHEN id = 6 THEN -1.0 ELSE d END, s, bin FROM nat WHERE ts IN '2024-01-01'");
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            Assert.assertEquals("name txn", txnBefore, partitionNameTxn(engine, "pq", 0));
            Assert.assertEquals("parquet file size", sizeBefore, parquetFileSize(engine, "pq", 0));
            Assert.assertEquals("the failed rewrite must remove its directory", dirsBefore, partitionDirCount(engine, "pq", "2024-01-01"));
            assertQuery("SELECT d FROM pq WHERE id = 6").noLeakCheck().returns("d\n9.0\n");

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            assertTwinsEqual(engine, sqlExecutionContext);
            Assert.assertNotEquals(txnBefore, partitionNameTxn(engine, "pq", 0));
            Assert.assertEquals(3, parquetRowGroupCount(engine, "pq", 0));
            assertQuery("SELECT d FROM pq WHERE id = 6").noLeakCheck().returns("d\n-1.0\n");
        });
    }

    @Test
    public void testDuplicateKeyInCommitFirstIdenticalStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // the commit holds id 6 twice: equal values first, a changed d last. Dedup keeps
            // the last row, so the commit changes the partition
            assertChangedCommitMerges(
                    """
                            SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'
                            UNION ALL
                            SELECT id, ts, v, sym, -1.0, s, bin FROM nat WHERE id = 6
                            """,
                    false,
                    "SELECT d FROM pq WHERE id = 6",
                    "d\n-1.0\n"
            );
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'").noLeakCheck().expectSize().noRandomAccess().returns("count\n12\n");
        });
    }

    @Test
    public void testDuplicateKeyInCommitLastIdenticalLeavesPartitionUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // the commit holds id 6 twice: a changed d first, equal values last. Dedup keeps
            // the last row, so every value the commit leaves equals the partition's
            assertIdenticalCommitIsNoop(
                    """
                            SELECT id, ts, v, sym, -1.0, s, bin FROM nat WHERE id = 6
                            UNION ALL
                            SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'
                            """,
                    false
            );
            assertQuery("SELECT d FROM pq WHERE id = 6").noLeakCheck().returns("d\n9.0\n");
        });
    }

    @Test
    public void testExistingDeadBytesCountTowardsRewrite() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // with the gate off, a new d for id 6 merges row group 1 in place and leaves its first version dead
            assertCommitWrites(rowGroup1WithNewD(6, "-1.0"), false, 4, 3);

            // Row group 1 identical and a new d for id 10, in row group 2: the identity check leaves row group 1 as
            // stored, so the gate counts the dead bytes and row group 2, not row group 1. A ratio between the share of
            // the dead bytes and row group 2 and the share of the dead bytes and row groups 1 and 2 updates the
            // partition in place
            long unusedBytes = parquetUnusedBytes(engine, "pq", 0);
            long parquetFileSize = parquetFileSize(engine, "pq", 0);
            final long rowGroup1Size = parquetRowGroupCompressedSize(engine, "pq", 0, 1);
            long rowGroup2Size = parquetRowGroupCompressedSize(engine, "pq", 0, 2);
            double ratio = (unusedBytes + rowGroup2Size + rowGroup1Size / 2.0) / parquetFileSize;
            Assert.assertFalse(isDeadBytesRewriteExpected(unusedBytes, parquetFileSize, rowGroup2Size, ratio, Long.MAX_VALUE));
            Assert.assertTrue(isDeadBytesRewriteExpected(unusedBytes, parquetFileSize, rowGroup1Size + rowGroup2Size, ratio, Long.MAX_VALUE));
            setRewriteRatio(ratio);
            assertCommitWrites(rowGroup1WithNewDOf10("-2.0"), false, 4, 3);

            // the same kind of commit under a ratio between the share of the dead bytes and the share of the dead bytes
            // and row group 2: row group 2 alone stays under it, the dead bytes the file holds take it over
            unusedBytes = parquetUnusedBytes(engine, "pq", 0);
            parquetFileSize = parquetFileSize(engine, "pq", 0);
            rowGroup2Size = parquetRowGroupCompressedSize(engine, "pq", 0, 2);
            ratio = (unusedBytes + rowGroup2Size / 2.0) / parquetFileSize;
            Assert.assertFalse(isDeadBytesRewriteExpected(0, parquetFileSize, rowGroup2Size, ratio, Long.MAX_VALUE));
            Assert.assertTrue(isDeadBytesRewriteExpected(unusedBytes, parquetFileSize, rowGroup2Size, ratio, Long.MAX_VALUE));
            setRewriteRatio(ratio);
            // the rewrite copies row groups 0 and 1, and merges row group 2
            assertCommitWrites(rowGroup1WithNewDOf10("-3.0"), true, 12, 3);
        });
    }

    @Test
    public void testGapInsertAfterDeferredRowGroupsInRewriteFlushesThem() throws Exception {
        assertMemoryLeak(() -> {
            // -1: the commit leaves no byte dead, so only a negative limit rewrites the partition
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, -1);
            createTwins();
            // row group 0 is a copy and row group 1 an identical MERGE, both stay as stored,
            // then one new row at 15:00 lands in the gap before row group 2 as a COPY_O3:
            // the rewrite must copy both row groups before its own
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
            Assert.assertEquals(4, parquetRowGroupCount(engine, "pq", 0));
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
            // d stays DOUBLE in the parquet file: the identity check compares the converted
            // FLOAT buffer, and the identical commit opens no rewrite
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T16:00' AND '2024-01-01T22:00'", true);
        });
    }

    /**
     * An identical commit leaves the partition's covering index files as they are, byte for byte: they still
     * describe the partition, and a compaction of it may have hard-linked them into its staging directory.
     */
    @Test
    public void testIdenticalCommitLeavesCoveringIndexFilesUntouched() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("ALTER TABLE nat ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (d)");
            execute("ALTER TABLE pq ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (d)");
            drainWalQueue();
            final String indexFilesBefore = indexFiles(engine, "pq", 0, "sym");
            final O3ParquetPathCounts paths = new O3ParquetPathCounts();
            assertIdenticalCommitIsNoop("SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'", false);
            Assert.assertEquals(
                    "inPlace=0, rewrite=0, dropInPlace=0, dropRewrite=0, replaceNoop=0, replaceRemoval=0, dedupIdentical=1",
                    paths.toString()
            );
            Assert.assertEquals("an identical commit wrote the covering index files", indexFilesBefore, indexFiles(engine, "pq", 0, "sym"));
            for (String sym : new String[]{"s0", "s1", "s2"}) {
                final String lookup = "SELECT sym, d, ts FROM %s WHERE sym = '" + sym + "'";
                assertQuery(String.format(lookup, "pq")).noLeakCheck().assertsPlanContaining("CoveringIndex");
                assertSqlCursors(String.format(lookup, "nat"), String.format(lookup, "pq"));
            }
        });
    }

    /**
     * One commit that leaves day 1 (parquet) as it is and changes every other day: a row lands in day 2 (parquet,
     * two row groups, updated in place), day 3 (parquet, one row group, rewritten) and day 4 (native, appended).
     * Each changed partition still gets its covering index rebuilt over its new version; without it a lookup reads
     * that partition's covered values as NULL.
     */
    @Test
    public void testIdenticalCommitStillResealsChangedPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (a INT, s SYMBOL INDEX TYPE POSTING INCLUDE (a), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("""
                    INSERT INTO m(a, s, ts) VALUES
                    (1, 'k1', '2020-01-01T00:00:00.000Z'), (2, 'k2', '2020-01-01T02:00:00.000Z'),
                    (3, 'k1', '2020-01-01T04:00:00.000Z'), (4, 'k2', '2020-01-01T06:00:00.000Z'),
                    (5, 'k1', '2020-01-01T08:00:00.000Z'), (6, 'k2', '2020-01-01T10:00:00.000Z'),
                    (7, 'k1', '2020-01-01T12:00:00.000Z'), (8, 'k2', '2020-01-01T14:00:00.000Z'),
                    (11, 'k1', '2020-01-02T00:00:00.000Z'), (12, 'k2', '2020-01-02T02:00:00.000Z'),
                    (13, 'k1', '2020-01-02T04:00:00.000Z'), (14, 'k2', '2020-01-02T06:00:00.000Z'),
                    (15, 'k1', '2020-01-02T08:00:00.000Z'), (16, 'k2', '2020-01-02T10:00:00.000Z'),
                    (17, 'k1', '2020-01-02T12:00:00.000Z'), (18, 'k2', '2020-01-02T14:00:00.000Z'),
                    (21, 'k1', '2020-01-03T00:00:00.000Z'), (22, 'k2', '2020-01-03T02:00:00.000Z'),
                    (31, 'k1', '2020-01-04T00:00:00.000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE m CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-04'");
            drainWalQueue();

            final O3ParquetPathCounts paths = new O3ParquetPathCounts();
            execute("""
                    INSERT INTO m(a, s, ts) VALUES
                    (1, 'k1', '2020-01-01T00:00:00.000Z'), (2, 'k2', '2020-01-01T02:00:00.000Z'),
                    (101, 'k2', '2020-01-02T03:00:00.000Z'),
                    (102, 'k2', '2020-01-03T03:00:00.000Z'),
                    (201, 'k2', '2020-01-04T03:00:00.000Z')
                    """);
            drainWalQueue();
            Assert.assertFalse("m suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("m")));
            Assert.assertEquals(
                    "inPlace=1, rewrite=1, dropInPlace=0, dropRewrite=0, replaceNoop=0, replaceRemoval=0, dedupIdentical=1",
                    paths.toString()
            );

            assertQuery("SELECT a, s, ts FROM m WHERE s = 'k2'")
                    .noLeakCheck()
                    .expectSize()
                    .inferRandomAccess()
                    .withPlanContaining("CoveringIndex")
                    .timestamp("ts")
                    .returns("""
                            a\ts\tts
                            2\tk2\t2020-01-01T02:00:00.000000Z
                            4\tk2\t2020-01-01T06:00:00.000000Z
                            6\tk2\t2020-01-01T10:00:00.000000Z
                            8\tk2\t2020-01-01T14:00:00.000000Z
                            12\tk2\t2020-01-02T02:00:00.000000Z
                            101\tk2\t2020-01-02T03:00:00.000000Z
                            14\tk2\t2020-01-02T06:00:00.000000Z
                            16\tk2\t2020-01-02T10:00:00.000000Z
                            18\tk2\t2020-01-02T14:00:00.000000Z
                            22\tk2\t2020-01-03T02:00:00.000000Z
                            102\tk2\t2020-01-03T03:00:00.000000Z
                            201\tk2\t2020-01-04T03:00:00.000000Z
                            """);
            // (2 + 4 + 6 + 8) + (12 + 101 + 14 + 16 + 18) + (22 + 102) + 201
            assertQuery("SELECT sum(a) FROM m WHERE s = 'k2'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("CoveringIndex")
                    .returns("sum\n506\n");
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
            // the identical commit opens no rewrite even when every commit that writes rewrites:
            // no new directory, no txn change
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, -1);
            createTwins();
            // row group 2 only: row groups 0 and 1 are copies the rewrite would start with
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
    public void testMaxBytesAtProjectionStaysInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // a new d for id 6: the merge of row group 1 leaves it dead, which does not exceed a limit of its size
            node1.setProperty(
                    PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES,
                    parquetRowGroupCompressedSize(engine, "pq", 0, 1)
            );
            assertCommitWrites(rowGroup1WithNewD(6, "-1.0"), false, 4, 3);
            // a limit one byte below the dead bytes and the new version of row group 1 rewrites: the rewrite copies
            // row groups 0 and 2, and merges row group 1
            node1.setProperty(
                    PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES,
                    parquetUnusedBytes(engine, "pq", 0) + parquetRowGroupCompressedSize(engine, "pq", 0, 1) - 1
            );
            assertCommitWrites(rowGroup1WithNewD(6, "-2.0"), true, 12, 3);
        });
    }

    @Test
    public void testMaxBytesMinusOneResendWithRowPastMaxRewrites() throws Exception {
        assertMemoryLeak(() -> {
            // the commit of testMaxBytesZeroResendWithRowPastMaxUpdatesInPlace leaves no byte dead, which exceeds a
            // limit of -1 bytes: the rewrite copies row groups 0, 1 and 2 before the new one
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, -1);
            createTwins();
            assertCommitWrites(rowGroup1WithNewRow(100, "23:00"), true, 13, 4);
        });
    }

    @Test
    public void testMaxBytesZeroResendWithRowPastMaxUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            // The identity check leaves row group 1 as stored, and the new row past the partition max (22:00) forms a
            // row group of its own: the commit leaves no byte dead, in a file that holds none, which does not exceed
            // a limit of 0 bytes
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 0);
            createTwins();
            Assert.assertFalse(isDeadBytesRewriteExpected(parquetUnusedBytes(engine, "pq", 0), parquetFileSize(engine, "pq", 0), 0, 1.0, 0));
            assertCommitWrites(rowGroup1WithNewRow(100, "23:00"), false, 1, 4);
        });
    }

    @Test
    public void testNewRowStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            final long txnBefore = partitionNameTxn(engine, "pq", 0);
            final long sizeBefore = parquetFileSize(engine, "pq", 0);
            // identical rows plus one new key (id 999 at an existing timestamp)
            insertBoth("""
                    SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'
                    UNION ALL
                    SELECT 999, ts, v, sym, d, s, bin FROM nat WHERE id = 6
                    """);
            assertTwinsEqual(engine, sqlExecutionContext);
            Assert.assertEquals(txnBefore, partitionNameTxn(engine, "pq", 0));
            Assert.assertNotEquals(sizeBefore, parquetFileSize(engine, "pq", 0));
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
            // row groups 0 and 2 identical, row group 1 changes one double: the rewrite
            // copies the leading identical row group as stored before the merge writes,
            // and merges the trailing one, which follows the change
            assertChangedCommitMerges(
                    "SELECT id, ts, v, sym, CASE WHEN id = 6 THEN -1.0 ELSE d END, s, bin FROM nat WHERE ts IN '2024-01-01'",
                    true,
                    "SELECT d FROM pq WHERE id = 6",
                    "d\n-1.0\n"
            );
            Assert.assertEquals(3, parquetRowGroupCount(engine, "pq", 0));
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
    public void testRatioZeroResendWithRowPastMaxUpdatesInPlaceThenRewrites() throws Exception {
        assertMemoryLeak(() -> {
            // the commit of testMaxBytesZeroResendWithRowPastMaxUpdatesInPlace leaves no byte dead in a file that
            // holds none, and a share of 0 does not exceed a ratio of 0
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.0");
            createTwins();
            Assert.assertFalse(isDeadBytesRewriteExpected(parquetUnusedBytes(engine, "pq", 0), parquetFileSize(engine, "pq", 0), 0, 0.0, Long.MAX_VALUE));
            assertCommitWrites(rowGroup1WithNewRow(100, "23:00"), false, 1, 4);
            // the in-place update left the footer it replaced dead, so the next such commit rewrites: the rewrite
            // copies row groups 0..3 before the new one (row group 3 holds one row, not fewer than a quarter of the
            // row group size)
            Assert.assertTrue(isDeadBytesRewriteExpected(parquetUnusedBytes(engine, "pq", 0), parquetFileSize(engine, "pq", 0), 0, 0.0, Long.MAX_VALUE));
            assertCommitWrites(rowGroup1WithNewRow(101, "23:30"), true, 14, 5);
        });
    }

    @Test
    public void testResendWithRowPastMaxAfterAddColumnRewrites() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("ALTER TABLE nat ADD COLUMN extra INT");
            execute("ALTER TABLE pq ADD COLUMN extra INT");
            drainWalQueue();
            // The commit of testMaxBytesZeroResendWithRowPastMaxUpdatesInPlace after a column is added, with the gate
            // off: the identity check leaves row group 1 as stored, extra NULL included, so the output opens at the new
            // row and the commit leaves no byte dead. The parquet file lacks extra, which still rewrites the partition:
            // the rewrite copies row groups 0, 1 and 2 with a NULL chunk for extra before the new one
            assertCommitWrites(
                    """
                            SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'
                            UNION ALL
                            SELECT 100, '2024-01-01T23:00'::TIMESTAMP, v, sym, d, s, bin, extra FROM nat WHERE id = 12
                            """,
                    true,
                    13,
                    4
            );
            assertQuery("SELECT count() FROM pq WHERE extra IS NULL").noLeakCheck().expectSize().noRandomAccess().returns("count\n37\n");
        });
    }

    @Test
    public void testTailInsertSingleRowGroupInRewriteFlushesDeferredCopy() throws Exception {
        assertMemoryLeak(() -> {
            // 12 rows per day -> one row group, which always takes the rewrite path;
            // 6 new rows fold into it only below 16 / 4 rows, so they form a COPY_O3
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
            createTwins();
            // after the only row group's max (22:00): its copy must be written
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
            Assert.assertEquals(2, parquetRowGroupCount(engine, "pq", 0));
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

    // rows 5..8 of day 1 (row group 1, 08:00..14:00), with a new d for the id
    private static String rowGroup1WithNewD(int id, String d) {
        return "SELECT id, ts, v, sym, CASE WHEN id = " + id + " THEN " + d + " ELSE d END, s, bin FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'";
    }

    // rows 5..8 of day 1 (row group 1) as stored, and id 10 (row group 2) with a new d
    private static String rowGroup1WithNewDOf10(String d) {
        return "SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00' UNION ALL SELECT id, ts, v, sym, " + d + ", s, bin FROM nat WHERE id = 10";
    }

    // rows 5..8 of day 1 (row group 1) as stored, and a new id at the time of day 1, with the values of id 12
    private static String rowGroup1WithNewRow(int id, String time) {
        return "SELECT * FROM nat WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00' UNION ALL SELECT " + id
                + ", '2024-01-01T" + time + "'::TIMESTAMP, v, sym, d, s, bin FROM nat WHERE id = 12";
    }

    private static void setRewriteRatio(double ratio) {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, Double.toString(ratio));
    }

    private void assertChangedCommitMerges(String select, boolean isRewriteExpected, String probe, String expected) throws Exception {
        final long txnBefore = partitionNameTxn(engine, "pq", 0);
        final long sizeBefore = parquetFileSize(engine, "pq", 0);
        insertBoth(select);
        assertTwinsEqual(engine, sqlExecutionContext);
        if (isRewriteExpected) {
            Assert.assertNotEquals(txnBefore, partitionNameTxn(engine, "pq", 0));
        } else {
            Assert.assertEquals(txnBefore, partitionNameTxn(engine, "pq", 0));
            Assert.assertNotEquals(sizeBefore, parquetFileSize(engine, "pq", 0));
            Assert.assertTrue(parquetUnusedBytes(engine, "pq", 0) > 0);
        }
        assertQuery(probe).noLeakCheck().returns(expected);
    }

    // Commits the rows of the query to pq and applies the commit, then to nat, so that the rows pq writes count apart
    // from those of nat. Asserts that the twins match, and that the commit rewrote day 1 of pq or updated it in place,
    // wrote the given rows and left it the given row groups.
    private void assertCommitWrites(String select, boolean isRewriteExpected, long expectedWrittenRows, int expectedRowGroups) throws Exception {
        final long txnBefore = partitionNameTxn(engine, "pq", 0);
        final O3ParquetPathCounts paths = new O3ParquetPathCounts();
        final long writtenRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        execute("INSERT INTO pq " + select);
        drainWalQueue();
        final long writtenRows = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenRowsBefore;
        execute("INSERT INTO nat " + select);
        drainWalQueue();
        assertTwinsEqual(engine, sqlExecutionContext);
        Assert.assertEquals("rewrites", isRewriteExpected ? 1 : 0, paths.getRewrite());
        Assert.assertEquals("in-place updates", isRewriteExpected ? 0 : 1, paths.getInPlace());
        if (isRewriteExpected) {
            Assert.assertNotEquals("name txn", txnBefore, partitionNameTxn(engine, "pq", 0));
        } else {
            Assert.assertEquals("name txn", txnBefore, partitionNameTxn(engine, "pq", 0));
        }
        Assert.assertEquals("written rows", expectedWrittenRows, writtenRows);
        Assert.assertEquals("row groups", expectedRowGroups, parquetRowGroupCount(engine, "pq", 0));
    }

    private void assertIdenticalCommitIsNoop(String select, boolean isRewriteMode) throws Exception {
        final long txnBefore = partitionNameTxn(engine, "pq", 0);
        final long sizeBefore = parquetFileSize(engine, "pq", 0);
        final long unusedBefore = parquetUnusedBytes(engine, "pq", 0);
        final int rowGroupsBefore = parquetRowGroupCount(engine, "pq", 0);
        final int dirsBefore = partitionDirCount(engine, "pq", "2024-01-01");
        insertBoth(select);
        assertTwinsEqual(engine, sqlExecutionContext);
        Assert.assertEquals("name txn", txnBefore, partitionNameTxn(engine, "pq", 0));
        Assert.assertEquals("parquet file size", sizeBefore, parquetFileSize(engine, "pq", 0));
        Assert.assertEquals("unused bytes", unusedBefore, parquetUnusedBytes(engine, "pq", 0));
        Assert.assertEquals("row groups", rowGroupsBefore, parquetRowGroupCount(engine, "pq", 0));
        Assert.assertEquals("partition dirs", dirsBefore, partitionDirCount(engine, "pq", "2024-01-01"));
        if (isRewriteMode) {
            Assert.assertEquals(0, unusedBefore);
        }
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
