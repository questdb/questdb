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
import io.questdb.cairo.MicrosTimestampDriver;
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
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.isDeadBytesRewriteExpected;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetFileSize;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetRowGroupCompressedSize;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetRowGroupCount;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetRowGroupRowCount;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetUnusedBytes;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.partitionDirCount;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.partitionNameTxn;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.replace;

/**
 * The dead-bytes rewrite gate of a DEDUP commit into a parquet partition counts the row groups that the commit
 * re-encodes, not the ones that the UPSERT identity check or the SKIP dedup leaves as stored: a commit that resends
 * stored rows together with a new or changed row updates the partition in place, writing about one row group, instead
 * of rewriting the whole file. The gate keeps the server defaults, a ratio of 0.5 and 1 GiB, unless a test sets a ratio
 * of its own. The parquet table pq goes through the commits, its native twin nat takes the same rows, and the two must
 * match after every commit, through the index of sym too. Whether a commit rewrites comes from the gate formula that
 * cairo/CLAUDE.md documents, fed with the sizes the {@code _pm} of the partition records before the commit and with the
 * row groups the commit re-encodes.
 * <p>
 * Day 2024-01-01 holds ids 1..10_000, one every 8.64 s, in row groups of 1000 rows: row group k holds ids
 * 1000k+1..1000k+1000. Day 2024-01-02 holds one row, so 2024-01-01 is not the last partition, unless the table is a
 * FORMAT PARQUET one. Every row has a timestamp of its own: a resend over a boundary of two row groups that share a
 * timestamp still rewrites the whole file, through the tie gate (hasCoalescableTie), which forces the rewrite before
 * the dedup decides whether the coalesced MERGE writes. That is a known issue, which these tests keep out.
 * <p>
 * A replace commit, which never deduplicates, counts the row groups it drops, as their bytes stay dead in the file.
 */
public class O3ParquetDedupRewriteGateTest extends AbstractCairoTest {
    // the timestamp of id 10_000, the last row of 2024-01-01
    private static final String DAY_MAX = "2024-01-01T23:59:51.360000Z";
    // 2024-01-01T00:00, the timestamp of id 1, in micros
    private static final long DAY_START = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
    // the server default of cairo.partition.encoder.parquet.o3.rewrite.unused.max.bytes
    private static final long DEFAULT_MAX_BYTES = 1024 * 1024 * 1024L;
    // the server default of cairo.partition.encoder.parquet.o3.rewrite.unused.ratio
    private static final double DEFAULT_RATIO = 0.5;
    // a lookup through the index of sym
    private static final String INDEX_LOOKUP = "SELECT * FROM %s WHERE sym = 's3'";
    private static final int ROW_GROUP_SIZE = 1000;
    // the micros between two rows of 2024-01-01
    private static final long ROW_STEP = 8_640_000L;
    private boolean hasBoolean;
    private String timestampType;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // the dead-bytes gate keeps the server defaults
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, ROW_GROUP_SIZE);
    }

    @Test
    public void testFormatParquetResendWithRowPastMaxAppendsInPlace() throws Exception {
        // the commits of testUpsertResendWithRowPastMaxAppendsInPlace into the last partition of a FORMAT PARQUET table
        assertMemoryLeak(() -> {
            createTwins("UPSERT", false, "TIMESTAMP", true);
            assertResendsWithRowPastMaxAppendInPlace(3);
        });
    }

    @Test
    public void testReplaceDropsCountTowardsRewrite() throws Exception {
        // A replace commit that brings no rows and covers whole row groups drops them, and their bytes stay dead in the
        // file as those of a merged row group do: the gate counts them. Dropping row group 9, about a tenth of the file,
        // stays under the gate and updates the partition in place. Dropping row groups 4..8 then passes the gate,
        // with the bytes row group 9 left dead, and rewrites the partition
        assertMemoryLeak(() -> {
            createTwins("UPSERT", false, "TIMESTAMP", false);
            assertGateFormula(false, 9);
            Assert.assertEquals(0, replaceBoth(9, 9, false));
            Assert.assertEquals(9, parquetRowGroupCount(engine, "pq", 0));

            assertGateFormula(true, 4, 5, 6, 7, 8);
            // the rewrite copies row groups 0..3
            Assert.assertEquals(4 * ROW_GROUP_SIZE, replaceBoth(4, 8, true));
            Assert.assertEquals(4, parquetRowGroupCount(engine, "pq", 0));
        });
    }

    @Test
    public void testSkipNewRowInEveryRowGroupCountsFirstWriterOnly() throws Exception {
        // A new id at the first timestamp of each row group: every MERGE writes its row group and the new row. The
        // output opens at the MERGE of row group 0, and the gate counts that row group alone, since the dedup of the
        // later MERGEs runs after the output opens: the commit updates the partition in place and leaves the 10 row
        // groups it replaced dead. The next commit into the partition counts them, and rewrites the partition
        assertMemoryLeak(() -> {
            createTwins("SKIP", false, "TIMESTAMP", false);
            assertGateFormula(false, 0);
            Assert.assertEquals(
                    10_010,
                    commitBoth(
                            "SELECT ('2024-01-01'::TIMESTAMP + (x - 1) * 1000 * 8_640_000L)::TIMESTAMP, (100_000 + x)::LONG, -1.5, 'new'::VARCHAR, 's3'::SYMBOL FROM long_sequence(10)",
                            false
                    )
            );
            Assert.assertEquals(10, parquetRowGroupCount(engine, "pq", 0));

            assertGateFormula(true, 0);
            commitBoth(newRow("'2024-01-01'::TIMESTAMP", 100_011), true);
            Assert.assertEquals(10, parquetRowGroupCount(engine, "pq", 0));
        });
    }

    @Test
    public void testSkipOpeningMergeCountsTowardsRewrite() throws Exception {
        // The commit of testSkipResendWithNewRowInFirstResentRowGroupUpdatesInPlace under a ratio between the share of
        // the bytes the file holds unused and the share of those and row group 4: the MERGE of row group 4 opens the
        // output, and the gate counts its row group, which takes the projection over the ratio. The rewrite copies row
        // groups 0..3, merges row group 4 with the new row, and copies row groups 5..9, whose O3 rows the dedup drops
        assertMemoryLeak(() -> {
            createTwins("SKIP", false, "TIMESTAMP", false);
            final long rowGroup4Size = parquetRowGroupCompressedSize(engine, "pq", 0, 4);
            final double ratio = (parquetUnusedBytes(engine, "pq", 0) + rowGroup4Size / 2.0) / parquetFileSize(engine, "pq", 0);
            assertGateFormula(ratio, false);
            assertGateFormula(ratio, true, 4);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, Double.toString(ratio));
            Assert.assertEquals(
                    10_001,
                    commitBoth(resend(4, 9) + " UNION ALL " + newRow("'2024-01-01'::TIMESTAMP + 4000 * 8_640_000L", 100_001), true)
            );
            Assert.assertEquals(10, parquetRowGroupCount(engine, "pq", 0));
        });
    }

    @Test
    public void testSkipResendWithNewRowInFirstResentRowGroupUpdatesInPlace() throws Exception {
        // Resends row groups 4..9 with a new id at the first timestamp of row group 4: the MERGE of row group 4 opens
        // the output and writes its 1000 rows and the new one, the dedup drops every O3 row of row groups 5..9. The
        // gate counts row group 4 alone, about a tenth of the file
        assertMemoryLeak(() -> {
            createTwins("SKIP", false, "TIMESTAMP", false);
            assertGateFormula(false, 4);
            Assert.assertEquals(
                    1001,
                    commitBoth(resend(4, 9) + " UNION ALL " + newRow("'2024-01-01'::TIMESTAMP + 4000 * 8_640_000L", 100_001), false)
            );
            Assert.assertEquals(10, parquetRowGroupCount(engine, "pq", 0));
        });
    }

    @Test
    public void testSkipResendWithRowPastMaxAppendsInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("SKIP", false, "TIMESTAMP", false);
            assertResendsWithRowPastMaxAppendInPlace(10);
        });
    }

    @Test
    public void testSkipResendWithRowPastMaxInPlaceOpenFaultResumes() throws Exception {
        assertResendWithRowPastMaxInPlaceOpenFaultResumes("SKIP");
    }

    @Test
    public void testUpsertBooleanResendWithRowPastMaxAppendsInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("UPSERT", true, "TIMESTAMP", false);
            assertResendsWithRowPastMaxAppendInPlace(10);
        });
    }

    @Test
    public void testUpsertChangeInFirstResentRowGroupRewrites() throws Exception {
        // Resends row groups 4..9 with a new v for id 4500: the identity check stops at row group 4, and every MERGE
        // from it on writes its row group, so the gate counts row groups 4..9, about 60% of the file
        assertMemoryLeak(() -> {
            createTwins("UPSERT", false, "TIMESTAMP", false);
            assertGateFormula(true, 4, 5, 6, 7, 8, 9);
            commitBoth("SELECT ts, id, CASE WHEN id = 4500 THEN -1.0 ELSE v END, s, sym FROM nat WHERE id BETWEEN 4001 AND 10_000", true);
            Assert.assertEquals(10, parquetRowGroupCount(engine, "pq", 0));
        });
    }

    @Test
    public void testUpsertNanosResendWithRowPastMaxAppendsInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("UPSERT", false, "TIMESTAMP_NS", false);
            assertResendsWithRowPastMaxAppendInPlace(10);
        });
    }

    @Test
    public void testUpsertResendWithChangeInLastRowGroupUpdatesInPlace() throws Exception {
        // Five commits, each resends row groups 4..9 with a new v for id 9500, in row group 9: the identity check
        // leaves row groups 4..8 as stored, and the merge of row group 9 writes its 1000 rows. The gate counts row
        // group 9 and the bytes the earlier commits left dead
        assertMemoryLeak(() -> {
            createTwins("UPSERT", false, "TIMESTAMP", false);
            for (int k = 1; k <= 5; k++) {
                assertGateFormula(false, 9);
                Assert.assertEquals(
                        1000,
                        commitBoth("SELECT ts, id, CASE WHEN id = 9500 THEN -" + k + ".0 ELSE v END, s, sym FROM nat WHERE id BETWEEN 4001 AND 10_000", false)
                );
                Assert.assertEquals(10, parquetRowGroupCount(engine, "pq", 0));
            }
        });
    }

    @Test
    public void testUpsertResendWithRowPastMaxAppendsInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("UPSERT", false, "TIMESTAMP", false);
            assertResendsWithRowPastMaxAppendInPlace(10);
        });
    }

    @Test
    public void testUpsertResendWithRowPastMaxInPlaceOpenFaultResumes() throws Exception {
        assertResendWithRowPastMaxInPlaceOpenFaultResumes("UPSERT");
    }

    // Asserts that the twins match, and that the one commit into pq since the counts and the name txn of 2024-01-01
    // were taken rewrote 2024-01-01 or updated it in place
    private static void assertCommitApplied(long nameTxn, O3ParquetPathCounts paths, boolean isRewriteExpected) throws Exception {
        assertTwinsEqual(engine, sqlExecutionContext);
        assertSqlCursors(String.format(INDEX_LOOKUP, "nat"), String.format(INDEX_LOOKUP, "pq"));
        Assert.assertEquals("rewrites", isRewriteExpected ? 1 : 0, paths.getRewrite());
        Assert.assertEquals("in-place updates", isRewriteExpected ? 0 : 1, paths.getInPlace());
        if (isRewriteExpected) {
            Assert.assertNotEquals("name txn", nameTxn, partitionNameTxn(engine, "pq", 0));
        } else {
            Assert.assertEquals("name txn", nameTxn, partitionNameTxn(engine, "pq", 0));
        }
    }

    // asserts that the gate formula, fed with the bytes 2024-01-01 of pq holds unused, the size of its parquet file and
    // the compressed sizes of the row groups that the next commit re-encodes, asks for a rewrite or not
    private static void assertGateFormula(boolean isRewriteExpected, int... reencodedRowGroups) {
        assertGateFormula(DEFAULT_RATIO, isRewriteExpected, reencodedRowGroups);
    }

    // assertGateFormula(boolean, int...) under the ratio
    private static void assertGateFormula(double ratio, boolean isRewriteExpected, int... reencodedRowGroups) {
        final long unusedBytes = parquetUnusedBytes(engine, "pq", 0);
        final long parquetFileSize = parquetFileSize(engine, "pq", 0);
        long deadBytes = 0;
        for (int rowGroup : reencodedRowGroups) {
            deadBytes += parquetRowGroupCompressedSize(engine, "pq", 0, rowGroup);
        }
        Assert.assertEquals(
                "gate formula [unusedBytes=" + unusedBytes + ", parquetFileSize=" + parquetFileSize + ", deadBytes=" + deadBytes + ", ratio=" + ratio + ']',
                isRewriteExpected,
                isDeadBytesRewriteExpected(unusedBytes, parquetFileSize, deadBytes, ratio, DEFAULT_MAX_BYTES)
        );
    }

    // Commits the rows of the query to pq, then to nat, each applied before the next one, so that the rows pq writes
    // count apart from those of nat. Asserts that the commit rewrote 2024-01-01 of pq or updated it in place, and that
    // the twins match. Returns the rows that the commit into pq wrote.
    private static long commitBoth(String select, boolean isRewriteExpected) throws Exception {
        final long nameTxn = partitionNameTxn(engine, "pq", 0);
        final O3ParquetPathCounts paths = new O3ParquetPathCounts();
        final long writtenRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        execute("INSERT INTO pq " + select);
        drainWalQueue();
        final long writtenRows = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenRowsBefore;
        execute("INSERT INTO nat " + select);
        drainWalQueue();
        assertCommitApplied(nameTxn, paths, isRewriteExpected);
        return writtenRows;
    }

    // Commits a replace of the rows of row groups rowGroupLo..rowGroupHi of 2024-01-01 that brings no rows to pq, then
    // to nat, as commitBoth() does. Returns the rows that the commit into pq wrote.
    private static long replaceBoth(int rowGroupLo, int rowGroupHi, boolean isRewriteExpected) throws Exception {
        // the timestamps of the first row of row group rowGroupLo and of the last row of row group rowGroupHi
        final long lo = DAY_START + (long) rowGroupLo * ROW_GROUP_SIZE * ROW_STEP;
        final long hi = DAY_START + ((long) (rowGroupHi + 1) * ROW_GROUP_SIZE - 1) * ROW_STEP;
        final String noRows = "SELECT id FROM nat WHERE id < 0";
        final long nameTxn = partitionNameTxn(engine, "pq", 0);
        final O3ParquetPathCounts paths = new O3ParquetPathCounts();
        final long writtenRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        replace(engine, sqlExecutionContext, "pq", noRows, lo, hi);
        drainWalQueue();
        final long writtenRows = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenRowsBefore;
        replace(engine, sqlExecutionContext, "nat", noRows, lo, hi);
        drainWalQueue();
        assertCommitApplied(nameTxn, paths, isRewriteExpected);
        return writtenRows;
    }

    // the rows of row groups rowGroupLo..rowGroupHi of 2024-01-01, as nat holds them
    private static String resend(int rowGroupLo, int rowGroupHi) {
        return "SELECT * FROM nat WHERE id BETWEEN " + (rowGroupLo * ROW_GROUP_SIZE + 1) + " AND " + (rowGroupHi + 1) * ROW_GROUP_SIZE;
    }

    // The commit of testSkipResendWithRowPastMaxAppendsInPlace fails as it opens the parquet file of 2024-01-01 of pq
    // for the in-place update: pq suspends, with the partition as it was. Once pq resumes, the commit appends the new
    // row in place.
    private void assertResendWithRowPastMaxInPlaceOpenFaultResumes(String dedup) throws Exception {
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger faultCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)
                        && Utf8s.containsAscii(name, "pq~")
                        && isArmed.compareAndSet(true, false)) {
                    faultCount.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTwins(dedup, false, "TIMESTAMP", false);
            final long nameTxn = partitionNameTxn(engine, "pq", 0);
            final long parquetFileSize = parquetFileSize(engine, "pq", 0);
            final int dirCount = partitionDirCount(engine, "pq", "2024-01-01");
            final String select = resend(4, 9) + " UNION ALL " + newRowPastMax(1);
            isArmed.set(true);
            execute("INSERT INTO pq " + select);
            drainWalQueue();
            Assert.assertEquals("faults", 1, faultCount.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            Assert.assertEquals("name txn", nameTxn, partitionNameTxn(engine, "pq", 0));
            Assert.assertEquals("parquet file size", parquetFileSize, parquetFileSize(engine, "pq", 0));
            Assert.assertEquals("partition dirs", dirCount, partitionDirCount(engine, "pq", "2024-01-01"));
            Assert.assertEquals("row groups", 10, parquetRowGroupCount(engine, "pq", 0));
            assertQuery("SELECT count() FROM pq").noLeakCheck().expectSize().noRandomAccess().returns("count\n10001\n");

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            Assert.assertEquals("name txn", nameTxn, partitionNameTxn(engine, "pq", 0));
            Assert.assertEquals("row groups", 11, parquetRowGroupCount(engine, "pq", 0));
            execute("INSERT INTO nat " + select);
            drainWalQueue();
            assertTwinsEqual(engine, sqlExecutionContext);
            assertSqlCursors(String.format(INDEX_LOOKUP, "nat"), String.format(INDEX_LOOKUP, "pq"));
        });
    }

    // Commits that each resend row groups 4..9 and add one new id past the partition max, k milliseconds after the
    // last stored row in commit k. The dedup leaves the resent row groups as stored, so each commit writes its new row
    // alone: the first one as a row group of its own (a COPY_O3, row group 9 holds 1000 rows, not fewer than a quarter
    // of the row group size), each later one into that small last row group, whose merge writes its rows again. Each
    // commit updates the partition in place; the writers and readers reopen after the fifth one.
    private void assertResendsWithRowPastMaxAppendInPlace(int commitCount) throws Exception {
        long writtenRowCount = 0;
        for (int k = 1; k <= commitCount; k++) {
            final int lastRowGroup = parquetRowGroupCount(engine, "pq", 0) - 1;
            final long lastRowGroupRowCount = parquetRowGroupRowCount(engine, "pq", 0, lastRowGroup);
            // the merge strategy merges the rows past the max into a last row group of fewer than a quarter of the row
            // group size, and makes a row group of them otherwise
            final boolean isMergedIntoLastRowGroup = lastRowGroupRowCount < ROW_GROUP_SIZE / 4;
            Assert.assertEquals("last row group rows", k == 1 ? ROW_GROUP_SIZE : k - 1, lastRowGroupRowCount);
            if (isMergedIntoLastRowGroup) {
                assertGateFormula(false, lastRowGroup);
            } else {
                assertGateFormula(false);
            }
            final long writtenRows = commitBoth(resend(4, 9) + " UNION ALL " + newRowPastMax(k), false);
            Assert.assertEquals("written rows", isMergedIntoLastRowGroup ? lastRowGroupRowCount + 1 : 1, writtenRows);
            Assert.assertEquals("row groups", 11, parquetRowGroupCount(engine, "pq", 0));
            writtenRowCount += writtenRows;
            if (k == 5) {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        }
        // 1 + 2 + ... + commitCount
        Assert.assertEquals(commitCount * (commitCount + 1) / 2, writtenRowCount);
    }

    // Creates nat (native) and pq with the dedup mode on KEYS(ts, id), the timestamp type and, with hasBoolean, a
    // BOOLEAN column b, see the class comment. Every 4th s is NULL. Converts 2024-01-01 of pq to parquet, unless pq is
    // a FORMAT PARQUET table, which writes it as parquet.
    private void createTwins(String dedup, boolean hasBoolean, String timestampType, boolean isFormatParquet) throws Exception {
        this.hasBoolean = hasBoolean;
        this.timestampType = timestampType;
        final String ddl = "CREATE TABLE %s (ts " + timestampType + ", id LONG, v DOUBLE, s VARCHAR, sym SYMBOL INDEX" + (hasBoolean ? ", b BOOLEAN" : "")
                + ") TIMESTAMP(ts) PARTITION BY DAY%s WAL DEDUP " + dedup + " KEYS(ts, id)";
        execute(String.format(ddl, "nat", ""));
        execute(String.format(ddl, "pq", isFormatParquet ? " FORMAT PARQUET" : ""));
        execute("INSERT INTO nat SELECT ('2024-01-01'::TIMESTAMP + (x - 1) * 8_640_000L)::" + timestampType
                + ", x, x * 1.5, CASE WHEN x % 4 = 3 THEN NULL ELSE 'v' || x END, 's' || (x % 7)"
                + (hasBoolean ? ", x % 2 = 0" : "") + " FROM long_sequence(10_000)");
        if (!isFormatParquet) {
            execute("INSERT INTO nat (ts, id) VALUES ('2024-01-02', 0)");
        }
        drainWalQueue();
        execute("INSERT INTO pq SELECT * FROM nat");
        drainWalQueue();
        if (!isFormatParquet) {
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            drainWalQueue();
        }
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");
        Assert.assertEquals("row groups", 10, parquetRowGroupCount(engine, "pq", 0));
        Assert.assertEquals("unused bytes", 0, parquetUnusedBytes(engine, "pq", 0));
        assertTwinsEqual(engine, sqlExecutionContext);
    }

    // a new row of 2024-01-01 at the timestamp, in micros, with the id
    private String newRow(String timestampMicros, long id) {
        return "SELECT (" + timestampMicros + ")::" + timestampType + ", " + id + "::LONG, -1.5, 'new'::VARCHAR, 's3'::SYMBOL" + (hasBoolean ? ", true" : "");
    }

    // a new row k milliseconds after the last stored row of 2024-01-01, past the partition max
    private String newRowPastMax(int k) {
        return newRow("'" + DAY_MAX + "'::TIMESTAMP + " + k + " * 1_000L", 100_000 + k);
    }
}
