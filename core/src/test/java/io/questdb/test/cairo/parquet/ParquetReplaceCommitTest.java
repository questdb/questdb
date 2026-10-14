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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class ParquetReplaceCommitTest extends AbstractCairoTest {
    private static final long DAY1 = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
    private static final int LARGE_ROWS_PER_DAY = 10_000;
    private static final int LARGE_ROW_GROUP_SIZE = 1_000;
    private static final long LARGE_STEP = Micros.DAY_MICROS / LARGE_ROWS_PER_DAY;

    @Override
    @Before
    public void setUp() {
        // super.setUp() resets per-node cairo state, so set overrides after it
        // (existing parquet tests set them in the test body, which runs later still).
        super.setUp();
        // 12 rows per day at 2h spacing -> 3 row groups of 4 per parquet partition.
        // Disable the dead-bytes rewrite triggers so only schema changes rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testAcrossRowGroupBoundaryUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T05:00:00.000000Z", "2024-01-01T09:00:00.000000Z", "2024-01-01T07:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            11
                            """);
        });
    }

    @Test
    public void testAfterAddColumnRewrites() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            execute("ALTER TABLE nat ADD COLUMN extra INT");
            execute("ALTER TABLE pq ADD COLUMN extra INT");
            drainWalQueue();
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z", "2024-01-01T09:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
        });
    }

    @Test
    public void testColumnAddedBeforeConvertThenInPlaceReplace() throws Exception {
        assertMemoryLeak(() -> {
            // extra is added before CONVERT, so _cv has no record for it in partition 0
            // (-1, absent: the column was added at the last partition) while the parquet
            // file encodes it as an all-NULL chunk, and no schema-change rewrite triggers.
            // The in-place replace then rewrites row group 1 (4 -> 5 rows: 10:00 is removed,
            // 09:00 and 10:30 are added) while its O3 rows carry values for extra, so _cv
            // must record top 0 or CONVERT TO NATIVE reads NULLs.
            createTwins(false, true, false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth(
                    true,
                    "2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z",
                    "2024-01-01T09:00:00.000000Z", "2024-01-01T10:30:00.000000Z"
            );
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            execute("ALTER TABLE pq CONVERT PARTITION TO NATIVE WHERE ts < '2024-01-03'");
            drainWalQueue();
            assertTwinsEqual();
            assertQuery("SELECT id, ts, extra FROM pq WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\textra
                            5\t2024-01-01T08:00:00.000000Z\tnull
                            1000\t2024-01-01T09:00:00.000000Z\t2000
                            1001\t2024-01-01T10:30:00.000000Z\t2001
                            7\t2024-01-01T12:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testCoveredRowGroupWithNewRowsUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth(
                    "2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z",
                    "2024-01-01T09:30:00.000000Z", "2024-01-01T11:30:00.000000Z"
            );
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
        });
    }

    @Test
    public void testDropCoveredRowGroupUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            Assert.assertEquals(3, parquetRowGroupCount("pq", 0));
            Assert.assertEquals(0, parquetUnusedBytes("pq", 0));
            // [07:00, 15:00] covers all of row group 1 (08:00..14:00) and brings no rows
            replaceBoth("2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z");
            assertTwinsEqual();
            // DROP runs in place: same directory, the file only grows (a new footer),
            // and the dropped row group's bytes are now unused
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(2, parquetRowGroupCount("pq", 0));
            Assert.assertTrue(parquetFileSize("pq", 0) > sizeBefore);
            Assert.assertTrue(parquetUnusedBytes("pq", 0) > 0);
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            8
                            """);
        });
    }

    @Test
    public void testDropFirstRowGroupWithGapRowUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            // [00:00, 07:00] covers all of row group 0 (00:00..06:00); the 07:00 row
            // falls in the gap before row group 1
            replaceBoth("2024-01-01T00:00:00.000000Z", "2024-01-01T07:00:00.000000Z", "2024-01-01T07:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT min(ts), count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tcount
                            2024-01-01T07:00:00.000000Z\t9
                            """);
        });
    }

    @Test
    public void testDropInPlaceFaultAfterFooterWrittenSuspendsThenConverges() throws Exception {
        // The DROP of row group 1 runs in update mode: end() appends the new parquet
        // footer and the new _pm footer. The next step maps data.parquet at its new,
        // larger size to update indexes; that map fails, before commitParquetMeta
        // publishes the _pm header. The rollback truncates data.parquet back to its
        // committed size and leaves the _pm tail as dead bytes, and the table
        // suspends. The retry after RESUME WAL must re-apply cleanly over both.
        final AtomicLong failMapAbove = new AtomicLong(-1);
        final AtomicInteger faults = new AtomicInteger();
        assertMemoryLeak(failGrownParquetMapOnce(failMapAbove, faults), () -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            final long pmLengthBefore = partitionFileLength("pq", 0, TableUtils.PARQUET_METADATA_FILE_NAME);
            failMapAbove.set(sizeBefore);
            replaceBoth("2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z");
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            // the failed update published nothing: data.parquet is truncated back, and
            // the appended _pm footer stays behind the committed header as a dead tail
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
            Assert.assertEquals(sizeBefore, partitionFileLength("pq", 0, TableUtils.PARQUET_PARTITION_NAME));
            Assert.assertTrue(partitionFileLength("pq", 0, TableUtils.PARQUET_METADATA_FILE_NAME) > pmLengthBefore);
            Assert.assertEquals(3, parquetRowGroupCount("pq", 0));
            Assert.assertEquals(0, parquetUnusedBytes("pq", 0));

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(2, parquetRowGroupCount("pq", 0));
            Assert.assertTrue(parquetUnusedBytes("pq", 0) > 0);
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            8
                            """);
        });
    }

    @Test
    public void testDropInPlaceFaultSuspendsThenConverges() throws Exception {
        // The DROP of row group 1 runs in update mode, appending a new footer to the
        // live data.parquet. That file is opened read-only, so the update's write
        // fails, the update rolls back, and the table suspends. The retry after
        // RESUME WAL must re-apply cleanly over the rolled-back _pm and data tail.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        assertMemoryLeak(readOnlyParquetOnce(armed, faults), () -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            armed.set(true);
            replaceBoth("2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z");
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            // the failed update published nothing
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
            Assert.assertEquals(3, parquetRowGroupCount("pq", 0));

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(2, parquetRowGroupCount("pq", 0));
            Assert.assertTrue(parquetUnusedBytes("pq", 0) > 0);
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            8
                            """);
        });
    }

    @Test
    public void testDropLastRowGroupUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            // [15:00, 23:59:59] covers all of row group 2 (16:00..22:00), no rows
            replaceBoth("2024-01-01T15:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(2, parquetRowGroupCount("pq", 0));
            assertQuery("SELECT max(ts), count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            max\tcount
                            2024-01-01T14:00:00.000000Z\t8
                            """);
        });
    }

    @Test
    public void testDropMergeAndO3InOneCommitUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            // [05:00, 15:00] in one commit: row group 0 merges (06:00 removed, 05:00
            // added), row group 1 (08:00..14:00) is dropped, the 15:00 row lands in
            // the gap before row group 2, which is copied
            replaceBoth(
                    "2024-01-01T05:00:00.000000Z", "2024-01-01T15:00:00.000000Z",
                    "2024-01-01T05:00:00.000000Z", "2024-01-01T15:00:00.000000Z"
            );
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            9
                            """);
            // a further in-place commit over the shifted layout
            replaceBoth("2024-01-01T17:00:00.000000Z", "2024-01-01T19:00:00.000000Z", "2024-01-01T17:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
        });
    }

    @Test
    public void testDropWithPinnedReader() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            final String fullScan = "SELECT * FROM pq";
            final String filtered = "SELECT id, ts, s FROM pq WHERE id IN (2, 6, 10) AND ts IN '2024-01-01'";
            final String oldFull = printedSql(fullScan);
            final String oldFiltered = printedSql(filtered);
            try (
                    RecordCursorFactory fullFactory = select(fullScan);
                    RecordCursor fullCursor = fullFactory.getCursor(sqlExecutionContext);
                    RecordCursorFactory filteredFactory = select(filtered);
                    RecordCursor filteredCursor = filteredFactory.getCursor(sqlExecutionContext)
            ) {
                final StringSink fullSink = new StringSink();
                CursorPrinter.println(fullFactory.getMetadata(), fullSink);
                // read into row group 0 of the parquet partition before the DROP lands
                for (int i = 0; i < 3; i++) {
                    Assert.assertTrue(fullCursor.hasNext());
                    CursorPrinter.println(fullCursor.getRecord(), fullFactory.getMetadata(), fullSink);
                }
                final StringSink filteredSink = new StringSink();
                CursorPrinter.println(filteredFactory.getMetadata(), filteredSink);

                // drops row group 1 (ids 5..8) in place while both cursors stay open
                replaceBoth("2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z");
                Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));

                readRest(fullCursor, fullFactory, fullSink);
                readRest(filteredCursor, filteredFactory, filteredSink);
                TestUtils.assertEquals(oldFull, fullSink);
                TestUtils.assertEquals(oldFiltered, filteredSink);
            }
            // a fresh reader sees the new snapshot
            assertTwinsEqual();
            assertQuery(filtered)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ts
                            2\t2024-01-01T02:00:00.000000Z\tstr2
                            10\t2024-01-01T18:00:00.000000Z\tstr10
                            """);
        });
    }

    @Test
    public void testFirstPartitionHeadUpdatesMinTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth("2024-01-01T00:00:00.000000Z", "2024-01-01T03:00:00.000000Z");
            assertTwinsEqual();
            assertQuery("SELECT min(ts) FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("min")
                    .returns("""
                            min
                            2024-01-01T04:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFormatParquetLastPartitionNoop() throws Exception {
        assertMemoryLeak(() -> {
            // The window falls in the gap between the last partition's row groups 1
            // (08:00-14:00) and 2 (16:00-22:00) and brings no rows, so every action is
            // a copy and the last-partition sink publishes mutates=0. A window inside a
            // row group's bounds would re-encode that row group instead.
            createTwins(true);
            final long txnBefore = partitionNameTxn("pq", 2);
            final long sizeBefore = parquetFileSize("pq", 2);
            replaceBoth("2024-01-03T14:30:00.000000Z", "2024-01-03T15:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 2));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 2));
            assertQuery("SELECT count(), max(ts) FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tmax
                            36\t2024-01-03T22:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFormatParquetLastPartitionTailUpdatesMaxTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            replaceBoth("2024-01-03T19:00:00.000000Z", "2024-01-03T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT max(ts) FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .timestampDesc("max")
                    .returns("""
                            max
                            2024-01-03T18:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFormatParquetRemovesLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            replaceBoth("2024-01-03T00:00:00.000000Z", "2024-01-03T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('pq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2024-01-01\t12\ttrue
                            2024-01-02\t12\ttrue
                            """);
        });
    }

    @Test
    public void testFormatParquetReplaceCreatesNewPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            replaceBoth(
                    "2024-01-03T08:30:00.000000Z", "2024-01-04T02:00:00.000000Z",
                    "2024-01-03T09:00:00.000000Z", "2024-01-04T01:00:00.000000Z"
            );
            assertTwinsEqual();
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('pq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2024-01-01\t12\ttrue
                            2024-01-02\t12\ttrue
                            2024-01-03\t6\ttrue
                            2024-01-04\t1\ttrue
                            """);
        });
    }

    @Test
    public void testInPlaceFilterMergeFaultSuspendsThenConverges() throws Exception {
        // A filter-only merge of row group 1 runs in update mode, appending to the
        // live data.parquet. That file is opened read-only, so the update's write
        // fails, the update rolls back, and the table suspends. The retry after
        // RESUME WAL must re-apply cleanly over the rolled-back _pm and data tail.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        assertMemoryLeak(readOnlyParquetOnce(armed, faults), () -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            armed.set(true);
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z");
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            11
                            """);
        });
    }

    @Test
    public void testInsideRowGroupNoRowsUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            // A zero-row commit whose window partially covers row group 1 makes a
            // filter-only merge with an empty O3 slice. The STRING and BINARY
            // columns share StringTypeDriver, whose getDataVectorSize() reads the
            // aux vector even for an empty row range, so the merge must not size
            // their O3 data.
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT id, ts, s, bin FROM pq WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ts\tbin
                            5\t2024-01-01T08:00:00.000000Z\tstr5\t
                            7\t2024-01-01T12:00:00.000000Z\t\t
                            8\t2024-01-01T14:00:00.000000Z\tstr8\t00000000 c4 91
                            """);
        });
    }

    @Test
    public void testInsideRowGroupUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth(
                    "2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z",
                    "2024-01-01T09:00:00.000000Z", "2024-01-01T10:30:00.000000Z"
            );
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT id, ts, v, s, bin FROM pq WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv\ts\tbin
                            5\t2024-01-01T08:00:00.000000Z\tv5\tstr5\t
                            1000\t2024-01-01T09:00:00.000000Z\tr0\trs0\t00000000 10 ab
                            1001\t2024-01-01T10:30:00.000000Z\tr1\trs1\t00000000 11 ab
                            7\t2024-01-01T12:00:00.000000Z\tv7\t\t
                            """);
        });
    }

    @Test
    public void testMissingDataIsNoop() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            replaceBoth("2024-01-01T22:30:00.000000Z", "2024-01-01T23:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            // An in-place update also keeps the name txn, but appends a footer and
            // grows the committed file size. Only the no-op short-circuit keeps it.
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
            assertQuery("SELECT count() FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            36
                            """);
        });
    }

    @Test
    public void testMostOfPartitionReplaceRewritesCleanFile() throws Exception {
        assertMemoryLeak(() -> {
            // Default dead-bytes thresholds: a replace that re-encodes every row group
            // in place would leave the old row groups behind as dead bytes (about half
            // the resulting file). The rewrite gate must count the bytes the planned
            // merges are about to kill and write one clean file instead.
            useDefaultRewriteThresholds();
            createLargeTwins();
            Assert.assertEquals(LARGE_ROWS_PER_DAY / LARGE_ROW_GROUP_SIZE, parquetRowGroupCount("pq", 0));
            Assert.assertEquals(0, parquetUnusedBytes("pq", 0));
            final long txnBefore = partitionNameTxn("pq", 0);
            // the whole day, with one new row between each pair of existing rows
            replaceBothLarge(
                    DAY1, DAY1 + Micros.DAY_MICROS - 1,
                    DAY1 + LARGE_STEP / 2, LARGE_STEP, LARGE_ROWS_PER_DAY
            );
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(0, parquetUnusedBytes("pq", 0));
            assertCleanAgainstFreshConversion();
        });
    }

    @Test
    public void testPlainO3AcrossPartitionRewritesAtDefaultThresholds() throws Exception {
        assertMemoryLeak(() -> {
            // Not a replace: a plain O3 insert that lands inside every row group merges
            // them all, so the same projected-dead-bytes gate rewrites the file.
            useDefaultRewriteThresholds();
            createLargeTwins();
            final long txnBefore = partitionNameTxn("pq", 0);
            final String insert = """
                    INSERT INTO %s
                    SELECT
                        (100_000 + x)::INT,
                        '2024-01-01'::TIMESTAMP + (x - 1) * %d + %d,
                        'o' || x,
                        'o',
                        -x * 1.5,
                        'os' || x,
                        NULL
                    FROM long_sequence(%d)
                    """;
            execute(String.format(insert, "nat", LARGE_STEP, LARGE_STEP / 2, LARGE_ROWS_PER_DAY));
            execute(String.format(insert, "pq", LARGE_STEP, LARGE_STEP / 2, LARGE_ROWS_PER_DAY));
            drainWalQueue();
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(0, parquetUnusedBytes("pq", 0));
            assertCleanAgainstFreshConversion();
        });
    }

    @Test
    public void testRandomReplaceCommitsDropInPlace() throws Exception {
        assertMemoryLeak(() -> assertRandomReplaceCommits(false));
    }

    @Test
    public void testRandomReplaceCommitsDropInPlaceFormatParquet() throws Exception {
        assertMemoryLeak(() -> assertRandomReplaceCommits(true));
    }

    @Test
    public void testRangeBetweenRowsLeavesDataIntact() throws Exception {
        assertMemoryLeak(() -> {
            // the window falls strictly between the rows at 08:00 and 10:00 of row group 1
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T09:30:00.000000Z");
            assertTwinsEqual();
            // The window holds none of row group 1's rows, so the commit must not
            // re-encode it: an in-place filter merge keeps the name txn but appends
            // a new row group and footer, growing the committed file size.
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
            assertQuery("SELECT count() FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            36
                            """);
        });
    }

    @Test
    public void testRangeEdgesOnRowGroupEdgesRemovesEdgeRows() throws Exception {
        assertMemoryLeak(() -> {
            // Row groups of day 1: [00:00, 06:00], [08:00, 14:00], [16:00, 22:00].
            // The inclusive window [06:00, 16:00] touches row group 0 only at its max
            // row and row group 2 only at its min row, so both edge rows must go.
            createTwins(false);
            replaceBoth("2024-01-01T06:00:00.000000Z", "2024-01-01T16:00:00.000000Z", "2024-01-01T11:00:00.000000Z");
            assertTwinsEqual();
            assertQuery("SELECT id, ts, v FROM pq WHERE ts BETWEEN '2024-01-01T04:00' AND '2024-01-01T18:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            3\t2024-01-01T04:00:00.000000Z\tv3
                            1000\t2024-01-01T11:00:00.000000Z\tr0
                            10\t2024-01-01T18:00:00.000000Z\tv10
                            """);
        });
    }

    @Test
    public void testRangeOnSingleRowRemovesIt() throws Exception {
        assertMemoryLeak(() -> {
            // The window [10:00, 10:00] holds exactly one row of row group 1 and brings
            // no rows. Both window ends equal that row, so the timestamp-only check that
            // skips no-op filter merges must count it as in range and keep the merge.
            createTwins(false);
            final long sizeBefore = parquetFileSize("pq", 0);
            replaceBoth("2024-01-01T10:00:00.000000Z", "2024-01-01T10:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertNotEquals(sizeBefore, parquetFileSize("pq", 0));
            assertQuery("SELECT id, ts FROM pq WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id	ts
                            5	2024-01-01T08:00:00.000000Z
                            7	2024-01-01T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRemovesFirstParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth("2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT min(ts) FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("min")
                    .returns("""
                            min
                            2024-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRemovesWholeParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth("2024-01-02T00:00:00.000000Z", "2024-01-02T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('pq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2024-01-01\t12\ttrue
                            2024-01-03\t12\tfalse
                            """);
        });
    }

    @Test
    public void testRepeatedDropsCompactDeadBytes() throws Exception {
        assertMemoryLeak(() -> {
            // Default thresholds: every DROP in place leaves its row group behind as
            // dead bytes, until the projected-dead-bytes gate rewrites a clean file.
            useDefaultRewriteThresholds();
            createLargeTwins();
            final int rowGroups = LARGE_ROWS_PER_DAY / LARGE_ROW_GROUP_SIZE;
            Assert.assertEquals(rowGroups, parquetRowGroupCount("pq", 0));
            final long txnBefore = partitionNameTxn("pq", 0);
            long unusedBefore = 0;
            int inPlaceDrops = 0;
            boolean rewritten = false;
            for (int g = 0; g < rowGroups - 1 && !rewritten; g++) {
                // exactly row group g of the original layout, no rows
                final long lo = DAY1 + (long) g * LARGE_ROW_GROUP_SIZE * LARGE_STEP;
                final long hi = lo + (LARGE_ROW_GROUP_SIZE - 1) * LARGE_STEP;
                replaceBothLarge(lo, hi, 0, 0, 0);
                assertTwinsEqual();
                Assert.assertEquals(rowGroups - g - 1, parquetRowGroupCount("pq", 0));
                final long unused = parquetUnusedBytes("pq", 0);
                if (partitionNameTxn("pq", 0) == txnBefore) {
                    Assert.assertTrue("drop " + g + ": " + unused + " <= " + unusedBefore, unused > unusedBefore);
                    unusedBefore = unused;
                    inPlaceDrops++;
                } else {
                    Assert.assertEquals(0, unused);
                    rewritten = true;
                }
            }
            Assert.assertTrue("the dead-bytes gate must rewrite eventually", rewritten);
            Assert.assertTrue("at least two DROPs must run in place first, got " + inPlaceDrops, inPlaceDrops >= 2);
            assertCleanAgainstFreshConversion();
        });
    }

    @Test
    public void testReplaceOnDedupTableReachingParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            // Both twins are DEDUP UPSERT KEYS(ts, id). The replace commit reaches a
            // converted partition and brings a row at 10:00, the timestamp of an
            // existing row (id 6). Replace and dedup are exclusive, so the range is
            // removed and the new rows land as-is, identically on both twins.
            createTwins(false, false, true);
            replaceBoth(
                    "2024-01-01T09:00:00.000000Z", "2024-01-01T11:00:00.000000Z",
                    "2024-01-01T10:00:00.000000Z", "2024-01-01T10:30:00.000000Z"
            );
            assertTwinsEqual();
            assertQuery("SELECT id, ts, v FROM pq WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            5\t2024-01-01T08:00:00.000000Z\tv5
                            1000\t2024-01-01T10:00:00.000000Z\tr0
                            1001\t2024-01-01T10:30:00.000000Z\tr1
                            7\t2024-01-01T12:00:00.000000Z\tv7
                            """);
        });
    }

    @Test
    public void testReplaceRemovesAllPartitionsFormatParquet() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            replaceBoth("2024-01-01T00:00:00.000000Z", "2024-01-03T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT count() FROM table_partitions('pq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            0
                            """);
            assertQuery("SELECT count() FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            0
                            """);

            final String insert = "INSERT INTO %s VALUES (1, '2024-01-02T05:00:00.000000Z', 'a', 's1', 1.5, 'b', NULL)";
            execute(String.format(insert, "nat"));
            execute(String.format(insert, "pq"));
            drainWalQueue();
            assertTwinsEqual();
            assertQuery("SELECT id, ts, v, sym, d FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv\tsym\td
                            1\t2024-01-02T05:00:00.000000Z\ta\ts1\t1.5
                            """);
        });
    }

    @Test
    public void testSingleRowGroupMissingDataIsNoop() throws Exception {
        assertMemoryLeak(() -> {
            // At the default row group size each parquet partition is one row group.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, (String) null);
            createTwins(false);
            Assert.assertEquals(1, parquetRowGroupCount("pq", 0));
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            // the window lies after the last row of day 1 (22:00), so it misses every row
            replaceBoth("2024-01-01T22:30:00.000000Z", "2024-01-01T23:30:00.000000Z");
            assertTwinsEqual();
            // A rewrite bumps the name txn; only the no-op short-circuit keeps both.
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
            assertQuery("SELECT count() FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            36
                            """);
        });
    }

    @Test
    public void testSingleRowGroupRangeBetweenRowsIsNoop() throws Exception {
        assertMemoryLeak(() -> {
            // At the default row group size each parquet partition is one row group,
            // so a filter merge of it would force a rewrite to a new name txn.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, (String) null);
            createTwins(false);
            Assert.assertEquals(1, parquetRowGroupCount("pq", 0));
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            // the window falls strictly between the rows at 08:00 and 10:00
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T09:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
            Assert.assertEquals(1, parquetRowGroupCount("pq", 0));
            assertQuery("SELECT count() FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            36
                            """);
        });
    }

    @Test
    public void testSingleRowGroupRemovesFirstPartition() throws Exception {
        assertMemoryLeak(() -> {
            // At the default row group size each parquet partition is one row group.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, (String) null);
            createTwins(false);
            Assert.assertEquals(1, parquetRowGroupCount("pq", 0));
            // the window covers the whole first partition, so the table min timestamp moves
            replaceBoth("2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('pq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2024-01-02\t12\ttrue
                            2024-01-03\t12\tfalse
                            """);
            assertQuery("SELECT min(ts), count() FROM pq")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tcount
                            2024-01-02T00:00:00.000000Z\t24
                            """);
        });
    }

    @Test
    public void testSmallReplaceAtDefaultThresholdsUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            // Control for the projected-dead-bytes gate: a replace inside one row group
            // of ten kills about a tenth of the file, well under the default ratio, so
            // it stays an in-place update.
            useDefaultRewriteThresholds();
            createLargeTwins();
            final long txnBefore = partitionNameTxn("pq", 0);
            final long lo = DAY1 + 4_500 * LARGE_STEP;
            replaceBothLarge(lo, lo + 2 * LARGE_STEP, lo + LARGE_STEP / 2, LARGE_STEP, 2);
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            // in place: the replaced row group's old bytes are now dead
            Assert.assertTrue(parquetUnusedBytes("pq", 0) > 0);
            Assert.assertEquals(LARGE_ROWS_PER_DAY / LARGE_ROW_GROUP_SIZE, parquetRowGroupCount("pq", 0));
        });
    }

    @Test
    public void testSpansNativeAndParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth(
                    "2024-01-02T20:00:00.000000Z", "2024-01-03T03:00:00.000000Z",
                    "2024-01-02T21:00:00.000000Z", "2024-01-03T01:00:00.000000Z"
            );
            assertTwinsEqual();
        });
    }

    /**
     * Appends one row per timestamp and commits them as a replace range. Every row
     * sets non-NULL STRING and BINARY values, so a replace that lands on a parquet
     * row group merges non-NULL var-size O3 values into it: {@code s} is "rs" + its
     * position and {@code bin} is the two bytes {0x10 + position, 0xab}.
     */
    private static void appendRows(TableToken token, boolean putExtra, String rangeLo, String rangeHi, String... rowTs) throws Exception {
        final long[] timestamps = new long[rowTs.length];
        for (int i = 0; i < rowTs.length; i++) {
            timestamps[i] = MicrosTimestampDriver.floor(rowTs[i]);
        }
        appendRows(token, putExtra, MicrosTimestampDriver.floor(rangeLo), MicrosTimestampDriver.floor(rangeHi), timestamps);
    }

    /**
     * As {@link #appendRows(TableToken, boolean, String, String, String...)}, with the
     * row timestamps and the inclusive replace range in micros.
     */
    private static void appendRows(TableToken token, boolean putExtra, long rangeLo, long rangeHi, long[] rowTs) throws Exception {
        final long binAddr = Unsafe.malloc(2, MemoryTag.NATIVE_DEFAULT);
        try (WalWriter ww = engine.getWalWriter(token)) {
            for (int i = 0; i < rowTs.length; i++) {
                TableWriter.Row row = ww.newRow(rowTs[i]);
                row.putInt(0, 1000 + i);
                row.putVarchar(2, new Utf8String("r" + i));
                row.putSym(3, "n");
                row.putDouble(4, -i);
                row.putStr(5, "rs" + i);
                Unsafe.putByte(binAddr, (byte) (0x10 + i));
                Unsafe.putByte(binAddr + 1, (byte) 0xab);
                row.putBin(6, binAddr, 2);
                if (putExtra) {
                    row.putInt(7, 2000 + i);
                }
                row.append();
            }
            ww.commitWithParams(rangeLo, rangeHi + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
        } finally {
            Unsafe.free(binAddr, 2, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * The first data.parquet map of the partition decoder larger than
     * {@code failMapAbove} fails while it is non-negative, then disarms. Only the
     * O3 job maps data.parquet past its committed size, and only after end() has
     * appended the new parquet and _pm footers.
     */
    private static FilesFacade failGrownParquetMapOnce(AtomicLong failMapAbove, AtomicInteger faults) {
        return new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                final long threshold = failMapAbove.get();
                if (memoryTag == MemoryTag.MMAP_PARQUET_PARTITION_DECODER
                        && threshold >= 0
                        && len > threshold
                        && failMapAbove.compareAndSet(threshold, -1)) {
                    faults.incrementAndGet();
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
        };
    }

    private static long parquetFileSize(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionParquetFileSize(partitionIndex);
        }
    }

    private static int parquetRowGroupCount(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            reader.openPartition(partitionIndex);
            return reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getRowGroupCount();
        }
    }

    private static long parquetUnusedBytes(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            reader.openPartition(partitionIndex);
            return reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getUnusedBytes();
        }
    }

    /**
     * On-disk length of {@code fileName} in the partition's directory.
     */
    private static long partitionFileLength(String table, int partitionIndex, CharSequence fileName) {
        try (TableReader reader = engine.getReader(table); Path path = new Path()) {
            path.of(root).concat(reader.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(partitionIndex),
                    reader.getTxFile().getPartitionNameTxn(partitionIndex)
            );
            return engine.getConfiguration().getFilesFacade().length(path.concat(fileName).$());
        }
    }

    private static long partitionNameTxn(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    private static String printedSql(CharSequence sql) throws Exception {
        final StringSink out = new StringSink();
        printSql(sql, out);
        return out.toString();
    }

    /**
     * The first data.parquet opened read-write while armed is handed a read-only fd
     * instead, so the O3 job's first write to it fails. The fault fires once and
     * disarms, so the rollback's own openRW of data.parquet still succeeds.
     */
    private static FilesFacade readOnlyParquetOnce(AtomicBoolean armed, AtomicInteger faults) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, "data.parquet") && armed.compareAndSet(true, false)) {
                    faults.incrementAndGet();
                    final long rwFd = super.openRW(name, opts);
                    super.close(rwFd);
                    return super.openRO(name);
                }
                return super.openRW(name, opts);
            }
        };
    }

    private static void readRest(RecordCursor cursor, RecordCursorFactory factory, StringSink sink) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            CursorPrinter.println(record, factory.getMetadata(), sink);
        }
    }

    private static void replaceBoth(String rangeLo, String rangeHi, String... rowTs) throws Exception {
        replaceBoth(false, rangeLo, rangeHi, rowTs);
    }

    /**
     * Commits the same replace range to nat and pq. With putExtra, each new row also
     * sets the {@code extra} INT column (index 7) to 2000 + its position.
     */
    private static void replaceBoth(boolean putExtra, String rangeLo, String rangeHi, String... rowTs) throws Exception {
        appendRows(engine.verifyTableName("nat"), putExtra, rangeLo, rangeHi, rowTs);
        appendRows(engine.verifyTableName("pq"), putExtra, rangeLo, rangeHi, rowTs);
        drainWalQueue();
    }

    /**
     * Commits the same replace range [rangeLo, rangeHi] (inclusive, micros) to nat and
     * pq, bringing {@code count} rows at {@code firstTs + i * step}.
     */
    private static void replaceBothLarge(long rangeLo, long rangeHi, long firstTs, long step, int count) throws Exception {
        final long[] timestamps = new long[count];
        for (int i = 0; i < count; i++) {
            timestamps[i] = firstTs + i * step;
        }
        appendRows(engine.verifyTableName("nat"), false, rangeLo, rangeHi, timestamps);
        appendRows(engine.verifyTableName("pq"), false, rangeLo, rangeHi, timestamps);
        drainWalQueue();
    }

    /**
     * Sets the dead-bytes rewrite thresholds back to their defaults (setUp disables them).
     */
    private static void useDefaultRewriteThresholds() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, (String) null);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, (String) null);
    }

    /**
     * Converts nat's first day to parquet and asserts pq's first-day file is no larger
     * than 110% of that freshly converted twin: a clean file, not one carrying the
     * replaced row groups as dead bytes. Call it last: nat is no longer all native.
     */
    private void assertCleanAgainstFreshConversion() throws Exception {
        execute("ALTER TABLE nat CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
        drainWalQueue();
        final long freshSize = parquetFileSize("nat", 0);
        final long pqSize = parquetFileSize("pq", 0);
        Assert.assertEquals(0, parquetUnusedBytes("nat", 0));
        Assert.assertTrue("pq=" + pqSize + ", fresh=" + freshSize, pqSize <= freshSize + freshSize / 10);
        assertSqlCursors("nat", "pq");
    }

    /**
     * Runs 60 random replace commits against the twins: each picks a day, a range of
     * 1 to 12 hours within it and 0 to 3 rows on minute boundaries inside the range,
     * with an occasional in-order append after it. The twins must match after every
     * commit. The seed comes from {@link TestUtils#generateRandom}, so a failure
     * reproduces from the seeds it logs.
     */
    private void assertRandomReplaceCommits(boolean formatParquet) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        createTwins(formatParquet);
        final long hour = Micros.HOUR_MICROS;
        final long minute = Micros.MINUTE_MICROS;
        long appendTs = MicrosTimestampDriver.floor("2024-01-03T23:00:00.000000Z");
        for (int round = 0; round < 60; round++) {
            final long dayStart = DAY1 + rnd.nextInt(3) * Micros.DAY_MICROS;
            final long lo = dayStart + rnd.nextInt(24) * hour;
            final long hi = Math.min(lo + (1 + rnd.nextInt(12)) * hour - 1, dayStart + Micros.DAY_MICROS - 1);
            final long[] ts = new long[rnd.nextInt(4)];
            for (int i = 0; i < ts.length; i++) {
                ts[i] = lo + rnd.nextLong(hi - lo + 1) / minute * minute;
            }
            Arrays.sort(ts);
            appendRows(engine.verifyTableName("nat"), false, lo, hi, ts);
            appendRows(engine.verifyTableName("pq"), false, lo, hi, ts);
            drainWalQueue();
            assertTwinsEqual();
            if (rnd.nextInt(5) == 0) {
                appendTs += hour / 7;
                execute("INSERT INTO nat (id, ts) VALUES (77, " + appendTs + "::TIMESTAMP)");
                execute("INSERT INTO pq (id, ts) VALUES (77, " + appendTs + "::TIMESTAMP)");
                drainWalQueue();
                assertTwinsEqual();
            }
        }
    }

    private void assertTwinsEqual() throws Exception {
        Assert.assertFalse("nat suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("nat")));
        Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
        assertSqlCursors("nat", "pq");
        assertSqlCursors("SELECT count(), min(ts), max(ts) FROM nat", "SELECT count(), min(ts), max(ts) FROM pq");
    }

    /**
     * Creates nat (native) and pq with 3 daily partitions of {@link #LARGE_ROWS_PER_DAY}
     * evenly spaced rows each, and converts pq's first two days to parquet with
     * {@link #LARGE_ROW_GROUP_SIZE} rows per row group. Large enough that row-group
     * data, not the parquet footer, dominates the file size.
     */
    private void createLargeTwins() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, LARGE_ROW_GROUP_SIZE);
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, v VARCHAR, sym SYMBOL, d DOUBLE, s STRING, bin BINARY) TIMESTAMP(ts) PARTITION BY DAY WAL";
        execute(String.format(ddl, "nat"));
        execute(String.format(ddl, "pq"));
        execute(String.format("""
                INSERT INTO nat
                SELECT
                    x::INT,
                    '2024-01-01'::TIMESTAMP + (x - 1) * %d,
                    'v' || x,
                    's' || (x %% 3),
                    x * 1.5,
                    CASE WHEN x %% 4 = 3 THEN NULL ELSE 'str' || x END,
                    rnd_bin(2, 2, 2)
                FROM long_sequence(%d)
                """, LARGE_STEP, 3 * LARGE_ROWS_PER_DAY));
        drainWalQueue();
        execute("INSERT INTO pq SELECT * FROM nat");
        drainWalQueue();
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        drainWalQueue();
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n2\n");
    }

    /**
     * Creates nat (native) and pq with 3 daily partitions of 12 rows at 2h spacing.
     * With formatParquet, pq is a FORMAT PARQUET table (every partition parquet,
     * including the last). Otherwise pq's first two days are converted to parquet
     * and the last stays native.
     */
    private void createTwins(boolean formatParquet) throws Exception {
        createTwins(formatParquet, false, false);
    }

    /**
     * As {@link #createTwins(boolean)}; with addColumnBeforeConvert, both tables get an
     * {@code extra INT} column after the inserts and before any parquet conversion.
     * With dedup, both tables are {@code DEDUP UPSERT KEYS(ts, id)}.
     */
    private void createTwins(boolean formatParquet, boolean addColumnBeforeConvert, boolean dedup) throws Exception {
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, v VARCHAR, sym SYMBOL, d DOUBLE, s STRING, bin BINARY) TIMESTAMP(ts) PARTITION BY DAY%s WAL%s";
        final String dedupClause = dedup ? " DEDUP UPSERT KEYS(ts, id)" : "";
        execute(String.format(ddl, "nat", "", dedupClause));
        execute(String.format(ddl, "pq", formatParquet ? " FORMAT PARQUET" : "", dedupClause));
        // rnd_bin() draws from the per-test seeded random, so pq copies nat's rows
        // rather than re-running the generator. Every 4th s and some bin are NULL.
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
        if (addColumnBeforeConvert) {
            execute("ALTER TABLE nat ADD COLUMN extra INT");
            execute("ALTER TABLE pq ADD COLUMN extra INT");
            drainWalQueue();
        }
        if (!formatParquet) {
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
            drainWalQueue();
        }
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns(formatParquet ? "count\n3\n" : "count\n2\n");
    }
}
