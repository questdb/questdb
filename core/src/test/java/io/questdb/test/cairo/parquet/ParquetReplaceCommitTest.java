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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class ParquetReplaceCommitTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // super.setUp() resets per-node cairo state, so set overrides after it
        // (existing parquet tests set them in the test body, which runs later still).
        super.setUp();
        // 12 rows per day at 2h spacing -> 3 row groups of 4 per parquet partition.
        // Disable the dead-bytes rewrite triggers so only DROP / schema changes rewrite.
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
    public void testDropCoveredRowGroupRewrites() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
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
    public void testDropRewriteFaultSuspendsThenConverges() throws Exception {
        // The DROP of row group 1 forces a rewrite into a new txn-named directory.
        // The new data.parquet is opened read-only, so the rewrite's first write
        // fails, the table suspends, and the retry after RESUME WAL must converge.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        assertMemoryLeak(readOnlyParquetOnce(armed, faults), () -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            armed.set(true);
            replaceBoth("2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z");
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
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
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
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
            assertQuery("SELECT id, ts, v FROM pq WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            5\t2024-01-01T08:00:00.000000Z\tv5
                            1000\t2024-01-01T09:00:00.000000Z\tr0
                            1001\t2024-01-01T10:30:00.000000Z\tr1
                            7\t2024-01-01T12:00:00.000000Z\tv7
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
    public void testRangeBetweenRowsLeavesDataIntact() throws Exception {
        assertMemoryLeak(() -> {
            // the window falls strictly between the rows at 08:00 and 10:00 of row group 1
            createTwins(false);
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T09:30:00.000000Z");
            assertTwinsEqual();
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

            final String insert = "INSERT INTO %s VALUES (1, '2024-01-02T05:00:00.000000Z', 'a', 's1', 1.5)";
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

    private static void appendRows(TableToken token, boolean putExtra, String rangeLo, String rangeHi, String... rowTs) throws Exception {
        try (WalWriter ww = engine.getWalWriter(token)) {
            for (int i = 0; i < rowTs.length; i++) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor(rowTs[i]));
                row.putInt(0, 1000 + i);
                row.putVarchar(2, new Utf8String("r" + i));
                row.putSym(3, "n");
                row.putDouble(4, -i);
                if (putExtra) {
                    row.putInt(5, 2000 + i);
                }
                row.append();
            }
            ww.commitWithParams(
                    MicrosTimestampDriver.floor(rangeLo),
                    MicrosTimestampDriver.floor(rangeHi) + 1,
                    WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
    }

    private static long parquetFileSize(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionParquetFileSize(partitionIndex);
        }
    }

    private static long partitionNameTxn(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
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

    private static void replaceBoth(String rangeLo, String rangeHi, String... rowTs) throws Exception {
        replaceBoth(false, rangeLo, rangeHi, rowTs);
    }

    /**
     * Commits the same replace range to nat and pq. With putExtra, each new row also
     * sets the {@code extra} INT column (index 5) to 2000 + its position.
     */
    private static void replaceBoth(boolean putExtra, String rangeLo, String rangeHi, String... rowTs) throws Exception {
        appendRows(engine.verifyTableName("nat"), putExtra, rangeLo, rangeHi, rowTs);
        appendRows(engine.verifyTableName("pq"), putExtra, rangeLo, rangeHi, rowTs);
        drainWalQueue();
    }

    private void assertTwinsEqual() throws Exception {
        Assert.assertFalse("nat suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("nat")));
        Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
        assertSqlCursors("nat", "pq");
        assertSqlCursors("SELECT count(), min(ts), max(ts) FROM nat", "SELECT count(), min(ts), max(ts) FROM pq");
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
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, v VARCHAR, sym SYMBOL, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY%s WAL%s";
        final String dedupClause = dedup ? " DEDUP UPSERT KEYS(ts, id)" : "";
        execute(String.format(ddl, "nat", "", dedupClause));
        execute(String.format(ddl, "pq", formatParquet ? " FORMAT PARQUET" : "", dedupClause));
        final String insert = """
                INSERT INTO %s
                SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 'v' || x, 's' || (x %% 3), x * 1.5
                FROM long_sequence(36)
                """;
        execute(String.format(insert, "nat"));
        execute(String.format(insert, "pq"));
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
