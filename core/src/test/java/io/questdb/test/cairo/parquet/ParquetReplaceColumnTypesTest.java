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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Replace and dedup commits on parquet partitions over every column type, with micro and
 * nano designated timestamps, compared against a native twin. Timestamps use the table's
 * own unit, since the replace path compares raw longs. Partitions hold 3 row groups of 4
 * rows a day with the dead-bytes rewrite gate off, so commits run in place by default.
 */
@RunWith(Parameterized.class)
public class ParquetReplaceColumnTypesTest extends AbstractCairoTest {
    // every column but the designated timestamp, in table order
    private static final String COLUMNS = "id, b, bt, sh, ch, i, l, dt, tsu, tsn, f, d, u, l128, l256, g1, g2, g4, g8, ip, v, s, sym, bin, " +
            "arr1, arr2, dec8, dec16, dec32, dec64, dec128, dec256";
    private static final int INITIAL_ROWS = 36;
    private final TimestampDriver driver;
    private final TestTimestampType timestampType;
    private long day;
    private long day1;
    private long hour;
    // the next src row a replace commit brings
    private int nextSrcId;

    public ParquetReplaceColumnTypesTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
        this.driver = timestampType.getDriver();
    }

    @Parameterized.Parameters(name = "ts={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO},
                {TestTimestampType.NANO},
        });
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        day1 = driver.fromMicros(1_704_067_200_000_000L); // 2024-01-01
        day = driver.fromDays(1);
        hour = driver.fromHours(1);
        nextSrcId = 100;
    }

    @Test
    public void testAcrossThreePartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            // the tail of parquet day 1 merges in place, parquet day 2 is covered and goes
            // away, the head of native day 3 merges
            replaceBoth(ts("2024-01-01T19:00"), ts("2024-01-03T03:00"), ts("2024-01-01T21:00"), ts("2024-01-03T01:00"));
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('pq')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2024-01-01\t11\ttrue
                            2024-01-03\t11\tfalse
                            """);
        });
    }

    @Test
    public void testDropMergeAndO3InOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            // row group 0 merges (06:00 removed, 05:00 added), row group 1 is dropped, 15:00
            // lands in the gap before row group 2
            replaceBoth(ts("2024-01-01T05:00"), ts("2024-01-01T15:00"), ts("2024-01-01T05:00"), ts("2024-01-01T15:00"));
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertDayCount("2024-01-01", 9);
        });
    }

    @Test
    public void testDropRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            // [07:00, 15:00] covers all of row group 1 (08:00..14:00) and brings no rows
            replaceBoth(ts("2024-01-01T07:00"), ts("2024-01-01T15:00"));
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(2, parquetRowGroupCount("pq", 0));
            Assert.assertTrue(parquetFileSize("pq", 0) > sizeBefore);
            assertDayCount("2024-01-01", 8);
        });
    }

    @Test
    public void testFilterMergeNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            // the window removes the 10:00 row of row group 1 and brings no rows: a
            // filter-only merge with an empty O3 slice for every column type
            replaceBoth(ts("2024-01-01T08:30"), ts("2024-01-01T11:30"));
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertDayCount("2024-01-01", 11);
        });
    }

    @Test
    public void testFilterMergeWithRows() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth(ts("2024-01-01T08:30"), ts("2024-01-01T11:30"), ts("2024-01-01T09:00"), ts("2024-01-01T10:30"));
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertDayCount("2024-01-01", 13);
        });
    }

    @Test
    public void testIdenticalDedupCommitAcrossTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            execute("ALTER TABLE nat DEDUP ENABLE UPSERT KEYS(ts, id)");
            execute("ALTER TABLE pq DEDUP ENABLE UPSERT KEYS(ts, id)");
            drainWalQueue();
            final String rowGroup1 = "ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T14:00'";

            // every row of row group 1 again, every value equal: the parquet partition stays
            // as it is, so every type's decoded values compare equal to its O3 values
            long txnBefore = partitionNameTxn("pq", 0);
            long sizeBefore = parquetFileSize("pq", 0);
            insertBoth("SELECT * FROM nat WHERE " + rowGroup1);
            assertTwinsEqual();
            Assert.assertEquals("identical commit, name txn", txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals("identical commit, parquet size", sizeBefore, parquetFileSize("pq", 0));

            // the same rows with one value of one column changed must merge, for every type
            final String[] columns = COLUMNS.split(", ");
            for (String column : columns) {
                if ("id".equals(column)) {
                    continue;
                }
                final long srcId = srcIdWithOtherValue(column);
                final StringSink select = new StringSink();
                select.put("SELECT * FROM nat WHERE ").put(rowGroup1).put(" AND id <> 6 UNION ALL SELECT ");
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        select.put(", ");
                    }
                    select.put(columns[i].equals(column) ? "s." : "n.").put(columns[i]);
                }
                select.put(", n.ts FROM nat n CROSS JOIN src s WHERE n.id = 6 AND s.id = ").put(srcId);
                sizeBefore = parquetFileSize("pq", 0);
                txnBefore = partitionNameTxn("pq", 0);
                insertBoth(select.toString());
                assertTwinsEqual();
                Assert.assertTrue(
                        "a change of " + column + " must update the parquet partition",
                        sizeBefore != parquetFileSize("pq", 0) || txnBefore != partitionNameTxn("pq", 0)
                );
            }
        });
    }

    @Test
    public void testNoop() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            final long txnBefore = partitionNameTxn("pq", 0);
            final long sizeBefore = parquetFileSize("pq", 0);
            // the window falls strictly between the rows at 08:00 and 10:00
            replaceBoth(ts("2024-01-01T08:30"), ts("2024-01-01T09:30"));
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
            // the window misses every row of the partition
            replaceBoth(ts("2024-01-01T22:00") + 1, ts("2024-01-01T23:30"));
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            Assert.assertEquals(sizeBefore, parquetFileSize("pq", 0));
        });
    }

    @Test
    public void testRandomReplaceCommits() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            if (rnd.nextBoolean()) {
                node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, (String) null);
                node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, (String) null);
                LOG.info().$("parquet rewrite gate: defaults").$();
            }
            createTwins(true);
            final StringSink symbols = new StringSink();
            printSql("SELECT DISTINCT sym FROM nat WHERE sym IS NOT NULL ORDER BY sym", symbols);
            final String[] symbolValues = symbols.toString().split("\n");
            final long minute = driver.fromMinutes(1);
            for (int round = 0; round < 40; round++) {
                final long dayStart = day1 + rnd.nextInt(3) * day;
                // lo on an hour: on a row on even hours
                final long lo = dayStart + rnd.nextInt(24) * hour;
                // hi on an hour or just below one, up to a day and a half on
                final long hi = lo + (1 + rnd.nextInt(36)) * hour - (rnd.nextBoolean() ? 1 : 0);
                final long[] timestamps = new long[rnd.nextInt(4)];
                for (int i = 0; i < timestamps.length; i++) {
                    timestamps[i] = lo + rnd.nextLong((hi - lo) / minute + 1) * minute;
                }
                Arrays.sort(timestamps);
                replaceBoth(lo, hi, timestamps);
                assertTwinsEqual();
                final String symbol = symbolValues[1 + rnd.nextInt(symbolValues.length - 1)];
                assertSqlCursors(
                        "SELECT * FROM nat WHERE sym = '" + symbol + "'",
                        "SELECT * FROM pq WHERE sym = '" + symbol + "'"
                );
            }
        });
    }

    @Test
    public void testRangeEdgesOnRowGroupEdges() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            // [06:00, 16:00] touches row group 0 only at its max row and row group 2 only
            // at its min row: both edge rows go
            replaceBoth(ts("2024-01-01T06:00"), ts("2024-01-01T16:00"), ts("2024-01-01T11:00"));
            assertTwinsEqual();
            assertDayCount("2024-01-01", 7);
            // [04:00, 04:00] holds exactly one row; hi - 1 one unit below the 18:00 row
            replaceBoth(ts("2024-01-01T04:00"), ts("2024-01-01T04:00"));
            assertTwinsEqual();
            replaceBoth(ts("2024-01-01T17:00"), ts("2024-01-01T18:00") - 1);
            assertTwinsEqual();
            assertDayCount("2024-01-01", 6);
        });
    }

    @Test
    public void testRemovesPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth(day1 + day, day1 + 2 * day - 1);
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
    public void testSingleRowGroupRewrites() throws Exception {
        assertMemoryLeak(() -> {
            // one row group per partition: any merge rewrites it
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, (String) null);
            createTwins(false);
            Assert.assertEquals(1, parquetRowGroupCount("pq", 0));
            final long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth(ts("2024-01-01T08:30"), ts("2024-01-01T11:30"), ts("2024-01-01T09:00"));
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
            final long txnAfterFirst = partitionNameTxn("pq", 0);
            replaceBoth(ts("2024-01-01T15:00"), ts("2024-01-01T18:00"));
            assertTwinsEqual();
            Assert.assertNotEquals(txnAfterFirst, partitionNameTxn("pq", 0));
        });
    }

    private static void insertBoth(String select) throws Exception {
        // both inserts read nat before either is applied, so both tables get the same rows
        execute("INSERT INTO pq " + select);
        execute("INSERT INTO nat " + select);
        drainWalQueue();
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

    private static long partitionNameTxn(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    private void assertDayCount(String day, int expected) throws Exception {
        assertQuery("SELECT count() FROM pq WHERE ts IN '" + day + "'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + expected + "\n");
    }

    private void assertTwinsEqual() throws Exception {
        Assert.assertFalse("nat suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("nat")));
        Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
        assertSqlCursors("nat", "pq");
        assertSqlCursors("SELECT count(), min(ts), max(ts) FROM nat", "SELECT count(), min(ts), max(ts) FROM pq");
    }

    /**
     * Creates src with 400 rows of random values in every column type, NULLs included, and
     * nat (native) and pq with its first 36 rows at 2h spacing over 3 days. pq's first two
     * days are converted to parquet. With isIndexed, sym has a bitmap index on both.
     */
    private void createTwins(boolean isIndexed) throws Exception {
        execute("""
                CREATE TABLE src AS (
                    SELECT
                        x id,
                        rnd_boolean() b,
                        rnd_byte(0, 100) bt,
                        rnd_short(-1000, 1000) sh,
                        rnd_char() ch,
                        rnd_int(-1000, 1000, 4) i,
                        rnd_long(-100_000, 100_000, 4) l,
                        CASE WHEN x % 6 = 0 THEN NULL ELSE (1_600_000_000_000L + x * 86_400_123L)::DATE END dt,
                        CASE WHEN x % 5 = 0 THEN NULL ELSE (1_700_000_000_000_000L + x * 1_001)::TIMESTAMP END tsu,
                        CASE WHEN x % 7 = 0 THEN NULL ELSE (1_700_000_000_000_000_000L + x * 7)::TIMESTAMP_NS END tsn,
                        rnd_float(4) f,
                        rnd_double(4) d,
                        rnd_uuid4(4) u,
                        to_long128(rnd_long(), rnd_long()) l128,
                        rnd_long256(4) l256,
                        rnd_geohash(5) g1,
                        rnd_geohash(15) g2,
                        rnd_geohash(25) g4,
                        rnd_geohash(50) g8,
                        rnd_ipv4() ip,
                        rnd_varchar(1, 30, 4) v,
                        rnd_str(1, 20, 4) s,
                        rnd_symbol(6, 1, 4, 4) sym,
                        rnd_bin(1, 16, 4) bin,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE rnd_double_array(1, 0, 5) END arr1,
                        CASE WHEN x % 5 = 1 THEN NULL ELSE rnd_double_array(2, 0, 3) END arr2,
                        rnd_decimal(2, 1, 4) dec8,
                        rnd_decimal(4, 2, 4) dec16,
                        rnd_decimal(9, 3, 4) dec32,
                        rnd_decimal(18, 4, 4) dec64,
                        rnd_decimal(38, 6, 4) dec128,
                        rnd_decimal(76, 10, 4) dec256
                    FROM long_sequence(400)
                )
                """);
        execute("CREATE TABLE nat AS (SELECT " + COLUMNS + ", dateadd('h', 2 * (id::INT - 1), '2024-01-01'::" + timestampType.getTypeName() + ") ts" +
                " FROM src WHERE id <= " + INITIAL_ROWS + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        execute("CREATE TABLE pq AS (SELECT * FROM nat) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        if (isIndexed) {
            execute("ALTER TABLE nat ALTER COLUMN sym ADD INDEX");
            execute("ALTER TABLE pq ALTER COLUMN sym ADD INDEX");
            drainWalQueue();
        }
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        drainWalQueue();
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n2\n");
        assertQuery("SELECT count() FROM pq")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + INITIAL_ROWS + "\n");
    }

    /**
     * Commits the same replace over the inclusive range [lo, hi] to nat and pq, bringing the
     * next src rows, one per timestamp.
     */
    private void replaceBoth(long lo, long hi, long... timestamps) throws Exception {
        final String select = "SELECT " + COLUMNS + " FROM src WHERE id >= " + nextSrcId + " AND id < " + (nextSrcId + timestamps.length);
        nextSrcId += timestamps.length;
        ReplaceCommitTestUtils.replace(engine, sqlExecutionContext, "nat", select, lo, hi, timestamps);
        ReplaceCommitTestUtils.replace(engine, sqlExecutionContext, "pq", select, lo, hi, timestamps);
        drainWalQueue();
    }

    /**
     * A src row whose value of column differs from that of nat's row 6, from the rows no
     * test commit uses.
     */
    private long srcIdWithOtherValue(String column) throws Exception {
        final StringSink natValue = new StringSink();
        printSql("SELECT " + column + " FROM nat WHERE id = 6", natValue);
        final StringSink candidates = new StringSink();
        printSql("SELECT id, " + column + " FROM src WHERE id > 300 LIMIT 30", candidates);
        final String value = natValue.toString().split("\n", -1)[1];
        final String[] lines = candidates.toString().split("\n");
        for (int i = 1; i < lines.length; i++) {
            final int tab = lines[i].indexOf('\t');
            if (!lines[i].substring(tab + 1).equals(value)) {
                return Long.parseLong(lines[i].substring(0, tab));
            }
        }
        throw new AssertionError("no src row with another " + column + " than " + value);
    }

    private long ts(String literal) throws NumericException {
        return driver.parseFloorLiteral(literal);
    }
}
