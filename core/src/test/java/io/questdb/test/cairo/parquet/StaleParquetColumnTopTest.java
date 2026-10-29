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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Repairs the stale {@code _cv} column top left behind by QuestDB 9.3.4 - 10.0.1 after an in-place O3 merge into a
 * parquet partition: {@code PartitionCompactionScanJob} picks such a partition, rebuilds its indexes from row 0 and
 * zeroes the stale tops.
 * <p>
 * The fixtures in {@code resources/parquet-stale-cv-top} are real WAL tables written by QuestDB 10.0.1 (docker
 * image {@code questdb/questdb:10.0.1}, started with {@code QDB_CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE=10},
 * {@code ..._O3_REWRITE_UNUSED_RATIO=0.99} and {@code ..._O3_REWRITE_UNUSED_MAX_BYTES=1000000000} so the O3 merge
 * stays in update mode). Each adds a column after day 1 has rows, converts day 1 to parquet, then O3-inserts rows
 * with values for that column into day 1. The merge puts real values into the parquet file and zeroes the
 * parquet-level column_top, but leaves the {@code _cv} top, so an index built from it misses the merged rows.
 * <p>
 * Two shapes of stale top, depending on when the column was added:
 * <ul>
 *     <li>{@code absent}: added once day 2 existed, {@code _cv} marks the column absent from day 1;</li>
 *     <li>{@code top}: added while day 1 was the last partition, {@code _cv} records a column top of 100.</li>
 * </ul>
 * Index-column fixtures ({@code bitmap_*}, {@code posting_*}, {@code covering_*}):
 * <pre>
 * create table T (ts timestamp, x long, v long) timestamp(ts) partition by day wal;
 * insert into T select dateadd('m', x::int * 10, '2024-01-01T00:00:00.000000Z'), x, x from long_sequence(100);
 * -- absent only: insert into T select dateadd('m', x::int * 10, '2024-01-02T00:00:00.000000Z'), x, x from long_sequence(10);
 * alter table T add column s symbol;
 * alter table T alter column s add index [type posting [include (v)]];
 * -- top only: insert into T select dateadd('m', x::int * 10, '2024-01-02T00:00:00.000000Z'), x, x, 'k' || (x % 3) from long_sequence(10);
 * alter table T convert partition to parquet list '2024-01-01';
 * insert into T (ts, x, v, s) values ('2024-01-01T05:05:00.000000Z', -1, 42, 'a'), ('2024-01-01T05:06:00.000000Z', -2, 43, 'b');
 * </pre>
 * Covered-column fixture {@code covered_top} (s is indexed from the start, the covered v has the stale top):
 * <pre>
 * create table covered_top (ts timestamp, x long, s symbol) timestamp(ts) partition by day wal;
 * insert into covered_top select dateadd('m', x::int * 10, '2024-01-01T00:00:00.000000Z'), x, 'k' || (x % 3) from long_sequence(100);
 * alter table covered_top add column v long;
 * alter table covered_top alter column s add index type posting include (v);
 * insert into covered_top select dateadd('m', x::int * 10, '2024-01-02T00:00:00.000000Z'), x, 'k' || (x % 3), x from long_sequence(10);
 * alter table covered_top convert partition to parquet list '2024-01-01';
 * insert into covered_top (ts, x, s, v) values ('2024-01-01T05:05:00.000000Z', -1, 'k1', 42), ('2024-01-01T05:06:00.000000Z', -2, 'k2', 43);
 * </pre>
 * 10.0.1 cannot produce {@code covering_top}: its O3 index rebuild trips an assertion and suspends the table. A
 * covered column marked absent ({@code covered_absent}) still returned the right values, so it has no fixture.
 * <p>
 * {@code stale_top~10} is {@code bitmap_absent} plus {@code dedup enable upsert keys(ts, s)}, for the dedup and
 * CONVERT TO NATIVE tests.
 */
public class StaleParquetColumnTopTest extends AbstractCairoTest {
    private static final long DAY1 = parseTimestamp("2024-01-01T00:00:00.000000Z");
    private static final String STALE_INDEX_ERROR = "index does not cover parquet partition data, partition compaction rebuilds it [table=";

    @Test
    public void testBitmapIndexColumnAbsent() throws Exception {
        assertCompactionRepairsIndex("bitmap_absent~9", "bitmap_absent", "select ts, x, v, s from bitmap_absent where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testBitmapIndexColumnTop() throws Exception {
        assertCompactionRepairsIndex("bitmap_top~10", "bitmap_top", "select ts, x, v, s from bitmap_top where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testCompactionKeepsColumnAddedAfterConversion() throws Exception {
        // The file lacks z, so the copy is fully materialized: z is written as NULLs from row 0.
        assertMemoryLeak(() -> {
            loadFixture("stale_top~10");
            execute("alter table stale_top add column z int");
            drainWalQueue();

            compactDay1("stale_top");

            assertQuery("select ts, x, v, s, z from stale_top where s = 'a' and ts in '2024-01-01'")
                    .timestamp("ts")
                    .returns("""
                            ts\tx\tv\ts\tz
                            2024-01-01T05:05:00.000000Z\t-1\t42\ta\tnull
                            """);
            assertQuery("select count() from stale_top where z != null").expectSize().noRandomAccess().returns("count\n0\n");
        });
    }

    @Test
    public void testCompactionRepairsConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("stale_top~10");

            compactDay1("stale_top");
            execute("alter table stale_top convert partition to native list '2024-01-01'");
            drainWalQueue();

            assertQuery("select ts, x, v, s from stale_top where v != null")
                    .timestamp("ts")
                    .returns("""
                            ts\tx\tv\ts
                            2024-01-01T05:05:00.000000Z\t-1\t42\ta
                            2024-01-01T05:06:00.000000Z\t-2\t43\tb
                            """);
            assertQuery("select ts, x, v, s from stale_top where s = 'a'")
                    .timestamp("ts")
                    .returns("""
                            ts\tx\tv\ts
                            2024-01-01T05:05:00.000000Z\t-1\t42\ta
                            """);
        });
    }

    @Test
    public void testCompactionRepairsDedup() throws Exception {
        // Dedup on (ts, s): the new row must replace the merged ('05:05', 'a') row, which a stale top of s would
        // have the parquet dedup merge read as NULL.
        assertMemoryLeak(() -> {
            loadFixture("stale_top~10");

            compactDay1("stale_top");
            execute("insert into stale_top (ts, x, v, s) values ('2024-01-01T05:05:00.000000Z', -10, 99, 'a')");
            drainWalQueue();

            assertQuery("select ts, x, v, s from stale_top where ts in '2024-01-01T05:05'")
                    .timestamp("ts")
                    .returns("""
                            ts\tx\tv\ts
                            2024-01-01T05:05:00.000000Z\t-10\t99\ta
                            """);
        });
    }

    @Test
    public void testCoveredColumnTop() throws Exception {
        // s is fine; the covered v reads NULL for the merged row inside v's stale top
        assertCompactionRepairsIndex("covered_top~18", "covered_top", "select ts, v from covered_top where s = 'k1'", "v", "k1", """
                ts\tv
                2024-01-01T05:05:00.000000Z\t42
                2024-01-01T05:10:00.000000Z\tnull
                2024-01-01T05:40:00.000000Z\tnull
                """, """
                ts\tv
                2024-01-02T00:10:00.000000Z\t1
                2024-01-02T00:40:00.000000Z\t4
                2024-01-02T01:10:00.000000Z\t7
                2024-01-02T01:40:00.000000Z\t10
                """);
    }

    @Test
    public void testCoveringIndexColumnAbsent() throws Exception {
        assertCompactionRepairsIndex("covering_absent~13", "covering_absent", "select ts, v from covering_absent where s = 'a'", "s", "a", "ts\tv\n2024-01-01T05:05:00.000000Z\t42\n", "ts\tv\n");
    }

    @Test
    public void testCoveringIndexColumnAbsentNonCoveredQuery() throws Exception {
        // x is not covered: the query reads the posting index, then the columns
        assertCompactionRepairsIndex("covering_absent~13", "covering_absent", "select ts, x, v, s from covering_absent where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testParquetPartitionsWithColumnTopsAreCompact() throws Exception {
        // No false positives: parquet partitions written by this version carry column tops (a column absent from
        // day 1, a part-way top on day 2) and their indexes are sound. Compaction leaves them, and their tops, alone.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by day wal");
            execute("insert into t select dateadd('h', x::int, '2024-01-01T00:00:00.000000Z'), x from long_sequence(20)");
            execute("insert into t select dateadd('h', x::int, '2024-01-02T00:00:00.000000Z'), x from long_sequence(5)");
            drainWalQueue();
            execute("alter table t add column v long");
            execute("alter table t add column b symbol index");
            execute("alter table t add column p symbol index type posting");
            execute("alter table t add column c symbol");
            execute("alter table t alter column c add index type posting include (v)");
            execute("alter table t add column bool boolean");
            execute("insert into t values ('2024-01-02T08:00:00.000000Z', 100, 7, 'a', 'a', 'a', true), ('2024-01-02T09:00:00.000000Z', 101, 8, 'b', 'b', 'b', true)");
            execute("insert into t values ('2024-01-03T00:00:00.000000Z', 102, 9, 'a', 'a', 'a', true)");
            drainWalQueue();
            execute("alter table t convert partition to parquet list '2024-01-01', '2024-01-02'");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("t");
            final long day2 = parseTimestamp("2024-01-02T00:00:00.000000Z");
            final long day2BoolTop = getColumnTop(token, day2, "bool");
            Assert.assertTrue(isParquetPartitionCompact(token, 0));
            Assert.assertTrue(isParquetPartitionCompact(token, 1));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, DAY1));
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day2));
                Assert.assertFalse("sound partitions must not be rebuilt", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertEquals(day2BoolTop, getColumnTop(token, day2, "bool"));

            final String expected = """
                    ts\tx\tv
                    2024-01-02T08:00:00.000000Z\t100\t7
                    2024-01-03T00:00:00.000000Z\t102\t9
                    """;
            assertQuery("select ts, x, v from t where b = 'a'")
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: b")
                    .returns(expected);
            assertQuery("select ts, x, v from t where p = 'a'")
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: p")
                    .returns(expected);
            assertQuery("select ts, v from t where c = 'a'")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("CoveringIndex on: c")
                    .returns("""
                            ts\tv
                            2024-01-02T08:00:00.000000Z\t7
                            2024-01-03T00:00:00.000000Z\t9
                            """);
            assertQuery("select count() from t where b is null")
                    .withPlanContaining("Index forward scan on: b")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n25\n");
            assertQuery("select count() from t where bool")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n3\n");
        });
    }

    @Test
    public void testParquetScanSeesMergedValues() throws Exception {
        // Sanity: the parquet data itself is intact; a scan does not use the index.
        assertMemoryLeak(() -> {
            loadFixture("stale_top~10");

            assertQuery("select ts, x, v, s from stale_top where ts between '2024-01-01T05:00' and '2024-01-01T05:10'")
                    .timestamp("ts")
                    .returns("""
                            ts\tx\tv\ts
                            2024-01-01T05:00:00.000000Z\t30\tnull\t
                            2024-01-01T05:05:00.000000Z\t-1\t42\ta
                            2024-01-01T05:06:00.000000Z\t-2\t43\tb
                            2024-01-01T05:10:00.000000Z\t31\tnull\t
                            """);
        });
    }

    @Test
    public void testPostingIndexColumnAbsent() throws Exception {
        assertCompactionRepairsIndex("posting_absent~11", "posting_absent", "select ts, x, v, s from posting_absent where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testPostingIndexColumnTop() throws Exception {
        assertCompactionRepairsIndex("posting_top~12", "posting_top", "select ts, x, v, s from posting_top where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testSweepRepairsStaleColumnTops() throws Exception {
        // The background sweep, not just a request, picks the stale partition although it has no dead bytes.
        assertMemoryLeak(() -> {
            loadFixture("bitmap_top~10");
            final TableToken token = engine.verifyTableName("bitmap_top");
            Assert.assertFalse(isParquetPartitionCompact(token, 0));

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            // The idle gate reads the parquet file's modification time, which is real wall-clock time.
            final Clock pastTheIdleTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), pastTheIdleTimeout)) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            Assert.assertTrue(isParquetPartitionCompact(token, 0));
            assertQuery("select ts, x, v, s from bitmap_top where s = 'a' and ts in '2024-01-01'")
                    .timestamp("ts")
                    .returns("ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n");
        });
    }

    private static void compactDay1(String table) {
        final TableToken token = engine.verifyTableName(table);
        Assert.assertFalse("stale column tops make the partition not compact", isParquetPartitionCompact(token, 0));
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, DAY1));
            Assert.assertTrue("the request must dispatch the rebuild", job.run());
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        Assert.assertTrue("the rebuilt partition must be compact", isParquetPartitionCompact(token, 0));
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
        }
    }

    private static long getColumnTop(TableToken token, long partitionTimestamp, String columnName) {
        try (TableReader reader = engine.getReader(token)) {
            final TableMetadata metadata = reader.getMetadata();
            final ColumnVersionReader cvr = reader.getColumnVersionReader();
            return cvr.getColumnTop(partitionTimestamp, metadata.getWriterIndex(metadata.getColumnIndex(columnName)));
        }
    }

    private static boolean isParquetPartitionCompact(TableToken token, int partitionIndex) {
        try (TableReader reader = engine.getReader(token)) {
            reader.openPartition(partitionIndex);
            return PartitionCompactionScanJob.isParquetPartitionCompact(
                    reader.getTxFile(),
                    partitionIndex,
                    reader.getMetadata(),
                    reader.getColumnVersionReader(),
                    reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata()
            );
        }
    }

    private static void loadFixture(String tableDirName) throws Exception {
        final byte[] buffer = new byte[1024 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = StaleParquetColumnTopTest.class.getResourceAsStream("/parquet-stale-cv-top/" + tableDirName + ".zip")) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File(root, ze.getName());
                    if (!ze.isDirectory()) {
                        final File dir = dest.getParentFile();
                        if (!dir.exists()) {
                            Assert.assertTrue(dir.mkdirs());
                        }
                        try (FileOutputStream fos = new FileOutputStream(dest)) {
                            int n;
                            while ((n = zip.read(buffer, 0, buffer.length)) > 0) {
                                fos.write(buffer, 0, n);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();
    }

    private static long parseTimestamp(String timestamp) {
        try {
            return MicrosFormatUtils.parseTimestamp(timestamp);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void assertIndexQuery(String query, boolean covering, String expected) throws Exception {
        if (covering) {
            assertQuery(query).timestamp("ts").noRandomAccess().expectSize().returns(expected);
        } else {
            assertQuery(query).timestamp("ts").returns(expected);
        }
    }

    /**
     * The index query on the broken day 1 fails, a scan still sees the merged row and the native day 2 index still
     * answers. A compaction of day 1 then repairs it: its index answers, its stale tops are zeroed and it is compact.
     */
    private void assertCompactionRepairsIndex(
            String fixture,
            String table,
            String indexQuery,
            String staleColumn,
            String mergedSymbol,
            String day1Hour5Result,
            String day2Result
    ) throws Exception {
        final boolean covering = indexQuery.startsWith("select ts, v from");
        assertMemoryLeak(() -> {
            loadFixture(fixture);

            assertQuery(indexQuery)
                    .noLeakCheck()
                    .assertsPlanContaining(covering ? "CoveringIndex on: s" : "Index forward scan on: s");
            assertExceptionNoLeakCheck(
                    indexQuery,
                    -1,
                    STALE_INDEX_ERROR + table + ", partition=2024-01-01, index=s, column=" + staleColumn + ']',
                    sqlExecutionContext
            );
            // the data is intact: a scan sees the merged row
            assertQuery("select ts, x, v, s from " + table + " where ts in '2024-01-01T05:05'")
                    .timestamp("ts")
                    .returns("ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\t" + mergedSymbol + "\n");
            // the native partition's index still answers
            assertIndexQuery(indexQuery + " and ts in '2024-01-02'", covering, day2Result);

            compactDay1(table);

            final TableToken token = engine.verifyTableName(table);
            Assert.assertEquals(0, getColumnTop(token, DAY1, "s"));
            Assert.assertEquals(0, getColumnTop(token, DAY1, "v"));
            assertIndexQuery(indexQuery + " and ts in '2024-01-01T05'", covering, day1Hour5Result);
            assertIndexQuery(indexQuery + " and ts in '2024-01-02'", covering, day2Result);
            // the whole-table index query agrees with a scan of the same filter
            printSql(indexQuery.replace(" where s = ", " where s::string = "));
            assertIndexQuery(indexQuery, covering, sink.toString());
        });
    }
}
