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

import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Reproduces the stale {@code _cv} column top left behind by QuestDB 9.3.4 - 10.0.1 after an in-place O3
 * merge into a parquet partition.
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
 * CONVERT TO NATIVE tests, which still reproduce unfixed bugs.
 */
public class StaleParquetColumnTopTest extends AbstractCairoTest {
    private static final String ROW_A = "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n";
    private static final String STALE_INDEX_ERROR = "index does not cover parquet partition data, rebuild it with REINDEX TABLE ";

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

    @Test
    public void testAddCoveringIndexRepairsStaleColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("covered_top~18");
            execute("alter table covered_top alter column s drop index");
            drainWalQueue();
            execute("alter table covered_top alter column s add index type posting include (v)");
            drainWalQueue();

            assertIndexQuery("select ts, v from covered_top where s = 'k1' and ts in '2024-01-01T05'", true, """
                    ts\tv
                    2024-01-01T05:05:00.000000Z\t42
                    2024-01-01T05:10:00.000000Z\tnull
                    2024-01-01T05:40:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testAddIndexRepairsStaleColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("bitmap_top~10");
            final File tableDir = new File(configuration.getDbRoot().toString(), "bitmap_top~10");
            execute("alter table bitmap_top alter column s drop index");
            drainWalQueue();
            execute("alter table bitmap_top alter column s add index type posting");
            drainWalQueue();

            // the stale partition got a new directory version with zeroed tops, the index covers the merged rows
            final String[] day1Dirs = tableDir.list((dir, name) -> name.startsWith("2024-01-01"));
            Assert.assertNotNull(day1Dirs);
            Assert.assertEquals(1, day1Dirs.length);
            Assert.assertNotEquals("2024-01-01.4", day1Dirs[0]);
            assertIndexQuery("select ts, x, v, s from bitmap_top where s = 'a' and ts in '2024-01-01T05'", false, ROW_A);
            assertIndexQuery("select ts, x, v, s from bitmap_top where s = 'k1' and ts in '2024-01-02T00:10'", false, """
                    ts\tx\tv\ts
                    2024-01-02T00:10:00.000000Z\t1\t1\tk1
                    """);
        });
    }

    @Test
    public void testBitmapIndexColumnAbsent() throws Exception {
        assertIndexQueryFails("bitmap_absent~9", "bitmap_absent", "select ts, x, v, s from bitmap_absent where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testBitmapIndexColumnTop() throws Exception {
        assertIndexQueryFails("bitmap_top~10", "bitmap_top", "select ts, x, v, s from bitmap_top where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testCoveredColumnTop() throws Exception {
        // s is fine; the covered v reads NULL for the merged row below v's stale top
        assertIndexQueryFails("covered_top~18", "covered_top", "select ts, v from covered_top where s = 'k1'", "v", "k1", """
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
        assertIndexQueryFails("covering_absent~13", "covering_absent", "select ts, v from covering_absent where s = 'a'", "s", "a", "ts\tv\n2024-01-01T05:05:00.000000Z\t42\n", "ts\tv\n");
    }

    @Test
    public void testCoveringIndexColumnAbsentNonCoveredQuery() throws Exception {
        // x is not covered: the query reads the posting index, then the columns
        assertIndexQueryFails("covering_absent~13", "covering_absent", "select ts, x, v, s from covering_absent where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testDedupOnParquetPartitionWithAbsentKeyColumn() throws Exception {
        // No regression: day 1 predates the key column s, which reads NULL there. A NULL-key late row upserts the
        // existing row, a non-NULL one does not match it.
        assertMemoryLeak(() -> {
            execute("create table d (ts timestamp, x long) timestamp(ts) partition by day wal");
            execute("insert into d select dateadd('m', x::int * 10, '2024-01-01T00:00:00.000000Z'), x from long_sequence(100)");
            execute("insert into d select dateadd('m', x::int * 10, '2024-01-02T00:00:00.000000Z'), x from long_sequence(10)");
            execute("alter table d add column s symbol");
            execute("alter table d dedup enable upsert keys(ts, s)");
            execute("alter table d convert partition to parquet list '2024-01-01'");
            drainWalQueue();

            execute("insert into d (ts, x, s) values ('2024-01-01T05:00:00.000000Z', 999, null), ('2024-01-01T05:10:00.000000Z', 888, 'q')");
            drainWalQueue();

            assertQuery("select ts, x, s from d where ts between '2024-01-01T05:00' and '2024-01-01T05:10'")
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ts
                            2024-01-01T05:00:00.000000Z\t999\t
                            2024-01-01T05:10:00.000000Z\t31\t
                            2024-01-01T05:10:00.000000Z\t888\tq
                            """);
        });
    }

    @Test
    public void testDedupMatchesMergedKey() throws Exception {
        // Dedup on (ts, s): the new row must replace the merged ('05:05', 'a') row. The parquet dedup merge
        // takes s's column top from the stale _cv, treats the existing 'a' as NULL and keeps both rows.
        assertMemoryLeak(() -> {
            loadFixture("stale_top~10");

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
    public void testIndexQueriesOnParquetPartitionsWithColumnTops() throws Exception {
        // No false positives: parquet partitions written by this version carry column tops (a column absent from
        // day 1, a mid-partition top on day 2) and their indexes are sound.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by day bypass wal");
            execute("insert into t select dateadd('h', x::int, '2024-01-01T00:00:00.000000Z'), x from long_sequence(20)");
            execute("insert into t select dateadd('h', x::int, '2024-01-02T00:00:00.000000Z'), x from long_sequence(5)");
            execute("alter table t add column v long");
            execute("alter table t add column b symbol index");
            execute("alter table t add column p symbol index type posting");
            execute("alter table t add column c symbol");
            execute("alter table t alter column c add index type posting include (v)");
            execute("insert into t values ('2024-01-02T08:00:00.000000Z', 100, 7, 'a', 'a', 'a'), ('2024-01-02T09:00:00.000000Z', 101, 8, 'b', 'b', 'b')");
            execute("insert into t values ('2024-01-03T00:00:00.000000Z', 102, 9, 'a', 'a', 'a')");
            execute("alter table t convert partition to parquet list '2024-01-01', '2024-01-02'");

            for (int pass = 0; pass < 2; pass++) {
                if (pass == 1) {
                    // a reindex of sound partitions changes no result
                    execute("reindex table t");
                }
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
            }

            // reindex keeps the tops of sound partitions: v and b are still absent from day 1
            try (TableReader reader = getReader("t")) {
                final long day1 = reader.getPartitionMinTimestampFromMetadata(0);
                Assert.assertEquals(-1, reader.getColumnVersionReader().getColumnTop(day1, reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("v"))));
                Assert.assertEquals(-1, reader.getColumnVersionReader().getColumnTop(day1, reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("b"))));
            }

            assertExceptionNoLeakCheck(
                    "reindex table t partition '2024-01-03'",
                    -1,
                    "partition is not parquet, use REINDEX ... LOCK EXCLUSIVE",
                    sqlExecutionContext
            );
            assertExceptionNoLeakCheck("reindex table t column b", "reindex table t column b".length(), "LOCK EXCLUSIVE expected", sqlExecutionContext);
        });
    }

    @Test
    public void testLateRowRewritesBitmapIndexColumnAbsent() throws Exception {
        assertLateRowRewritesPartition("bitmap_absent~9", "bitmap_absent", "select ts, x, v, s from bitmap_absent where s = 'a'", ROW_A);
    }

    @Test
    public void testLateRowRewritesBitmapIndexColumnTop() throws Exception {
        assertLateRowRewritesPartition("bitmap_top~10", "bitmap_top", "select ts, x, v, s from bitmap_top where s = 'a'", ROW_A);
    }

    @Test
    public void testLateRowRewritesCoveredColumnTop() throws Exception {
        assertLateRowRewritesPartition("covered_top~18", "covered_top", "select ts, v from covered_top where s = 'k1'", """
                ts\tv
                2024-01-01T05:05:00.000000Z\t42
                2024-01-01T05:10:00.000000Z\tnull
                2024-01-01T05:40:00.000000Z\tnull
                """);
    }

    @Test
    public void testLateRowRewritesCoveringIndexColumnAbsent() throws Exception {
        assertLateRowRewritesPartition("covering_absent~13", "covering_absent", "select ts, v from covering_absent where s = 'a'", "ts\tv\n2024-01-01T05:05:00.000000Z\t42\n");
    }

    @Test
    public void testLateRowRewritesPostingIndexColumnAbsent() throws Exception {
        assertLateRowRewritesPartition("posting_absent~11", "posting_absent", "select ts, x, v, s from posting_absent where s = 'a'", ROW_A);
    }

    @Test
    public void testLateRowRewritesPostingIndexColumnTop() throws Exception {
        assertLateRowRewritesPartition("posting_top~12", "posting_top", "select ts, x, v, s from posting_top where s = 'a'", ROW_A);
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
    public void testReindexKeepsPartitionVersionOfOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("bitmap_top~10");
            final File tableDir = new File(configuration.getDbRoot().toString(), "bitmap_top~10");
            final File oldDir = new File(tableDir, "2024-01-01.4");

            try (RecordCursorFactory factory = select("select ts, x, v, s from bitmap_top where ts in '2024-01-01T05:00;10m'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    execute("reindex table bitmap_top");
                    // the open cursor's reader still holds the replaced version
                    Assert.assertTrue(oldDir.exists());
                    println(factory, cursor);
                    TestUtils.assertEquals("""
                            ts\tx\tv\ts
                            2024-01-01T05:00:00.000000Z\t30\t30\t
                            2024-01-01T05:05:00.000000Z\t-1\t42\ta
                            2024-01-01T05:06:00.000000Z\t-2\t43\tb
                            """, sink);
                }
            }

            engine.releaseAllReaders();
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                //noinspection StatementWithEmptyBody
                while (purgeJob.run()) {
                }
            }
            Assert.assertFalse(oldDir.exists());
            assertQuery("select ts, x, v, s from bitmap_top where s = 'a' and ts in '2024-01-01T05'")
                    .timestamp("ts")
                    .returns("ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n");
        });
    }

    @Test
    public void testReindexRepairsConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("stale_top~10");

            execute("reindex table stale_top");
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
    public void testReindexRepairsDedup() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("stale_top~10");

            execute("reindex table stale_top");
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
    public void testPostingIndexColumnAbsent() throws Exception {
        assertIndexQueryFails("posting_absent~11", "posting_absent", "select ts, x, v, s from posting_absent where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    @Test
    public void testPostingIndexColumnTop() throws Exception {
        assertIndexQueryFails("posting_top~12", "posting_top", "select ts, x, v, s from posting_top where s = 'a'", "s", "a", "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n", "ts\tx\tv\ts\n");
    }

    private void assertIndexQuery(String query, boolean covering, String expected) throws Exception {
        if (covering) {
            assertQuery(query).timestamp("ts").noRandomAccess().expectSize().returns(expected);
        } else {
            assertQuery(query).timestamp("ts").returns(expected);
        }
    }

    /**
     * A late row into the broken day 1 takes the rewrite path: the commit zeroes the column tops and the indexes are
     * rebuilt from row 0 in a new partition directory, so the index answers again.
     */
    private void assertLateRowRewritesPartition(String fixture, String table, String indexQuery, String day1Hour5Result) throws Exception {
        final boolean covering = indexQuery.startsWith("select ts, v from");
        assertMemoryLeak(() -> {
            loadFixture(fixture);
            final File tableDir = new File(configuration.getDbRoot().toString(), fixture);
            final String[] before = tableDir.list((dir, name) -> name.startsWith("2024-01-01"));
            Assert.assertNotNull(before);
            Assert.assertEquals(1, before.length);

            execute("insert into " + table + " (ts, x, s) values ('2024-01-01T07:07:00.000000Z', -7, 'z')");
            drainWalQueue();

            final String[] after = tableDir.list((dir, name) -> name.startsWith("2024-01-01"));
            Assert.assertNotNull(after);
            Assert.assertEquals(1, after.length);
            Assert.assertNotEquals(before[0], after[0]);
            assertIndexQuery(indexQuery + " and ts in '2024-01-01T05'", covering, day1Hour5Result);
            assertQuery("select count() from " + table + " where s = 'z'")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    /**
     * The index query on the broken day 1 fails, a scan still sees the merged row and the native day 2 index still
     * answers. REINDEX then repairs day 1.
     */
    private void assertIndexQueryFails(
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
                    STALE_INDEX_ERROR + table + " PARTITION '2024-01-01' [table=" + table
                            + ", partition=2024-01-01, index=s, column=" + staleColumn + ']',
                    sqlExecutionContext
            );
            // the data is intact: a scan sees the merged row
            assertQuery("select ts, x, v, s from " + table + " where ts in '2024-01-01T05:05'")
                    .timestamp("ts")
                    .returns("ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\t" + mergedSymbol + "\n");
            // the native partition's index still answers
            assertIndexQuery(indexQuery + " and ts in '2024-01-02'", covering, day2Result);

            execute("reindex table " + table);

            assertIndexQuery(indexQuery + " and ts in '2024-01-01T05'", covering, day1Hour5Result);
            assertIndexQuery(indexQuery + " and ts in '2024-01-02'", covering, day2Result);
            // a second run finds nothing to repair and rebuilds the same indexes
            execute("reindex table " + table + " partition '2024-01-01'");
            assertIndexQuery(indexQuery + " and ts in '2024-01-01T05'", covering, day1Hour5Result);
            // each run writes a new partition directory version and removes the replaced one
            final File tableDir = new File(configuration.getDbRoot().toString(), fixture);
            final String[] day1Dirs = tableDir.list((dir, name) -> name.startsWith("2024-01-01"));
            Assert.assertNotNull(day1Dirs);
            Assert.assertEquals(1, day1Dirs.length);
            Assert.assertNotEquals("2024-01-01.4", day1Dirs[0]);
        });
    }
}
