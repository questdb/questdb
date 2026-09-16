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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoError;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class CreateTableAsSelectTest extends AbstractCairoTest {
    private static final String UNINHERITABLE_TIMESTAMP_ERROR =
            "cannot inherit the designated timestamp of an unordered SELECT into a non-partitioned table " +
                    "[timestamp=ts]; add PARTITION BY so the writer sorts the rows, or ORDER BY ts to order the SELECT";

    @Test
    public void testCreateAsSelectAndLikeIsInvalid() throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            assertQuery("create table dest as (select * from src) like src")
                    .fails(41, "unexpected token [like]");
        });
    }

    @Test
    public void testCreateAsSelectDoesNotPropagateParquetConfig() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (ts TIMESTAMP, v LONG PARQUET(delta_binary_packed, zstd(3))) TIMESTAMP(ts) PARTITION BY DAY;");
            execute("INSERT INTO src VALUES('2024-01-01', 42);");
            execute("CREATE TABLE dest AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY;");

            // CTAS derives columns from SELECT metadata, which does not carry
            // per-column parquet encoding config from the source table.
            assertQuery("SHOW CREATE TABLE dest")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'dest' (\s
                            \tts TIMESTAMP,
                            \tv LONG
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testCreateAsSelectNonCairoExceptionCleansUpTable() throws Exception {
        final LongHashSet destTableColumnFds = new LongHashSet();
        final AtomicBoolean failed = new AtomicBoolean(false);

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                destTableColumnFds.remove(fd);
                return super.close(fd);
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (destTableColumnFds.contains(fd) && failed.compareAndSet(false, true)) {
                    throw new CairoError("simulated mmap error");
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (Utf8s.containsAscii(name, File.separator + "dest") && Utf8s.endsWithAscii(name, ".d")) {
                    destTableColumnFds.add(fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            createSrcTable();
            try {
                execute("create table dest as (select * from src)");
            } catch (CairoError e) {
                Assert.assertTrue(e.getMessage().contains("simulated mmap error"));
            }

            Assert.assertNull("dest table should have been cleaned up", engine.getTableTokenIfExists("dest"));
        });
    }

    @Test
    public void testCreateAsSelectParquetConfig() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, v long PARQUET(DELTA_BINARY_PACKED, zstd(3))) timestamp(ts) partition by day;");
            execute("create table dest (like src)");

            assertQuery("SHOW CREATE TABLE dest")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'dest' (\s
                            \tts TIMESTAMP,
                            \tv LONG PARQUET(delta_binary_packed, zstd(3))
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """);
        });
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectOverKeyedGroupByFails() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // The same defect through a second, unrelated shape: a keyed GROUP BY returns its rows
            // in hash-table order, so it declares SCAN_DIRECTION_OTHER just as the union does. The
            // GROUP BY itself drops the designated timestamp, so timestamp(ts) puts one back - that
            // is the select that carries a timestamp it cannot hand over. No union and no index is
            // involved, which is what makes this the branch's behaviour rather than a feature's.
            assertQuery("create table dest as (select * from (select ts, count() c from pa group by ts) timestamp(ts));")
                    .fails(22, UNINHERITABLE_TIMESTAMP_ERROR);

            Assert.assertNull("dest must not exist after the error", engine.getTableTokenIfExists("dest"));
        });
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectOverUnionAllFails() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // No PARTITION BY, over a select that declares SCAN_DIRECTION_OTHER while carrying a
            // designated timestamp. The target runs ROW_ACTION_NO_PARTITION and cannot take that
            // timestamp. It used to be dropped without a word, handing back a table that was not a
            // time-series table at all; now the statement says so, and names both ways out.
            assertQuery("create table dest as ((pa union all pb) timestamp(ts));")
                    .fails(22, UNINHERITABLE_TIMESTAMP_ERROR);

            Assert.assertNull("dest must not exist after the error", engine.getTableTokenIfExists("dest"));
        });
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectOverUnionAllOrderedSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // The second remedy the error names: ORDER BY ts makes the select scan forward, so a
            // non-partitioned target can take the timestamp after all.
            execute("create table dest as (((pa union all pb) timestamp(ts)) order by ts);");

            assertQuery("select ts, v from dest limit 24,26")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2024-01-02T00:00:00.000000Z\t301
                            2024-01-02T01:00:00.000000Z\t302
                            """);
            assertSqlCursors("select ts, v from ((pa union all pb) timestamp(ts)) order by ts", "select ts, v from dest");
        });
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectOverUnionAllWithoutTimestampSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // The same SCAN_DIRECTION_OTHER select, but with no designated timestamp to inherit -
            // the union is not wrapped in timestamp(ts). Nothing is being thrown away, so there is
            // nothing to report, and this must keep working exactly as it did.
            execute("create table dest as (pa union all pb);");

            assertQuery("select count() c, min(ts) lo, max(ts) hi from dest")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c\tlo\thi
                            144\t2024-01-01T00:00:00.000000Z\t2024-01-06T23:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectOverUnionAllWithTimestampClauseFails() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // An explicit TIMESTAMP(ts) clause hands the designated timestamp to a non-partitioned
            // target regardless of the select's scan direction, and the writer is the one that says
            // no. This protection must survive the partitioned-target widening.
            assertQuery("create table dest as ((pa union all pb) timestamp(ts)) timestamp(ts);")
                    .fails(13, "cannot insert rows out of order to non-partitioned table.");
        });
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            assertQuery("create table dest as (select * from src where v % 2 = 0 order by ts desc) timestamp(ts);")
                    .fails(13, "cannot insert rows out of order to non-partitioned table.");
        });
    }

    @Test
    public void testCreatePartitionedTableAsSelectOverUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // The union concatenates its branches and restarts the timestamp at the branch
            // boundary, so it declares an INDETERMINATE scan direction. A partitioned target
            // O3-sorts on insert, so it takes the declared timestamp anyway.
            assertQuery("((pa union all pb) timestamp(ts)) limit 3")
                    .timestampUnordered("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            """);

            execute("create table dest as ((pa union all pb) timestamp(ts)) partition by day;");

            assertUnionDataLandedInDayPartitions();
        });
    }

    @Test
    public void testCreatePartitionedTableAsSelectOverUnionAllBatched() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // Same, but committing in batches smaller than a partition, so the O3 path is
            // re-entered on every batch rather than once at the end.
            execute("create batch 17 o3MaxLag 1000ms table dest as ((pa union all pb) timestamp(ts)) partition by day;");

            assertUnionDataLandedInDayPartitions();
        });
    }

    @Test
    public void testCreatePartitionedTableAsSelectOverUnionAllWal() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // The other partitioned writer: rows go into WAL segments in the order the union
            // produced them and are O3-sorted when the segments are applied.
            execute("create table dest as ((pa union all pb) timestamp(ts)) partition by day wal;");
            drainWalQueue();

            assertUnionDataLandedInDayPartitions();
        });
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampAscOrder() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts asc");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampAscOrderBatched() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts asc", 54, "");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampAscOrderBatchedAndLagged() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts asc", 26, "1000ms");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrder() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts desc");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrderBatched() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts desc", 54, "");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrderBatchedAndLagged() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts desc", 26, "1000ms");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrderWithoutTimestampClause() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSrcTables();

            // A backward scan is out of ascending order too, and a partitioned target absorbs it
            // just the same. Without the TIMESTAMP(ts) clause this used to fail the PARTITION BY
            // check because the timestamp was dropped.
            execute("create table dest as (pa order by ts desc) partition by day;");

            assertQuery("select ts, v from dest limit 3")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            """);
            assertQuery("select ts, count() from dest sample by 1d")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tcount
                            2024-01-01T00:00:00.000000Z\t24
                            2024-01-03T00:00:00.000000Z\t24
                            2024-01-05T00:00:00.000000Z\t24
                            """);
            assertSqlCursors("select ts, v from pa order by ts", "select ts, v from dest");
        });
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampNoOrder() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampNoOrderBatched() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("", 54, "");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampNoOrderBatchedAndLagged() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("", 26, "1000ms");
    }

    @Test
    public void testCreatePartitionedTableAtomicAsSelectTimestampAscOrder() throws Exception {
        createPartitionedTableAtomicAsSelectWithOrderBy("order by ts asc");
    }

    @Test
    public void testCreatePartitionedTableAtomicAsSelectTimestampDescOrder() throws Exception {
        createPartitionedTableAtomicAsSelectWithOrderBy("order by ts desc");
    }

    @Test
    public void testCreatePartitionedTableAtomicAsSelectTimestampNoOrder() throws Exception {
        createPartitionedTableAtomicAsSelectWithOrderBy("");
    }

    private void assertUnionDataLandedInDayPartitions() throws Exception {
        // every row is there, in ascending timestamp order, with each day whole
        assertQuery("select ts, count() from dest sample by 1d")
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tcount
                        2024-01-01T00:00:00.000000Z\t24
                        2024-01-02T00:00:00.000000Z\t24
                        2024-01-03T00:00:00.000000Z\t24
                        2024-01-04T00:00:00.000000Z\t24
                        2024-01-05T00:00:00.000000Z\t24
                        2024-01-06T00:00:00.000000Z\t24
                        """);
        // one partition per day, each holding exactly its own day and nothing else
        assertQuery("select name, minTimestamp, maxTimestamp, numRows from table_partitions('dest') order by name")
                .expectSize()
                .returns("""
                        name\tminTimestamp\tmaxTimestamp\tnumRows
                        2024-01-01\t2024-01-01T00:00:00.000000Z\t2024-01-01T23:00:00.000000Z\t24
                        2024-01-02\t2024-01-02T00:00:00.000000Z\t2024-01-02T23:00:00.000000Z\t24
                        2024-01-03\t2024-01-03T00:00:00.000000Z\t2024-01-03T23:00:00.000000Z\t24
                        2024-01-04\t2024-01-04T00:00:00.000000Z\t2024-01-04T23:00:00.000000Z\t24
                        2024-01-05\t2024-01-05T00:00:00.000000Z\t2024-01-05T23:00:00.000000Z\t24
                        2024-01-06\t2024-01-06T00:00:00.000000Z\t2024-01-06T23:00:00.000000Z\t24
                        """);
        // and row for row it is the union, sorted
        assertSqlCursors("select ts, v from ((pa union all pb) timestamp(ts)) order by ts", "select ts, v from dest");
    }

    /**
     * Two partitioned tables holding alternating days: pa has 2024-01-01, -03 and -05, pb has -02,
     * -04 and -06, 24 hourly rows each. {@code pa UNION ALL pb} therefore restarts the timestamp at
     * the branch boundary - the concatenation is genuinely out of ascending order, which is what
     * makes the union declare SCAN_DIRECTION_OTHER.
     */
    private void createInterleavedSrcTables() throws SqlException {
        execute("create table pa (ts timestamp, v long) timestamp(ts) partition by day;");
        execute("create table pb (ts timestamp, v long) timestamp(ts) partition by day;");
        execute("insert into pa select timestamp_sequence('2024-01-01T00:00:00.000000Z', 3600000000L) ts, x v from long_sequence(24);");
        execute("insert into pa select timestamp_sequence('2024-01-03T00:00:00.000000Z', 3600000000L) ts, 100 + x v from long_sequence(24);");
        execute("insert into pa select timestamp_sequence('2024-01-05T00:00:00.000000Z', 3600000000L) ts, 200 + x v from long_sequence(24);");
        execute("insert into pb select timestamp_sequence('2024-01-02T00:00:00.000000Z', 3600000000L) ts, 300 + x v from long_sequence(24);");
        execute("insert into pb select timestamp_sequence('2024-01-04T00:00:00.000000Z', 3600000000L) ts, 400 + x v from long_sequence(24);");
        execute("insert into pb select timestamp_sequence('2024-01-06T00:00:00.000000Z', 3600000000L) ts, 500 + x v from long_sequence(24);");
    }

    private void createPartitionedTableAsSelectWithOrderBy(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            execute("create table dest as (select * from src where v % 2 = 0 " + orderByClause + ") timestamp(ts) partition by day;");

            String expected = """
                    ts\tv
                    1970-01-01T00:00:00.000000Z\t0
                    1970-01-01T00:00:00.020000Z\t2
                    1970-01-01T00:00:00.040000Z\t4
                    """;

            assertQuery("dest")
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        });
    }

    private void createPartitionedTableAsSelectWithOrderBy(String orderByClause, int batchSize, String o3MaxLag) throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            String sql = "create ";

            if (batchSize != -1) {
                sql += "batch " + batchSize;
            }

            if (!o3MaxLag.isEmpty()) {
                sql += " o3MaxLag " + o3MaxLag;
            }

            sql += " table dest as ";

            sql += "(select * from src where v % 2 = 0 " + orderByClause + ") timestamp(ts) partition by day;";
            execute(sql);

            String expected = """
                    ts\tv
                    1970-01-01T00:00:00.000000Z\t0
                    1970-01-01T00:00:00.020000Z\t2
                    1970-01-01T00:00:00.040000Z\t4
                    """;

            assertQuery("dest")
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        });
    }

    private void createPartitionedTableAtomicAsSelectWithOrderBy(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            String sql = "create atomic table dest as ";


            sql += "(select * from src where v % 2 = 0 " + orderByClause + ") timestamp(ts) partition by day;";
            execute(sql);

            String expected = """
                    ts\tv
                    1970-01-01T00:00:00.000000Z\t0
                    1970-01-01T00:00:00.020000Z\t2
                    1970-01-01T00:00:00.040000Z\t4
                    """;

            assertQuery("dest")
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        });
    }

    private void createSrcTable() throws SqlException {
        execute("create table src (ts timestamp, v long) timestamp(ts) partition by day;");
        execute("insert into src values (0, 0);");
        execute("insert into src values (10000, 1);");
        execute("insert into src values (20000, 2);");
        execute("insert into src values (30000, 3);");
        execute("insert into src values (40000, 4);");
    }
}
