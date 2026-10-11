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
    public void testCreateNonPartitionedTableAsSelectTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            assertQuery("create table dest as (select * from src where v % 2 = 0 order by ts desc) timestamp(ts);")
                    .fails(13, "cannot insert rows out of order to non-partitioned table.");
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

    @Test
    public void testCreateTableAsSelectOverCrossJoinedFirstTable() throws Exception {
        // CREATE TABLE AS SELECT keeps the plan that runs a cross-joined first table last at every level
        // of its SELECT, while the same SELECT run as a query streams that table and declares its
        // timestamp. A Parquet export compiles the SELECT of its temp table the way the query runs it;
        // a user's CREATE TABLE AS SELECT must not follow the export.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tal (ts TIMESTAMP, v INT, b BINARY) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tb2 (rts TIMESTAMP, v INT, c1 INT) TIMESTAMP(rts) PARTITION BY DAY");
            execute("CREATE TABLE tc2 (cts TIMESTAMP, v INT) TIMESTAMP(cts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tal VALUES
                        ('2020-01-01T00:10', 1, from_base64('3q2+7w==')),
                        ('2020-01-01T02:10', 2, from_base64('AQIDBAUG'))
                    """);
            execute("""
                    INSERT INTO tb2 VALUES
                        ('2020-01-01T00:20', 1, 10),
                        ('2020-01-01T01:20', 2, 20),
                        ('2020-01-01T03:20', 1, 30)
                    """);
            execute("""
                    INSERT INTO tc2 VALUES
                        ('2020-01-01T00:20', 5),
                        ('2020-01-01T01:20', 0),
                        ('2020-01-01T03:20', 100)
                    """);
            final String query = "SELECT l.ts lts, r.c1, l.b FROM tal l CROSS JOIN tb2 r JOIN tc2 n ON n.cts = r.rts";
            final String rowsOfFirstTableOrder = """
                    lts\tc1\tb
                    2020-01-01T00:10:00.000000Z\t10\t00000000 de ad be ef
                    2020-01-01T00:10:00.000000Z\t20\t00000000 de ad be ef
                    2020-01-01T00:10:00.000000Z\t30\t00000000 de ad be ef
                    2020-01-01T02:10:00.000000Z\t10\t00000000 01 02 03 04 05 06
                    2020-01-01T02:10:00.000000Z\t20\t00000000 01 02 03 04 05 06
                    2020-01-01T02:10:00.000000Z\t30\t00000000 01 02 03 04 05 06
                    """;

            // the query streams tal and declares its timestamp
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("lts")
                    .returns(rowsOfFirstTableOrder);

            // the SELECT of the statement streams tb2, whose timestamp the projection does not hold
            execute("CREATE TABLE tj AS (" + query + ")");
            execute("CREATE TABLE tj_day AS (" + query + ") TIMESTAMP(lts) PARTITION BY DAY");
            execute("CREATE TABLE tj_limit AS (" + query + " LIMIT 2)");
            assertQuery("SELECT table_name, designatedTimestamp FROM tables() WHERE table_name LIKE 'tj%' ORDER BY table_name")
                    .noLeakCheck()
                    .returns("""
                            table_name\tdesignatedTimestamp
                            tj\t
                            tj_day\tlts
                            tj_limit\t
                            """);
            assertQuery("SELECT * FROM tj")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            lts\tc1\tb
                            2020-01-01T00:10:00.000000Z\t10\t00000000 de ad be ef
                            2020-01-01T02:10:00.000000Z\t10\t00000000 01 02 03 04 05 06
                            2020-01-01T00:10:00.000000Z\t20\t00000000 de ad be ef
                            2020-01-01T02:10:00.000000Z\t20\t00000000 01 02 03 04 05 06
                            2020-01-01T00:10:00.000000Z\t30\t00000000 de ad be ef
                            2020-01-01T02:10:00.000000Z\t30\t00000000 01 02 03 04 05 06
                            """);
            // the partitioned table sorts the rows it receives in tb2's order; the out-of-order merge
            // decides the order of the rows of one timestamp, which differs from the query's
            assertQuery("SELECT * FROM tj_day")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("lts")
                    .returns("""
                            lts\tc1\tb
                            2020-01-01T00:10:00.000000Z\t20\t00000000 de ad be ef
                            2020-01-01T00:10:00.000000Z\t30\t00000000 de ad be ef
                            2020-01-01T00:10:00.000000Z\t10\t00000000 de ad be ef
                            2020-01-01T02:10:00.000000Z\t20\t00000000 01 02 03 04 05 06
                            2020-01-01T02:10:00.000000Z\t30\t00000000 01 02 03 04 05 06
                            2020-01-01T02:10:00.000000Z\t10\t00000000 01 02 03 04 05 06
                            """);
            // LIMIT reads the order of tb2
            assertQuery("SELECT * FROM tj_limit")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            lts\tc1\tb
                            2020-01-01T00:10:00.000000Z\t10\t00000000 de ad be ef
                            2020-01-01T02:10:00.000000Z\t10\t00000000 01 02 03 04 05 06
                            """);

            // a non-partitioned table timestamped on tal's column receives the rows in tb2's order
            assertQuery("CREATE TABLE tj_lts AS (" + query + ") TIMESTAMP(lts)")
                    .noLeakCheck()
                    .fails(13, "cannot insert rows out of order to non-partitioned table.");
            Assert.assertNull("tj_lts table should have been cleaned up", engine.getTableTokenIfExists("tj_lts"));

            // EXPLAIN shows the plan that CREATE TABLE AS SELECT runs
            assertQuery("CREATE TABLE tj_plan AS (" + query + ")")
                    .noLeakCheck()
                    .assertsPlan("""
                            Create table: tj_plan
                                SelectedRecord
                                    Cross Join
                                        Hash Join Light
                                          condition: n.cts=r.rts
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tb2
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tc2
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tal
                            """);
        });
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
