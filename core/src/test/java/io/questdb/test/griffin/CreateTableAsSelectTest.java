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
import io.questdb.cairo.ColumnType;
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
    public void testCtasCastLong256ToUuidFails() throws Exception {
        assertException(
                "CREATE TABLE dst AS (SELECT rnd_long256() l256 FROM long_sequence(3)), CAST(l256 AS UUID)",
                76,
                "unsupported cast [column=l256, from=LONG256, to=UUID]"
        );
    }

    @Test
    public void testCtasCastBetweenUuidAndLong128() throws Exception {
        // CTAS copies the 16 bytes unchanged between UUID and LONG128, as on master; a round trip
        // gives the UUID back
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (u UUID, l LONG128, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src VALUES ('11111111-2222-3333-4444-555555555555', to_long128(1, 2), 0), (NULL, NULL, 1)");
            execute("CREATE TABLE l_as_u AS (SELECT l, ts FROM src), CAST(l AS UUID)");
            execute("CREATE TABLE u_as_l AS (SELECT u, ts FROM src), CAST(u AS LONG128)");
            execute("CREATE TABLE u_back AS (SELECT u, ts FROM u_as_l), CAST(u AS UUID)");
            assertQuery("SELECT l FROM l_as_u")
                    .noLeakCheck()
                    .columnType(0, ColumnType.UUID)
                    .expectSize()
                    .returns("""
                            l
                            00000000-0000-0002-0000-000000000001
                            
                            """);
            assertQuery("SELECT u FROM u_back")
                    .noLeakCheck()
                    .columnType(0, ColumnType.UUID)
                    .expectSize()
                    .returns("""
                            u
                            11111111-2222-3333-4444-555555555555
                            
                            """);
        });
    }

    @Test
    public void testCtasCastToCharWithoutCopierArmFails() throws Exception {
        // the copiers have no arm from these types into CHAR (bug y), so the cast clause refuses
        // them at its position, before the table exists
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE src AS (
                        SELECT 1::BYTE b, 2::SHORT s, 3 i, 4L l, 5::DATE d, 6::TIMESTAMP t, 7::TIMESTAMP_NS n, 8.0f f, 9.0 x
                        FROM long_sequence(1)
                    )
                    """);
            final String[][] pairs = {
                    {"b", "BYTE"}, {"s", "SHORT"}, {"i", "INT"}, {"l", "LONG"}, {"d", "DATE"},
                    {"t", "TIMESTAMP"}, {"n", "TIMESTAMP_NS"}, {"f", "FLOAT"}, {"x", "DOUBLE"}
            };
            for (String[] pair : pairs) {
                final String sql = "CREATE TABLE dst AS (SELECT " + pair[0] + " FROM src), CAST(" + pair[0] + " AS CHAR)";
                assertExceptionNoLeakCheck(sql, sql.indexOf(pair[0] + " AS CHAR"), "unsupported cast [column=" + pair[0] + ", from=" + pair[1] + ", to=CHAR]");
                Assert.assertNull(sql, engine.getTableTokenIfExists("dst"));
            }
        });
    }

    @Test
    public void testCtasCastUuidToStringAndVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (s UUID, v UUID)");
            execute("""
                    INSERT INTO src VALUES
                        ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222'),
                        (NULL, NULL)
                    """);
            execute("CREATE TABLE dst AS (SELECT * FROM src), CAST(s AS STRING), CAST(v AS VARCHAR)");

            assertQuery("dst")
                    .noLeakCheck()
                    .columnType(0, ColumnType.STRING)
                    .columnType(1, ColumnType.VARCHAR)
                    .expectSize()
                    .returns("""
                            s\tv
                            11111111-1111-1111-1111-111111111111\t22222222-2222-2222-2222-222222222222
                            \t
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
