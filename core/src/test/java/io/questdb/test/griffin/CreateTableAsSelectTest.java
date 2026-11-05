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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.RecordToRowCopierUtils;
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
    public void testCtasCastCharToGeoHashValues() throws Exception {
        // a CHAR is one base32 character: u is 11010, 7 is 00111, z is 11111; a column of fewer
        // than 5 bits keeps the high bits
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (k INT, c CHAR)");
            execute("INSERT INTO src VALUES (0, 'u'), (1, 'U'), (2, '7'), (3, 'z'), (4, NULL), (10, 'a'), (11, 'i'), (12, 'l'), (13, 'o'), (14, '!')");
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                // a, i, l and o are not base32 characters
                final char[] refused = {'a', 'i', 'l', 'o', '!'};
                for (int r = 0; r < refused.length; r++) {
                    assertExceptionNoLeakCheck(
                            "CREATE TABLE dst AS (SELECT c FROM src WHERE k = " + (10 + r) + "), CAST(c AS GEOHASH(1c))",
                            -1,
                            "inconvertible value: " + refused[r] + " [CHAR -> GEOHASH(1c)]",
                            false
                    );
                    Assert.assertNull(engine.getTableTokenIfExists("dst"));
                }
                execute("CREATE TABLE dst AS (SELECT k, c c5, c c3, c c1" + chunkingPads(copierType) + " FROM src WHERE k < 10), CAST(c5 AS GEOHASH(1c)), CAST(c3 AS GEOHASH(3b)), CAST(c1 AS GEOHASH(1b))");
                assertQuery("SELECT k, c5, c3, c1 FROM dst")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\tc5\tc3\tc1
                                0\tu\t110\t1
                                1\tu\t110\t1
                                2\t7\t001\t0
                                3\tz\t111\t1
                                4\t\t\t
                                """);
                execute("DROP TABLE dst");
            }
        });
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
    public void testCtasCastTextToGeoHashValues() throws Exception {
        // text reads as its first characters, as many as the column's bits need, narrowed to the
        // bits: u33d8b121234 starts u 11010, 3 00011, 3 00011
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (k INT, s STRING, v VARCHAR)");
            execute("""
                    INSERT INTO src VALUES
                        (0, 'u33d8b121234', 'u33d8b121234'),
                        (1, 'U33D8B121234', 'U33D8B121234'),
                        (2, NULL, NULL),
                        (10, 'u33d8b12123', 'u33d8b12123'),
                        (11, 'u33d8b12123a', 'u33d8b12123a'),
                        (12, '', ''),
                        (13, 'й33d8b121234', 'й33d8b121234')
                    """);
            // 11 characters, one short of GEOHASH(12c); a is not a base32 character; empty; not ASCII
            final String[] refused = {"u33d8b12123", "u33d8b12123a", "", "й33d8b121234"};
            // the source expressions and the type their message names
            final String[][] sources = {{"s", "STRING"}, {"v", "VARCHAR"}, {"s::VARCHAR", "VARCHAR"}};
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                for (String[] source : sources) {
                    for (int r = 0; r < refused.length; r++) {
                        assertExceptionNoLeakCheck(
                                "CREATE TABLE dst AS (SELECT " + source[0] + " x FROM src WHERE k = " + (10 + r) + "), CAST(x AS GEOHASH(12c))",
                                -1,
                                r == 3 ? "inconvertible value: `" : "inconvertible value: `" + refused[r] + "` [" + source[1] + " -> GEOHASH(12c)]",
                                false
                        );
                        Assert.assertNull(engine.getTableTokenIfExists("dst"));
                    }
                    execute(
                            "CREATE TABLE dst AS (SELECT k, x b3, x c1, x b7, x c2, x b13, x c4, x c6, x c8, x c12" + chunkingPads(copierType) + " FROM (SELECT k, "
                                    + source[0] + " x FROM src WHERE k < 10)), CAST(b3 AS GEOHASH(3b)), CAST(c1 AS GEOHASH(1c)),"
                                    + " CAST(b7 AS GEOHASH(7b)), CAST(c2 AS GEOHASH(2c)), CAST(b13 AS GEOHASH(13b)), CAST(c4 AS GEOHASH(4c)),"
                                    + " CAST(c6 AS GEOHASH(6c)), CAST(c8 AS GEOHASH(8c)), CAST(c12 AS GEOHASH(12c))"
                    );
                    assertQuery("SELECT k, b3, c1, b7, c2, b13, c4, c6, c8, c12 FROM dst")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    k\tb3\tc1\tb7\tc2\tb13\tc4\tc6\tc8\tc12
                                    0\t110\tu\t1101000\tu3\t1101000011000\tu33d\tu33d8b\tu33d8b12\tu33d8b121234
                                    1\t110\tu\t1101000\tu3\t1101000011000\tu33d\tu33d8b\tu33d8b12\tu33d8b121234
                                    2\t\t\t\t\t\t\t\t\t
                                    """);
                    execute("DROP TABLE dst");
                }
            }
        });
    }

    @Test
    public void testCtasCastTextToUuidValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (k INT, s STRING, v VARCHAR)");
            execute("""
                    INSERT INTO src VALUES
                        (0, '11111111-2222-3333-4444-555555555555', '11111111-2222-3333-4444-555555555555'),
                        (1, 'ABCDEF01-2345-6789-ABCD-EF0123456789', 'ABCDEF01-2345-6789-ABCD-EF0123456789'),
                        (2, '', ''),
                        (3, NULL, NULL),
                        (10, '11111111222233334444555555555555', '11111111222233334444555555555555'),
                        (11, '11111111-2222-3333-4444-55555555555', '11111111-2222-3333-4444-55555555555'),
                        (12, '11111111-2222-3333-4444-5555555555555', '11111111-2222-3333-4444-5555555555555'),
                        (13, '1111111g-2222-3333-4444-555555555555', '1111111g-2222-3333-4444-555555555555'),
                        (14, 'й1111111-2222-3333-4444-555555555555', 'й1111111-2222-3333-4444-555555555555')
                    """);
            // no dashes; 35 and 37 characters; g is not hex; not ASCII
            final String[] refused = {
                    "11111111222233334444555555555555",
                    "11111111-2222-3333-4444-55555555555",
                    "11111111-2222-3333-4444-5555555555555",
                    "1111111g-2222-3333-4444-555555555555",
                    "й1111111-2222-3333-4444-555555555555"
            };
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                for (String source : new String[]{"s", "v", "s::VARCHAR"}) {
                    for (int r = 0; r < refused.length; r++) {
                        // the message names STRING for VARCHAR text too
                        assertExceptionNoLeakCheck(
                                "CREATE TABLE dst AS (SELECT " + source + " x FROM src WHERE k = " + (10 + r) + "), CAST(x AS UUID)",
                                -1,
                                r == 4 ? "inconvertible value: `" : "inconvertible value: `" + refused[r] + "` [STRING -> UUID]",
                                false
                        );
                        Assert.assertNull(engine.getTableTokenIfExists("dst"));
                    }
                    execute("CREATE TABLE dst AS (SELECT k, " + source + " u" + chunkingPads(copierType) + " FROM src WHERE k < 10), CAST(u AS UUID)");
                    assertQuery("SELECT k, u FROM dst")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    k\tu
                                    0\t11111111-2222-3333-4444-555555555555
                                    1\tabcdef01-2345-6789-abcd-ef0123456789
                                    2\t
                                    3\t
                                    """);
                    execute("DROP TABLE dst");
                }
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

    // the chunked copier splits only a schema wider than one chunk; below that it hands the copy to
    // the single-method copier, so copier type 2 gets 500 extra INT columns
    private static String chunkingPads(int copierType) {
        final StringBuilder pads = new StringBuilder();
        if (copierType == RecordToRowCopierUtils.COPIER_TYPE_CHUNKED) {
            for (int p = 0; p < 500; p++) {
                pads.append(", 0 p").append(p);
            }
        }
        return pads.toString();
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
