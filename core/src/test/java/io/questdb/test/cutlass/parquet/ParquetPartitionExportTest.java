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

package io.questdb.test.cutlass.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.HybridColumnMaterializer;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static io.questdb.cutlass.parquet.ParquetExportMode.DIRECT_PAGE_FRAME;
import static io.questdb.cutlass.parquet.ParquetExportMode.PAGE_FRAME_BACKED;

/**
 * Parquet export (COPY TO) of tables with Parquet partitions. A Parquet partition keeps the
 * columns its table had when it was converted, in their order then, so an export reads each
 * exported column from the file column that holds it: by column, not by position. The zero-copy
 * export modes read Parquet frames themselves, DIRECT_PAGE_FRAME through the encoder and
 * PAGE_FRAME_BACKED through the hybrid materializer, so the tests pin the mode of each query.
 * Expected rows come from the data the tests insert, or from the same query run by the query
 * engine, which reads Parquet partitions through its own code.
 */
public class ParquetPartitionExportTest extends AbstractCairoTest {
    // Every column type a Parquet partition stores, each with NULLs where the type has them.
    private static final String ALL_TYPES_COLUMNS = """
            x::INT id,
            CASE WHEN x % 7 = 0 THEN NULL ELSE x % 2 = 0 END a_boolean,
            (x % 120)::BYTE a_byte,
            (x * 31)::SHORT a_short,
            CASE WHEN x % 7 = 1 THEN NULL ELSE rnd_char() END a_char,
            CASE WHEN x % 7 = 2 THEN NULL ELSE (x * 1_000_003)::INT END an_int,
            CASE WHEN x % 7 = 3 THEN NULL ELSE x * 10_000_000_019 END a_long,
            CASE WHEN x % 7 = 4 THEN NULL ELSE (x / 4.0)::FLOAT END a_float,
            CASE WHEN x % 7 = 5 THEN NULL ELSE x / 3.0 END a_double,
            CASE WHEN x % 7 = 6 THEN NULL ELSE ('sym' || (x % 5))::SYMBOL END a_symbol,
            CASE WHEN x % 7 = 0 THEN NULL ELSE rnd_geohash(4) END a_geo_byte,
            CASE WHEN x % 7 = 1 THEN NULL ELSE rnd_geohash(8) END a_geo_short,
            CASE WHEN x % 7 = 2 THEN NULL ELSE rnd_geohash(16) END a_geo_int,
            CASE WHEN x % 7 = 3 THEN NULL ELSE rnd_geohash(32) END a_geo_long,
            CASE WHEN x % 7 = 4 THEN NULL WHEN x % 7 = 5 THEN '' ELSE 'str_' || x END::STRING a_string,
            CASE WHEN x % 7 = 5 THEN NULL WHEN x % 7 = 6 THEN '' WHEN x % 2 = 0 THEN 'v' || x ELSE 'a long varchar value that is not inlined, ' || x || ', ганьба 😀' END::VARCHAR a_varchar,
            CASE WHEN x % 7 = 6 THEN NULL ELSE rnd_ipv4() END an_ip,
            CASE WHEN x % 7 = 0 THEN NULL ELSE rnd_uuid4() END a_uuid,
            CASE WHEN x % 7 = 1 THEN NULL ELSE rnd_long256() END a_long256,
            CASE WHEN x % 7 = 2 THEN NULL ELSE (x * 86_400_000)::DATE END a_date,
            CASE WHEN x % 7 = 3 THEN NULL ELSE (x * 1_000_000)::TIMESTAMP END a_timestamp,
            CASE WHEN x % 7 = 4 THEN NULL ELSE (x * 1_000_000_001)::TIMESTAMP_NS END a_timestamp_ns,
            CASE WHEN x % 7 = 5 THEN NULL ELSE (x % 99 / 10.0)::DECIMAL(2, 1) END a_decimal8,
            CASE WHEN x % 7 = 6 THEN NULL ELSE (x % 9_999 / 100.0)::DECIMAL(4, 2) END a_decimal16,
            CASE WHEN x % 7 = 0 THEN NULL ELSE (x * 7 / 1000.0)::DECIMAL(9, 3) END a_decimal32,
            CASE WHEN x % 7 = 1 THEN NULL ELSE (x * 13 / 10000.0)::DECIMAL(18, 4) END a_decimal64,
            CASE WHEN x % 7 = 2 THEN NULL ELSE (x * 17 / 100000.0)::DECIMAL(38, 5) END a_decimal128,
            CASE WHEN x % 7 = 3 THEN NULL ELSE (x * 19 / 10000000000.0)::DECIMAL(76, 10) END a_decimal256,
            CASE WHEN x % 7 = 4 THEN NULL ELSE ARRAY[x / 2.0, x / 4.0] END an_array,
            CASE WHEN x % 7 = 5 THEN NULL ELSE rnd_bin(1, 16, 0) END a_bin,
            timestamp_sequence('2024-01-01', 3_600_000_000) ts""";
    private static final String[] ALL_TYPES_COLUMN_NAMES = {
            "id", "a_boolean", "a_byte", "a_short", "a_char", "an_int", "a_long", "a_float", "a_double",
            "a_symbol", "a_geo_byte", "a_geo_short", "a_geo_int", "a_geo_long", "a_string", "a_varchar",
            "an_ip", "a_uuid", "a_long256", "a_date", "a_timestamp", "a_timestamp_ns", "a_decimal8",
            "a_decimal16", "a_decimal32", "a_decimal64", "a_decimal128", "a_decimal256", "an_array", "a_bin", "ts"
    };

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        inputRoot = exportRoot;
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
    }

    @Test
    public void testAddColumnAfterConversion() throws Exception {
        // Columns added after the conversion are missing from the Parquet file, so the
        // Parquet partition reads them as NULL.
        assertMemoryLeak(() -> {
            createSmallTable("ta", true);
            convertFirstDay("ta");
            execute("ALTER TABLE ta ADD COLUMN d INT");
            execute("ALTER TABLE ta ADD COLUMN s SYMBOL");
            execute("ALTER TABLE ta ADD COLUMN v VARCHAR");
            drainWalQueue();
            execute("INSERT INTO ta VALUES (7, 7_000, '2024-01-02T20:00:00.000000Z', 70, 's7', 'v7')");
            drainWalQueue();

            final String expected = """
                    d\ts\tv\ta\tts
                    null\t\t\t1\t2024-01-01T00:00:00.000000Z
                    null\t\t\t2\t2024-01-01T08:00:00.000000Z
                    null\t\t\t3\t2024-01-01T16:00:00.000000Z
                    null\t\t\t4\t2024-01-02T00:00:00.000000Z
                    null\t\t\t5\t2024-01-02T08:00:00.000000Z
                    null\t\t\t6\t2024-01-02T16:00:00.000000Z
                    70\ts7\tv7\t7\t2024-01-02T20:00:00.000000Z
                    """;
            copyToParquet("SELECT d, s, v, a, ts FROM ta", DIRECT_PAGE_FRAME, "add_direct");
            assertParquetFile("add_direct", "ts", expected);
            assertExportMatchesQuery("SELECT * FROM ta", DIRECT_PAGE_FRAME, "add_all");

            // Only columns the Parquet file lacks: its frames decode no column.
            copyToParquet("SELECT d, s FROM ta", DIRECT_PAGE_FRAME, "add_only_missing");
            assertParquetFile("add_only_missing", null, """
                    d\ts
                    null\t
                    null\t
                    null\t
                    null\t
                    null\t
                    null\t
                    70\ts7
                    """);

            copyToParquet("SELECT d + 1 d1, s, v, a * 10 a10, ts FROM ta", PAGE_FRAME_BACKED, "add_computed");
            assertParquetFile("add_computed", "ts", """
                    d1\ts\tv\ta10\tts
                    null\t\t\t10\t2024-01-01T00:00:00.000000Z
                    null\t\t\t20\t2024-01-01T08:00:00.000000Z
                    null\t\t\t30\t2024-01-01T16:00:00.000000Z
                    null\t\t\t40\t2024-01-02T00:00:00.000000Z
                    null\t\t\t50\t2024-01-02T08:00:00.000000Z
                    null\t\t\t60\t2024-01-02T16:00:00.000000Z
                    71\ts7\tv7\t70\t2024-01-02T20:00:00.000000Z
                    """);
            copyToParquet("SELECT concat(v, s) vs FROM ta", PAGE_FRAME_BACKED, "add_computed_only_missing");
            assertParquetFile("add_computed_only_missing", null, """
                    vs
                    
                    
                    
                    
                    
                    
                    v7s7
                    """);
        });
    }

    @Test
    public void testAddColumnBetweenConversionsNonWal() throws Exception {
        assertMemoryLeak(() -> assertAddColumnBetweenConversions(engine, sqlExecutionContext, "add_between_nonwal", false));
    }

    @Test
    public void testAddColumnBetweenConversionsUnmappedFile() throws Exception {
        final CairoConfiguration configuration = newUnmappedFileConfiguration();
        assertMemoryLeak(() -> TestUtils.execute(
                null,
                (engine, compiler, sqlExecutionContext) -> assertAddColumnBetweenConversions(engine, sqlExecutionContext, "add_between_unmapped", false),
                configuration,
                LOG
        ));
    }

    @Test
    public void testAddColumnBetweenConversionsWal() throws Exception {
        assertMemoryLeak(() -> assertAddColumnBetweenConversions(engine, sqlExecutionContext, "add_between_wal", true));
    }

    @Test
    public void testAllColumnTypesNonWal() throws Exception {
        assertMemoryLeak(() -> assertAllColumnTypes(false));
    }

    @Test
    public void testAllColumnTypesWal() throws Exception {
        assertMemoryLeak(() -> assertAllColumnTypes(true));
    }

    @Test
    public void testColumnMappedToAnotherColumnFailsExport() throws Exception {
        // The encoder must never receive a buffer laid out for another type, so the export checks
        // the type of every file column it reads.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tv (a INT, v VARCHAR, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tv SELECT x::INT, 'v' || x, 's' || x, timestamp_sequence('2024-01-01', 28_800_000_000) FROM long_sequence(6)");
            convertFirstDay("tv");
            // The writer indexes of a, v and s are 0, 1 and 2.
            assertExportDirectlyFails("SELECT v, ts FROM tv", 0, "parquet export cannot convert column type [column=v, fileType=INT, columnType=VARCHAR]");
            // Only read_parquet() reads a SYMBOL file column as another type, VARCHAR.
            assertExportDirectlyFails("SELECT a, ts FROM tv", 2, "parquet export cannot convert column type [column=a, fileType=SYMBOL, columnType=INT]");
        });
    }

    @Test
    public void testColumnSubsetsAndOrders() throws Exception {
        assertMemoryLeak(() -> {
            createSmallTable("tn", true);
            convertFirstDay("tn");

            copyToParquet("SELECT a, ts FROM tn", DIRECT_PAGE_FRAME, "subset");
            assertParquetFile("subset", "ts", """
                    a\tts
                    1\t2024-01-01T00:00:00.000000Z
                    2\t2024-01-01T08:00:00.000000Z
                    3\t2024-01-01T16:00:00.000000Z
                    4\t2024-01-02T00:00:00.000000Z
                    5\t2024-01-02T08:00:00.000000Z
                    6\t2024-01-02T16:00:00.000000Z
                    """);
            copyToParquet("SELECT ts, c, a FROM tn", DIRECT_PAGE_FRAME, "reordered");
            assertParquetFile("reordered", "ts", """
                    ts\tc\ta
                    2024-01-01T00:00:00.000000Z\t1000\t1
                    2024-01-01T08:00:00.000000Z\t2000\t2
                    2024-01-01T16:00:00.000000Z\t3000\t3
                    2024-01-02T00:00:00.000000Z\t4000\t4
                    2024-01-02T08:00:00.000000Z\t5000\t5
                    2024-01-02T16:00:00.000000Z\t6000\t6
                    """);
            // The same column twice: the file column is decoded for each.
            copyToParquet("SELECT c, a, c c2 FROM tn", DIRECT_PAGE_FRAME, "repeated");
            assertParquetFile("repeated", null, """
                    c\ta\tc2
                    1000\t1\t1000
                    2000\t2\t2000
                    3000\t3\t3000
                    4000\t4\t4000
                    5000\t5\t5000
                    6000\t6\t6000
                    """);

            execute("CREATE VIEW vn_sub AS (SELECT a, ts FROM tn)");
            drainWalQueue();
            copyToParquet("SELECT * FROM vn_sub", DIRECT_PAGE_FRAME, "view_subset");
            assertParquetFile("view_subset", "ts", """
                    a\tts
                    1\t2024-01-01T00:00:00.000000Z
                    2\t2024-01-01T08:00:00.000000Z
                    3\t2024-01-01T16:00:00.000000Z
                    4\t2024-01-02T00:00:00.000000Z
                    5\t2024-01-02T08:00:00.000000Z
                    6\t2024-01-02T16:00:00.000000Z
                    """);

            copyToParquet("SELECT a + 1 a1, ts FROM tn", PAGE_FRAME_BACKED, "computed");
            assertParquetFile("computed", "ts", """
                    a1\tts
                    2\t2024-01-01T00:00:00.000000Z
                    3\t2024-01-01T08:00:00.000000Z
                    4\t2024-01-01T16:00:00.000000Z
                    5\t2024-01-02T00:00:00.000000Z
                    6\t2024-01-02T08:00:00.000000Z
                    7\t2024-01-02T16:00:00.000000Z
                    """);
            copyToParquet("SELECT ts, c - a ca, c FROM tn", PAGE_FRAME_BACKED, "computed_reordered");
            assertParquetFile("computed_reordered", "ts", """
                    ts\tca\tc
                    2024-01-01T00:00:00.000000Z\t999\t1000
                    2024-01-01T08:00:00.000000Z\t1998\t2000
                    2024-01-01T16:00:00.000000Z\t2997\t3000
                    2024-01-02T00:00:00.000000Z\t3996\t4000
                    2024-01-02T08:00:00.000000Z\t4995\t5000
                    2024-01-02T16:00:00.000000Z\t5994\t6000
                    """);
        });
    }

    @Test
    public void testComputedExportFailsAtQueryMemoryLimit() throws Exception {
        // The hybrid path charges the row groups it decodes to the export's memory tracker,
        // as a query over the same partition does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tl (s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tl SELECT rnd_varchar(200, 256, 0), timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(50_000)");
            execute("INSERT INTO tl VALUES ('z', '2024-01-02T00:00:00.000000Z')");
            convertFirstDay("tl");
            final String query = "SELECT length(s) l, ts FROM tl";
            // 50_000 values of 200 to 256 bytes decode to over 10 MiB.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                runCopy(job, query, PAGE_FRAME_BACKED, "limit_breached", "");
                assertQuery("SELECT status, message ~ 'query memory limit exceeded' is_limit FROM \"sys.copy_export_log\" LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("status\tis_limit\nfailed\ttrue\n");
                Assert.assertFalse(exportFileExists("limit_breached"));

                // The same job exports the table once the limit allows it.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                runCopy(job, query, PAGE_FRAME_BACKED, "limit_lifted", "");
                assertExportFinished(engine, sqlExecutionContext);
            }
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, query, readParquet("limit_lifted"));
            }
        });
    }

    @Test
    public void testComputedShortVarcharColumnTop() throws Exception {
        // Days 1 to 3 hold no v, which a later ALTER adds, so their frames compute NULL from it. Row i holds
        // x = i + 1, and v = 'w' || x from day 4 on.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tt (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tt SELECT x, timestamp_sequence('2024-01-01', 86_400_000) FROM long_sequence(3_000)");
            execute("ALTER TABLE tt ADD COLUMN v VARCHAR");
            execute("INSERT INTO tt SELECT x + 3_000, timestamp_sequence('2024-01-04', 86_400_000), 'w' || (x + 3_000) FROM long_sequence(3_000)");
            final String query = "SELECT ts, left(v, 6) w FROM tt";
            assertColumnType(query, "w", "VARCHAR");
            assertExportMatchesQuery(engine, sqlExecutionContext, query, PAGE_FRAME_BACKED, "column_top", " ROW_GROUP_SIZE 1500");
            // Rows 2999, 3000 and 5999.
            assertQuery("SELECT ts, w, w IS NULL is_null FROM " + readParquet("column_top")
                    + " WHERE ts IN ('2024-01-03T23:58:33.600000Z', '2024-01-04T00:00:00.000000Z', '2024-01-06T23:58:33.600000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tw\tis_null
                            2024-01-03T23:58:33.600000Z\t\ttrue
                            2024-01-04T00:00:00.000000Z\tw3001\tfalse
                            2024-01-06T23:58:33.600000Z\tw6000\tfalse
                            """);
        });
    }

    @Test
    public void testComputedShortVarcharCoveringIndexScan() throws Exception {
        // A covering index scan of several keys merges the rows of the keys in each partition into a frame,
        // here of about 667 rows. The expected rows come from the same query without the covering index, in
        // ts order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (sym SYMBOL INDEX TYPE POSTING INCLUDE (ts), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tc SELECT 's' || (x % 3), timestamp_sequence('2024-01-01', 86_400_000) FROM long_sequence(6_000)");
            final String query = "SELECT ts, sym::VARCHAR s FROM tc WHERE sym IN ('s1', 's2')";
            assertQuery(query).noLeakCheck().assertsPlanContaining("CoveringIndex");
            assertColumnType(query, "s", "VARCHAR");
            copyToParquet(engine, sqlExecutionContext, query, PAGE_FRAME_BACKED, "covering", " ROW_GROUP_SIZE 1500");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEqualsExactOrder(
                        compiler,
                        sqlExecutionContext,
                        "SELECT /*+ no_covering(tc) */ ts, sym::VARCHAR s FROM tc WHERE sym IN ('s1', 's2') ORDER BY ts",
                        readParquet("covering")
                );
            }
        });
    }

    @Test
    public void testComputedShortVarcharDefaultRowGroupSize() throws Exception {
        // The default row group of 100_000 rows takes a frame of 70_000 rows, whose computed values take more
        // than one buffer page, and the first 30_000 rows of the next frame, of 40_000 rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tb (a LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tb SELECT x + 1_000_000, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(70_000)");
            execute("INSERT INTO tb SELECT x + 1_070_000, timestamp_sequence('2024-01-02', 1_000_000) FROM long_sequence(40_000)");
            // Values of 7 bytes.
            final String castQuery = "SELECT ts, a::VARCHAR s FROM tb";
            assertColumnType(castQuery, "s", "VARCHAR");
            assertExportMatchesQuery(engine, sqlExecutionContext, castQuery, PAGE_FRAME_BACKED, "default_row_group_cast", "");
            final String constantQuery = "SELECT ts, 'abc'::VARCHAR c FROM tb";
            assertColumnType(constantQuery, "c", "VARCHAR");
            assertExportMatchesQuery(engine, sqlExecutionContext, constantQuery, PAGE_FRAME_BACKED, "default_row_group_constant", "");
        });
    }

    @Test
    public void testComputedShortVarcharExportFailsAtQueryMemoryLimit() throws Exception {
        // An export that fails frees the computed values it holds for the writer. A decoded 1000-row group of
        // s and ts takes 1000 * (16 + 200 + 8) = 224_000 bytes, charged to the query memory limit: 600 KiB
        // holds two of them, not three. The rows of the first two frames wait in the writer for a 3000-row
        // row group when the third frame decodes.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
            execute("CREATE TABLE tg (s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tg SELECT rpad(x::VARCHAR, 200, 'v'), timestamp_sequence('2024-01-01', 2_000_000) FROM long_sequence(32_000)");
            execute("INSERT INTO tg VALUES ('z', '2024-01-02T00:00:00.000000Z')");
            convertFirstDay("tg");
            assertRowGroupCount("tg", 32);
            final String query = "SELECT left(s, 6) s6, ts FROM tg";
            assertColumnType(query, "s6", "VARCHAR");
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 600 * 1024L);
                runCopy(job, query, PAGE_FRAME_BACKED, "short_limit_breached", " ROW_GROUP_SIZE 3000");
                assertQuery("SELECT status, message ~ 'in row group 2: query memory limit exceeded' is_third_frame FROM \"sys.copy_export_log\" LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("status\tis_third_frame\nfailed\ttrue\n");
                Assert.assertFalse(exportFileExists("short_limit_breached"));

                // The same job exports the table once the limit allows it.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                runCopy(job, query, PAGE_FRAME_BACKED, "short_limit_lifted", " ROW_GROUP_SIZE 3000");
                assertExportFinished(engine, sqlExecutionContext);
            }
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, query, readParquet("short_limit_lifted"));
            }
        });
    }

    @Test
    public void testComputedShortVarcharFrames() throws Exception {
        // A computed VARCHAR value of at most 9 bytes or NULL, and a NULL array, take no bytes in the data
        // buffer of the column, only in its aux buffer. The export holds the computed values of a frame until
        // it writes the row group that the last rows of the frame land in. Table t has five days of 1000 rows
        // and one of 300, which the export reads as frames of the same rows. In row groups of 1500 rows, the
        // writer still holds rows of a frame when the export computes the next one.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
            execute("CREATE TABLE t (sym SYMBOL, v VARCHAR, u VARCHAR, x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Row i holds x = i + 1 at 2024-01-01 plus i * 86.4 seconds. v takes 16 bytes, and u 7 bytes with a
            // 2-byte and a 4-byte character.
            execute("""
                    INSERT INTO t SELECT 's' || x, (x + 100_000)::VARCHAR || '-long-tail', 'ü😀' || (x % 10), x,
                    timestamp_sequence('2024-01-01', 86_400_000)
                    FROM long_sequence(5_300)""");
            assertShortVarcharExports("native");

            // Parquet partitions of 1000-row row groups.
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02', '2024-01-04'");
            drainWalQueue();
            Assert.assertEquals("[0 0 0 1000][1 0 0 1000][3 0 0 1000]", parquetFrames(engine, sqlExecutionContext, "SELECT ts, sym::VARCHAR s FROM t"));
            assertShortVarcharExports("parquet");
        });
    }

    @Test
    public void testComputedShortVarcharPageFrameMaxRows() throws Exception {
        // A table without partitions reads as frames of cairo.sql.page.frame.max.rows rows. Row i holds x = i + 1.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1000);
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
            execute("CREATE TABLE tn (sym SYMBOL, x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tn SELECT 's' || x, x, timestamp_sequence('2024-01-01', 86_400_000) FROM long_sequence(3_300)");
            final String symbolQuery = "SELECT ts, sym::VARCHAR s FROM tn";
            assertColumnType(symbolQuery, "s", "VARCHAR");
            assertExportMatchesQuery(engine, sqlExecutionContext, symbolQuery, PAGE_FRAME_BACKED, "max_rows_symbol", " ROW_GROUP_SIZE 1500");
            // Rows 1000 to 1999 compute NULL.
            final String nullQuery = "SELECT ts, CASE WHEN x > 1000 AND x <= 2000 THEN NULL ELSE sym::VARCHAR END s FROM tn";
            assertColumnType(nullQuery, "s", "VARCHAR");
            assertExportMatchesQuery(engine, sqlExecutionContext, nullQuery, PAGE_FRAME_BACKED, "max_rows_null", " ROW_GROUP_SIZE 1500");
        });
    }

    @Test
    public void testConstantFalseWindowJoin() throws Exception {
        // A WINDOW JOIN whose ON clause folds to false pages through its master table and pads
        // the aggregates with NULL columns that no table, and no Parquet file, holds.
        assertMemoryLeak(() -> {
            createSmallTable("tw", true);
            convertFirstDay("tw");
            copyToParquet(
                    "SELECT t.c, t.ts, sum(p.a) s FROM tw t WINDOW JOIN tw p ON (1 = 0) RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING",
                    DIRECT_PAGE_FRAME,
                    "window_join"
            );
            assertParquetFile("window_join", "ts", """
                    c\tts\ts
                    1000\t2024-01-01T00:00:00.000000Z\tnull
                    2000\t2024-01-01T08:00:00.000000Z\tnull
                    3000\t2024-01-01T16:00:00.000000Z\tnull
                    4000\t2024-01-02T00:00:00.000000Z\tnull
                    5000\t2024-01-02T08:00:00.000000Z\tnull
                    6000\t2024-01-02T16:00:00.000000Z\tnull
                    """);
        });
    }

    @Test
    public void testConvertedColumnType() throws Exception {
        // ALTER COLUMN TYPE leaves a Parquet partition with the column's old type, which only a
        // conversion reads right. COPY routes such queries through a temp table, which converts;
        // the zero-copy reader does not convert, so it refuses a column whose type changed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tv (a INT, t TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tv SELECT x::INT, (x * 1_000_000)::TIMESTAMP, timestamp_sequence('2024-01-01', 28_800_000_000) FROM long_sequence(6)");
            convertFirstDay("tv");
            execute("ALTER TABLE tv ALTER COLUMN a TYPE LONG");
            execute("ALTER TABLE tv ALTER COLUMN t TYPE TIMESTAMP_NS");
            assertQuery("SELECT name, isParquet FROM table_partitions('tv')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            2024-01-01\ttrue
                            2024-01-02\tfalse
                            """);

            copyToParquet("SELECT a, t, ts FROM tv", ParquetExportMode.TEMP_TABLE, "converted");
            assertParquetFile("converted", "ts", """
                    a\tt\tts
                    1\t1970-01-01T00:00:01.000000000Z\t2024-01-01T00:00:00.000000Z
                    2\t1970-01-01T00:00:02.000000000Z\t2024-01-01T08:00:00.000000Z
                    3\t1970-01-01T00:00:03.000000000Z\t2024-01-01T16:00:00.000000Z
                    4\t1970-01-01T00:00:04.000000000Z\t2024-01-02T00:00:00.000000Z
                    5\t1970-01-01T00:00:05.000000000Z\t2024-01-02T08:00:00.000000Z
                    6\t1970-01-01T00:00:06.000000000Z\t2024-01-02T16:00:00.000000Z
                    """);

            assertExportDirectlyFails("SELECT ts, a FROM tv", null, "parquet export cannot convert column type [column=a, fileType=INT, columnType=LONG]");
            // The type tag stays TIMESTAMP, but the values change unit.
            assertExportDirectlyFails("SELECT t FROM tv", null, "parquet export cannot convert column type [column=t, fileType=TIMESTAMP, columnType=TIMESTAMP_NS]");
        });
    }

    @Test
    public void testDecodedFramesReleasedPerRowGroup() throws Exception {
        // The hybrid path keeps a decoded Parquet frame until the export writes the row group its rows
        // land in, then decodes a later frame into its buffers: the export holds the decoded frames of
        // one output row group plus the frame it reads, however many row groups the partition has, and
        // allocates no more decode buffers than that. A decoded 1000-row group of this table takes
        // 1000 * (4 + 16 + 200 + 8) = 228_000 bytes in the native layout (INT, VARCHAR aux and data,
        // TIMESTAMP), charged to the query memory limit. 2 MiB holds the 2 frames the export holds at
        // ROW_GROUP_SIZE 1000 and the 4 at ROW_GROUP_SIZE 3000, not the 32 of the partition; 320 KiB
        // holds one frame, not two. Only the count of allocated buffers tells reuse from freeing a
        // buffer and allocating a new one, which holds as little memory.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
            execute("CREATE TABLE tg (a INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tg SELECT x::INT, rpad(x::VARCHAR, 200, 'v'), timestamp_sequence('2024-01-01', 2_000_000) FROM long_sequence(32_000)");
            execute("INSERT INTO tg VALUES (0, 'z', '2024-01-02T00:00:00.000000Z')");
            convertFirstDay("tg");
            assertRowGroupCount("tg", 32);
            final String query = "SELECT a * 2 a2, s, ts FROM tg";
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                // read_parquet() reads the same rows from a file of 1000-row row groups.
                runCopy(job, "SELECT * FROM tg", DIRECT_PAGE_FRAME, "released_source", " ROW_GROUP_SIZE 1000");
                assertExportFinished(engine, sqlExecutionContext);

                // The first frame's rows wait in the writer for a 3000-row row group, so its decoded
                // buffers stay pinned while the second frame decodes and breaches the limit. The
                // job's later exports find their memory tracker clean.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 320 * 1024L);
                runCopy(job, query, PAGE_FRAME_BACKED, "released_breached", " ROW_GROUP_SIZE 3000");
                assertQuery("SELECT status, message ~ 'in row group 1: query memory limit exceeded' is_second_frame FROM \"sys.copy_export_log\" LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("status\tis_second_frame\nfailed\ttrue\n");
                Assert.assertFalse(exportFileExists("released_breached"));

                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
                Assert.assertEquals(2, runCopyCountingDecodeBuffers(job, query, "released_1000", " ROW_GROUP_SIZE 1000"));
                // An output row group spans three frames.
                Assert.assertEquals(4, runCopyCountingDecodeBuffers(job, query, "released_3000", " ROW_GROUP_SIZE 3000"));
                final String readParquetQuery = "SELECT a * 2 a2, s, ts FROM " + readParquet("released_source");
                Assert.assertEquals(2, runCopyCountingDecodeBuffers(job, readParquetQuery, "released_read_parquet", " ROW_GROUP_SIZE 1000"));
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, query, readParquet("released_1000"));
                TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, query, readParquet("released_3000"));
                TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, query, readParquet("released_read_parquet"));
            }
        });
    }

    @Test
    public void testDecodedFramesReleasedWithExport() throws Exception {
        // The memory tracker of an export returns to a pool when the export ends; the decoded
        // Parquet frames charged to it must be freed by then, not by the job's next export.
        assertMemoryLeak(() -> {
            createSmallTable("tj", false);
            convertFirstDay("tj");
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                for (int i = 0; i < 3; i++) {
                    runCopy(job, "SELECT a * 2 a2, c, ts FROM tj", PAGE_FRAME_BACKED, "reused_job_" + i, "");
                    assertExportFinished(engine, sqlExecutionContext);
                    assertParquetFile("reused_job_" + i, "ts", """
                            a2\tc\tts
                            2\t1000\t2024-01-01T00:00:00.000000Z
                            4\t2000\t2024-01-01T08:00:00.000000Z
                            6\t3000\t2024-01-01T16:00:00.000000Z
                            8\t4000\t2024-01-02T00:00:00.000000Z
                            10\t5000\t2024-01-02T08:00:00.000000Z
                            12\t6000\t2024-01-02T16:00:00.000000Z
                            """);
                }
            }
        });
    }

    @Test
    public void testDropColumnAfterConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE td (a INT, b LONG, s SYMBOL, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO td SELECT x::INT, x * 1_000, 's' || x, 'v' || x, timestamp_sequence('2024-01-01', 28_800_000_000)
                    FROM long_sequence(6)""");
            drainWalQueue();
            convertFirstDay("td");
            execute("ALTER TABLE td DROP COLUMN b");
            drainWalQueue();
            execute("ALTER TABLE td DROP COLUMN s");
            drainWalQueue();

            final String expected = """
                    a\tv\tts
                    1\tv1\t2024-01-01T00:00:00.000000Z
                    2\tv2\t2024-01-01T08:00:00.000000Z
                    3\tv3\t2024-01-01T16:00:00.000000Z
                    4\tv4\t2024-01-02T00:00:00.000000Z
                    5\tv5\t2024-01-02T08:00:00.000000Z
                    6\tv6\t2024-01-02T16:00:00.000000Z
                    """;
            copyToParquet("SELECT * FROM td", DIRECT_PAGE_FRAME, "drop_all");
            assertParquetFile("drop_all", "ts", expected);
            execute("CREATE VIEW vd AS (SELECT * FROM td)");
            drainWalQueue();
            copyToParquet("SELECT * FROM vd", DIRECT_PAGE_FRAME, "drop_view");
            assertParquetFile("drop_view", "ts", expected);

            copyToParquet("SELECT a, v, ts, a * 2 a2 FROM td", PAGE_FRAME_BACKED, "drop_computed");
            assertParquetFile("drop_computed", "ts", """
                    a\tv\tts\ta2
                    1\tv1\t2024-01-01T00:00:00.000000Z\t2
                    2\tv2\t2024-01-01T08:00:00.000000Z\t4
                    3\tv3\t2024-01-01T16:00:00.000000Z\t6
                    4\tv4\t2024-01-02T00:00:00.000000Z\t8
                    5\tv5\t2024-01-02T08:00:00.000000Z\t10
                    6\tv6\t2024-01-02T16:00:00.000000Z\t12
                    """);
        });
    }

    @Test
    public void testDropColumnBeforeConversionNonWal() throws Exception {
        assertMemoryLeak(() -> assertDropColumnBeforeConversion(engine, sqlExecutionContext, "drop_before_nonwal", false));
    }

    @Test
    public void testDropColumnBeforeConversionUnmappedFile() throws Exception {
        final CairoConfiguration configuration = newUnmappedFileConfiguration();
        assertMemoryLeak(() -> TestUtils.execute(
                null,
                (engine, compiler, sqlExecutionContext) -> assertDropColumnBeforeConversion(engine, sqlExecutionContext, "drop_before_unmapped", false),
                configuration,
                LOG
        ));
    }

    @Test
    public void testDropColumnBeforeConversionWal() throws Exception {
        assertMemoryLeak(() -> assertDropColumnBeforeConversion(engine, sqlExecutionContext, "drop_before_wal", true));
    }

    @Test
    public void testManyRowGroups() throws Exception {
        // Each Parquet partition holds several row groups, which the export reads as separate
        // frames, and the export cuts row groups of another size: a written row group spans
        // frames of both formats, and the decoded buffers of a frame outlive it. An interval
        // whose bounds cut row groups makes the first and the last Parquet frames parts of row
        // groups, which the hybrid path decodes from the frame's own first row.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
            execute("CREATE TABLE tm (a INT, s SYMBOL, v VARCHAR, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tm SELECT x::INT, 's' || (x % 17), 'value_' || x, x / 8.0, timestamp_sequence('2024-01-01', 216_000_000)
                    FROM long_sequence(1_600)""");
            execute("ALTER TABLE tm CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-03'");
            assertRowGroupCount("tm", 4);
            // 400 rows a day: the interval starts at row 217 of day 1, row 17 of its row group 2,
            // and ends before row 217 of day 3.
            final String partialQuery = "SELECT a, a * 2 a2, s, v, d, ts FROM tm WHERE ts >= '2024-01-01T13:00' AND ts < '2024-01-03T13:00'";
            Assert.assertEquals("[0 2 17 100][0 3 0 100][2 0 0 100][2 1 0 100][2 2 0 17]", parquetFrames(engine, sqlExecutionContext, partialQuery));

            for (String rowGroupSize : new String[]{"37", "1000"}) {
                final String options = " ROW_GROUP_SIZE " + rowGroupSize;
                assertExportMatchesQuery(engine, sqlExecutionContext, "SELECT v, ts, a FROM tm", DIRECT_PAGE_FRAME, "many_direct_" + rowGroupSize, options);
                assertExportMatchesQuery(engine, sqlExecutionContext, "SELECT s, v, d * 2 d2, a, ts FROM tm", PAGE_FRAME_BACKED, "many_computed_" + rowGroupSize, options);
                assertExportMatchesQuery(engine, sqlExecutionContext, "SELECT ts, s, length(v) l FROM tm WHERE ts IN '2024-01-01T12;2d'", PAGE_FRAME_BACKED, "many_interval_" + rowGroupSize, options);
                assertExportMatchesQuery(engine, sqlExecutionContext, partialQuery, PAGE_FRAME_BACKED, "many_partial_" + rowGroupSize, options);
                assertQuery("SELECT count() n, min(a) a_lo, max(a) a_hi, min(ts) ts_lo, max(ts) ts_hi FROM " + readParquet("many_partial_" + rowGroupSize))
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                n\ta_lo\ta_hi\tts_lo\tts_hi
                                800\t218\t1017\t2024-01-01T13:01:12.000000Z\t2024-01-03T12:57:36.000000Z
                                """);
            }
        });
    }

    @Test
    public void testRandomProjections() throws Exception {
        // Random column subsets and orders, with computed columns mixed in, over every type. About
        // half of the queries take an interval at whole hours within days 1 to 4, whose bounds may
        // cut the row group of a Parquet day, so that a frame starts or ends inside it. Of the
        // first 12 queries, about a third hold a CASE whose branches are all NULL, which the export
        // writes as a STRING column of NULLs, so the query engine reads that CASE cast to STRING,
        // and about a third compute no column, which the export writes in DIRECT_PAGE_FRAME mode.
        // Each of the last 4 queries computes a single column, whose values take no bytes in its
        // data buffer in some frames.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            createAllTypesTable("tr", rnd.nextBoolean());
            final StringSink sink = new StringSink();
            for (int i = 0; i < 16; i++) {
                sink.clear();
                final boolean isShortComputed = i >= 12;
                final boolean isComputed = !isShortComputed && rnd.nextBoolean();
                final int columnCount = 1 + rnd.nextInt(8);
                // The NULL CASE goes before column nullCaseColumn, or after the last one when that
                // is columnCount; -1 for none.
                final int nullCaseColumn = !isShortComputed && rnd.nextInt(3) == 0 ? rnd.nextInt(columnCount + 1) : -1;
                for (int c = 0; c < columnCount; c++) {
                    if (c > 0) {
                        sink.put(", ");
                    }
                    if (c == nullCaseColumn) {
                        putNullCase(sink, rnd);
                        sink.put(", ");
                    }
                    sink.put(ALL_TYPES_COLUMN_NAMES[rnd.nextInt(ALL_TYPES_COLUMN_NAMES.length)]).put(" c").put(c);
                }
                if (nullCaseColumn == columnCount) {
                    sink.put(", ");
                    putNullCase(sink, rnd);
                }
                if (isComputed) {
                    sink.put(", id * ").put(rnd.nextInt(100)).put(" computed");
                } else if (isShortComputed) {
                    putShortComputed(sink, rnd);
                }
                sink.put(" FROM tr");
                if (rnd.nextBoolean()) {
                    // Hours since 2024-01-01T00:00, the lower bound before the upper one.
                    final int loHour = rnd.nextInt(95);
                    final int hiHour = loHour + 1 + rnd.nextInt(95 - loHour);
                    sink.put(" WHERE ts >= ");
                    putHourTimestamp(sink, loHour);
                    sink.put(" AND ts < ");
                    putHourTimestamp(sink, hiHour);
                }
                final String query = "SELECT " + sink;
                LOG.info().$("exporting [query=").$(query).I$();
                assertExportMatchesQuery(
                        engine,
                        sqlExecutionContext,
                        query,
                        query.replace("THEN NULL END", "THEN NULL::STRING END"),
                        isComputed || isShortComputed || nullCaseColumn >= 0 ? PAGE_FRAME_BACKED : DIRECT_PAGE_FRAME,
                        "random_" + i,
                        " ROW_GROUP_SIZE " + (1 + rnd.nextInt(60))
                );
            }
        });
    }

    @Test
    public void testReadParquetFileWithoutColumnIds() throws Exception {
        // A Parquet file that QuestDB did not write has no column ids: read_parquet() identifies
        // its columns by position. The rows are in sqllogictest's test_parquet_scan.test.
        assertMemoryLeak(() -> {
            final java.nio.file.Path source = java.nio.file.Paths.get(exportRoot, "alltypes_plain.parquet");
            java.nio.file.Files.copy(
                    java.nio.file.Paths.get(TestUtils.getTestResourcePath("/sqllogictest/data/parquet-testing/arrow/alltypes_plain.parquet")),
                    source,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING
            );
            final String file = "read_parquet('" + source + "')";
            copyToParquet("SELECT double_col, id, bigint_col FROM " + file, DIRECT_PAGE_FRAME, "no_ids_subset");
            assertParquetFile("no_ids_subset", null, """
                    double_col\tid\tbigint_col
                    0.0\t4\t0
                    10.1\t5\t10
                    0.0\t6\t0
                    10.1\t7\t10
                    0.0\t2\t0
                    10.1\t3\t10
                    0.0\t0\t0
                    10.1\t1\t10
                    """);
            copyToParquet("SELECT bigint_col + id s, int_col FROM " + file, PAGE_FRAME_BACKED, "no_ids_computed");
            assertParquetFile("no_ids_computed", null, """
                    s\tint_col
                    4\t0
                    15\t1
                    6\t0
                    17\t1
                    2\t0
                    13\t1
                    0\t0
                    11\t1
                    """);
        });
    }

    @Test
    public void testReadParquetPartialFieldIds() throws Exception {
        // DuckDB wrote these files with field ids on some columns only, each id the position of another column.
        // The export reads each column of a read_parquet() file from the file column of its name. The expected
        // rows are DuckDB's.
        assertMemoryLeak(() -> {
            // a without a field id at position 0, and b of field id 0 at position 1.
            final String narrow = copyParquetTestFile("partial_field_ids.parquet");
            final String narrowRows = """
                    a\tb
                    1\t2
                    3\t4
                    """;
            copyToParquet("SELECT a, b FROM " + narrow, DIRECT_PAGE_FRAME, "narrow_ab");
            assertParquetFile("narrow_ab", null, narrowRows);
            copyToParquet("SELECT * FROM " + narrow, DIRECT_PAGE_FRAME, "narrow_all");
            assertParquetFile("narrow_all", null, narrowRows);
            copyToParquet("SELECT b, a FROM " + narrow, DIRECT_PAGE_FRAME, "narrow_ba");
            assertParquetFile("narrow_ba", null, """
                    b\ta
                    2\t1
                    4\t3
                    """);
            copyToParquet("SELECT a * 2 a2, b FROM " + narrow, PAGE_FRAME_BACKED, "narrow_computed");
            assertParquetFile("narrow_computed", null, """
                    a2\tb
                    2\t2
                    6\t4
                    """);

            // BIGINT b without a field id at position 0, and INT a of field id 0 at position 1, and the same
            // columns with field id 5 on both.
            final String wideRows = """
                    b\ta
                    0\t0
                    10\t1
                    20\t2
                    30\t3
                    40\t4
                    50\t5
                    60\t6
                    70\t7
                    80\t8
                    90\t9
                    """;
            final String wide = copyParquetTestFile("partial_field_ids_widths.parquet");
            copyToParquet("SELECT a, b FROM " + wide, DIRECT_PAGE_FRAME, "wide_ab");
            assertParquetFile("wide_ab", null, """
                    a\tb
                    0\t0
                    1\t10
                    2\t20
                    3\t30
                    4\t40
                    5\t50
                    6\t60
                    7\t70
                    8\t80
                    9\t90
                    """);
            copyToParquet("SELECT * FROM " + wide, DIRECT_PAGE_FRAME, "wide_all");
            assertParquetFile("wide_all", null, wideRows);
            copyToParquet("SELECT b, a FROM " + wide, DIRECT_PAGE_FRAME, "wide_ba");
            assertParquetFile("wide_ba", null, wideRows);
            copyToParquet("SELECT a * 2 a2, b FROM " + wide, PAGE_FRAME_BACKED, "wide_computed");
            assertParquetFile("wide_computed", null, """
                    a2\tb
                    0\t0
                    2\t10
                    4\t20
                    6\t30
                    8\t40
                    10\t50
                    12\t60
                    14\t70
                    16\t80
                    18\t90
                    """);
            final String repeated = copyParquetTestFile("repeated_field_ids.parquet");
            copyToParquet("SELECT * FROM " + repeated, DIRECT_PAGE_FRAME, "repeated_all");
            assertParquetFile("repeated_all", null, wideRows);

            // INT a without a field id at position 0, and VARCHAR b of field id 0 at position 1.
            final String varchar = copyParquetTestFile("partial_field_ids_varchar.parquet");
            copyToParquet("SELECT b, a FROM " + varchar, DIRECT_PAGE_FRAME, "varchar_ba");
            assertParquetFile("varchar_ba", null, """
                    b\ta
                    v0\t0
                    v1\t1
                    v2\t2
                    v3\t3
                    v4\t4
                    v5\t5
                    v6\t6
                    v7\t7
                    v8\t8
                    v9\t9
                    """);

            // DOUBLE[] arr, INT x, DECIMAL(38, 10) d, BIGINT y, UUID u and SMALLINT s, with field ids 0, 2 and 4
            // on x, y and s.
            final String types = copyParquetTestFile("partial_field_ids_types.parquet");
            copyToParquet("SELECT * FROM " + types, DIRECT_PAGE_FRAME, "types_all");
            assertParquetFile("types_all", null, """
                    arr\tx\td\ty\tu\ts
                    [0.0,-0.25]\t-150\t-1.0000000000\tnull\t11111111-2222-3333-4444-555555555550\t-2000
                    [1.5,-1.25]\t-50\t1234567889.0123456789\t9999999999\t\t-1000
                    null\t50\t2469135779.0246913578\t19999999999\t11111111-2222-3333-4444-555555555552\t0
                    [4.5,-3.25]\tnull\t-9999999999999999999999999999.9999999999\t29999999999\t11111111-2222-3333-4444-555555555553\t1000
                    [6.0,-4.25]\t250\t\t39999999999\t11111111-2222-3333-4444-555555555554\t2000
                    """);
            copyToParquet("SELECT s, u, y, d, x, arr FROM " + types, DIRECT_PAGE_FRAME, "types_reversed");
            assertParquetFile("types_reversed", null, """
                    s\tu\ty\td\tx\tarr
                    -2000\t11111111-2222-3333-4444-555555555550\tnull\t-1.0000000000\t-150\t[0.0,-0.25]
                    -1000\t\t9999999999\t1234567889.0123456789\t-50\t[1.5,-1.25]
                    0\t11111111-2222-3333-4444-555555555552\t19999999999\t2469135779.0246913578\t50\tnull
                    1000\t11111111-2222-3333-4444-555555555553\t29999999999\t-9999999999999999999999999999.9999999999\tnull\t[4.5,-3.25]
                    2000\t11111111-2222-3333-4444-555555555554\t39999999999\t\t250\t[6.0,-4.25]
                    """);

            // A table partition is found by writer index: its Parquet file, converted after DROP COLUMN b,
            // holds c, of writer index 2, at position 1.
            execute("CREATE TABLE td (a INT, b LONG, c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO td SELECT x::INT, x * 10, (x * 100)::INT, timestamp_sequence('2024-01-01', 43_200_000_000) FROM long_sequence(4)");
            execute("ALTER TABLE td DROP COLUMN b");
            convertFirstDay("td");
            Assert.assertEquals("[0,2,3]", parquetColumnIds(engine, "td"));
            copyToParquet("SELECT c, a FROM td", DIRECT_PAGE_FRAME, "table_ca");
            assertParquetFile("table_ca", null, """
                    c\ta
                    100\t1
                    200\t2
                    300\t3
                    400\t4
                    """);
            copyToParquet("SELECT c * 2 c2, a FROM td", PAGE_FRAME_BACKED, "table_computed");
            assertParquetFile("table_computed", null, """
                    c2\ta
                    200\t1
                    400\t2
                    600\t3
                    800\t4
                    """);
        });
    }

    @Test
    public void testReadParquetPartialFieldIdsRowGroups() throws Exception {
        // DuckDB wrote 2_500 rows in row groups of 2_048 and 452 rows. b, s, f and n have no field id, and a,
        // ts, d and k have field ids 0, 2, 4 and 6, each the position of the column before. The export reads
        // every row group of the file; table expected holds the rows by DuckDB's formulas, with i = x - 1.
        assertMemoryLeak(() -> {
            final String file = copyParquetTestFile("partial_field_ids_row_groups.parquet");
            execute("""
                    CREATE TABLE expected AS (
                        SELECT
                            CASE WHEN i % 7 = 0 THEN NULL ELSE i * 10 END b,
                            CASE WHEN i % 5 = 0 THEN NULL ELSE i::INT END a,
                            CASE WHEN i % 3 = 0 THEN NULL ELSE ('v' || i)::VARCHAR END s,
                            '2024-01-01'::TIMESTAMP + i * 1_000_000 ts,
                            i % 2 = 0 f,
                            i / 4.0 d,
                            NULL::INT n,
                            (i % 100)::INT k
                        FROM (SELECT x - 1 i FROM long_sequence(2_500))
                    )""");
            for (String columns : new String[]{"*", "k, n, d, f, ts, s, a, b"}) {
                assertExportMatchesQuery(
                        engine,
                        sqlExecutionContext,
                        "SELECT " + columns + " FROM " + file,
                        "SELECT " + columns + " FROM expected",
                        DIRECT_PAGE_FRAME,
                        "*".equals(columns) ? "row_groups_all" : "row_groups_reversed",
                        ""
                );
            }
            final String computed = "b * 2 b2, a, s, ts, f, d, n, k";
            assertExportMatchesQuery(
                    engine,
                    sqlExecutionContext,
                    "SELECT " + computed + " FROM " + file,
                    "SELECT " + computed + " FROM expected",
                    PAGE_FRAME_BACKED,
                    "row_groups_computed",
                    ""
            );
        });
    }

    @Test
    public void testReadParquetSource() throws Exception {
        // read_parquet() pages through a file of its own column set and order, just like a
        // Parquet partition does.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE src AS (
                        SELECT x::INT i, x / 2.0 db, ('s' || x)::STRING s, 'v' || x vc, ('y' || (x % 3))::SYMBOL sym,
                        timestamp_sequence('2024-01-01', 3_600_000_000) ts
                        FROM long_sequence(5)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            copyToParquet("SELECT * FROM src", DIRECT_PAGE_FRAME, "src_all");
            final String source = readParquet("src_all");

            copyToParquet("SELECT sym, vc, s FROM " + source, DIRECT_PAGE_FRAME, "rp_subset");
            assertParquetFile("rp_subset", null, """
                    sym\tvc\ts
                    y1\tv1\ts1
                    y2\tv2\ts2
                    y0\tv3\ts3
                    y1\tv4\ts4
                    y2\tv5\ts5
                    """);
            copyToParquet("SELECT i, i + 1 ii, db * 2 dd, vc, length(vc) lv FROM " + source, PAGE_FRAME_BACKED, "rp_computed");
            assertParquetFile("rp_computed", null, """
                    i\tii\tdd\tvc\tlv
                    1\t2\t1.0\tv1\t2
                    2\t3\t2.0\tv2\t2
                    3\t4\t3.0\tv3\t2
                    4\t5\t4.0\tv4\t2
                    5\t6\t5.0\tv5\t2
                    """);
        });
    }

    @Test
    public void testRenameColumnAfterConversion() throws Exception {
        // RENAME keeps the column, and the Parquet file still holds it under its old name.
        assertMemoryLeak(() -> {
            createSmallTable("tre", false);
            convertFirstDay("tre");
            execute("ALTER TABLE tre RENAME COLUMN a TO c2");
            execute("ALTER TABLE tre RENAME COLUMN c TO a");

            // The file holds a and c in this order: a by-position read takes c2's values for a,
            // and a by-name one takes the file's a.
            copyToParquet("SELECT a, c2, ts FROM tre", DIRECT_PAGE_FRAME, "renamed");
            assertParquetFile("renamed", "ts", """
                    a\tc2\tts
                    1000\t1\t2024-01-01T00:00:00.000000Z
                    2000\t2\t2024-01-01T08:00:00.000000Z
                    3000\t3\t2024-01-01T16:00:00.000000Z
                    4000\t4\t2024-01-02T00:00:00.000000Z
                    5000\t5\t2024-01-02T08:00:00.000000Z
                    6000\t6\t2024-01-02T16:00:00.000000Z
                    """);
            copyToParquet("SELECT a, c2 * 3 c3, ts FROM tre", PAGE_FRAME_BACKED, "renamed_computed");
            assertParquetFile("renamed_computed", "ts", """
                    a\tc3\tts
                    1000\t3\t2024-01-01T00:00:00.000000Z
                    2000\t6\t2024-01-01T08:00:00.000000Z
                    3000\t9\t2024-01-01T16:00:00.000000Z
                    4000\t12\t2024-01-02T00:00:00.000000Z
                    5000\t15\t2024-01-02T08:00:00.000000Z
                    6000\t18\t2024-01-02T16:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testUnmappedParquetFile() throws Exception {
        // A Parquet partition whose file is not mapped locally reports a zero file address,
        // and the export decodes its row groups through the partition decoder instead.
        final CairoConfiguration configuration = newUnmappedFileConfiguration();
        assertMemoryLeak(() -> TestUtils.execute(null, (engine, compiler, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE tu (a INT, s SYMBOL, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);
            engine.execute("""
                    INSERT INTO tu SELECT x::INT, 's' || (x % 3), 'value_' || x, timestamp_sequence('2024-01-01', 28_800_000_000)
                    FROM long_sequence(6)""", sqlExecutionContext);
            engine.execute("ALTER TABLE tu CONVERT PARTITION TO PARQUET LIST '2024-01-01'", sqlExecutionContext);
            engine.execute("ALTER TABLE tu ADD COLUMN d INT", sqlExecutionContext);
            engine.execute("INSERT INTO tu VALUES (7, 's7', 'value_7', '2024-01-02T20:00:00.000000Z', 70)", sqlExecutionContext);

            copyToParquet(engine, sqlExecutionContext, "SELECT d, v, s, ts FROM tu", DIRECT_PAGE_FRAME, "unmapped", "");
            assertParquetFile(engine, sqlExecutionContext, "unmapped", "ts", """
                    d\tv\ts\tts
                    null\tvalue_1\ts1\t2024-01-01T00:00:00.000000Z
                    null\tvalue_2\ts2\t2024-01-01T08:00:00.000000Z
                    null\tvalue_3\ts0\t2024-01-01T16:00:00.000000Z
                    null\tvalue_4\ts1\t2024-01-02T00:00:00.000000Z
                    null\tvalue_5\ts2\t2024-01-02T08:00:00.000000Z
                    null\tvalue_6\ts0\t2024-01-02T16:00:00.000000Z
                    70\tvalue_7\ts7\t2024-01-02T20:00:00.000000Z
                    """);
            copyToParquet(engine, sqlExecutionContext, "SELECT d FROM tu", DIRECT_PAGE_FRAME, "unmapped_only_missing", "");
            assertParquetFile(engine, sqlExecutionContext, "unmapped_only_missing", null, """
                    d
                    null
                    null
                    null
                    null
                    null
                    null
                    70
                    """);
        }, configuration, LOG));
    }

    @Test
    public void testVarcharAtPositionOfAnotherType() throws Exception {
        // VARCHAR and STRING columns where the file holds an INT and a VARCHAR.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc (a INT, v VARCHAR, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tc SELECT x::INT, 'value_' || x, 'string_' || x, timestamp_sequence('2024-01-01', 28_800_000_000)
                    FROM long_sequence(6)""");
            drainWalQueue();
            convertFirstDay("tc");

            copyToParquet("SELECT v, s, ts FROM tc", DIRECT_PAGE_FRAME, "varchar_first");
            assertParquetFile("varchar_first", "ts", """
                    v\ts\tts
                    value_1\tstring_1\t2024-01-01T00:00:00.000000Z
                    value_2\tstring_2\t2024-01-01T08:00:00.000000Z
                    value_3\tstring_3\t2024-01-01T16:00:00.000000Z
                    value_4\tstring_4\t2024-01-02T00:00:00.000000Z
                    value_5\tstring_5\t2024-01-02T08:00:00.000000Z
                    value_6\tstring_6\t2024-01-02T16:00:00.000000Z
                    """);
            execute("CREATE VIEW vc AS (SELECT v, ts FROM tc)");
            drainWalQueue();
            assertExportMatchesQuery("SELECT * FROM vc", DIRECT_PAGE_FRAME, "varchar_view");
            copyToParquet("SELECT s, v, a + 1 a1 FROM tc", PAGE_FRAME_BACKED, "varchar_computed");
            assertParquetFile("varchar_computed", null, """
                    s\tv\ta1
                    string_1\tvalue_1\t2
                    string_2\tvalue_2\t3
                    string_3\tvalue_3\t4
                    string_4\tvalue_4\t5
                    string_5\tvalue_5\t6
                    string_6\tvalue_6\t7
                    """);
        });
    }

    // A computed STRING column takes bytes in its data buffer whatever its values, so a test that means to
    // compute VARCHAR or array values checks the type of the column.
    private static void assertColumnType(String query, String columnName, String expectedType) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            final RecordMetadata metadata = factory.getMetadata();
            Assert.assertEquals(query, expectedType, ColumnType.nameOf(metadata.getColumnType(metadata.getColumnIndex(columnName))));
        }
    }

    private static void assertExportMode(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String query,
            ParquetExportMode expectedMode
    ) throws Exception {
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            Assert.assertEquals(query, expectedMode, ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext));
        }
    }

    private static void convertFirstDay(String tableName) throws Exception {
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }

    // Copies a file of sqllogictest's data/parquet-testing directory to the input root, and returns the
    // read_parquet() call that reads it.
    private static String copyParquetTestFile(String fileName) throws IOException {
        java.nio.file.Files.copy(
                java.nio.file.Paths.get(TestUtils.getTestResourcePath("/sqllogictest/data/parquet-testing/" + fileName)),
                java.nio.file.Paths.get(exportRoot, fileName),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING
        );
        return "read_parquet('" + fileName + "')";
    }

    // Two rows a day over three days. The INT columns c, d and e differ in value, so an export
    // that reads one of them for another shows it, and b is the column the tests drop.
    private static void createTableWithColumnToDrop(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String tableName,
            boolean isWal
    ) throws Exception {
        engine.execute(
                "CREATE TABLE " + tableName + " (a INT, b LONG, c INT, d INT, e INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)"
                        + " TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : ""),
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO " + tableName
                        + " SELECT x::INT, x * 10, (x * 100)::INT, (x * 1_000)::INT, (x * 10_000)::INT, 's' || x, 'v' || x,"
                        + " timestamp_sequence('2024-01-01', 43_200_000_000) FROM long_sequence(6)",
                sqlExecutionContext
        );
        TestUtils.drainWalQueue(engine);
    }

    // A Parquet partition whose file is not mapped locally reports a zero file address.
    private static CairoConfiguration newUnmappedFileConfiguration() {
        return new CairoConfigurationWrapper(AbstractCairoTest.configuration) {
            @Override
            public ParquetPartitionDecoder newParquetPartitionDecoder() {
                return new ParquetPartitionDecoder() {
                    @Override
                    public long getFileAddr() {
                        return 0;
                    }
                };
            }
        };
    }

    // The column ids of each Parquet partition's file in file column order, such as [0,2][0,1]
    // for two Parquet partitions.
    private static String parquetColumnIds(CairoEngine engine, String tableName) {
        final StringSink sink = new StringSink();
        try (TableReader reader = engine.getReader(tableName)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                    reader.openPartition(i);
                    final ParquetPartitionDecoder decoder = reader.getAndInitParquetPartitionDecoder(i);
                    sink.put('[');
                    for (int c = 0, m = decoder.getColumnCount(); c < m; c++) {
                        if (c > 0) {
                            sink.put(',');
                        }
                        sink.put(decoder.getColumnId(c));
                    }
                    sink.put(']');
                }
            }
        }
        return sink.toString();
    }

    // The Parquet frames of the page frame cursor that a hybrid export of the query reads, as
    // [partitionIndex rowGroup rowGroupLo rowGroupHi], with the native frames left out.
    private static String parquetFrames(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String query) throws Exception {
        final StringSink sink = new StringSink();
        try (
                RecordCursorFactory factory = engine.select(query, sqlExecutionContext);
                PageFrameCursor cursor = ((VirtualRecordCursorFactory) ParquetExportMode.unwrapFactory(factory)).getBaseFactory()
                        .getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)
        ) {
            PageFrame frame;
            while ((frame = cursor.next()) != null) {
                if (frame.getFormat() == PartitionFormat.PARQUET) {
                    sink.put('[').put(frame.getPartitionIndex())
                            .put(' ').put(frame.getParquetRowGroup())
                            .put(' ').put(frame.getParquetRowGroupLo())
                            .put(' ').put(frame.getParquetRowGroupHi())
                            .put(']');
                }
            }
        }
        return sink.toString();
    }

    // A timestamp literal such as '2024-01-02T05:00', hours after 2024-01-01T00:00.
    private static void putHourTimestamp(StringSink sink, int hour) {
        final int hourOfDay = hour % 24;
        sink.put("'2024-01-0").put(1 + hour / 24).put('T').put(hourOfDay < 10 ? "0" : "").put(hourOfDay).put(":00'");
    }

    // A CASE whose branches are all NULL, named n, whose condition holds on every m-th id, m from 1
    // to 10.
    private static void putNullCase(StringSink sink, Rnd rnd) {
        sink.put("CASE WHEN id % ").put(1 + rnd.nextInt(10)).put(" = 0 THEN NULL END n");
    }

    // A column named computed whose values take no bytes in its data buffer in some frames: a VARCHAR of at
    // most 9 bytes or NULL, or a NULL array. The CASEs compute NULL on days 1 and 2, and the CASE of a_varchar
    // computes values of over 9 bytes on odd ids after them.
    private static void putShortComputed(StringSink sink, Rnd rnd) {
        switch (rnd.nextInt(5)) {
            case 0 -> sink.put(", a_symbol::VARCHAR computed");
            case 1 -> sink.put(", left(a_varchar, ").put(1 + rnd.nextInt(9)).put(") computed");
            case 2 -> sink.put(", CASE WHEN ts >= '2024-01-03' THEN left(a_varchar, 5) END computed");
            case 3 -> sink.put(", CASE WHEN ts >= '2024-01-03' THEN a_varchar END computed");
            default -> sink.put(", CASE WHEN ts >= '2024-01-03' THEN an_array END computed");
        }
    }

    private static String readParquet(String fileName) {
        return "read_parquet('" + exportRoot + File.separator + fileName + ".parquet')";
    }

    private static void runCopy(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            CopyExportRequestJob job,
            String query,
            String fileName,
            String options
    ) throws Exception {
        try (
                RecordCursorFactory factory = engine.select("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET" + options, sqlExecutionContext);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
        }
        Assert.assertTrue("the queued export must run", job.run());
        TestUtils.drainWalQueue(engine);
    }

    private static void runCopy(CopyExportRequestJob job, String query, ParquetExportMode expectedMode, String fileName, String options) throws Exception {
        assertExportMode(engine, sqlExecutionContext, query, expectedMode);
        runCopy(engine, sqlExecutionContext, job, query, fileName, options);
    }

    // Day 2 is converted before DROP COLUMN b and ADD COLUMN f, day 1 after them. The export reads
    // day 1's file, which holds f at position 7, before day 2's, which holds ts there and lacks f.
    private void assertAddColumnBetweenConversions(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String tableName,
            boolean isWal
    ) throws Exception {
        createTableWithColumnToDrop(engine, sqlExecutionContext, tableName, isWal);
        engine.execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-02'", sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
        engine.execute("ALTER TABLE " + tableName + " DROP COLUMN b", sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
        engine.execute("ALTER TABLE " + tableName + " ADD COLUMN f INT", sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
        engine.execute(
                "INSERT INTO " + tableName + " (a, c, d, e, s, v, ts, f) VALUES (7, 700, 7_000, 70_000, 's7', 'v7', '2024-01-01T18:00:00.000000Z', 7)",
                sqlExecutionContext
        );
        TestUtils.drainWalQueue(engine);
        engine.execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'", sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
        // CONVERT tags each file column with its writer index and skips dropped columns: day 1's
        // file holds f, id 8, at position 7, where day 2's file holds ts.
        Assert.assertEquals("[0,2,3,4,5,6,7,8][0,1,2,3,4,5,6,7]", parquetColumnIds(engine, tableName));

        // f reads NULL on day 2, written before f existed, and on day 3, which has no f data.
        copyToParquet(engine, sqlExecutionContext, "SELECT f, c, ts FROM " + tableName, DIRECT_PAGE_FRAME, tableName + "_fcts", "");
        assertParquetFile(engine, sqlExecutionContext, tableName + "_fcts", "ts", """
                f\tc\tts
                null\t100\t2024-01-01T00:00:00.000000Z
                null\t200\t2024-01-01T12:00:00.000000Z
                7\t700\t2024-01-01T18:00:00.000000Z
                null\t300\t2024-01-02T00:00:00.000000Z
                null\t400\t2024-01-02T12:00:00.000000Z
                null\t500\t2024-01-03T00:00:00.000000Z
                null\t600\t2024-01-03T12:00:00.000000Z
                """);
        copyToParquet(engine, sqlExecutionContext, "SELECT f + 1 f1, c, ts FROM " + tableName, PAGE_FRAME_BACKED, tableName + "_computed", "");
        assertParquetFile(engine, sqlExecutionContext, tableName + "_computed", "ts", """
                f1\tc\tts
                null\t100\t2024-01-01T00:00:00.000000Z
                null\t200\t2024-01-01T12:00:00.000000Z
                8\t700\t2024-01-01T18:00:00.000000Z
                null\t300\t2024-01-02T00:00:00.000000Z
                null\t400\t2024-01-02T12:00:00.000000Z
                null\t500\t2024-01-03T00:00:00.000000Z
                null\t600\t2024-01-03T12:00:00.000000Z
                """);
    }

    private void assertAllColumnTypes(boolean isWal) throws Exception {
        createAllTypesTable("tt", isWal);

        // Every column in reverse order, so that no column sits where the file holds it.
        final StringSink reversed = new StringSink();
        for (int i = ALL_TYPES_COLUMN_NAMES.length - 1; i >= 0; i--) {
            reversed.put(ALL_TYPES_COLUMN_NAMES[i]).put(i > 0 ? ", " : "");
        }
        assertExportMatchesQuery("SELECT " + reversed + " FROM tt", DIRECT_PAGE_FRAME, "types_reversed");
        assertExportMatchesQuery("SELECT * FROM tt", DIRECT_PAGE_FRAME, "types_all");
        // Every other column.
        final StringSink alternate = new StringSink();
        for (int i = 1; i < ALL_TYPES_COLUMN_NAMES.length; i += 2) {
            alternate.put(i > 1 ? ", " : "").put(ALL_TYPES_COLUMN_NAMES[i]);
        }
        assertExportMatchesQuery("SELECT " + alternate + " FROM tt", DIRECT_PAGE_FRAME, "types_alternate");
        // The hybrid path passes every column through, and computes one from each type family.
        final String computedQuery = "SELECT " + reversed + ", an_int + 1 int1, concat(a_varchar, a_symbol) vs, a_double * 2 d2 FROM tt";
        assertExportMatchesQuery(computedQuery, PAGE_FRAME_BACKED, "types_computed");
        // The same over an interval that cuts the row groups of days 1 and 4: the hybrid path
        // decodes day 1 from row 5 of its row group, and day 4 up to row 19 of its row group.
        final String intervalQuery = computedQuery + " WHERE ts >= '2024-01-01T05:00' AND ts < '2024-01-04T19:00'";
        Assert.assertEquals("[0 0 5 24][1 0 0 24][3 0 0 19]", parquetFrames(engine, sqlExecutionContext, intervalQuery));
        assertExportMatchesQuery(intervalQuery, PAGE_FRAME_BACKED, "types_computed_interval");
    }

    // Day 1 is converted before DROP COLUMN b, day 2 after it, and day 3 stays native. Day 2's file
    // holds each column after b one position before its column id, so one export reads files of
    // both layouts.
    private void assertDropColumnBeforeConversion(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String tableName,
            boolean isWal
    ) throws Exception {
        createTableWithColumnToDrop(engine, sqlExecutionContext, tableName, isWal);
        engine.execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'", sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
        engine.execute("ALTER TABLE " + tableName + " DROP COLUMN b", sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
        engine.execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-02'", sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
        // CONVERT tags each file column with its writer index and skips dropped columns: day 2's
        // column ids differ from its positions.
        Assert.assertEquals("[0,1,2,3,4,5,6,7][0,2,3,4,5,6,7]", parquetColumnIds(engine, tableName));

        // A lookup by position would read d and e, of the same type, for c and d on day 2, and
        // nothing for ts, whose id 7 is past the file's last position.
        copyToParquet(engine, sqlExecutionContext, "SELECT c, d, ts FROM " + tableName, DIRECT_PAGE_FRAME, tableName + "_cdts", "");
        assertParquetFile(engine, sqlExecutionContext, tableName + "_cdts", "ts", """
                c\td\tts
                100\t1000\t2024-01-01T00:00:00.000000Z
                200\t2000\t2024-01-01T12:00:00.000000Z
                300\t3000\t2024-01-02T00:00:00.000000Z
                400\t4000\t2024-01-02T12:00:00.000000Z
                500\t5000\t2024-01-03T00:00:00.000000Z
                600\t6000\t2024-01-03T12:00:00.000000Z
                """);
        copyToParquet(engine, sqlExecutionContext, "SELECT * FROM " + tableName, DIRECT_PAGE_FRAME, tableName + "_all", "");
        assertParquetFile(engine, sqlExecutionContext, tableName + "_all", "ts", """
                a\tc\td\te\ts\tv\tts
                1\t100\t1000\t10000\ts1\tv1\t2024-01-01T00:00:00.000000Z
                2\t200\t2000\t20000\ts2\tv2\t2024-01-01T12:00:00.000000Z
                3\t300\t3000\t30000\ts3\tv3\t2024-01-02T00:00:00.000000Z
                4\t400\t4000\t40000\ts4\tv4\t2024-01-02T12:00:00.000000Z
                5\t500\t5000\t50000\ts5\tv5\t2024-01-03T00:00:00.000000Z
                6\t600\t6000\t60000\ts6\tv6\t2024-01-03T12:00:00.000000Z
                """);
        copyToParquet(engine, sqlExecutionContext, "SELECT ts, v, s, e, a FROM " + tableName, DIRECT_PAGE_FRAME, tableName + "_reordered", "");
        assertParquetFile(engine, sqlExecutionContext, tableName + "_reordered", "ts", """
                ts\tv\ts\te\ta
                2024-01-01T00:00:00.000000Z\tv1\ts1\t10000\t1
                2024-01-01T12:00:00.000000Z\tv2\ts2\t20000\t2
                2024-01-02T00:00:00.000000Z\tv3\ts3\t30000\t3
                2024-01-02T12:00:00.000000Z\tv4\ts4\t40000\t4
                2024-01-03T00:00:00.000000Z\tv5\ts5\t50000\t5
                2024-01-03T12:00:00.000000Z\tv6\ts6\t60000\t6
                """);
        // The same column twice: the projection's page frame cursor maps its columns to the
        // table's writer indexes itself.
        copyToParquet(engine, sqlExecutionContext, "SELECT c, d, c c2, ts FROM " + tableName, DIRECT_PAGE_FRAME, tableName + "_repeated", "");
        assertParquetFile(engine, sqlExecutionContext, tableName + "_repeated", "ts", """
                c\td\tc2\tts
                100\t1000\t100\t2024-01-01T00:00:00.000000Z
                200\t2000\t200\t2024-01-01T12:00:00.000000Z
                300\t3000\t300\t2024-01-02T00:00:00.000000Z
                400\t4000\t400\t2024-01-02T12:00:00.000000Z
                500\t5000\t500\t2024-01-03T00:00:00.000000Z
                600\t6000\t600\t2024-01-03T12:00:00.000000Z
                """);
        copyToParquet(engine, sqlExecutionContext, "SELECT c + 1 c1, d, s, v, ts FROM " + tableName, PAGE_FRAME_BACKED, tableName + "_computed", "");
        assertParquetFile(engine, sqlExecutionContext, tableName + "_computed", "ts", """
                c1\td\ts\tv\tts
                101\t1000\ts1\tv1\t2024-01-01T00:00:00.000000Z
                201\t2000\ts2\tv2\t2024-01-01T12:00:00.000000Z
                301\t3000\ts3\tv3\t2024-01-02T00:00:00.000000Z
                401\t4000\ts4\tv4\t2024-01-02T12:00:00.000000Z
                501\t5000\ts5\tv5\t2024-01-03T00:00:00.000000Z
                601\t6000\ts6\tv6\t2024-01-03T12:00:00.000000Z
                """);
    }

    private void assertExportDirectlyFails(String query, @Nullable Integer firstColumnWriterIndex, String expectedMessage) throws Exception {
        try {
            exportDirectly(query, "refused", firstColumnWriterIndex);
            Assert.fail("the export must refuse the column");
        } catch (CairoException e) {
            TestUtils.assertEquals(expectedMessage, e.getFlyweightMessage());
        }
        // The table is still readable.
        assertQuery("SELECT count() FROM tv").noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
    }

    private void assertExportFinished(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws Exception {
        assertQuery("SELECT status, message FROM \"sys.copy_export_log\" LIMIT -1")
                .withEngine(engine)
                .withContext(sqlExecutionContext)
                .noLeakCheck()
                .expectSize()
                .returns("status\tmessage\nfinished\t\n");
    }

    private void assertExportMatchesQuery(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String query,
            ParquetExportMode expectedMode,
            String fileName,
            String options
    ) throws Exception {
        assertExportMatchesQuery(engine, sqlExecutionContext, query, query, expectedMode, fileName, options);
    }

    // Exports query and compares the file with the rows of oracleQuery, which returns the rows of
    // query in the column types of the file.
    private void assertExportMatchesQuery(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String query,
            String oracleQuery,
            ParquetExportMode expectedMode,
            String fileName,
            String options
    ) throws Exception {
        copyToParquet(engine, sqlExecutionContext, query, expectedMode, fileName, options);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            // Names, types (the string types as one) and values, row by row.
            TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, oracleQuery, readParquet(fileName));
        }
    }

    private void assertExportMatchesQuery(String query, ParquetExportMode expectedMode, String fileName) throws Exception {
        assertExportMatchesQuery(engine, sqlExecutionContext, query, expectedMode, fileName, "");
    }

    private void assertParquetFile(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String fileName,
            String timestamp,
            String expected
    ) throws Exception {
        assertQuery("SELECT * FROM " + readParquet(fileName))
                .withEngine(engine)
                .withContext(sqlExecutionContext)
                .noLeakCheck()
                .expectSize()
                .timestamp(timestamp)
                .returns(expected);
    }

    private void assertParquetFile(String fileName, String timestamp, String expected) throws Exception {
        assertParquetFile(engine, sqlExecutionContext, fileName, timestamp, expected);
    }

    private void assertRowGroupCount(String tableName, int expected) {
        try (TableReader reader = engine.getReader(tableName)) {
            int parquetPartitionCount = 0;
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                    reader.openPartition(i);
                    Assert.assertEquals(expected, reader.getAndInitParquetPartitionDecoder(i).metadata().getRowGroupCount());
                    parquetPartitionCount++;
                }
            }
            Assert.assertTrue(parquetPartitionCount > 0);
        }
    }

    // Exports the query in row groups of 1500 rows, and compares the file with the rows of the query engine.
    // columnName names the computed column, of type columnType.
    private void assertShortVarcharExport(
            String query,
            String columnName,
            String columnType,
            ParquetExportMode expectedMode,
            String fileName
    ) throws Exception {
        assertColumnType(query, columnName, columnType);
        assertExportMatchesQuery(engine, sqlExecutionContext, query, expectedMode, fileName, " ROW_GROUP_SIZE 1500");
    }

    // Exports queries of table t of testComputedShortVarcharFrames() whose computed column takes no bytes in
    // its data buffer in some frames, and compares the files with the rows of the query engine and, at some
    // rows, with the values that t holds.
    private void assertShortVarcharExports(String prefix) throws Exception {
        final String symbolQuery = "SELECT ts, sym::VARCHAR s FROM t";
        assertColumnType(symbolQuery, "s", "VARCHAR");
        // In row groups of 1000 rows the frames end on row group boundaries, and in row groups of
        // 2_147_483_647 rows the writer holds every frame until the export ends.
        for (String rowGroupSize : new String[]{"1000", "1500", "2_147_483_647"}) {
            final String fileName = prefix + "_symbol_" + rowGroupSize;
            assertExportMatchesQuery(engine, sqlExecutionContext, symbolQuery, PAGE_FRAME_BACKED, fileName, " ROW_GROUP_SIZE " + rowGroupSize);
            // Rows 999, 1000, 1499, 1500 and 5299.
            assertQuery("SELECT ts, s FROM " + readParquet(fileName)
                    + " WHERE ts IN ('2024-01-01T23:58:33.600000Z', '2024-01-02T00:00:00.000000Z', '2024-01-02T11:58:33.600000Z',"
                    + " '2024-01-02T12:00:00.000000Z', '2024-01-06T07:10:33.600000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\ts
                            2024-01-01T23:58:33.600000Z\ts1000
                            2024-01-02T00:00:00.000000Z\ts1001
                            2024-01-02T11:58:33.600000Z\ts1500
                            2024-01-02T12:00:00.000000Z\ts1501
                            2024-01-06T07:10:33.600000Z\ts5300
                            """);
        }
        // read_parquet() reads the file of 1000-row row groups as frames of 1000 rows.
        assertShortVarcharExport("SELECT ts, left(s, 9) s9 FROM " + readParquet(prefix + "_symbol_1000"), "s9", "VARCHAR", PAGE_FRAME_BACKED, prefix + "_read_parquet");
        assertShortVarcharExport("SELECT ts, left(v, 6) v6 FROM t", "v6", "VARCHAR", PAGE_FRAME_BACKED, prefix + "_left");
        // Days 2 and 5 compute NULL.
        assertShortVarcharExport(
                "SELECT ts, CASE WHEN ts IN '2024-01-02' OR ts IN '2024-01-05' THEN NULL ELSE sym::VARCHAR END s FROM t",
                "s",
                "VARCHAR",
                PAGE_FRAME_BACKED,
                prefix + "_null"
        );
        // Values of 6 bytes on day 1, and of 16 bytes after it.
        assertShortVarcharExport(
                "SELECT ts, CASE WHEN x > 1000 THEN left(v, 30) ELSE left(v, 6) END w FROM t",
                "w",
                "VARCHAR",
                PAGE_FRAME_BACKED,
                prefix + "_short_then_long"
        );
        // NULL arrays on day 1, and arrays after it.
        assertShortVarcharExport(
                "SELECT ts, CASE WHEN x > 1000 THEN ARRAY[x::DOUBLE, 1.0] END a FROM t",
                "a",
                "DOUBLE[]",
                PAGE_FRAME_BACKED,
                prefix + "_array"
        );
        assertShortVarcharExport(
                "SELECT ts, CASE WHEN x > 1000 THEN ARRAY[[x::DOUBLE, 1.0], [2.0, 3.0]] END a FROM t",
                "a",
                "DOUBLE[][]",
                PAGE_FRAME_BACKED,
                prefix + "_array_2d"
        );
        // A class of values a day: empty, of 9 bytes, of 7 bytes with a 2-byte and a 4-byte character, NULL,
        // and of 10 bytes, the shortest value that takes bytes in the data buffer.
        assertShortVarcharExport(
                """
                        SELECT ts, (CASE WHEN x <= 1000 THEN '' WHEN x <= 2000 THEN rpad(x::VARCHAR, 9, 'z') WHEN x <= 3000 THEN u
                        WHEN x <= 4000 THEN NULL ELSE rpad(x::VARCHAR, 10, 'z') END)::VARCHAR w FROM t""",
                "w",
                "VARCHAR",
                PAGE_FRAME_BACKED,
                prefix + "_classes"
        );
        // The first row of each class.
        assertQuery("SELECT ts, w, w IS NULL is_null FROM " + readParquet(prefix + "_classes")
                + " WHERE ts IN ('2024-01-01T00:00:00.000000Z', '2024-01-02T00:00:00.000000Z', '2024-01-03T00:00:00.000000Z',"
                + " '2024-01-04T00:00:00.000000Z', '2024-01-05T00:00:00.000000Z')")
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        ts\tw\tis_null
                        2024-01-01T00:00:00.000000Z\t\tfalse
                        2024-01-02T00:00:00.000000Z\t1001zzzzz\tfalse
                        2024-01-03T00:00:00.000000Z\tü😀1\tfalse
                        2024-01-04T00:00:00.000000Z\t\ttrue
                        2024-01-05T00:00:00.000000Z\t4001zzzzzz\tfalse
                        """);
        // A computed LONG takes bytes in its data buffer in every frame.
        assertShortVarcharExport("SELECT ts, x * 2 x2, sym::VARCHAR s FROM t", "s", "VARCHAR", PAGE_FRAME_BACKED, prefix + "_with_long");
        // A join exports row-wise, in batches of one row group.
        assertShortVarcharExport("SELECT t.sym::VARCHAR s FROM t CROSS JOIN long_sequence(1)", "s", "VARCHAR", ParquetExportMode.CURSOR_BASED, prefix + "_cursor");
    }

    private void copyToParquet(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String query,
            ParquetExportMode expectedMode,
            String fileName,
            String options
    ) throws Exception {
        assertExportMode(engine, sqlExecutionContext, query, expectedMode);
        try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
            runCopy(engine, sqlExecutionContext, job, query, fileName, options);
        }
        assertExportFinished(engine, sqlExecutionContext);
    }

    private void copyToParquet(String query, ParquetExportMode expectedMode, String fileName) throws Exception {
        copyToParquet(engine, sqlExecutionContext, query, expectedMode, fileName, "");
    }

    private void createAllTypesTable(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + " AS (SELECT " + ALL_TYPES_COLUMNS + " FROM long_sequence(120)) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : ""));
        drainWalQueue();
        // Days 1, 2 and 4 to Parquet; day 3 and the last, partial, day stay native.
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02', '2024-01-04'");
        drainWalQueue();
    }

    private void createSmallTable(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + " (a INT, c LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : ""));
        execute("INSERT INTO " + tableName + " SELECT x::INT, x * 1_000, timestamp_sequence('2024-01-01', 28_800_000_000) FROM long_sequence(6)");
        drainWalQueue();
    }

    // Exports a query in DIRECT_PAGE_FRAME mode whatever ParquetExportMode.determineExportMode()
    // picks for it, writing the file the way COPY does. A firstColumnWriterIndex, when given,
    // replaces the writer index that the page frame cursor reports for the first column.
    private void exportDirectly(String query, String fileName, @Nullable Integer firstColumnWriterIndex) throws Exception {
        final FilesFacade ff = configuration.getFilesFacade();
        final CopyExportContext copyExportContext = engine.getCopyExportContext();
        final long[] fileOffset = {0};
        long fd = -1;
        CopyExportContext.ExportTaskEntry entry = null;
        try (
                Path path = new Path();
                RecordCursorFactory factory = select(query);
                PageFrameCursor baseCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                CopyExportRequestTask task = new CopyExportRequestTask()
        ) {
            final PageFrameCursor pageFrameCursor = firstColumnWriterIndex != null
                    ? new RemappedPageFrameCursor(baseCursor, firstColumnWriterIndex)
                    : baseCursor;
            fd = ff.openRW(path.of(exportRoot).concat(fileName).put(".parquet").$(), configuration.getWriterFileOpenOpts());
            Assert.assertTrue(fd > -1);
            final long exportFd = fd;
            entry = copyExportContext.assignExportEntry(sqlExecutionContext.getSecurityContext(), query, fileName, null, CopyExportContext.CopyTrigger.SQL);
            task.of(
                    entry,
                    null,
                    null,
                    fileName,
                    configuration.getParquetExportCompressionCodec(),
                    configuration.getParquetExportCompressionLevel(),
                    configuration.getParquetExportRowGroupSize(),
                    configuration.getParquetExportDataPageSize(),
                    configuration.isParquetExportStatisticsEnabled(),
                    configuration.getParquetExportVersion(),
                    configuration.isParquetExportRawArrayEncoding(),
                    sqlExecutionContext.getNowTimestampType(),
                    sqlExecutionContext.getNow(sqlExecutionContext.getNowTimestampType()),
                    false,
                    pageFrameCursor,
                    factory.getMetadata(),
                    (dataPtr, dataLen) -> {
                        Assert.assertEquals(dataLen, ff.write(exportFd, dataPtr, dataLen, fileOffset[0]));
                        fileOffset[0] += dataLen;
                    },
                    DIRECT_PAGE_FRAME,
                    null,
                    null,
                    -1,
                    configuration.getParquetExportBloomFilterFpp(),
                    null
            );
            task.setUpStreamPartitionParquetExporter();
            final CopyExportRequestTask.StreamPartitionParquetExporter exporter = task.getStreamPartitionParquetExporter();
            PageFrame frame;
            while ((frame = pageFrameCursor.next()) != null) {
                exporter.writePageFrame(pageFrameCursor, frame);
            }
            exporter.finishExport();
        } finally {
            ff.close(fd);
            if (entry != null) {
                copyExportContext.releaseEntry(entry);
            }
        }
    }

    private boolean exportFileExists(String fileName) {
        try (Path path = new Path()) {
            return configuration.getFilesFacade().exists(path.of(exportRoot).concat(fileName).put(".parquet").$());
        }
    }

    // Runs a PAGE_FRAME_BACKED export of the query on the job, asserts that it finished and returns
    // the number of decode buffers it allocated.
    private long runCopyCountingDecodeBuffers(CopyExportRequestJob job, String query, String fileName, String options) throws Exception {
        final long allocationCount = HybridColumnMaterializer.getDecodeBufferAllocationCountForTesting();
        runCopy(job, query, PAGE_FRAME_BACKED, fileName, options);
        assertExportFinished(engine, sqlExecutionContext);
        return HybridColumnMaterializer.getDecodeBufferAllocationCountForTesting() - allocationCount;
    }

    // Reports another writer index for the first column of its base cursor.
    private static class RemappedPageFrameCursor implements PageFrameCursor {
        private final PageFrameCursor base;
        private final ColumnMapping columnMapping = new ColumnMapping();

        RemappedPageFrameCursor(PageFrameCursor base, int firstColumnWriterIndex) {
            this.base = base;
            final ColumnMapping baseMapping = base.getColumnMapping();
            for (int i = 0, n = baseMapping.getColumnCount(); i < n; i++) {
                final int writerIndex = i == 0 ? firstColumnWriterIndex : baseMapping.getWriterIndex(i);
                columnMapping.addColumn(baseMapping.getColumnIndex(i), writerIndex, writerIndex);
            }
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            base.calculateSize(counter);
        }

        @Override
        public void close() {
            // the test closes the base cursor
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return columnMapping;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return base.getRemainingRowsInInterval();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean isExternal() {
            return base.isExternal();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            return base.next(skipTarget);
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public boolean supportsSizeCalculation() {
            return base.supportsSizeCalculation();
        }

        @Override
        public void toTop() {
            base.toTop();
        }
    }
}
