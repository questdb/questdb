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

package io.questdb.test.cutlass.http;

import io.questdb.griffin.SqlException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertSql;

public class ExpParquetExportTest extends AbstractBootstrapTest {

    private static String exportRoot;
    private static TestHttpClient testHttpClient;
    private CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        testHttpClient = new TestHttpClient();
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration("cairo.sql.copy.export.root", exportRoot));
    }

    @Test
    public void testBasicParquetExport() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE basic_parquet_test AS (" +
                            "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                            "FROM long_sequence(5)" +
                            ")", sqlExecutionContext);

                    testHttpClient.assertGetParquet("/exp", 1282, "basic_parquet_test");
                });
    }

    @Test
    public void testBasics() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    testHttpClient.assertGetParquet(
                            "/exp",
                            "PAR1\u0015\u0000\u0015",
                            "generate_series(0, '1971-01-01', '5s');"
                    );
                });
    }

    @Test
    public void testExpCsvExportStillWorks() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE csv_export_test AS (" +
                            "SELECT x FROM long_sequence(3)" +
                            ")", sqlExecutionContext);

                    // Test CSV export (explicit format)
                    String expectedCsv = "\"x\"\r\n" +
                            "1\r\n" +
                            "2\r\n" +
                            "3\r\n";

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "SELECT * FROM csv_export_test");
                    params.put("format", "csv");

                    testHttpClient.assertGet("/exp", expectedCsv, params, null, null);
                });
    }

    @Test
    public void testExpDefaultFormatIsCsv() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create test table
                    engine.execute("CREATE TABLE default_format_test AS (" +
                            "SELECT x FROM long_sequence(2)" +
                            ")", sqlExecutionContext);

                    // Test without format parameter (should default to CSV)
                    String expectedCsv = "\"x\"\r\n" +
                            "1\r\n" +
                            "2\r\n";

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "SELECT * FROM default_format_test");
                    testHttpClient.assertGet("/exp", expectedCsv, params, null, null);
                });
    }

    @Test
    public void testExpInvalidFormatReturnsError() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE invalid_format_test AS (" +
                            "SELECT x FROM long_sequence(2)" +
                            ")", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from invalid_format_test");
                    params.put("fmt", "invalid");
                    testHttpClient.assertGet("/exp", "{\"query\":\"select * from invalid_format_test\",\"error\":\"unrecognised format [format=invalid]\",\"position\":0}", params, null, null);
                });
    }

    @Test
    public void testExpRejectsNonSelectForParquet() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE reject_non_select_test AS (SELECT x FROM long_sequence(2))", sqlExecutionContext);
                    testHttpClient.assertGetParquet("/exp", "{\"query\":\"INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test\",\"error\":\"/exp endpoint only accepts SELECT\",\"position\":0}", "INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test");
                });
    }

    @Test
    public void testOnParquetPartition() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
                    engine.execute("insert into test_table values ('2020-01-01T00:00:00.000000Z', 0), ('2020-01-02T00:00:00.000000Z', 1)");
                    drainWalQueue(engine);
                    engine.execute("alter table test_table convert partition to parquet where ts < '2020-01-02T00:00:00.000000Z'");
                    drainWalQueue(engine);
                    params.clear();
                    params.put("fmt", "parquet");
                    testHttpClient.assertGetParquet("/exp", 602, params, "SELECT * FROM test_table");
                });
    }

    @Test
    public void testParquetExportCancel() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    params.clear();
                    params.put("query", "generate_series(0, '9999-01-01', '1U')");
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "gzip");
                    params.put("row_group_size", "500");

                    Thread thread = new Thread(() -> {
                        try {
                            long copyID;
                            do {
                                copyID = engine.getCopyExportContext().getActiveExportID();
                            } while (copyID == -1);
                            Os.sleep(500);

                            StringSink sink = new StringSink();
                            Numbers.appendHex(sink, copyID, true);
                            String copyIDStr = sink.toString();
                            sink.clear();
                            sink.put("COPY '").put(copyIDStr).put("' CANCEL;");
                            try {
                                assertSql(engine, sqlExecutionContext, sink.toString(), sink, "id\tstatus\n" +
                                        copyIDStr + "\tcancelled\n");
                            } catch (SqlException e) {
                                throw new RuntimeException(e);
                            }
                            // wait cancel finish
                            do {
                                copyID = engine.getCopyExportContext().getActiveExportID();
                            } while (copyID != -1);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    });
                    thread.start();
                    String expectedError = "{\"query\":\"generate_series(0, '9999-01-01', '1U')\",\"error\":\"copy task failed [id=0, phase=populating_data_to_temp_table, status=cancelled, message=cancelled by user]\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);
                    thread.join();
                });
    }

    @Test
    public void testParquetExportChunkedSending() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create test table with various data types
                    engine.execute("CREATE TABLE chunked_parquet_test AS (" +
                            "SELECT " +
                            "x::int as int_col, " +
                            "x::long as long_col, " +
                            "x * 1.5 as double_col, " +
                            "(x % 2) = 0 as bool_col, " +
                            "'chunk_data_' || x as str_col, " +
                            "timestamp_sequence(0, 50000L) as ts_col " +
                            "FROM long_sequence(100)" +
                            ")", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from chunked_parquet_test");
                    params.put("fmt", "parquet");
                    params.put("filename", "chunked_test");

                    // Test that chunked parquet export request is properly handled
                    testHttpClient.assertGet("/exp", "PAR1\u0015\u0000\u0015", params, null, null);
                });
    }

    @Test
    public void testParquetExportCompressionCode() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE codec_zstd_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);
                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "lz4_raw");
                    testHttpClient.assertGetParquet("/exp", 395, params, "SELECT * FROM codec_zstd_test");

                    params.put("compression_codec", "lzo");
                    testHttpClient.assertGetParquet("/exp", 349, params, "SELECT * FROM codec_zstd_test");

                    params.put("compression_codec", "lz4");
                    testHttpClient.assertGetParquet("/exp", 349, params, "SELECT * FROM codec_zstd_test");

                    params.put("compression_codec", "brotli");
                    testHttpClient.assertGetParquet("/exp", 406, params, "SELECT * FROM codec_zstd_test");
                });
    }

    @Test
    public void testParquetExportCompressionCodecGzip() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE codec_gzip_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);
                    params.clear();
                    params.put("query", "SELECT * FROM codec_gzip_test");
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "gzip");
                    testHttpClient.assertGet("/exp", "PAR1\u0015\u0000\u0015\\\u0015", params, null, null);
                });
    }

    @Test
    public void testParquetExportCompressionCodecSnappy() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE codec_snappy_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);
                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "snappy");
                    testHttpClient.assertGetParquet("/exp", 396, params, "SELECT * FROM codec_snappy_test");
                });
    }

    @Test
    public void testParquetExportCompressionCodecUncompressed() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE codec_uncompressed_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);
                    params.clear();
                    params.put("query", "SELECT * FROM codec_uncompressed_test");
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "uncompressed");
                    testHttpClient.assertGetParquet("/exp", 402, params, "SELECT * FROM codec_uncompressed_test");
                });
    }

    @Test
    public void testParquetExportCompressionCodecZstd() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE codec_zstd_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);
                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "zstd");
                    testHttpClient.assertGetParquet("/exp", 395, params, "SELECT * FROM codec_zstd_test");
                });
    }

    @Test
    public void testParquetExportCompressionLevelInvalid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE level_invalid_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);
                    params.clear();
                    params.put("query", "SELECT * FROM level_invalid_test");
                    params.put("fmt", "parquet");
                    params.put("compression_level", "not_a_number");

                    String expectedError = "{\"query\":\"SELECT * FROM level_invalid_test\",\"error\":\"invalid compression level:not_a_number\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);
                });
    }

    @Test
    public void testParquetExportCompressionLevelValid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE level_valid_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "gzip");
                    params.put("compression_level", "3");
                    testHttpClient.assertGetParquet("/exp", 396, params, "SELECT * FROM level_valid_test");

                    params.put("compression_codec", "zstd");
                    params.put("compression_level", "5");
                    testHttpClient.assertGetParquet("/exp", 401, params, "SELECT * FROM level_valid_test");

                    params.put("compression_codec", "zstd");
                    params.put("compression_level", "15");
                    testHttpClient.assertGetParquet("/exp", 395, params, "SELECT * FROM level_valid_test");
                });
    }

    @Test
    public void testParquetExportDataPageSizeInvalid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE page_size_invalid_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    params.clear();
                    params.put("query", "SELECT * FROM page_size_invalid_test");
                    params.put("fmt", "parquet");
                    params.put("data_page_size", "not_a_number");

                    String expectedError = "{\"query\":\"SELECT * FROM page_size_invalid_test\",\"error\":\"invalid data page size:not_a_number\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);
                });
    }

    @Test
    public void testParquetExportDataPageSizeValid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE page_size_valid_test AS (SELECT x FROM long_sequence(5000))", sqlExecutionContext);
                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("data_page_size", "1024");
                    testHttpClient.assertGetParquet("/exp", 44745, params, "SELECT * FROM page_size_valid_test");
                    params.put("data_page_size", "2048");
                    testHttpClient.assertGetParquet("/exp", 43144, params, "SELECT * FROM page_size_valid_test");
                });
    }

    @Test
    public void testParquetExportInvalidCompressionCodec() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE codec_invalid_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    params.clear();
                    params.put("query", "SELECT * FROM codec_invalid_test");
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "invalid_codec");
                    String expectedError = "{\"query\":\"SELECT * FROM codec_invalid_test\",\"error\":\"invalid compression codec[invalid_codec], expected one of: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd, lz4_raw\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);
                });
    }

    @Test
    public void testParquetExportLargeDataset() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create a larger table to test export handling with substantial data
                    engine.execute("CREATE TABLE large_export_test AS (" +
                            "SELECT x as id, 'data_' || x as content, x * 2.5 as value, " +
                            "timestamp_sequence(0, 100000L) as ts " +
                            "FROM long_sequence(50000)" +
                            ")", sqlExecutionContext);
                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("filename", "large_export_test");
                    testHttpClient.assertGetParquet("/exp", 1879525, params, "SELECT * FROM large_export_test");
                });
    }

    @Test
    public void testParquetExportMultipleOptions() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE multiple_options_test AS (SELECT x FROM long_sequence(10))", sqlExecutionContext);

                    params.clear();
                    params.put("query", "SELECT * FROM multiple_options_test");
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "gzip");
                    params.put("compression_level", "6");
                    params.put("row_group_size", "2000");
                    params.put("data_page_size", "1024");
                    params.put("statistics_enabled", "true");
                    params.put("parquet_version", "2");
                    params.put("raw_array_encoding", "false");

                    testHttpClient.assertGet("/exp", "PAR1\u0015\u0006\u0015", params, null, null);
                });
    }

    @Test
    public void testParquetExportOnlyAcceptsSelectQueries() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE ddl_rejection_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "CREATE TABLE another_test AS (SELECT * FROM ddl_rejection_test)");
                    params.put("fmt", "parquet");
                    testHttpClient.assertGet("/exp", "{\"query\":\"CREATE TABLE another_test AS (SELECT * FROM ddl_rejection_test)\",\"error\":\"/exp endpoint only accepts SELECT\",\"position\":0}", params, null, null);
                });
    }

    @Test
    public void testParquetExportParquetVersionInvalid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE version_invalid_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "SELECT * FROM version_invalid_test");
                    params.put("fmt", "parquet");
                    params.put("parquet_version", "3");

                    String expectedError = "{\"query\":\"SELECT * FROM version_invalid_test\",\"error\":\"invalid parquet version: 3, supported versions: 1, 2\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);

                    params.put("parquet_version", "not_a_number");
                    expectedError = "{\"query\":\"SELECT * FROM version_invalid_test\",\"error\":\"invalid parquet version:not_a_number\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);
                });
    }

    @Test
    public void testParquetExportParquetVersionValid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE version_valid_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("fmt", "parquet");
                    params.put("parquet_version", "1");
                    testHttpClient.assertGetParquet("/exp", 402, params, "SELECT * FROM version_valid_test");
                    params.put("parquet_version", "2");
                    testHttpClient.assertGetParquet("/exp", 403, params, "SELECT * FROM version_valid_test");
                });
    }

    @Test
    public void testParquetExportPartitionByRejected() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE partition_by_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "SELECT * FROM partition_by_test");
                    params.put("fmt", "parquet");
                    params.put("partition_by", "x");

                    String expectedError = "{\"query\":\"SELECT * FROM partition_by_test\",\"error\":\"partitionBy is temporarily not supported:x\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);
                });
    }

    @Test
    public void testParquetExportResumability() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create a larger dataset to test resumable sending
                    engine.execute("CREATE TABLE resumable_parquet_test AS (" +
                            "SELECT x as id, 'data_' || x as content, x * 2.5 as value, " +
                            "timestamp_sequence(0, 100000L) as ts " +
                            "FROM long_sequence(1000)" +
                            ")", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from resumable_parquet_test");
                    params.put("fmt", "parquet");
                    params.put("filename", "resumable_test");

                    // This test verifies that the parquet export can be properly initiated
                    // The actual resumable functionality is tested at the HTTP processor level
                    testHttpClient.assertGet("/exp", "PAR1\u0015\u0000\u0015", params, null, null);
                });
    }

    @Test
    public void testParquetExportRowGroupSizeInvalid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE row_group_invalid_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "SELECT * FROM row_group_invalid_test");
                    params.put("fmt", "parquet");
                    params.put("row_group_size", "not_a_number");

                    String expectedError = "{\"query\":\"SELECT * FROM row_group_invalid_test\",\"error\":\"invalid row group size:not_a_number\",\"position\":0}";
                    testHttpClient.assertGet("/exp", expectedError, params, null, null);
                });
    }

    @Test
    public void testParquetExportRowGroupSizeValid() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE row_group_valid_test AS (SELECT x FROM long_sequence(5000))", sqlExecutionContext);

                    params.clear();
                    params.put("query", "SELECT * FROM row_group_valid_test");
                    params.put("fmt", "parquet");
                    params.put("row_group_size", "1000");
                    testHttpClient.assertGetParquet("/exp", 44745, params, "SELECT * FROM row_group_valid_test");

                    params.put("row_group_size", "5000");
                    testHttpClient.assertGetParquet("/exp", 44121, params, "SELECT * FROM row_group_valid_test");
                });
    }

    @Test
    public void testParquetExportStatisticsEnabled() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE statistics_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("statistics_enabled", "true");
                    testHttpClient.assertGetParquet("/exp", 402, params, "SELECT * FROM statistics_test");

                    params.put("statistics_enabled", "false");
                    testHttpClient.assertGetParquet("/exp", 287, params, "SELECT * FROM statistics_test");
                });
    }

    @Test
    public void testParquetExportWithMultipleDataTypes() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    String tableName = "multi_type_parquet_test_" + System.currentTimeMillis();
                    engine.execute("CREATE TABLE " + tableName + " AS (" +
                            "SELECT " +
                            "x::int as int_col, " +
                            "x::long as long_col, " +
                            "x * 1.5 as double_col, " +
                            "(x % 2) = 0 as bool_col, " +
                            "'str_' || x as str_col, " +
                            "cast('sym_' || (x % 3) as symbol) as sym_col, " +
                            "timestamp_sequence(0, 1000000L) as ts_col " +
                            "FROM long_sequence(3)" +
                            ")", sqlExecutionContext);


                    testHttpClient.assertGetParquet("/exp", 1995, tableName);
                });
    }
}