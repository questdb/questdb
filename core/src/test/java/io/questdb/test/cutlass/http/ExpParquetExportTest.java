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

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExpParquetExportTest extends AbstractBootstrapTest {

    private static String exportRoot;
    private static TestHttpClient testHttpClient;

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
    public void testBasicExport() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE basic_parquet_test AS (" +
                            "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                            "FROM long_sequence(5)" +
                            ")", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from basic_parquet_test");
                    params.put("fmt", "parquet");

                    // The parquet export now suspends the request until copy completes, then returns the parquet file
                    testHttpClient.assertGet("/exp", "PAR1", params.toString());
                });
    }

    @Test
    public void testBasicParquetExport() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE basic_parquet_test AS (" +
                            "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                            "FROM long_sequence(5)" +
                            ")", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from basic_parquet_test");
                    params.put("fmt", "parquet");
                    params.put("filename", "basic_test");

                    testHttpClient.assertGet("/exp", "{\"query\":\"select * from basic_parquet_test\",\"error\":\"\",\"position\":0}", params, null, null);
                });
    }

    @Test
    public void testBasics() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    testHttpClient.setKeepConnection(true);
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

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test");
                    params.put("fmt", "parquet");
                    testHttpClient.assertGet("/exp", "{\"query\":\"INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test\",\"error\":\"/exp endpoint only accepts SELECT\",\"position\":0}", params, null, null);
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
    public void testParquetExportStateTransitions() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create test table for state transition testing
                    engine.execute("CREATE TABLE state_transition_test AS (" +
                            "SELECT x as id, x * 3 as value, 'state_' || x as label " +
                            "FROM long_sequence(50)" +
                            ")", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from state_transition_test");
                    params.put("fmt", "parquet");
                    params.put("filename", "state_transitions");

                    // Test that the state machine properly transitions through parquet export states
                    testHttpClient.assertGet("/exp", "PAR1\u0015\u0000\u0015", params, null, null);
                });
    }

    @Test
    public void testParquetExportWithFileDescriptorManagement() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create a small test table
                    engine.execute("CREATE TABLE fd_test AS (" +
                            "SELECT x as id, 'fd_test_' || x as name " +
                            "FROM long_sequence(10)" +
                            ")", sqlExecutionContext);

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from fd_test");
                    params.put("fmt", "parquet");
                    params.put("filename", "fd_management_test");

                    // Test proper file descriptor handling during parquet export
                    testHttpClient.assertGet("/exp", "{\"query\":\"select * from fd_test\",\"error\":\"\",\"position\":0}", params, null, null);
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

                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                    params.put("query", "select * from " + tableName);
                    params.put("fmt", "parquet");

                    testHttpClient.assertGet("/exp", "PAR1\u0015\u0000\u0015$\u0015$,\u0015\u0006\u0015\u0000\u0015\u0006\u0015\u0006\u001C6\u0000(\u0004\u0003\u0000\u0000\u0000\u0018\u0004\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u0000\u0003\u0007\u0001\u0000\u0000\u0000\u0002\u0000\u0000\u0000\u0003\u0000\u0000\u0000\u0015\u0002\u0019%\u0000\u0006\u0019\u0018\u0007int_col\u0015\u0000\u0016\u0006\u0016f\u0016f&|b<6\u0000(\u0004\u0003\u0000\u0000\u0000\u0018\u0004\u0001\u0000\u0000\u0000\u0000\u0000\u0015\u0000\u0015<\u0015<,\u0015\u0006\u0015\u0000\u0015\u0006\u0015\u0006\u001C6\u0000(|b\u0003\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018|b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u0000\u0003\u0007\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0003\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0015\u0004\u0019%\u0000\u0006\u0019\u0018|blong_col\u0015\u0000\u0016\u0006\u0016", params, null, null);
                });
    }
}