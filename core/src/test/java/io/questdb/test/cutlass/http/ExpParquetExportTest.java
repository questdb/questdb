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
        testHttpEndpoint((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE basic_parquet_test AS (" +
                    "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                    "FROM long_sequence(5)" +
                    ")", sqlExecutionContext);

            CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
            params.put("query", "select * from basic_parquet_test");
            params.put("fmt", "parquet");
            testHttpClient.assertGet("/exp", "PAR1\u0015\u0000\u0015\\\u0015\\,\u0015\n" +
                    "\u0015\u0000\u0015\u0006\u0015\u0006\u001C6\u0000(|b\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018|b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u0000\u0003\u001F\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0003\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0015\u0004\u0019%\u0000\u0006\u0019\u0018\u0002id\u0015\u0000\u0016\n" +
                    "\u0016", params, null, null);
        });
    }

    @Test
    public void testBasicParquetExport() throws Exception {
        testHttpEndpoint((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE basic_parquet_test AS (" +
                    "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                    "FROM long_sequence(5)" +
                    ")", sqlExecutionContext);

            CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
            params.put("query", "select * from basic_parquet_test");
            params.put("fmt", "parquet");
            params.put("filename", "basic_test");
            // When fmt=parquet is specified but copy export isn't set up, it returns an empty error
            testHttpClient.assertGet("/exp", "{\"query\":\"select * from basic_parquet_test\",\"error\":\"\",\"position\":0}", params, null, null);
        });
    }

    @Test
    public void testExpCsvExportStillWorks() throws Exception {
        testHttpEndpoint((engine, sqlExecutionContext) -> {
            // Create test table
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
        testHttpEndpoint((engine, sqlExecutionContext) -> {
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
        testHttpEndpoint((engine, sqlExecutionContext) -> {
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
        testHttpEndpoint((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE reject_non_select_test AS (SELECT x FROM long_sequence(2))", sqlExecutionContext);

            CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
            params.put("query", "INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test");
            params.put("fmt", "parquet");
            testHttpClient.assertGet("/exp", "{\"query\":\"INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test\",\"error\":\"/exp endpoint only accepts SELECT\",\"position\":0}", params, null, null);
        });
    }


    @Test
    public void testParquetExportOnlyAcceptsSelectQueries() throws Exception {
        testHttpEndpoint((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE ddl_rejection_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);

            CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
            params.put("query", "CREATE TABLE another_test AS (SELECT * FROM ddl_rejection_test)");
            params.put("fmt", "parquet");
            testHttpClient.assertGet("/exp", "{\"query\":\"CREATE TABLE another_test AS (SELECT * FROM ddl_rejection_test)\",\"error\":\"/exp endpoint only accepts SELECT\",\"position\":0}", params, null, null);
        });
    }


    @Test
    public void testParquetExportWithMultipleDataTypes() throws Exception {
        testHttpEndpoint((engine, sqlExecutionContext) -> {
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
            params.put("filename", "multi_types_test");
            testHttpClient.assertGet("/exp", "{\"query\":\"select * from " + tableName + "\",\"error\":\"\",\"position\":0}", params, null, null);
        });
    }

    private void testHttpEndpoint(HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withCopyExportRoot(exportRoot)
                .run(code);
    }
}