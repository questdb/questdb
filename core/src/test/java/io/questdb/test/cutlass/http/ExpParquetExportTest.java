/*******************************************************************************
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

package io.questdb.test.cutlass.http;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.AbstractTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.test.tools.TestUtils.*;

public class ExpParquetExportTest extends AbstractBootstrapTest {

    private static String exportRoot;
    private static TestHttpClient testHttpClient;
    private final CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        testHttpClient = new TestHttpClient();
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractTest.tearDownStatic();
        Misc.free(testHttpClient);
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Before
    public void setUp() {
        super.setUp();
    }

    @Test
    public void testBasicParquetExport() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE basic_parquet_test AS (" +
                            "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                            "FROM long_sequence(5)" +
                            ")", sqlExecutionContext);
                    testHttpClient.assertGetParquet("/exp", 1293, "basic_parquet_test");

                    var sink = new StringSink();
                    printSqlToString(engine, sqlExecutionContext, "SELECT id FROM sys.copy_export_log limit 1", sink);
                    int idStart = sink.indexOf("\n") + 1;
                    var copyID = sink.subSequence(idStart, sink.length() - 1);

                    sink.clear();
                    sink.put("tmp_");
                    sink.put(copyID);

                    String copyIDStr = sink.toString();
                    Path path = Path.getThreadLocal(root);
                    path.concat("export");
                    path.concat(copyIDStr);

                    assertEventually(() -> Assert.assertFalse("Temporary export directory was not cleaned up", Files.exists(path.slash$())));
                });
    }

    @Test
    public void testBasics() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> testHttpClient.assertGetParquet(
                        "/exp",
                        "PAR1\u0015\u0000\u0015",
                        "generate_series(0, '1970-01-02', '1m');"
                ));
    }

    @Test
    public void testConcurrentParquetExports() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create test table
                    engine.execute("CREATE TABLE concurrent_export_test AS (" +
                            "SELECT x as id, 'data_' || x as content, x * 1.5 as value " +
                            "FROM long_sequence(1000)" +
                            ")", sqlExecutionContext);

                    int threadCount = 15;
                    CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    AtomicInteger successCount = new AtomicInteger(0);
                    AtomicInteger errorCount = new AtomicInteger(0);
                    Thread[] threads = new Thread[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        final int threadId = i;
                        threads[i] = new Thread(() -> {
                            HttpClient client = null;
                            try {
                                barrier.await();
                                client = HttpClientFactory.newPlainTextInstance();
                                HttpClient.Request req = client.newRequest("localhost", 9001);
                                req.GET().url("/exp")
                                        .query("query", "SELECT * FROM concurrent_export_test limit " + (10 * (threadId + 1)))
                                        .query("fmt", "parquet")
                                        .query("filename", "concurrent_test_" + threadId);
                                try (var respHeaders = req.send()) {
                                    respHeaders.await();
                                    TestUtils.assertEquals("200", respHeaders.getStatusCode());
                                }
                                successCount.incrementAndGet();
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                LOG.error().$(e).$();
                            } finally {
                                Misc.free(client);
                                Path.clearThreadLocals();
                            }
                        });
                        threads[i].start();
                    }

                    for (Thread thread : threads) {
                        thread.join();
                    }

                    Assert.assertEquals("Expected all concurrent exports to succeed", threadCount, successCount.get());
                    Assert.assertEquals("Expected no errors", 0, errorCount.get());
                });
    }

    @Test
    public void testConcurrentParquetExportsLargeData() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create larger test table
                    engine.execute("CREATE TABLE concurrent_large_test AS (" +
                            "SELECT x as id, 'data_string_' || x as content, x * 2.5 as value, " +
                            "timestamp_sequence(0, 100000L) as ts " +
                            "FROM long_sequence(5000)" +
                            ")", sqlExecutionContext);

                    int threadCount = 4;
                    CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    AtomicInteger successCount = new AtomicInteger(0);
                    Thread[] threads = new Thread[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        final int threadId = i;
                        threads[i] = new Thread(() -> {
                            HttpClient client = null;
                            try {
                                barrier.await();
                                client = HttpClientFactory.newPlainTextInstance();
                                HttpClient.Request req = client.newRequest("localhost", 9001);
                                req.GET().url("/exp")
                                        .query("query", "SELECT * FROM concurrent_large_test")
                                        .query("fmt", "parquet")
                                        .query("compression_codec", "gzip")
                                        .query("row_group_size", "1000")
                                        .query("filename", "concurrent_large_" + threadId);
                                try (var respHeaders = req.send()) {
                                    respHeaders.await();
                                    TestUtils.assertEquals("200", respHeaders.getStatusCode());
                                }
                                successCount.incrementAndGet();
                            } catch (Exception e) {
                                LOG.error().$(e).$();
                            } finally {
                                Misc.free(client);
                                Path.clearThreadLocals();
                            }
                        });
                        threads[i].start();
                    }

                    for (Thread thread : threads) {
                        thread.join();
                    }

                    Assert.assertEquals("Expected all concurrent large exports to succeed",
                            threadCount, successCount.get());
                });
    }

    @Test
    public void testConcurrentParquetExportsWithCancel() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    int threadCount = 3;
                    CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    AtomicInteger completedCount = new AtomicInteger(0);
                    Thread[] threads = new Thread[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        final int threadId = i;
                        threads[i] = new Thread(() -> {
                            HttpClient client = null;
                            try {
                                barrier.await();
                                client = HttpClientFactory.newPlainTextInstance();
                                HttpClient.Request req = client.newRequest("localhost", 9001);
                                req.GET().url("/exp")
                                        .query("query", "generate_series(0, '9999-01-01', '1U')")
                                        .query("fmt", "parquet")
                                        .query("filename", "concurrent_cancel_" + threadId)
                                        .query("row_group_size", "1000");

                                try (var ignore = req.send()) {

                                    // Let the export start
                                    Os.sleep(100);
                                }
                                completedCount.incrementAndGet();
                            } catch (Exception e) {
                                LOG.error().$(e).$();
                            } finally {
                                Misc.free(client);
                                Path.clearThreadLocals();
                            }
                        });
                        threads[i].start();
                    }

                    for (Thread thread : threads) {
                        thread.join();
                    }

                    // Wait for all exports to be cancelled
                    int maxWait = 100;
                    for (int i = 0; i < maxWait; i++) {
                        if (engine.getCopyExportContext().getActiveExportId() == -1) {
                            break;
                        }
                        Os.sleep(100);
                    }

                    Assert.assertEquals("Expected all threads to complete", threadCount, completedCount.get());
                    Assert.assertEquals("Expected no active exports after cancellation",
                            -1, engine.getCopyExportContext().getActiveExportId());
                });
    }

    @Test
    public void testConcurrentParquetExportsWithDifferentCompression() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create test table
                    engine.execute("CREATE TABLE concurrent_compression_test AS (" +
                            "SELECT x as id, 'data_' || x as content " +
                            "FROM long_sequence(500)" +
                            ")", sqlExecutionContext);

                    String[] codecs = {"gzip", "snappy", "uncompressed", "zstd"};
                    int threadCount = codecs.length;
                    CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    AtomicInteger successCount = new AtomicInteger(0);
                    Thread[] threads = new Thread[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        final int threadId = i;
                        final String codec = codecs[i];
                        threads[i] = new Thread(() -> {
                            HttpClient client = null;
                            try {
                                barrier.await();
                                client = HttpClientFactory.newPlainTextInstance();
                                HttpClient.Request req = client.newRequest("localhost", 9001);
                                req.GET().url("/exp")
                                        .query("query", "SELECT * FROM concurrent_compression_test")
                                        .query("fmt", "parquet")
                                        .query("compression_codec", codec)
                                        .query("filename", "concurrent_codec_" + threadId);
                                try (var respHeaders = req.send()) {
                                    respHeaders.await();
                                    TestUtils.assertEquals("200", respHeaders.getStatusCode());
                                }
                                successCount.incrementAndGet();
                            } catch (Exception e) {
                                LOG.error().$(e).$();
                            } finally {
                                Misc.free(client);
                                Path.clearThreadLocals();
                            }
                        });
                        threads[i].start();
                    }

                    for (Thread thread : threads) {
                        thread.join();
                    }

                    Assert.assertEquals("Expected all concurrent exports with different codecs to succeed",
                            threadCount, successCount.get());
                });
    }

    @Test
    public void testEmptyTable() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE codec_zstd_test AS (SELECT x FROM long_sequence(5))", sqlExecutionContext);
                    params.clear();
                    params.put("fmt", "parquet");
                    params.put("query", "SELECT * FROM codec_zstd_test where 1 = 2");
                    testHttpClient.assertGet(
                            "/exp",
                            "PAR1\u0015\u0002\u0019,H\u0019zzz.copy.0000000000000000\u0015\u0002\u0000\u0015\u0004%\u0002\u0018\u0001xU\u0000\u0000\u0016\u0000\u0019\f\u0019\u001C\u0018\u0007questdb\u00189{\"version\":1,\"schema\":[{\"column_type\":6,\"column_top\":0}]}\u0000\u0018\u0013QuestDB version 9.0\u0000",
                            params,
                            null,
                            null
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
                    String expectedCsv = """
                            "x"\r
                            1\r
                            2\r
                            3\r
                            """;

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
                    String expectedCsv = """
                            "x"\r
                            1\r
                            2\r
                            """;

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
    public void testExportDropConnection() throws Exception {
        assertMemoryLeak(() -> getExportTester()
                .runNoLeakCheck(null, (engine, sqlExecutionContext) -> {
                    HttpClient client = testHttpClient.getHttpClient();
                    HttpClient.Request req = client.newRequest("localhost", 9001);
                    req.GET().url("/exp").query("query", "generate_series(0, '9999-01-01', '1U');");
                    req.query("fmt", "parquet");
                    try (var ignore = req.send()) {
                        for (int i = 0; i < 50; i++) {
                            if (engine.getCopyExportContext().getActiveExportId() != -1) {
                                break;
                            }
                            Os.sleep(100);
                        }
                    }
                    client.disconnect();


                    int count = 100;
                    for (int i = 0; i < count; i++) {
                        if (engine.getCopyExportContext().getActiveExportId() == -1) {
                            break;
                        }
                        if (i == count - 1) {
                            Assert.fail("Failed to cancel export");
                        }
                        Os.sleep(100);
                    }
                }));
    }

    @Test
    public void testExportWithNowFunc() throws Exception {
        getExportTester().withMicrosecondClock(() -> 3000000L)
                .run((engine, sqlExecutionContext) -> {
                    sqlExecutionContext.setNowAndFixClock(1000000L, ColumnType.TIMESTAMP_MICRO);
                    engine.execute("CREATE TABLE basic_parquet_test AS (" +
                            "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                            "FROM long_sequence(5)" +
                            ")", sqlExecutionContext);
                    testHttpClient.assertGetParquet("/exp", 1262, "select * from basic_parquet_test where ts < now()");
                });
    }

    @Test
    public void testJsonConnectionCounterDoesNotGoNegativeWithConcurrentExports() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create a test table using the engine to ensure it's created successfully
                    engine.execute("CREATE TABLE test_json_conn_table AS (" +
                                    "SELECT x as id, 'test_' || x as name FROM long_sequence(100))",
                            sqlExecutionContext);

                    final int numExportThreads = 4;
                    final int numJsonThreads = 3;
                    final int requestsPerThread = 5;
                    final AtomicInteger errors = new AtomicInteger();
                    final ObjList<Thread> threads = new ObjList<>();
                    final CyclicBarrier expBarrier = new CyclicBarrier(numExportThreads);
                    final CyclicBarrier jsonBarrier = new CyclicBarrier(numJsonThreads);

                    // Start export threads
                    for (int i = 0; i < numExportThreads; i++) {
                        Thread thread = new Thread(() -> {
                            TestUtils.await(expBarrier);
                            try {
                                try (TestHttpClient client = new TestHttpClient()) {
                                    client.port = testHttpClient.port;
                                    client.setKeepConnection(true);
                                    CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                                    for (int n = 0; n < requestsPerThread; n++) {
                                        params.clear();
                                        params.put("fmt", "parquet");
                                        params.put("query", "test_json_conn_table");
                                        client.assertGetParquet("/exp", 926, params, null);
                                    }
                                }
                            } catch (Throwable e) {
                                LOG.error().$(e.getMessage()).$();
                                errors.incrementAndGet();
                            } finally {
                                Path.clearThreadLocals();
                            }
                        });
                        threads.add(thread);
                    }

                    // Start JSON query threads
                    for (int i = 0; i < numJsonThreads; i++) {
                        Thread thread = new Thread(() -> {
                            TestUtils.await(jsonBarrier);
                            try {
                                try (final HttpClient client = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                                    for (int n = 0; n < requestsPerThread; n++) {
                                        try (var respHeaders = client.newRequest("localhost", testHttpClient.port)
                                                .GET()
                                                .url("/exec")
                                                .query("query", "SELECT * FROM test_json_conn_table LIMIT 50")
                                                .send()) {
                                            respHeaders.await();
                                        }
                                    }
                                }
                            } catch (Throwable e) {
                                LOG.error().$(e.getMessage()).$();
                                errors.incrementAndGet();
                            }
                        });
                        threads.add(thread);
                    }

                    // Start all threads
                    for (int i = 0; i < threads.size(); i++) {
                        threads.getQuick(i).start();
                    }

                    // Wait for all threads to complete, then verify connection count returns to 0
                    try {
                        for (int i = 0; i < threads.size(); i++) {
                            threads.getQuick(i).join();
                        }
                    } finally {
                        threads.clear();
                    }

                    // All requests should have completed successfully
                    Assert.assertEquals("No errors should occur during concurrent export/json operations", 0, errors.get());
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
                    testHttpClient.assertGetParquet("/exp", 633, params, "test_table");
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

                    Thread thread = startCancelThread(engine, sqlExecutionContext);
                    thread.start();
                    String expectedError = "cancelled by user";
                    testHttpClient.assertGetContains("/exp", expectedError, params);
                    thread.join();
                });
    }

    @Test
    public void testParquetExportCancelNodelay() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    params.clear();
                    params.put("query", "generate_series(0, '9999-01-01', '1U')");
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "gzip");
                    params.put("row_group_size", "500");
                    params.put("rmode", "nodelay");

                    Thread thread = startCancelThread(engine, sqlExecutionContext);
                    thread.start();
                    String expectedError = "cancelled by user";
                    try {
                        testHttpClient.assertGetContains("/exp", expectedError, params);
                        Assert.fail("server should disconnect");
                    } catch (HttpClientException e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }
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
                    params.put("query", "SELECT * FROM codec_zstd_test");

                    params.put("compression_codec", "lz4_raw");
                    testHttpClient.assertGet(
                            "/exp",
                            "PAR1\u0015\u0000\u0015\\\u0015N,\u0015\n" +
                                    "\u0015\u0000\u0015\u0006\u0015\u0006\u001C6\u0000(\b\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018\b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
                            params,
                            null,
                            null
                    );

                    params.put("compression_codec", "lzo");
                    testHttpClient.assertGet(
                            "/exp",
                            "{\"query\":\"SELECT * FROM codec_zstd_test\"," +
                                    "\"error\":\"invalid compression codec[lzo], expected one of: uncompressed, snappy, gzip, brotli, zstd, lz4_raw\"," +
                                    "\"position\":0}",
                            params,
                            null,
                            null
                    );

                    params.put("compression_codec", "lz4");
                    testHttpClient.assertGet(
                            "/exp",
                            "{\"query\":\"SELECT * FROM codec_zstd_test\"," +
                                    "\"error\":\"invalid compression codec[lz4], expected one of: uncompressed, snappy, gzip, brotli, zstd, lz4_raw\"," +
                                    "\"position\":0}",
                            params,
                            null,
                            null
                    );

                    params.put("compression_codec", "brotli");
                    testHttpClient.assertGet(
                            "/exp",
                            "PAR1\u0015\u0000\u0015\\\u00150,\u0015\n" +
                                    "\u0015\u0000\u0015\u0006\u0015\u0006\u001C6\u0000(\b\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018\b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
                            params,
                            null,
                            null
                    );

                    // Check nodealy when client sends PAR first produces exactly same output
                    params.put("rmode", "nodelay");
                    params.put("compression_codec", "brotli");
                    testHttpClient.assertGet(
                            "/exp",
                            "PAR1\u0015\u0000\u0015\\\u00150,\u0015\n" +
                                    "\u0015\u0000\u0015\u0006\u0015\u0006\u001C6\u0000(\b\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018\b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
                            params,
                            null,
                            null
                    );

                    params.put("compression_codec", "lz4_raw");
                    testHttpClient.assertGet(
                            "/exp",
                            "PAR1\u0015\u0000\u0015\\\u0015N,\u0015\n" +
                                    "\u0015\u0000\u0015\u0006\u0015\u0006\u001C6\u0000(\b\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018\b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
                            params,
                            null,
                            null
                    );

                    params.put("parquet_version", "2");
                    params.put("compression_codec", "brotli");
                    testHttpClient.assertGet(
                            "/exp",
                            "PAR1\u0015\u0006\u0015T\u0015*\\\u0015\n\u0015\u0000\u0015\n\u0015\u0000\u0015\u0004\u0015\u0000\u0011\u001C6\u0000(\b\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018\b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0003\u001F",
                            params,
                            null,
                            null
                    );
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
                    params.put("compression_level", "0");
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
                    testHttpClient.assertGetParquet("/exp", 400, params, "SELECT * FROM codec_snappy_test");
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
                    testHttpClient.assertGetParquet("/exp", 406, params, "SELECT * FROM codec_uncompressed_test");
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
                    testHttpClient.assertGetParquet("/exp", 405, params, "SELECT * FROM codec_zstd_test");
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
                    testHttpClient.assertGetParquet("/exp", 400, params, "SELECT * FROM level_valid_test");

                    params.put("compression_codec", "zstd");
                    params.put("compression_level", "5");
                    testHttpClient.assertGetParquet("/exp", 405, params, "SELECT * FROM level_valid_test");

                    params.put("compression_codec", "zstd");
                    params.put("compression_level", "15");
                    testHttpClient.assertGetParquet("/exp", 399, params, "SELECT * FROM level_valid_test");
                });
    }

    @Test
    public void testParquetExportCopyRootNotSet() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation),
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "0.0.0.0:0",
                    PropertyKey.LINE_TCP_ENABLED.toString(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                    PropertyKey.HTTP_SECURITY_READONLY.getEnvVarName(), "true",
                    PropertyKey.QUERY_TRACING_ENABLED.getEnvVarName(), "false",
                    PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT.getEnvVarName(), ""
            )) {
                serverMain.execute("CREATE TABLE basic_parquet_test AS (" +
                        "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                        "FROM long_sequence(5)" +
                        ")");

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.GET()
                            .url("/exp")
                            .query("fmt", "parquet")
                            .query("query", "basic_parquet_test");

                    try (var headers = request.send()) {
                        headers.await();
                        TestUtils.assertEquals("400", headers.getStatusCode());
                    }
                }
            }
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
                    testHttpClient.assertGetParquet("/exp", 10497, params, "SELECT * FROM page_size_valid_test");
                    params.put("data_page_size", "2048");
                    testHttpClient.assertGetParquet("/exp", 7997, params, "SELECT * FROM page_size_valid_test");
                });
    }

    @Test
    public void testParquetExportDisabledReadOnlyInstance() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation),
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "0.0.0.0:0",
                    PropertyKey.LINE_TCP_ENABLED.toString(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                    PropertyKey.QUERY_TRACING_ENABLED.getEnvVarName(), "false"
            )) {
                serverMain.execute("CREATE TABLE basic_parquet_test AS (" +
                        "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                        "FROM long_sequence(5)" +
                        ")");
            }

            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation),
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "0.0.0.0:0",
                    PropertyKey.LINE_TCP_ENABLED.toString(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                    PropertyKey.READ_ONLY_INSTANCE.getEnvVarName(), "true",
                    PropertyKey.QUERY_TRACING_ENABLED.getEnvVarName(), "false"
            )) {
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.GET()
                            .url("/exp")
                            .query("fmt", "parquet")
                            .query("query", "basic_parquet_test");

                    try (var headers = request.send()) {
                        headers.await();
                        TestUtils.assertEquals("400", headers.getStatusCode());
                    }
                }

                // Check no regression, table cannot be created on read-only instance
                try {
                    serverMain.execute("create table test (x int)");
                    Assert.fail("read only should fail creating a table");
                } catch (AssertionError ex) {
                    TestUtils.assertContains(ex.getMessage(), "Could not create table, instance is read only");
                }
            }
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
                    String expectedError = "{\"query\":\"SELECT * FROM codec_invalid_test\",\"error\":\"invalid compression codec[invalid_codec], expected one of: uncompressed, snappy, gzip, brotli, zstd, lz4_raw\",\"position\":0}";
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
                    testHttpClient.assertGetParquet("/exp", 259695, params, "SELECT * FROM large_export_test");
                });
    }

    @Test
    public void testParquetExportLimit() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            int requests = 30;
            int requestExpLimit = 2;
            int requestJsonLimit = 2;

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


            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation),
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "0.0.0.0:0",
                    PropertyKey.LINE_TCP_ENABLED.toString(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                    PropertyKey.HTTP_MIN_ENABLED.getPropertyPath(), "false",
                    PropertyKey.HTTP_EXPORT_CONNECTION_LIMIT.getEnvVarName(), String.valueOf(requestExpLimit),
                    PropertyKey.HTTP_JSON_QUERY_CONNECTION_LIMIT.getEnvVarName(), String.valueOf(requestJsonLimit)
            )) {
                serverMain.execute("CREATE TABLE basic_parquet_test AS (" +
                        "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                        "FROM long_sequence(5)" +
                        ")");
                serverMain.execute("CREATE TABLE multiple_options_test AS (SELECT x FROM long_sequence(5000))");

                // Exceed the limit
                ObjList<HttpClient.ResponseHeaders> respHeaders = new ObjList<>();
                ObjList<HttpClient> clients = new ObjList<>();
                int requestLimit = requestExpLimit + 4;

                try {
                    params.clear();
                    params.put("query", "multiple_options_test");
                    params.put("fmt", "parquet");
                    params.put("compression_codec", "gzip");
                    params.put("compression_level", "6");
                    params.put("row_group_size", "2000");
                    params.put("data_page_size", "1024");
                    params.put("statistics_enabled", "true");
                    params.put("parquet_version", "2");
                    params.put("raw_array_encoding", "false");

                    try {
                        for (int i = 0; i < requestLimit; i++) {
                            HttpClient client = HttpClientFactory.newPlainTextInstance();
                            clients.add(client);
                            HttpClient.ResponseHeaders resp = startExport(client, serverMain, params);
                            respHeaders.add(resp);
                        }
                    } finally {
                        for (int i = 0; i < respHeaders.size(); i++) {
                            respHeaders.get(i).await();
                        }
                        for (int i = 0; i < clients.size(); i++) {
                            clients.get(i).close();
                        }
                        clients.clear();
                    }

                    assertEventually(() ->
                            Assert.assertEquals(
                                    0,
                                    serverMain.getActiveConnectionCount(ActiveConnectionTracker.PROCESSOR_EXPORT_HTTP
                                    )
                            )
                    );

                    LOG.info().$("=========== testing limit reached but not exceeded, limit: ").$(requestExpLimit).$();
                    AtomicInteger errors = new AtomicInteger();

                    AtomicInteger connectionExpCount = new AtomicInteger();
                    ObjList<Thread> threads = new ObjList<>();
                    for (int i = 0; i < requestExpLimit; i++) {
                        Thread thread = new Thread(() -> {
                            try {
                                for (int n = 0; n < requests; n++) {
                                    // Http server may take time to close the connection, so we need to wait
                                    // until the active connection count is less than the limit
                                    connectionExpCount.incrementAndGet();
                                    while (serverMain.getActiveConnectionCount(ActiveConnectionTracker.PROCESSOR_EXPORT_HTTP) + connectionExpCount.get() > requestExpLimit) {
                                        Os.pause();
                                    }

                                    try (TestHttpClient client = new TestHttpClient()) {
                                        client.port = serverMain.getHttpServerPort();
                                        CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
                                        params.put("fmt", "parquet");
                                        params.put("query", "basic_parquet_test");
                                        client.assertGetParquet("/exp", 1293, params, null);
                                    } catch (Throwable ex) {
                                        LOG.error().$(ex.getMessage()).$();
                                        errors.incrementAndGet();
                                        throw ex;
                                    }

                                    connectionExpCount.decrementAndGet();
                                }
                            } catch (Exception e) {
                                LOG.error().$(e.getMessage()).$();
                                errors.incrementAndGet();
                                throw new RuntimeException(e);
                            } finally {
                                Path.clearThreadLocals();
                            }
                        });
                        threads.add(thread);
                    }

                    AtomicInteger connectionJsonCount = new AtomicInteger();
                    for (int i = 0; i < requestJsonLimit; i++) {
                        Thread thread = new Thread(() -> {
                            for (int n = 0; n < requests; n++) {
                                // Http server may take time to close the connection, so we need to wait
                                // until the active connection count is less than the limit
                                connectionJsonCount.incrementAndGet();
                                while (serverMain.getActiveConnectionCount(ActiveConnectionTracker.PROCESSOR_JSON) + connectionJsonCount.get() > requestJsonLimit) {
                                    Os.pause();
                                }

                                try (TestHttpClient client = new TestHttpClient()) {
                                    client.port = serverMain.getHttpServerPort();
                                    client.assertGet("/exec",
                                            "{\"query\":\"basic_parquet_test\",\"columns\":[{\"name\":\"id\",\"type\":\"LONG\"},{\"name\":\"name\",\"type\":\"STRING\"},{\"name\":\"value\",\"type\":\"DOUBLE\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"}],\"timestamp\":-1,\"dataset\":[[1,\"test_1\",1.5,\"1970-01-01T00:00:00.000000Z\"],[2,\"test_2\",3.0,\"1970-01-01T00:00:01.000000Z\"],[3,\"test_3\",4.5,\"1970-01-01T00:00:02.000000Z\"],[4,\"test_4\",6.0,\"1970-01-01T00:00:03.000000Z\"],[5,\"test_5\",7.5,\"1970-01-01T00:00:04.000000Z\"]],\"count\":5}",
                                            "basic_parquet_test",
                                            "localhost",
                                            serverMain.getHttpServerPort(),
                                            null,
                                            null,
                                            null
                                    );
                                } catch (Throwable ex) {
                                    LOG.error().$(ex.getMessage()).$();
                                    errors.incrementAndGet();
                                    throw new RuntimeException(ex);
                                }
                                connectionJsonCount.decrementAndGet();
                            }
                        });
                        threads.add(thread);
                    }

                    for (int i = 0; i < threads.size(); i++) {
                        threads.get(i).start();
                    }
                    for (int i = 0; i < threads.size(); i++) {
                        threads.get(i).join();
                    }
                    Assert.assertEquals(0, errors.get());
                } finally {
                    for (int i = 0; i < clients.size(); i++) {
                        clients.get(i).close();
                    }
                }
            }
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
                    testHttpClient.assertGetParquet("/exp", 405, params, "SELECT * FROM version_valid_test");
                    params.put("parquet_version", "2");
                    testHttpClient.assertGetParquet("/exp", 406, params, "SELECT * FROM version_valid_test");
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
    public void testParquetExportReadOnlyHttp() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation),
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "0.0.0.0:0",
                    PropertyKey.LINE_TCP_ENABLED.toString(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                    PropertyKey.HTTP_SECURITY_READONLY.getEnvVarName(), "true",
                    PropertyKey.QUERY_TRACING_ENABLED.getEnvVarName(), "false",
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getPropertyPath(), Integer.toString(fragmentation),
                    PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getPropertyPath(), Integer.toString(fragmentation),
                    PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT.getEnvVarName(), exportRoot
            )) {
                serverMain.execute("CREATE TABLE basic_parquet_test AS (" +
                        "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                        "FROM long_sequence(5)" +
                        ")");

                try (var httpClient = new TestHttpClient()) {
                    httpClient.assertGetParquet(serverMain.getHttpServerPort(), "/exp", "200", 1293, "basic_parquet_test");
                }
            }
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
                    testHttpClient.assertGetParquet("/exp", 6644, params, "SELECT * FROM row_group_valid_test");

                    params.put("row_group_size", "5000");
                    testHttpClient.assertGetParquet("/exp", 5511, params, "SELECT * FROM row_group_valid_test");
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
                    testHttpClient.assertGetParquet("/exp", 405, params, "SELECT * FROM statistics_test");

                    params.put("statistics_enabled", "false");
                    testHttpClient.assertGetParquet("/exp", 290, params, "SELECT * FROM statistics_test");
                });
    }

    @Test
    public void testParquetExportTimeout() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    params.clear();
                    params.put("query", "generate_series(0, '9999-01-01', '1U')");
                    params.put("fmt", "parquet");
                    params.put("timeout", "1");
                    String expectedError = "timeout, query aborted";
                    testHttpClient.assertGetContains("/exp", expectedError, params);
                });
    }

    @Test
    public void testParquetExportTimeoutNodelay() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    params.clear();
                    params.put("query", "generate_series(0, '9999-01-01', '1U')");
                    params.put("fmt", "parquet");
                    params.put("timeout", "1");
                    params.put("rmode", "nodelay");

                    try {
                        testHttpClient.assertGetContains("/exp", "nothing", params);
                        Assert.fail();
                    } catch (HttpClientException e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }

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


                    testHttpClient.assertGetParquet("/exp", 2075, tableName);
                });
    }

    private static @NotNull Thread startCancelThread(CairoEngine engine, SqlExecutionContext sqlExecutionContext) {
        return new Thread(() -> {
            try {
                long copyID;
                do {
                    copyID = engine.getCopyExportContext().getActiveExportId();
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
                    copyID = engine.getCopyExportContext().getActiveExportId();
                } while (copyID != -1);
            } finally {
                Path.clearThreadLocals();
            }
        });
    }

    private static HttpClient.ResponseHeaders startExport(
            HttpClient client,
            TestServerMain serverMain,
            @Nullable CharSequenceObjHashMap<String> queryParams
    ) {
        HttpClient.Request req = client.newRequest("localhost", serverMain.getHttpServerPort());
        req.GET().url("/exp");
        req.query("query", "multiple_options_test");
        if (queryParams != null) {
            for (int i = 0, n = queryParams.size(); i < n; i++) {
                CharSequence name = queryParams.keys().getQuick(i);
                req.query(name, queryParams.get(name));
            }
        }
        return req.send();
    }
}