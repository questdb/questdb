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
import io.questdb.cairo.CairoConfiguration;
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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.AbstractTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
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
    private Rnd rnd;

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
        rnd = TestUtils.generateRandom(LOG);
    }

    @Test
    public void testBasicParquetExport() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE basic_parquet_test AS (" +
                            "SELECT x as id, 'test_' || x as name, x * 1.5 as value, timestamp_sequence(0, 1000000L) as ts " +
                            "FROM long_sequence(5)" +
                            ")", sqlExecutionContext);
                    testHttpClient.assertGetParquet("/exp", 1231, "basic_parquet_test");

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
                .run((engine, sqlExecutionContext) ->
                        testHttpClient.assertGetParquet("/exp", 44690, params, "generate_series(0, '1970-01-02', '1m');"));
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
                                    respHeaders.getResponse().discard();
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
                                    respHeaders.getResponse().discard();
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
                                    respHeaders.getResponse().discard();
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
                    // Validate the response is valid Parquet with expected metadata.
                    // Exact binary layout may differ between export paths.
                    testHttpClient.assertGetContains(
                            "/exp",
                            "{\"version\":1,\"schema\":[{\"column_type\":6,\"column_top\":0}]}",
                            params
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
    public void testExportParquetFuzz() throws Exception {
        getExportTester()
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> {
                            engine.execute("""
                                    create table xyz as (select
                                        rnd_int() a,
                                        rnd_double() b,
                                        timestamp_sequence(0,1000) ts
                                        from long_sequence(1000)
                                    ) timestamp(ts) partition by hour""");

                            String[] queries = new String[]{
                                    "select count() from xyz",
                                    "select a from xyz limit 1",
                                    "select b from xyz limit 5",
                                    "select ts, b from xyz limit 150",
                            };
                            assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, 100, 1191);
                        }
                );
    }

    @Test
    public void testExportWithAddColumn() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
                    engine.execute("insert into test_table values ('2020-01-01T00:00:00.000000Z', 0), ('2020-01-02T00:00:00.000000Z', 1), ('2020-01-03T00:00:00.000000Z', 3)");
                    drainWalQueue(engine);
                    engine.execute("alter table test_table add column y int");
                    drainWalQueue(engine);
                    engine.execute("insert into test_table values ('2020-01-01T00:00:00.000001Z', 4, 100), ('2020-01-01T00:00:00.000002Z', 5, 101), ('2020-01-03T00:00:00.000000Z', 6, 102)");
                    drainWalQueue(engine);
                    params.clear();
                    params.put("fmt", "parquet");
                    testHttpClient.assertGetParquet("/exp", 1578, params, "test_table");
                });
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
                    testHttpClient.assertGetParquet("/exp", 1189, "select * from basic_parquet_test where ts < now()");
                });
    }

    @Test
    public void testExportWithProjection() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
                    engine.execute("insert into test_table values ('2020-01-01T00:00:00.000000Z', 0), ('2020-01-02T00:00:00.000000Z', 1), ('2020-01-03T00:00:00.000000Z', 2)");
                    drainWalQueue(engine);
                    params.clear();
                    params.put("fmt", "parquet");
                    testHttpClient.assertGetParquet("/exp", 869, params, "select x, ts from test_table");
                });
    }

    @Test
    public void testExportWithTimestampDescending() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("create table test_table (ts TIMESTAMP, x int)");
                    engine.execute("insert into test_table values ('2020-01-01T00:00:00.000000Z', 0), ('2020-01-02T00:00:00.000000Z', 1), ('2020-01-03T00:00:00.000000Z', 2)");
                    drainWalQueue(engine);
                    params.clear();
                    params.put("fmt", "parquet");
                    testHttpClient.assertGetParquet("/exp", 607, params, "select * from test_table order by ts desc");
                    testHttpClient.assertGetParquet("/exp", 590, params, "select * from test_table order by ts desc limit 2");
                });
    }

    @Test
    public void testExportWithoutTimestamp() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("create table test_table (ts TIMESTAMP, x int)");
                    engine.execute("insert into test_table values ('2020-01-01T00:00:00.000000Z', 0), ('2020-01-02T00:00:00.000000Z', 1), ('2020-01-03T00:00:00.000000Z', 2)");
                    drainWalQueue(engine);
                    params.clear();
                    params.put("fmt", "parquet");
                    testHttpClient.assertGetParquet("/exp", 602, params, "test_table");
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
                                        client.assertGetParquet("/exp", 1683, params, null);
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
                    engine.execute("create table test_table (ts TIMESTAMP, x int, sym symbol) timestamp(ts) partition by day wal;");
                    engine.execute("insert into test_table " +
                            "select dateadd('d', ((x-1)/1000)::int, '2020-01-01T00:00:00.000000Z'::timestamp) + ((x-1) % 1000) * 1000000L, " +
                            "x::int, " +
                            "case when x % 2 = 0 then 'symA' else 'symB' end " +
                            "from long_sequence(10000)");
                    drainWalQueue(engine);
                    assertSql(
                            engine,
                            sqlExecutionContext,
                            "select day(ts), count(*) from test_table group by day(ts) order by day(ts)",
                            new StringSink(),
                            """
                                    day	count
                                    1	1000
                                    2	1000
                                    3	1000
                                    4	1000
                                    5	1000
                                    6	1000
                                    7	1000
                                    8	1000
                                    9	1000
                                    10	1000
                                    """
                    );
                    engine.execute("alter table test_table convert partition to parquet where ts in '2020-01-01'");
                    engine.execute("alter table test_table convert partition to parquet where ts in '2020-01-03'");
                    engine.execute("alter table test_table convert partition to parquet where ts in '2020-01-05'");
                    engine.execute("alter table test_table convert partition to parquet where ts in '2020-01-07'");
                    engine.execute("alter table test_table convert partition to parquet where ts in '2020-01-10'");
                    drainWalQueue(engine);
                    params.clear();
                    params.put("fmt", "parquet");
                    testHttpClient.assertGetParquet("/exp", 103501, params, "test_table");
                    params.put("row_group_size", "1000");
                    testHttpClient.assertGetParquet("/exp", 107317, params, "test_table");
                    params.put("row_group_size", "500");
                    testHttpClient.assertGetParquet("/exp", 113699, params, "test_table");
                    params.put("row_group_size", "999");
                    testHttpClient.assertGetParquet("/exp", 109701, params, "test_table");
                    params.put("row_group_size", "201");
                    testHttpClient.assertGetParquet("/exp", 134752, params, "test_table");
                    params.put("row_group_size", "2001");
                    testHttpClient.assertGetParquet("/exp", 106002, params, "test_table");
                    params.put("row_group_size", "10000");
                    testHttpClient.assertGetParquet("/exp", 103501, params, "test_table");
                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, new String[]{"test_table"}, 10, 10091);
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
                    // With direct export (no temp table), data may start streaming before
                    // the cancel arrives, causing the server to disconnect.
                    try {
                        testHttpClient.assertGetContains("/exp", "cancelled by user", params);
                    } catch (HttpClientException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(
                                "unexpected error: " + msg,
                                msg.contains("peer disconnect") || msg.contains("malformed chunk")
                        );
                    }
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
                        // Cancel during streaming can cause either a clean disconnect
                        // or a malformed chunk (if data was already partially sent)
                        String msg = e.getMessage();
                        Assert.assertTrue(
                                "expected 'peer disconnect' or 'malformed chunk' but got: " + msg,
                                msg.contains("peer disconnect") || msg.contains("malformed chunk")
                        );
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
                    params.put("compression_level", "9");
                    testHttpClient.assertGet(
                            "/exp",
                            "PAR1\u0015\u0000\u0015\\\u0015.,\u0015\n" +
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
                            "PAR1\u0015\u0000\u0015\\\u0015.,\u0015\n" +
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
                            "PAR1\u0015\u0006\u0015T\u0015*\\\u0015\n\u0015\u0000\u0015\n\u0015\u0000\u0015\u0004\u0015\u0000\u0011\u001c6\u0000(\b\u0005\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0018\b\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\n\u0001",
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
                    testHttpClient.assertGetParquet("/exp", 375, params, "SELECT * FROM codec_snappy_test");
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
                    testHttpClient.assertGetParquet("/exp", 381, params, "SELECT * FROM codec_uncompressed_test");
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
                    params.put("compression_level", "9");
                    testHttpClient.assertGetParquet("/exp", 380, params, "SELECT * FROM codec_zstd_test");
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
                    testHttpClient.assertGetParquet("/exp", 375, params, "SELECT * FROM level_valid_test");

                    params.put("compression_codec", "zstd");
                    params.put("compression_level", "5");
                    testHttpClient.assertGetParquet("/exp", 380, params, "SELECT * FROM level_valid_test");

                    params.put("compression_codec", "zstd");
                    params.put("compression_level", "15");
                    testHttpClient.assertGetParquet("/exp", 374, params, "SELECT * FROM level_valid_test");
                });
    }

    @Test
    public void testParquetExportCursorBasedMultipleRowGroups() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // CROSS JOIN produces a CURSOR_BASED factory (no page frame support).
                    // 10 x 500 = 5000 rows with computed columns and small row groups
                    // exercises the buffer pinning fix in the cursor-based path.
                    engine.execute("""
                            CREATE TABLE cb_t1 AS (
                                SELECT x AS a FROM long_sequence(10)
                            )""", sqlExecutionContext);
                    engine.execute("""
                            CREATE TABLE cb_t2 AS (
                                SELECT x AS b,
                                rnd_str(5, 10, 0) AS s,
                                rnd_varchar(5, 10, 0) AS vc,
                                rnd_double_array(1, 5) AS arr
                                FROM long_sequence(500)
                            )""", sqlExecutionContext);

                    String[] queries = {
                            "SELECT cb_t1.a + cb_t2.b AS sum_ab, cb_t2.s FROM cb_t1 CROSS JOIN cb_t2",
                            "SELECT cb_t1.a * cb_t2.b AS product, cb_t2.s FROM cb_t1 CROSS JOIN cb_t2",
                            // VARCHAR and ARRAY through cursor-based buffers
                            "SELECT cb_t1.a + cb_t2.b AS sum_ab, cb_t2.vc FROM cb_t1 CROSS JOIN cb_t2",
                            "SELECT cb_t1.a + cb_t2.b AS sum_ab, cb_t2.arr FROM cb_t1 CROSS JOIN cb_t2",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length * 3, 200);
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
                    testHttpClient.assertGetParquet("/exp", 24145, params, "SELECT * FROM page_size_valid_test");
                    params.put("data_page_size", "2048");
                    testHttpClient.assertGetParquet("/exp", 22694, params, "SELECT * FROM page_size_valid_test");
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
                    testHttpClient.assertGetParquet("/exp", 927480, params, "SELECT * FROM large_export_test");
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
                                        client.assertGetParquet("/exp", 1231, params, null);
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

    /**
     * Tests streaming parquet export via page frame cursor with various column types.
     * Uses timestamp filtering to skip rows at the start, which still supports
     * page frame cursor (unlike LIMIT which requires temp table).
     * Uses odd row counts to create unaligned tails for SIMD processing.
     */
    @Test
    public void testParquetExportPageFrameAllTypes() throws Exception {
        getExportTesterPageFrame()
                .run((engine, sqlExecutionContext) -> {
                    // Create table with all fixed-size numeric types
                    // 1000 rows with 1 second intervals = ~16 minutes of data in one partition
                    engine.execute("CREATE TABLE pageframe_test AS (" +
                            "SELECT " +
                            "x::byte as byte_col, " +
                            "x::short as short_col, " +
                            "x::int as int_col, " +
                            "x::long as long_col, " +
                            "(x * 1.5)::float as float_col, " +
                            "x * 1.5 as double_col, " +
                            "cast(null as double) double_null, " +
                            "cast(null as float) float_null, " +
                            "cast(null as int) int_null, " +
                            "cast(null as long) long_null, " +
                            "cast(null as timestamp) timestamp_null, " +
                            "timestamp_sequence('2020-01-01T00:00:00', 1000000L) as ts " +
                            "FROM long_sequence(1000)" +
                            ") timestamp(ts) partition by day", sqlExecutionContext);

                    // Use timestamp filtering to skip rows - this still supports page frame cursor
                    // Each filter skips different number of rows, creating odd result counts
                    String[] queries = {
                            // Skip 1 row (ts > row 1's timestamp), 999 rows result (odd)
                            "SELECT byte_col, int_col, timestamp_null, long_null, int_null, float_null FROM pageframe_test WHERE ts > '2020-01-01T00:00:01'",
                            // Skip 3 rows, 997 rows result (odd)
                            "SELECT * FROM pageframe_test WHERE ts > '2020-01-01T00:00:03'",
                            // Skip 7 rows, 993 rows result (odd)
                            "SELECT * FROM pageframe_test WHERE ts > '2020-01-01T00:00:07'",
                            // Skip 15 rows, 985 rows result (odd)
                            "SELECT * FROM pageframe_test WHERE ts > '2020-01-01T00:00:15'",
                    };

                    // Verify data correctness
                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length, 99);
                });
    }

    /**
     * Tests streaming parquet export via page frame cursor with BYTE column.
     * Uses timestamp filtering to achieve row offsets while keeping page frame support.
     * Tests various offsets including near SIMD boundaries with odd row counts.
     */
    @Test
    public void testParquetExportPageFrameByteColumn() throws Exception {
        getExportTesterPageFrame()
                .run((engine, sqlExecutionContext) -> {
                    // Create table with byte column, 10000 rows with 1ms intervals
                    engine.execute("CREATE TABLE byte_pageframe_test AS (" +
                            "SELECT " +
                            "x::byte as b, " +
                            "x::int as i, " +
                            "x::long as l, " +
                            "timestamp_sequence('2020-01-01T00:00:00', 1000L) as ts " +
                            "FROM long_sequence(10000)" +
                            ") timestamp(ts) partition by day", sqlExecutionContext);

                    // Use timestamp filtering to skip rows, creating odd result counts
                    // Timestamps are in milliseconds: row N has ts = base + N ms
                    String[] queries = {
                            // Skip 1 row, 9999 rows (odd)
                            "SELECT * FROM byte_pageframe_test WHERE ts > '2020-01-01T00:00:00.001'",
                            // Skip 2 rows, 9998 rows (even) - still tests offset 2
                            "SELECT * FROM byte_pageframe_test WHERE ts > '2020-01-01T00:00:00.002'",
                            // Skip 3 rows, 9997 rows (odd)
                            "SELECT * FROM byte_pageframe_test WHERE ts > '2020-01-01T00:00:00.003'",
                            // Skip 5 rows, 9995 rows (odd)
                            "SELECT * FROM byte_pageframe_test WHERE ts > '2020-01-01T00:00:00.005'",
                            // Skip 127 rows, 9873 rows (odd) - near SIMD boundary
                            "SELECT * FROM byte_pageframe_test WHERE ts > '2020-01-01T00:00:00.127'",
                            // Skip 128 rows, 9872 rows (even) - at SIMD boundary
                            "SELECT * FROM byte_pageframe_test WHERE ts > '2020-01-01T00:00:00.128'",
                            // Skip 129 rows, 9871 rows (odd) - past SIMD boundary
                            "SELECT * FROM byte_pageframe_test WHERE ts > '2020-01-01T00:00:00.129'",
                    };

                    // Verify data correctness
                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length, 999);
                });
    }

    @Test
    public void testParquetExportPageFrameCircuitBreaker() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // ~100 daily partitions with 1000 rows each.  The heavy
                    // rnd_str() computation per row makes each page frame
                    // slow enough for the 1ms timeout to trip the breaker.
                    engine.execute("""
                            CREATE TABLE cb_test AS (
                                SELECT x,
                                timestamp_sequence('2024-01-01', 864_000_000L) AS ts
                                FROM long_sequence(100_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""", sqlExecutionContext);

                    params.clear();
                    // x + 1 forces PAGE_FRAME_BACKED; rnd_str generates
                    // large strings that dominate per-frame cost.
                    params.put("query", "SELECT x + 1 AS cx, rnd_str(500, 1000, 0) AS big, ts FROM cb_test");
                    params.put("fmt", "parquet");
                    params.put("timeout", "1");
                    // With a very short timeout the circuit breaker should trip
                    // during PAGE_FRAME_BACKED export.  Depending on which code
                    // path checks first, the error is either "timeout, query
                    // aborted" (from the page-frame factory) or "cancelled by
                    // user" (from the HTTP exporter).  The server may also just
                    // disconnect.
                    try {
                        testHttpClient.assertGetContains("/exp", "timeout, query aborted", params);
                    } catch (AssertionError ae) {
                        TestUtils.assertContains(ae.getMessage(), "cancelled by user");
                    } catch (HttpClientException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(
                                "unexpected error: " + msg,
                                msg.contains("peer disconnect") || msg.contains("malformed chunk")
                        );
                    }
                });
    }

    @Test
    public void testParquetExportPageFrameColumnTop() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    // Create a WAL table with data across multiple partitions
                    engine.execute(
                            "CREATE TABLE coltop_test (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL",
                            sqlExecutionContext
                    );
                    engine.execute("""
                            INSERT INTO coltop_test VALUES
                                (1, '2024-01-01T00:00:00.000000Z'),
                                (2, '2024-01-01T12:00:00.000000Z'),
                                (3, '2024-01-02T00:00:00.000000Z'),
                                (4, '2024-01-02T12:00:00.000000Z')""", sqlExecutionContext);
                    drainWalQueue(engine);

                    // Add a new column — older partitions will have column tops (page address == 0)
                    engine.execute("ALTER TABLE coltop_test ADD COLUMN y INT", sqlExecutionContext);
                    drainWalQueue(engine);

                    // Insert rows with the new column populated in a new partition
                    engine.execute("""
                            INSERT INTO coltop_test VALUES
                                (5, '2024-01-03T00:00:00.000000Z', 100),
                                (6, '2024-01-03T12:00:00.000000Z', 101)""", sqlExecutionContext);
                    drainWalQueue(engine);

                    // Query with a computed column to force PAGE_FRAME_BACKED mode.
                    // The pass-through column y has a column top (zero page address)
                    // in the 2024-01-01 and 2024-01-02 partitions.
                    String[] queries = {
                            "SELECT x + 1 AS cx, y, ts FROM coltop_test",
                            "SELECT x * 2 AS doubled, y::LONG AS y_long, ts FROM coltop_test",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length * 3, 50);
                });
    }

    @Test
    public void testParquetExportPageFrameComputedColumns() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE hybrid_test AS (" +
                            "SELECT x, x * 2.0 AS dbl_col, " +
                            "timestamp_sequence('2024-01-01', 1_000_000L) AS ts " +
                            "FROM long_sequence(100)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);

                    String[] queries = {
                            "SELECT x + 1 AS computed_x, dbl_col, ts FROM hybrid_test",
                            "SELECT x::INT AS x_int, dbl_col::FLOAT AS dbl_float FROM hybrid_test",
                            "SELECT x, x * 3 + 1 AS expr_col FROM hybrid_test WHERE ts > '2024-01-01T00:00:10'",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length, 50);
                });
    }

    @Test
    public void testParquetExportPageFrameComputedColumnsNulls() throws Exception {
        // PAGE_FRAME_BACKED: NULL values in computed columns across multiple row group
        // boundaries. assertParquetExportDataCorrectness uses random row_group_size,
        // exercising buffer pinning with NULL sentinel values across flushes.
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("""
                            CREATE TABLE null_hybrid AS (
                                SELECT
                                    CASE WHEN x % 3 = 0 THEN NULL::INT ELSE x::INT END AS val,
                                    CASE WHEN x % 5 = 0 THEN NULL ELSE rnd_str(3, 8, 0) END AS name,
                                    timestamp_sequence('2024-01-01', 100_000L) AS ts
                                FROM long_sequence(500)
                            ) TIMESTAMP(ts) PARTITION BY HOUR""", sqlExecutionContext);

                    String[] queries = {
                            "SELECT val::LONG AS val_long, name, ts FROM null_hybrid",
                            "SELECT val + 1 AS val_inc, name, ts FROM null_hybrid",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length * 3, 50);
                });
    }

    @Test
    public void testParquetExportPageFrameCursorBasedNulls() throws Exception {
        // CURSOR_BASED: NULL values in a non-page-frame factory (CROSS JOIN)
        // across multiple row groups. Exercises cursor-based buffer pinning
        // with NULL sentinels for both fixed-size and var-size columns.
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("""
                            CREATE TABLE cb_null_a AS (
                                SELECT
                                    CASE WHEN x % 4 = 0 THEN NULL::INT ELSE x::INT END AS a,
                                    CASE WHEN x % 3 = 0 THEN NULL ELSE rnd_str(2, 6, 0) END AS s
                                FROM long_sequence(30)
                            )""", sqlExecutionContext);
                    engine.execute("CREATE TABLE cb_null_b AS (SELECT x AS b FROM long_sequence(10))", sqlExecutionContext);

                    String[] queries = {
                            "SELECT cb_null_a.a, cb_null_b.b, cb_null_a.s FROM cb_null_a CROSS JOIN cb_null_b",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length * 3, 50);
                });
    }

    @Test
    public void testParquetExportPageFrameDescending() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("""
                            CREATE TABLE desc_test AS (
                                SELECT x, x * 2.0 AS dbl_col,
                                timestamp_sequence('2024-01-01', 1_000_000L) AS ts
                                FROM long_sequence(200)
                            ) TIMESTAMP(ts) PARTITION BY DAY""", sqlExecutionContext);

                    String[] queries = {
                            "SELECT x + 1 AS computed_x, dbl_col, ts FROM desc_test ORDER BY ts DESC",
                            "SELECT x::INT AS x_int, dbl_col::FLOAT AS dbl_float, ts FROM desc_test ORDER BY ts DESC",
                            "SELECT x, x * 3 + 1 AS expr_col, ts FROM desc_test ORDER BY ts DESC",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length * 3, 50);
                });
    }

    @Test
    public void testParquetExportPageFrameDescendingAfterRecompile() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("""
                            CREATE TABLE recomp_desc AS (
                                SELECT x, x * 2.0 AS dbl_col,
                                timestamp_sequence('2024-01-01', 1_000_000L) AS ts
                                FROM long_sequence(200)
                            ) TIMESTAMP(ts) PARTITION BY DAY""", sqlExecutionContext);

                    // Descending query with expressions → PAGE_FRAME_BACKED mode initially.
                    // The descending override should downgrade it to CURSOR_BASED.
                    String query = "SELECT x + 1 AS computed_x, dbl_col, ts FROM recomp_desc ORDER BY ts DESC";

                    try (TestHttpClient httpClient = new TestHttpClient();
                         var sink = new DirectUtf8Sink(16_384)
                    ) {
                        httpClient.setKeepConnection(true);

                        // First request: succeeds and caches the factory in the connection's select cache.
                        HttpClient.Request req = httpClient.getHttpClient().newRequest("localhost", 9001);
                        req.GET().url("/exp");
                        req.query("query", query);
                        req.query("fmt", "parquet");
                        req.query("row_group_size", "50");
                        sink.clear();
                        httpClient.reqToSink(req, sink, null, null, null, null);
                        assertParquetMatchesQuery(engine, sqlExecutionContext, sink, query, "recomp_first.parquet");

                        // ALTER TABLE to change the schema version, making the cached factory stale.
                        engine.execute("ALTER TABLE recomp_desc ADD COLUMN extra_col INT", sqlExecutionContext);

                        // Second request: the stale cached factory triggers
                        // TableReferenceOutOfDateException, causing recompilation.
                        // Without the fix, the descending-to-CURSOR_BASED override is not
                        // re-applied after recompile, so PAGE_FRAME_BACKED produces rows
                        // in storage (ascending) order for pass-through columns.
                        req = httpClient.getHttpClient().newRequest("localhost", 9001);
                        req.GET().url("/exp");
                        req.query("query", query);
                        req.query("fmt", "parquet");
                        req.query("row_group_size", "50");
                        sink.clear();
                        httpClient.reqToSink(req, sink, null, null, null, null);
                        assertParquetMatchesQuery(engine, sqlExecutionContext, sink, query, "recomp_second.parquet");
                    }
                });
    }

    @Test
    public void testParquetExportPageFrameFullMaterialization() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE fm_t1 (x INT, label STRING)", sqlExecutionContext);
                    engine.execute("CREATE TABLE fm_t2 (id INT, name STRING)", sqlExecutionContext);
                    engine.execute("INSERT INTO fm_t1 VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')", sqlExecutionContext);
                    engine.execute("INSERT INTO fm_t2 VALUES (1, 'one'), (2, 'two'), (3, 'three')", sqlExecutionContext);

                    String[] queries = {
                            "SELECT fm_t1.x, fm_t2.name FROM fm_t1 CROSS JOIN fm_t2 WHERE fm_t1.x = fm_t2.id",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length, 50);
                });
    }

    @Test
    public void testParquetExportPageFrameHybridSymbol() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE sym_test AS (" +
                            "SELECT x, rnd_symbol('A','B','C') AS sym, " +
                            "timestamp_sequence('2024-01-01', 1_000_000L) AS ts " +
                            "FROM long_sequence(100)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);

                    // sym is pass-through, x + 1 is computed: exercises hybrid path with symbols
                    String[] queries = {
                            "SELECT sym, x + 1 AS next_x FROM sym_test",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length, 50);
                });
    }

    @Test
    public void testParquetExportPageFrameMultipleRowGroups() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("""
                            CREATE TABLE rg_test AS (
                                SELECT x, x * 2.0 AS dbl_col,
                                timestamp_sequence('2024-01-01', 100_000L) AS ts
                                FROM long_sequence(5000)
                            ) TIMESTAMP(ts) PARTITION BY HOUR""", sqlExecutionContext);

                    String[] queries = {
                            "SELECT x + 1 AS computed_x, dbl_col, ts FROM rg_test",
                            "SELECT x::INT AS x_int, dbl_col::FLOAT AS dbl_float, ts FROM rg_test",
                            // Computed STRING column: exercises var-size buffer pinning in PAGE_FRAME_BACKED
                            "SELECT x::STRING AS str_x, dbl_col, ts FROM rg_test",
                    };

                    // Use a small max_row_group to force multiple row groups
                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length * 3, 200);
                });
    }

    @Test
    public void testParquetExportPageFrameVarcharAndArrayColumns() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("""
                            CREATE TABLE vca_test AS (
                                SELECT x,
                                rnd_varchar(1, 15, 1) AS vc_col,
                                rnd_double_array(1, 5) AS arr_col,
                                timestamp_sequence('2024-01-01', 1_000_000L) AS ts
                                FROM long_sequence(200)
                            ) TIMESTAMP(ts) PARTITION BY DAY""", sqlExecutionContext);

                    // x + 1 forces PAGE_FRAME_BACKED; vc_col and arr_col are pass-through
                    String[] queries = {
                            "SELECT x + 1 AS computed_x, vc_col, arr_col, ts FROM vca_test",
                            "SELECT x * 2 AS doubled, vc_col, ts FROM vca_test",
                            "SELECT x + 1 AS computed_x, arr_col FROM vca_test",
                    };

                    assertParquetExportDataCorrectness(engine, sqlExecutionContext, queries, queries.length * 3, 50);
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
                    testHttpClient.assertGetParquet("/exp", 374, params, "SELECT * FROM version_valid_test");
                    params.put("parquet_version", "2");
                    testHttpClient.assertGetParquet("/exp", 369, params, "SELECT * FROM version_valid_test");
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
                    httpClient.assertGetParquet(serverMain.getHttpServerPort(), "/exp", "200", 1231, "basic_parquet_test");
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
                    testHttpClient.assertGetParquet("/exp", 21949, params, "SELECT * FROM row_group_valid_test");

                    params.put("row_group_size", "5000");
                    testHttpClient.assertGetParquet("/exp", 20990, params, "SELECT * FROM row_group_valid_test");

                    params.put("row_group_size", "5010");
                    testHttpClient.assertGetParquet("/exp", 20990, params, "SELECT * FROM row_group_valid_test");

                    params.put("row_group_size", "1500");
                    testHttpClient.assertGetParquet("/exp", 21720, params, "SELECT * FROM row_group_valid_test");

                    params.put("row_group_size", "1510");
                    testHttpClient.assertGetParquet("/exp", 21720, params, "SELECT * FROM row_group_valid_test");
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
                    testHttpClient.assertGetParquet("/exp", 374, params, "SELECT * FROM statistics_test");

                    params.put("statistics_enabled", "false");
                    testHttpClient.assertGetParquet("/exp", 258, params, "SELECT * FROM statistics_test");
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
                    // With direct export, data may start streaming before the timeout
                    // fires, causing the server to disconnect rather than send an error.
                    try {
                        testHttpClient.assertGetContains("/exp", "timeout, query aborted", params);
                    } catch (HttpClientException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue(
                                "unexpected error: " + msg,
                                msg.contains("peer disconnect") || msg.contains("malformed chunk")
                        );
                    }
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
                        String msg = e.getMessage();
                        Assert.assertTrue(
                                "unexpected error: " + msg,
                                msg.contains("peer disconnect") || msg.contains("malformed chunk")
                        );
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


                    testHttpClient.assertGetParquet("/exp", 1962, tableName);
                });
    }

    @Test
    public void testParquetExportWithPivot() throws Exception {
        getExportTester()
                .run((engine, sqlExecutionContext) -> {
                    engine.execute("CREATE TABLE monthly_sales (empid INT, amount INT, month SYMBOL)", sqlExecutionContext);
                    engine.execute("INSERT INTO monthly_sales VALUES " +
                            "(1, 10000, 'JAN'), (1, 400, 'JAN'), (2, 4500, 'JAN'), (2, 35000, 'JAN'), " +
                            "(1, 5000, 'FEB'), (1, 3000, 'FEB'), (2, 200, 'FEB'), (2, 90500, 'FEB'), " +
                            "(1, 6000, 'MAR'), (1, 5000, 'MAR'), (2, 2500, 'MAR'), (2, 9500, 'MAR')", sqlExecutionContext);
                    testHttpClient.setKeepConnection(true);
                    testHttpClient.assertGetParquet(
                            "/exp",
                            1105,
                            "monthly_sales PIVOT (SUM(amount) FOR month IN (select distinct month from monthly_sales order by month) GROUP BY empid) ORDER BY empid"
                    );
                    engine.execute("INSERT INTO monthly_sales VALUES (3, 9000, 'APRIL')", sqlExecutionContext);
                    testHttpClient.setKeepConnection(false);
                    testHttpClient.assertGetParquet(
                            "/exp",
                            1370,
                            "monthly_sales PIVOT (SUM(amount) FOR month IN (select distinct month from monthly_sales order by month) GROUP BY empid) ORDER BY empid"
                    );
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

    private void assertParquetExportDataCorrectness(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String[] queries,
            int iterations,
            int max_row_group
    ) throws Exception {
        String[] hostnames = {"localhost", "127.0.0.1"};
        int hostnameIndex = 0;
        try (TestHttpClient testHttpClient = new TestHttpClient();
             var sink = new DirectUtf8Sink(16_384)
        ) {
            testHttpClient.setKeepConnection(true);
            for (int i = 0; i < iterations; i++) {
                int index = rnd.nextInt(queries.length);
                if (rnd.nextInt(100) < 5) {
                    hostnameIndex = 1 - hostnameIndex;
                }

                // Export to Parquet
                HttpClient.Request req = testHttpClient.getHttpClient().newRequest(hostnames[hostnameIndex], 9001);
                req.GET().url("/exp");
                String query = queries[index];
                req.query("query", query);
                req.query("fmt", "parquet");
                if (rnd.nextBoolean()) {
                    req.query("rmode", "nodelay");
                }
                req.query("row_group_size", String.valueOf(10 + rnd.nextInt(max_row_group)));
                sink.clear();
                testHttpClient.reqToSink(req, sink, null, null, null, null);
                int bytesReceived = sink.size();

                // Save to file
                String filename = "test_export_" + i + ".parquet";
                Path path = Path.getThreadLocal(root);
                path.concat("export").concat(filename).$();
                Files.mkdirs(path, engine.getConfiguration().getMkDirMode());
                long fd = Files.openRW(path.$(), CairoConfiguration.O_NONE);
                try {
                    Files.truncate(fd, bytesReceived);
                    long bytesWritten = Files.write(fd, sink.ptr(), bytesReceived, 0);
                    Assert.assertEquals(bytesReceived, bytesWritten);
                } finally {
                    Files.close(fd);
                }

                String selectFromParquet = "read_parquet('" + filename + "')";
                var expectedSink = new StringSink();
                var actualSink = new StringSink();
                TestUtils.printSql(engine, sqlExecutionContext, query, expectedSink);
                TestUtils.printSql(engine, sqlExecutionContext, selectFromParquet, actualSink);
                TestUtils.assertEquals(expectedSink, actualSink);
            }
        }
    }

    private void assertParquetMatchesQuery(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            DirectUtf8Sink parquetData,
            String query,
            String filename
    ) throws Exception {
        int bytesReceived = parquetData.size();
        Path path = Path.getThreadLocal(root);
        path.concat("export").concat(filename).$();
        Files.mkdirs(path, engine.getConfiguration().getMkDirMode());
        long fd = Files.openRW(path.$(), CairoConfiguration.O_NONE);
        try {
            Files.truncate(fd, bytesReceived);
            long bytesWritten = Files.write(fd, parquetData.ptr(), bytesReceived, 0);
            Assert.assertEquals(bytesReceived, bytesWritten);
        } finally {
            Files.close(fd);
        }

        String selectFromParquet = "read_parquet('" + filename + "')";
        var expectedSink = new StringSink();
        var actualSink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, expectedSink);
        TestUtils.printSql(engine, sqlExecutionContext, selectFromParquet, actualSink);
        TestUtils.assertEquals(expectedSink, actualSink);
    }

    private HttpQueryTestBuilder getExportTester() {
        return new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4099)))
                .withCopyExportRoot(root + "/export")
                .withCopyInputRoot(root + "/export");
    }

    /**
     * Returns an export tester configured with a FilesFacade that fails if
     * temp tables (zzz.copy.*) are created. This ensures the page frame
     * export path is used instead of copying to a temp table.
     */
    private HttpQueryTestBuilder getExportTesterPageFrame() {
        FilesFacade noTempTableFacade = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (path.toString().contains("zzz.copy.")) {
                    Assert.fail("Expected page frame export but temp table directory was created: " + path);
                }
                return super.mkdirs(path, mode);
            }

            @Override
            public long openAppend(LPSZ name) {
                checkNoTempTable(name);
                return super.openAppend(name);
            }

            @Override
            public long openCleanRW(LPSZ name, long size) {
                checkNoTempTable(name);
                return super.openCleanRW(name, size);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                checkNoTempTable(name);
                return super.openRW(name, opts);
            }

            private void checkNoTempTable(LPSZ name) {
                if (name.toString().contains("zzz.copy.")) {
                    Assert.fail("Expected page frame export but temp table was created: " + name);
                }
            }
        };

        return new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withSendBufferSize(Math.max(1024, rnd.nextInt(4099)))
                .withCopyExportRoot(root + "/export")
                .withCopyInputRoot(root + "/export")
                .withFilesFacade(noTempTableFacade);
    }
}