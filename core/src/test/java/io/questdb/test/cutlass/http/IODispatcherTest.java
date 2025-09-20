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

import io.questdb.DefaultServerConfiguration;
import io.questdb.Metrics;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxnScoreboardPoolFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestProcessorSelector;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.RescheduleContext;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.StaticContentProcessorFactory;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.functions.test.TestDataUnavailableFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.HeartBeatException;
import io.questdb.network.IOContext;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.SuspendEvent;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;
import io.questdb.test.AbstractTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.cutlass.NetUtils;
import io.questdb.test.cutlass.suspend.TestCase;
import io.questdb.test.cutlass.suspend.TestCases;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestMicroClock;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static io.questdb.test.cutlass.http.HttpUtils.urlEncodeQuery;
import static io.questdb.test.tools.TestUtils.assertEventually;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertTrue;

public class IODispatcherTest extends AbstractTest {
    public static final String INSERT_QUERY_RESPONSE = "0c\r\n" +
            "{\"dml\":\"OK\"}\r\n" +
            "00\r\n" +
            "\r\n";
    public static final String JSON_DDL_RESPONSE = "0c\r\n" +
            "{\"ddl\":\"OK\"}\r\n" +
            "00\r\n" +
            "\r\n";
    private static final Log LOG = LogFactory.getLog(IODispatcherTest.class);
    private static final String QUERY_TIMEOUT_SELECT = "select i, avg(l), max(l) from t group by i order by i asc limit 3";
    private static final String QUERY_TIMEOUT_TABLE_DDL = "create table t as (select cast(x%10 as int) as i, x as l from long_sequence(100))";
    private static final String VALID_IMPORT_RESPONSE = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain; charset=utf-8\r\n" +
            "\r\n" +
            "0666\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|      Location:  |                          fhv_tripdata_2017-02.csv  |        Pattern  | Locale  |      Errors  |\r\n" +
            "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
            "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|   Rows handled  |                                                24  |                 |         |              |\r\n" +
            "|  Rows imported  |                                                24  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|              0  |                              Dispatching_base_num  |                  VARCHAR  |           0  |\r\n" +
            "|              1  |                                   Pickup_DateTime  |                     DATE  |           0  |\r\n" +
            "|              2  |                                  DropOff_datetime  |                  VARCHAR  |           0  |\r\n" +
            "|              3  |                                      PUlocationID  |                  VARCHAR  |           0  |\r\n" +
            "|              4  |                                      DOlocationID  |                  VARCHAR  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    private static final RescheduleContext emptyRescheduleContext = (retry) -> {
    };
    private static CairoConfiguration configuration;
    private static TestHttpClient testHttpClient;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();
    private long configuredMaxQueryResponseRowLimit = Long.MAX_VALUE;
    private Clock nanosecondClock = NanosecondClockImpl.INSTANCE;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        // this method could be called for multiple iterations within single test
        // we have some synthetic re-runs
        testHttpClient = Misc.free(testHttpClient);
        testHttpClient = new TestHttpClient();
        configuration = new DefaultCairoConfiguration(root);
    }

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient = Misc.free(testHttpClient);
        AbstractTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = false;
        SharedRandom.RANDOM.set(new Rnd());
        testHttpClient.setKeepConnection(false);
        Metrics.ENABLED.clear();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        TestDataUnavailableFunctionFactory.eventCallback = null;
    }

    @Test
    public void testBadImplicitStrToLongCast() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet("{\"ddl\":\"OK\"}", "create table tab (value int, when long, ts timestamp) timestamp(ts) partition by day bypass wal;");
            testHttpClient.assertGet("{\"dml\":\"OK\"}", "insert into tab values(1, now(), now());");
            testHttpClient.assertGet(
                    "{\"query\":\"select * from tab where when = '2023-10-17';\",\"error\":\"inconvertible value: `2023-10-17` [STRING -> LONG]\",\"position\":0}",
                    "select * from tab where when = '2023-10-17';"
            );
        });
    }

    @Test
    public void testBiasWrite() throws Exception {
        LOG.info().$("started testBiasWrite").$();

        assertMemoryLeak(() -> {
            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);

            try (IODispatcher<HelloContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public int getInitialBias() {
                            return IODispatcherConfiguration.BIAS_WRITE;
                        }
                    },
                    fd -> {
                        connectLatch.countDown();
                        return new HelloContext(fd, contextClosedLatch);
                    }
            )) {
                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    try {
                        while (serverRunning.get()) {
                            dispatcher.run(0);
                            dispatcher.processIOQueue(
                                    (operation, context, dispatcher1) -> {
                                        if (operation == IOOperation.WRITE) {
                                            Assert.assertEquals(1024, Net.send(context.getFd(), context.buffer, 1024));
                                            dispatcher1.disconnect(context, IODispatcher.DISCONNECT_REASON_TEST);
                                        }
                                        return true;
                                    }
                            );
                        }
                    } finally {
                        serverHaltLatch.countDown();
                    }
                }).start();

                long fd = Net.socketTcp(true);
                try {
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        try {
                            TestUtils.assertConnect(fd, sockAddr);

                            connectLatch.await();

                            long buffer = Unsafe.malloc(2048, MemoryTag.NATIVE_DEFAULT);
                            try {
                                Assert.assertEquals(1024, Net.recv(fd, buffer, 1024));
                            } finally {
                                Unsafe.free(buffer, 2048, MemoryTag.NATIVE_DEFAULT);
                            }

                            Assert.assertEquals(0, Net.close(fd));
                            LOG.info().$("closed [fd=").$(fd).$(']').$();
                            fd = -1;

                            contextClosedLatch.await();
                        } finally {
                            serverRunning.set(false);
                            serverHaltLatch.await();
                        }
                        Assert.assertEquals(0, dispatcher.getConnectionCount());
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    if (fd != -1) {
                        Net.close(fd);
                    }
                }
            }
        });
    }

    @Test
    public void testCanUpdateO3MaxLagAndMaxUncommittedRowsIfTableExistsAndOverwriteIsTrue() throws Exception {
        importWithO3MaxLagAndMaxUncommittedRowsTableExists(
                true,
                true,
                PartitionBy.DAY,
                180_000_000,
                721,
                180_000_000,
                721
        );
    }

    @Test
    public void testCanUpdateO3MaxLagAndMaxUncommittedRowsToZeroIfTableExistsAndOverwriteIsTrue() throws Exception {
        importWithO3MaxLagAndMaxUncommittedRowsTableExists(
                true,
                false,
                0,
                PartitionBy.DAY,
                0,
                0,
                0
        );
    }

    @SuppressWarnings("resource")
    @Test
    public void testCannotSetNonBlocking() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration serverConfiguration = new DefaultHttpServerConfiguration(configuration);
            final NetworkFacade nf = new NetworkFacadeImpl() {
                long theFd;

                @Override
                public long accept(long serverFd) {
                    long fd = super.accept(serverFd);
                    theFd = fd;
                    return fd;
                }

                @Override
                public int configureNonBlocking(long fd) {
                    if (fd == theFd) {
                        return -1;
                    }
                    return super.configureNonBlocking(fd);
                }
            };

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public NetworkFacade getNetworkFacade() {
                            return nf;
                        }
                    },
                    fd -> new HttpConnectionContext(serverConfiguration, PlainSocketFactory.INSTANCE).of(fd)
            )) {
                // spin up dispatcher thread
                AtomicBoolean dispatcherRunning = new AtomicBoolean(true);
                SOCountDownLatch dispatcherHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    try {
                        while (dispatcherRunning.get()) {
                            dispatcher.run(0);
                        }
                    } finally {
                        dispatcherHaltLatch.countDown();
                    }
                }).start();

                try {
                    long socketAddr = Net.sockaddr(Net.parseIPv4("127.0.0.1"), 9001);
                    long fd = Net.socketTcp(true);
                    try {
                        TestUtils.assertConnect(fd, socketAddr);

                        int bufLen = 512;
                        long mem = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                        try {
                            Assert.assertEquals(-2, Net.recv(fd, mem, bufLen));
                        } finally {
                            Unsafe.free(mem, bufLen, MemoryTag.NATIVE_DEFAULT);
                        }
                    } finally {
                        Net.close(fd);
                        Net.freeSockAddr(socketAddr);
                    }
                } finally {
                    dispatcherRunning.set(false);
                    dispatcherHaltLatch.await();
                }
            }
        });
    }

    @Test
    public void testCannotUpdateO3MaxLagAndMaxUncommittedRowsIfTableExistsAndOverwriteIsFalse() throws Exception {
        importWithO3MaxLagAndMaxUncommittedRowsTableExists(
                false,
                true,
                PartitionBy.DAY,
                3_600_000_000L, // 1 hour, micro precision
                1,
                300000000,
                1000
        );
    }

    @Test
    public void testConnectDisconnect() throws Exception {
        LOG.info().$("started testConnectDisconnect").$();

        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(configuration);

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    DefaultIODispatcherConfiguration.INSTANCE,
                    new IOContextFactory<HttpConnectionContext>() {
                        @SuppressWarnings("resource")
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE) {
                                @Override
                                public void close() {
                                    // it is possible that context is closed twice in error
                                    // when crashes occur put debug line here to see how many times
                                    // context is closed
                                    if (closeCount.incrementAndGet() == 1) {
                                        super.close();
                                        contextClosedLatch.countDown();
                                    }
                                }
                            }.of(fd);
                        }
                    }
            )) {
                HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HttpRequestProcessor() {
                        };
                    }

                    @Override
                    public HttpRequestProcessor select(HttpRequestHeader requestHeader) {
                        return null;
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    try {
                        while (serverRunning.get()) {
                            dispatcher.run(0);
                            dispatcher.processIOQueue(
                                    (operation, context, dispatcher1) -> handleClientOperation(context, operation, selector, dispatcher1)
                            );
                        }
                    } finally {
                        serverHaltLatch.countDown();
                    }
                }).start();

                long fd = Net.socketTcp(true);
                try {
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        try {
                            TestUtils.assertConnect(fd, sockAddr);

                            connectLatch.await();

                            Assert.assertEquals(0, Net.close(fd));
                            LOG.info().$("closed [fd=").$(fd).$(']').$();
                            fd = -1;

                            contextClosedLatch.await();

                        } finally {
                            serverRunning.set(false);
                            serverHaltLatch.await();
                        }
                        Assert.assertEquals(0, dispatcher.getConnectionCount());
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    if (fd != -1) {
                        Net.close(fd);
                    }
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testCursorTypeUnsupportedByCompiler() throws Exception {
        String expectedErrorResponse = "HTTP/1.1 400 Bad request\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: application/json; charset=utf-8\r\n" +
                "Keep-Alive: timeout=5, max=10000\r\n" +
                "\r\n" +
                "93\r\n" +
                "{\"query\":\"select query_activity() from long_sequence(1)\",\"error\":\"cursor function cannot be used as a column [column=query_activity]\",\"position\":7}\r\n" +
                "00\r\n" +
                "\r\n";

        testJsonQuery(
                20,
                "GET /query?query=select%20query_activity%28%29%20from%20long_sequence%281%29 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                expectedErrorResponse
        );
    }

    @Test
    public void testDDLInExp() throws Exception {
        testJsonQuery(
                20,
                "GET /exp?query=create%20table%20balance%20(money%20float) HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "67\r\n" +
                        "{\"query\":\"create table balance (money float)\",\"error\":\"/exp endpoint only accepts SELECT\",\"position\":0}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testEmptyQuotedString() throws Exception {
        testJsonQuery0(
                1, (engine, sqlExecutionContext) -> sendAndReceive(
                        NetworkFacadeImpl.INSTANCE,
                        // select '' from long_sequence(1)
                        "GET /exec?query=select%20%27%27%20from%20long_sequence%281%29 HTTP/1.1\n" +
                                "Host: localhost:9000\r\n" +
                                "Connection: keep-alive\r\n" +
                                "Accept: */*\r\n" +
                                "X-Requested-With: XMLHttpRequest\r\n" +
                                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                                "Sec-Fetch-Site: same-origin\r\n" +
                                "Sec-Fetch-Mode: cors\r\n" +
                                "Referer: http://localhost:9000/index.html\r\n" +
                                "Accept-Encoding: gzip, deflate, br\r\n" +
                                "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                                "\r\n",
                        "HTTP/1.1 200 OK\r\n" +
                                "Server: questDB/1.0\r\n" +
                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                "Transfer-Encoding: chunked\r\n" +
                                "Content-Type: application/json; charset=utf-8\r\n" +
                                "Keep-Alive: timeout=5, max=10000\r\n" +
                                "\r\n" +
                                "83\r\n" +
                                "{\"query\":\"select '' from long_sequence(1)\",\"columns\":[{\"name\":\"column\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"\"]],\"count\":1}\r\n" +
                                "00\r\n" +
                                "\r\n",
                        1,
                        0,
                        false
                ), false
        );
    }

    @Test
    public void testExceptionAfter1100Rows() throws Exception {
        final StringSink sink = new StringSink();
        final int numOfRows = 1100;
        for (int i = 0, n = numOfRows / 2; i < n; i++) {
            sink.put("[true],[false],");
        }
        sink.put("[]");

        getSimpleTester().run((engine, sqlExecutionContext) ->
                testHttpClient.assertGet(
                        "{" +
                                "\"query\":\"select simulate_crash('P') \\n\\rfrom\\tlong_sequence(" + (numOfRows + 5) + ")\"," +
                                "\"columns\":[{\"name\":\"simulate_crash\",\"type\":\"BOOLEAN\"}]," +
                                "\"timestamp\":-1," +
                                "\"dataset\":[" + sink + "]," +
                                "\"count\":" + (numOfRows + 1) + "," +
                                "\"error\":\"simulated cairo exception\", " +
                                "\"errorPos\":222" +
                                "}",
                        "select simulate_crash('P') \n\rfrom\tlong_sequence(" + (numOfRows + 5) + ")"
                )
        );
    }

    @Test
    public void testExceptionAfterHeader() throws Exception {
        testExceptionAfterHeader(0, "[]");
        testExceptionAfterHeader(1, "[true],[]");
        testExceptionAfterHeader(2, "[true],[false],[]");
    }

    @Test
    public void testExceptionAuthorization() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet(
                    "{" +
                            "\"query\":\"select simulate_crash('A') from long_sequence(5)\"," +
                            "\"columns\":[{\"name\":\"simulate_crash\",\"type\":\"BOOLEAN\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[]]," +
                            "\"count\":1," +
                            "\"error\":\"simulated authorization exception\", " +
                            "\"errorPos\":0" +
                            "}",
                    "select simulate_crash('A') from long_sequence(5)"
            );
            Assert.assertEquals(0, engine.getMetrics().healthMetrics().unhandledErrorsCount());
        });
    }

    @Test
    public void testExecuteAndCancelSqlCommandsOnExpEndpoint() throws Exception {
        testExecuteAndCancelSqlCommands("/exp");
    }

    @Test
    public void testExecuteAndCancelSqlCommandsOnQueryEndpoint() throws Exception {
        testExecuteAndCancelSqlCommands("/query");
    }

    @Test
    public void testExistentCheckBadArg() throws Exception {
        testJsonQuery(
                20,
                "GET /chk?f=json&x=clipboard-1580645706714&_=1580598041784 HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "14\r\n" +
                        "table name missing\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testExistentCheckDoesNotExist() throws Exception {
        testJsonQuery(
                20,
                "GET /chk?f=json&j=clipboard-1580645706714&_=1580598041784 HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "1b\r\n" +
                        "{\"status\":\"Does not exist\"}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testExistentCheckExists() throws Exception {
        testJsonQuery(
                20,
                "GET /chk?f=json&j=x HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "13\r\n" +
                        "{\"status\":\"Exists\"}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testExistentCheckExistsPlain() throws Exception {
        testJsonQuery(
                20,
                "GET /chk?j=x HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "08\r\n" +
                        "Exists\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testExpCustomDelimiter() throws Exception {
        testJsonQuery(
                20,
                "GET /exp?count=true&src=con&query=select+rnd_symbol(%27a%27%2C%27b%27%2C%27c%27)+sym+%2C+rnd_int(0%2C10%2C0)+num+from+long_sequence(10%2C+33%2C+55)&delimiter=%09 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "54\r\n" +
                        "\"sym\"\t\"num\"\r\n" +
                        "\"c\"\t9\r\n" +
                        "\"b\"\t5\r\n" +
                        "\"a\"\t0\r\n" +
                        "\"a\"\t0\r\n" +
                        "\"a\"\t5\r\n" +
                        "\"a\"\t7\r\n" +
                        "\"a\"\t4\r\n" +
                        "\"a\"\t8\r\n" +
                        "\"a\"\t2\r\n" +
                        "\"c\"\t10\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testExpExplainQueryPlan() throws Exception {
        testJsonQuery(
                1,
                "GET /exp?query=explain+select+1+from+x+where+f>systimestamp()+and+f<0+limit+1 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01f9\r\n" +
                        "\"QUERY PLAN\"\r\n" +
                        "\"VirtualRecord\"\r\n" +
                        "\"&nbsp;&nbsp;functions: [1]\"\r\n" +
                        "\"&nbsp;&nbsp;&nbsp;&nbsp;Async Filter workers: 2\"\r\n" +
                        "\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;limit: 1\"\r\n" +
                        "\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;filter: (systimestamp()&lt;f and f&lt;0) [pre-touch]\"\r\n" +
                        "\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;PageFrame\"\r\n" +
                        "\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Row forward scan\"\r\n" +
                        "\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Frame forward scan on: x\"\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testExpNull() throws Exception {
        testJsonQuery(
                0,
                "GET /exp?query=select+null+from+long_sequence(1)&limit=1&src=con HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.392867896.1580123365\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0a\r\n" +
                        "\"null\"\r\n" +
                        "\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testExpRecordTypeSelect() throws Exception {
        testJsonQuery(
                1,
                "GET /exp?limit=0%2C1000&explain=true&count=true&src=con&query=%0D%0A%0D%0A%0D%0Aselect%20pg_catalog.pg_class()%20x%2C%20(pg_catalog.pg_class()).relnamespace%20from%20long_sequence(2) HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "27\r\n" +
                        "\"x1\",\"column\"\r\n" +
                        ",11\r\n" +
                        ",2200\r\n" +
                        ",11\r\n" +
                        ",2200\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testExplainQueryPlan() throws Exception {
        testJsonQuery(
                1,
                "GET /query?query=explain+select+1+from+x+where+f>systimestamp()+and+f<0+limit+1 HTTP/1.1\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cookie: _ga=GA1.1.1723668823.1636741549\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Referer: http://localhost:9000/\r\n" +
                        "Sec-Fetch-Dest: empty\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36\r\n" +
                        "sec-ch-ua: \" Not A;Brand\";v=\"99\", \"Chromium\";v=\"100\", \"Google Chrome\";v=\"100\"\r\n" +
                        "sec-ch-ua-mobile: ?0\r\n" +
                        "sec-ch-ua-platform: \"Windows\"\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0294\r\n" +
                        "{\"query\":\"explain select 1 from x where f>systimestamp() and f<0 limit 1\",\"columns\":[{\"name\":\"QUERY PLAN\",\"type\":\"STRING\"}]," +
                        "\"timestamp\":-1,\"dataset\":" +
                        "[[\"VirtualRecord\"]," +
                        "[\"&nbsp;&nbsp;functions: [1]\"]," +
                        "[\"&nbsp;&nbsp;&nbsp;&nbsp;Async Filter workers: 2\"]," +
                        "[\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;limit: 1\"]," +
                        "[\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;filter: (systimestamp()&lt;f and f&lt;0) [pre-touch]\"]," +
                        "[\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;PageFrame\"]," +
                        "[\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Row forward scan\"]," +
                        "[\"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Frame forward scan on: x\"]]," +
                        "\"count\":8}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testFailsOnBadMaxUncommittedRows() throws Exception {
        String command = "POST /upload?fmt=json&" +
                "maxUncommittedRows=two&" +
                "name=test HTTP/1.1\r\n";
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "37\r\n" +
                        "{\"status\":\"invalid maxUncommittedRows, must be an int\"}\r\n" +
                        "00\r\n" +
                        "\r\n",
                command +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "2021-01-01 00:00:00,1\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testFailsOnBadO3MaxLag() throws Exception {
        String command = "POST /upload?fmt=json&" +
                "o3MaxLag=2seconds+please&" +
                "name=test HTTP/1.1\r\n";
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "33\r\n" +
                        "{\"status\":\"invalid o3MaxLag value, must be a long\"}\r\n" +
                        "00\r\n" +
                        "\r\n",
                command +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "2021-01-01 00:00:00,1\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testHttpClientSupportsConnectionReuse() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.setKeepConnection(true);
            testHttpClient.assertGet(
                    "{\"query\":\"SELECT 'foo'\",\"columns\":[{\"name\":\"foo\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"foo\"]],\"count\":1}",
                    "SELECT 'foo'"
            );
            // This time it's fine to disconnect after the request.
            testHttpClient.setKeepConnection(false);
            testHttpClient.assertGet(
                    "{\"query\":\"SELECT 'bar'\",\"columns\":[{\"name\":\"bar\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"bar\"]],\"count\":1}",
                    "SELECT 'bar'"
            );
        });
    }

    @Test
    public void testHttpEscapesErrorMessage() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "{\"query\":\"into into testlike2 select ('foo', 'bar') from long_sequence(1);\",\"error\":\"table and column names that are SQL keywords have to be enclosed in double quotes, such as \\\"into\\\"\",\"position\":0}",
                "into into testlike2 select ('foo', 'bar') from long_sequence(1);"
        ));
    }

    @Test
    public void testHttpLong256AndCharImport() {
        // this script uploads text file:
        // 0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060,a
        // 0x19f1df2c7ee6b464720ad28e903aeda1a5ad8780afc22f0b960827bd4fcf656d,b
        // 0x9e6e19637bb625a8ff3d052b7c2fe57dc78c55a15d258d77c43d5a9c160b0384,p
        // 0xcb9378977089c773c074045b20ede2cdcc3a6ff562f4e64b51b20c5205234525,w
        // 0xd23ae9b2e5c68caf2c5663af5ba27679dc3b3cb781c4dc698abbd17d63e32e9f,t

        final String uploadScript = ">504f5354202f696d703f666d743d6a736f6e266f76657277726974653d7472756520485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a436f6e74656e742d4c656e6774683a203534380d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37362e302e333830392e313030205361666172692f3533372e33360d0a5365632d46657463682d4d6f64653a20636f72730d0a436f6e74656e742d547970653a206d756c7469706172742f666f726d2d646174613b20626f756e646172793d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a4f726967696e3a20687474703a2f2f6c6f63616c686f73743a393030300d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a0d0a\n" +
                ">2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a436f6e74656e742d446973706f736974696f6e3a20666f726d2d646174613b206e616d653d2264617461223b2066696c656e616d653d2273616d706c652e637376220d0a436f6e74656e742d547970653a206170706c69636174696f6e2f766e642e6d732d657863656c0d0a0d0a3078356335303465643433326362353131333862636630396161356538613431306464346131653230346566383462666564316265313664666261316232323036302c610d0a3078313966316466326337656536623436343732306164323865393033616564613161356164383738306166633232663062393630383237626434666366363536642c620d0a3078396536653139363337626236323561386666336430353262376332666535376463373863353561313564323538643737633433643561396331363062303338342c700d0a3078636239333738393737303839633737336330373430343562323065646532636463633361366666353632663465363462353162323063353230353233343532352c770d0a3078643233616539623265356336386361663263353636336166356261323736373964633362336362373831633464633639386162626431376436336533326539662c740d0a0d0a2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f2d2d0d0a\n" +
                "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a\n" +
                "<0d0a64640d0a7b22737461747573223a224f4b222c226c6f636174696f6e223a2273616d706c652e637376222c22726f777352656a6563746564223a302c22726f7773496d706f72746564223a352c22686561646572223a66616c73652c22706172746974696f6e4279223a224e4f4e45222c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536222c2273697a65223a33322c226572726f7273223a307d2c7b226e616d65223a226631222c2274797065223a2243484152222c2273697a65223a322c226572726f7273223a307d5d7d0d0a30300d0a0d0a\n";

        final String expectedTableMetadata = "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"f0\",\"type\":\"LONG256\"},{\"index\":1,\"name\":\"f1\",\"type\":\"CHAR\"}],\"timestampIndex\":-1}";

        final String baseDir = root;
        final HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(
                configuration,
                new DefaultHttpContextConfiguration() {
                    @Override
                    public MillisecondClock getMillisecondClock() {
                        return StationaryMillisClock.INSTANCE;
                    }

                    @Override
                    public Clock getNanosecondClock() {
                        return StationaryNanosClock.INSTANCE;
                    }
                });

        final ServerConfiguration serverConfiguration = new DefaultServerConfiguration(baseDir) {
            @Override
            public HttpFullFatServerConfiguration getHttpServerConfiguration() {
                return httpServerConfiguration;
            }
        };

        final CairoConfiguration cairoConfiguration = serverConfiguration.getCairoConfiguration();
        final TestWorkerPool workerPool = new TestWorkerPool(2, serverConfiguration.getMetrics());
        try (
                CairoEngine cairoEngine = new CairoEngine(cairoConfiguration);
                HttpServer ignored = createHttpServer(serverConfiguration, cairoEngine, workerPool)
        ) {

            workerPool.start(LOG);
            try {
                // upload file
                NetUtils.playScript(NetworkFacadeImpl.INSTANCE, uploadScript, "127.0.0.1", 9001);

                TableToken tableToken = cairoEngine.verifyTableName("sample.csv");
                try (TableReader reader = cairoEngine.getReader(tableToken)) {
                    StringSink sink = new StringSink();
                    reader.getMetadata().toJson(sink);
                    TestUtils.assertEquals(expectedTableMetadata, sink);
                }

                final String selectAsJsonScript = ">504f5354202f696d703f666d743d6a736f6e266f76657277726974653d7472756520485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a436f6e74656e742d4c656e6774683a203534380d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37362e302e333830392e313030205361666172692f3533372e33360d0a5365632d46657463682d4d6f64653a20636f72730d0a436f6e74656e742d547970653a206d756c7469706172742f666f726d2d646174613b20626f756e646172793d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a4f726967696e3a20687474703a2f2f6c6f63616c686f73743a393030300d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a0d0a2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a436f6e74656e742d446973706f736974696f6e3a20666f726d2d646174613b206e616d653d2264617461223b2066696c656e616d653d2273616d706c652e637376220d0a436f6e74656e742d547970653a206170706c69636174696f6e2f766e642e6d732d657863656c0d0a0d0a3078356335303465643433326362353131333862636630396161356538613431306464346131653230346566383462666564316265313664666261316232323036302c610d0a3078313966316466326337656536623436343732306164323865393033616564613161356164383738306166633232663062393630383237626434666366363536642c620d0a3078396536653139363337626236323561386666336430353262376332666535376463373863353561313564323538643737633433643561396331363062303338342c700d0a3078636239333738393737303839633737336330373430343562323065646532636463633361366666353632663465363462353162323063353230353233343532352c770d0a3078643233616539623265356336386361663263353636336166356261323736373964633362336362373831633464633639386162626431376436336533326539662c740d0a0d0a2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f2d2d0d0a\n" +
                        "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a\n" +
                        "<0d0a64640d0a7b22737461747573223a224f4b222c226c6f636174696f6e223a2273616d706c652e637376222c22726f777352656a6563746564223a302c22726f7773496d706f72746564223a352c22686561646572223a66616c73652c22706172746974696f6e4279223a224e4f4e45222c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536222c2273697a65223a33322c226572726f7273223a307d2c7b226e616d65223a226631222c2274797065223a2243484152222c2273697a65223a322c226572726f7273223a307d5d7d0d0a30300d0a0d0a\n" +
                        ">474554202f657865633f71756572793d25304125304125323773616d706c652e637376253237266c696d69743d302532433130303026636f756e743d7472756520485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37362e302e333830392e313030205361666172692f3533372e33360d0a5365632d46657463682d4d6f64653a20636f72730d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a0d0a\n" +
                        "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a4b6565702d416c6976653a2074696d656f75743d352c206d61783d31303030300d0a\n" +
                        "<0d0a303166660d0a7b227175657279223a225c6e5c6e2773616d706c652e63737627222c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536227d2c7b226e616d65223a226631222c2274797065223a2243484152227d5d2c2274696d657374616d70223a2d312c2264617461736574223a5b5b22307835633530346564343332636235313133386263663039616135653861343130646434613165323034656638346266656431626531366466626131623232303630222c2261225d2c5b22307831396631646632633765653662343634373230616432386539303361656461316135616438373830616663323266306239363038323762643466636636353664222c2262225d2c5b22307839653665313936333762623632356138666633643035326237633266653537646337386335356131356432353864373763343364356139633136306230333834222c2270225d2c5b22307863623933373839373730383963373733633037343034356232306564653263646363336136666635363266346536346235316232306335323035323334353235222c2277225d2c5b22307864323361653962326535633638636166326335363633616635626132373637396463336233636237383163346463363938616262643137643633653332653966222c2274225d5d2c22636f756e74223a357d0d0a30300d0a0d0a\n";

                // select * from 'sample.csv'
                NetUtils.playScript(NetworkFacadeImpl.INSTANCE, selectAsJsonScript, "127.0.0.1", 9001);

                final String downloadAsCsvScript = ">474554202f6578703f71756572793d25304125304125323773616d706c652e63737625323720485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a557067726164652d496e7365637572652d52657175657374733a20310d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37362e302e333830392e313030205361666172692f3533372e33360d0a5365632d46657463682d4d6f64653a206e617669676174650d0a4163636570743a20746578742f68746d6c2c6170706c69636174696f6e2f7868746d6c2b786d6c2c6170706c69636174696f6e2f786d6c3b713d302e392c696d6167652f776562702c696d6167652f61706e672c2a2f2a3b713d302e382c6170706c69636174696f6e2f7369676e65642d65786368616e67653b763d62330d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a0d0a\n" +
                        "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a20746578742f6373763b20636861727365743d7574662d380d0a436f6e74656e742d446973706f736974696f6e3a206174746163686d656e743b2066696c656e616d653d22717565737464622d71756572792d302e637376220d0a4b6565702d416c6976653a2074696d656f75743d352c206d61783d31303030300d0a\n" +
                        "<0d0a303136390d0a\n" +
                        "<226630222c226631220d0a3078356335303465643433326362353131333862636630396161356538613431306464346131653230346566383462666564316265313664666261316232323036302c610d0a3078313966316466326337656536623436343732306164323865393033616564613161356164383738306166633232663062393630383237626434666366363536642c620d0a3078396536653139363337626236323561386666336430353262376332666535376463373863353561313564323538643737633433643561396331363062303338342c700d0a3078636239333738393737303839633737336330373430343562323065646532636463633361366666353632663465363462353162323063353230353233343532352c770d0a3078643233616539623265356336386361663263353636336166356261323736373964633362336362373831633464633639386162626431376436336533326539662c740d0a\n" +
                        "<0d0a30300d0a\n" +
                        "<0d0a";

                // download select * from 'sample.csv' as csv
                NetUtils.playScript(NetworkFacadeImpl.INSTANCE, downloadAsCsvScript, "127.0.0.1", 9001);
            } finally {
                workerPool.halt();
            }
        }
    }

    @Test
    public void testHttpLong256AndCharImportLimitColumns() {
        // this script uploads text file:
        // 0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060,a
        // 0x19f1df2c7ee6b464720ad28e903aeda1a5ad8780afc22f0b960827bd4fcf656d,b
        // 0x9e6e19637bb625a8ff3d052b7c2fe57dc78c55a15d258d77c43d5a9c160b0384,p
        // 0xcb9378977089c773c074045b20ede2cdcc3a6ff562f4e64b51b20c5205234525,w
        // 0xd23ae9b2e5c68caf2c5663af5ba27679dc3b3cb781c4dc698abbd17d63e32e9f,t

        final String uploadScript = ">504f5354202f696d703f666d743d6a736f6e266f76657277726974653d7472756520485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a436f6e74656e742d4c656e6774683a203534380d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37362e302e333830392e313030205361666172692f3533372e33360d0a5365632d46657463682d4d6f64653a20636f72730d0a436f6e74656e742d547970653a206d756c7469706172742f666f726d2d646174613b20626f756e646172793d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a4f726967696e3a20687474703a2f2f6c6f63616c686f73743a393030300d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a0d0a\n" +
                ">2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a436f6e74656e742d446973706f736974696f6e3a20666f726d2d646174613b206e616d653d2264617461223b2066696c656e616d653d2273616d706c652e637376220d0a436f6e74656e742d547970653a206170706c69636174696f6e2f766e642e6d732d657863656c0d0a0d0a3078356335303465643433326362353131333862636630396161356538613431306464346131653230346566383462666564316265313664666261316232323036302c610d0a3078313966316466326337656536623436343732306164323865393033616564613161356164383738306166633232663062393630383237626434666366363536642c620d0a3078396536653139363337626236323561386666336430353262376332666535376463373863353561313564323538643737633433643561396331363062303338342c700d0a3078636239333738393737303839633737336330373430343562323065646532636463633361366666353632663465363462353162323063353230353233343532352c770d0a3078643233616539623265356336386361663263353636336166356261323736373964633362336362373831633464633639386162626431376436336533326539662c740d0a0d0a2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f2d2d0d0a\n" +
                "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a\n" +
                "<0d0a64640d0a7b22737461747573223a224f4b222c226c6f636174696f6e223a2273616d706c652e637376222c22726f777352656a6563746564223a302c22726f7773496d706f72746564223a352c22686561646572223a66616c73652c22706172746974696f6e4279223a224e4f4e45222c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536222c2273697a65223a33322c226572726f7273223a307d2c7b226e616d65223a226631222c2274797065223a2243484152222c2273697a65223a322c226572726f7273223a307d5d7d0d0a30300d0a0d0a\n";

        final String expectedTableMetadata = "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"f0\",\"type\":\"LONG256\"},{\"index\":1,\"name\":\"f1\",\"type\":\"CHAR\"}],\"timestampIndex\":-1}";

        final String baseDir = root;
        final HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(
                configuration,
                new DefaultHttpContextConfiguration() {
                    @Override
                    public MillisecondClock getMillisecondClock() {
                        return StationaryMillisClock.INSTANCE;
                    }

                    @Override
                    public Clock getNanosecondClock() {
                        return StationaryNanosClock.INSTANCE;
                    }
                });
        final ServerConfiguration serverConfiguration = new DefaultServerConfiguration(baseDir) {
            @Override
            public HttpFullFatServerConfiguration getHttpServerConfiguration() {
                return httpServerConfiguration;
            }
        };
        final CairoConfiguration cairoConfiguration = serverConfiguration.getCairoConfiguration();
        TestWorkerPool workerPool = new TestWorkerPool(2, serverConfiguration.getMetrics());
        try (
                CairoEngine cairoEngine = new CairoEngine(cairoConfiguration);
                HttpServer ignored = createHttpServer(serverConfiguration, cairoEngine, workerPool)
        ) {

            workerPool.start(LOG);
            try {
                // upload file
                NetUtils.playScript(NetworkFacadeImpl.INSTANCE, uploadScript, "127.0.0.1", 9001);

                TableToken tableToken = cairoEngine.verifyTableName("sample.csv");
                try (TableReader reader = cairoEngine.getReader(tableToken)) {
                    StringSink sink = new StringSink();
                    reader.getMetadata().toJson(sink);
                    TestUtils.assertEquals(expectedTableMetadata, sink);
                }

                final String selectAsJsonScript = ">504f5354202f696d703f666d743d6a736f6e266f76657277726974653d7472756520485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a436f6e74656e742d4c656e6774683a203534380d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37362e302e333830392e313030205361666172692f3533372e33360d0a5365632d46657463682d4d6f64653a20636f72730d0a436f6e74656e742d547970653a206d756c7469706172742f666f726d2d646174613b20626f756e646172793d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a4f726967696e3a20687474703a2f2f6c6f63616c686f73743a393030300d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a0d0a2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f0d0a436f6e74656e742d446973706f736974696f6e3a20666f726d2d646174613b206e616d653d2264617461223b2066696c656e616d653d2273616d706c652e637376220d0a436f6e74656e742d547970653a206170706c69636174696f6e2f766e642e6d732d657863656c0d0a0d0a3078356335303465643433326362353131333862636630396161356538613431306464346131653230346566383462666564316265313664666261316232323036302c610d0a3078313966316466326337656536623436343732306164323865393033616564613161356164383738306166633232663062393630383237626434666366363536642c620d0a3078396536653139363337626236323561386666336430353262376332666535376463373863353561313564323538643737633433643561396331363062303338342c700d0a3078636239333738393737303839633737336330373430343562323065646532636463633361366666353632663465363462353162323063353230353233343532352c770d0a3078643233616539623265356336386361663263353636336166356261323736373964633362336362373831633464633639386162626431376436336533326539662c740d0a0d0a2d2d2d2d2d2d5765624b6974466f726d426f756e64617279386c75374239696e37567a5767614a4f2d2d0d0a\n" +
                        "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a\n" +
                        "<0d0a64640d0a7b22737461747573223a224f4b222c226c6f636174696f6e223a2273616d706c652e637376222c22726f777352656a6563746564223a302c22726f7773496d706f72746564223a352c22686561646572223a66616c73652c22706172746974696f6e4279223a224e4f4e45222c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536222c2273697a65223a33322c226572726f7273223a307d2c7b226e616d65223a226631222c2274797065223a2243484152222c2273697a65223a322c226572726f7273223a307d5d7d0d0a30300d0a0d0a\n" +
                        ">474554202f657865633f71756572793d25323773616d706c652e63737625323726636f756e743d66616c736526636f6c733d66302532436631267372633d76697320485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37392e302e333934352e313330205361666172692f3533372e33360d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a5365632d46657463682d4d6f64653a20636f72730d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a436f6f6b69653a205f67613d4741312e312e323132343933323030312e313537333832343636393b205f6769643d4741312e312e3339323836373839362e313538303132333336350d0a0d0a\n" +
                        "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a4b6565702d416c6976653a2074696d656f75743d352c206d61783d31303030300d0a\n" +
                        "<0d0a303166620d0a7b227175657279223a222773616d706c652e63737627222c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536227d2c7b226e616d65223a226631222c2274797065223a2243484152227d5d2c2274696d657374616d70223a2d312c2264617461736574223a5b5b22307835633530346564343332636235313133386263663039616135653861343130646434613165323034656638346266656431626531366466626131623232303630222c2261225d2c5b22307831396631646632633765653662343634373230616432386539303361656461316135616438373830616663323266306239363038323762643466636636353664222c2262225d2c5b22307839653665313936333762623632356138666633643035326237633266653537646337386335356131356432353864373763343364356139633136306230333834222c2270225d2c5b22307863623933373839373730383963373733633037343034356232306564653263646363336136666635363266346536346235316232306335323035323334353235222c2277225d2c5b22307864323361653962326535633638636166326335363633616635626132373637396463336233636237383163346463363938616262643137643633653332653966222c2274225d5d2c22636f756e74223a357d0d0a30300d0a0d0a";

                // select * from 'sample.csv' and limit columns to f0,f1
                NetUtils.playScript(NetworkFacadeImpl.INSTANCE, selectAsJsonScript, "127.0.0.1", 9001);
            } finally {
                workerPool.halt();
            }
        }
    }

    @Test
    public void testIPv4JSON() throws Exception {
        getSimpleTester()
                .run((engine, sqlExecutionContext) -> {
                    // select 1 as "select"
                    // with select being the column name to check double quote parsing
                    testHttpClient.assertGet(
                            "{\"query\":\"select rnd_int(1,5,0)::ipv4, cast(null as ipv4) ip2, timestamp_sequence(0, 100000000) from long_sequence(10, 33, 55)\",\"columns\":[{\"name\":\"cast\",\"type\":\"IPv4\"},{\"name\":\"ip2\",\"type\":\"IPv4\"},{\"name\":\"timestamp_sequence\",\"type\":\"TIMESTAMP\"}],\"timestamp\":-1,\"dataset\":[[\"0.0.0.3\",null,\"1970-01-01T00:00:00.000000Z\"],[\"0.0.0.5\",null,\"1970-01-01T00:01:40.000000Z\"],[\"0.0.0.3\",null,\"1970-01-01T00:03:20.000000Z\"],[\"0.0.0.4\",null,\"1970-01-01T00:05:00.000000Z\"],[\"0.0.0.2\",null,\"1970-01-01T00:06:40.000000Z\"],[\"0.0.0.1\",null,\"1970-01-01T00:08:20.000000Z\"],[\"0.0.0.5\",null,\"1970-01-01T00:10:00.000000Z\"],[\"0.0.0.4\",null,\"1970-01-01T00:11:40.000000Z\"],[\"0.0.0.1\",null,\"1970-01-01T00:13:20.000000Z\"],[\"0.0.0.4\",null,\"1970-01-01T00:15:00.000000Z\"]],\"count\":10}",
                            "select rnd_int(1,5,0)::ipv4, cast(null as ipv4) ip2, timestamp_sequence(0, 100000000) from long_sequence(10, 33, 55)"
                    );
                });
    }

    @Test
    public void testImplicitUuidCastOnInsert() throws Exception {
        testJsonQuery0(
                1, (engine, sqlExecutionContext) -> {
                    // create table
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=create+table+xx+(value+uuid,+ts+timestamp)+timestamp(ts)&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );
                    // insert one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=insert+into+xx+values('12345678-1234-1234-5678-123456789012',+0)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    INSERT_QUERY_RESPONSE,
                            1,
                            0,
                            false
                    );
                    // check if we have one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=select+*+from+xx+latest+on+ts+partition+by+value&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "f3\r\n" +
                                    "{\"query\":\"select * from xx latest on ts partition by value\",\"columns\":[{\"name\":\"value\",\"type\":\"UUID\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"}],\"timestamp\":1,\"dataset\":[[\"12345678-1234-1234-5678-123456789012\",\"1970-01-01T00:00:00.000000Z\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );
                }, false
        );
    }

    public void testImport(
            String response,
            String request,
            NetworkFacade nf,
            boolean expectReceiveDisconnect,
            int requestCount
    ) throws Exception {
        testImport(
                response, request, nf, null, expectReceiveDisconnect, requestCount, (engine, sqlExecutionContext) -> {
                }
        );
    }

    public void testImport(
            String response,
            String request,
            NetworkFacade nf,
            CairoConfiguration configuration,
            boolean expectReceiveDisconnect,
            int requestCount,
            HttpQueryTestBuilder.HttpClientCode createTable
    ) throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(nf)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run(
                        configuration,
                        (engine, sqlExecutionContext) -> {
                            createTable.run(engine, sqlExecutionContext);
                            sendAndReceive(
                                    nf,
                                    request,
                                    response,
                                    requestCount,
                                    0,
                                    false,
                                    expectReceiveDisconnect
                            );
                        }
                );
    }

    @Test
    public void testImportAfterColumnWasDropped() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                                engine.execute("create table test (col_a int, col_b long, ts timestamp) timestamp(ts) partition by week", executionContext);
                                engine.execute("alter table test drop column col_b", executionContext);

                                sendAndReceive(
                                        NetworkFacadeImpl.INSTANCE,
                                        "POST /upload?name=test HTTP/1.1\r\n" +
                                                "Host: localhost:9000\r\n" +
                                                "User-Agent: curl/7.71.1\r\n" +
                                                "Accept: */*\r\n" +
                                                "Content-Length: 243\r\n" +
                                                "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "Content-Disposition: form-data; name=\"data\"\r\n" +
                                                "\r\n" +
                                                "col_a,ts\r\n" +
                                                "1000,1000\r\n" +
                                                "2000,2000\r\n" +
                                                "3000,3000\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "0507\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|      Location:  |                                              test  |        Pattern  | Locale  |      Errors  |\r\n" +
                                                "|   Partition by  |                                              WEEK  |                 |         |              |\r\n" +
                                                "|      Timestamp  |                                                ts  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|   Rows handled  |                                                 3  |                 |         |              |\r\n" +
                                                "|  Rows imported  |                                                 3  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|              0  |                                             col_a  |                      INT  |           0  |\r\n" +
                                                "|              1  |                                                ts  |                TIMESTAMP  |           0  |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "\r\n" +
                                                "00\r\n" +
                                                "\r\n",
                                        1,
                                        0,
                                        false
                                );

                                StringSink sink = new StringSink();
                                TestUtils.assertSql(
                                        engine,
                                        executionContext,
                                        "test",
                                        sink,
                                        "col_a\tts\n" +
                                                "1000\t1970-01-01T00:00:00.001000Z\n" +
                                                "2000\t1970-01-01T00:00:00.002000Z\n" +
                                                "3000\t1970-01-01T00:00:00.003000Z\n"
                                );
                            }
                        }
                );
    }

    @Test
    public void testImportAfterColumnWasRecreated() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                                engine.execute("create table test (col_a int, col_b long)", executionContext);
                                engine.execute("alter table test drop column col_a", executionContext);
                                engine.execute("alter table test add column col_a long", executionContext);

                                sendAndReceive(
                                        NetworkFacadeImpl.INSTANCE,
                                        "POST /upload?name=test HTTP/1.1\r\n" +
                                                "Host: localhost:9000\r\n" +
                                                "User-Agent: curl/7.71.1\r\n" +
                                                "Accept: */*\r\n" +
                                                "Content-Length: 243\r\n" +
                                                "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "Content-Disposition: form-data; name=\"data\"\r\n" +
                                                "\r\n" +
                                                "col_a,col_b\r\n" +
                                                "1000,1000\r\n" +
                                                "2000,2000\r\n" +
                                                "3000,3000\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "0507\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|      Location:  |                                              test  |        Pattern  | Locale  |      Errors  |\r\n" +
                                                "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                                                "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|   Rows handled  |                                                 3  |                 |         |              |\r\n" +
                                                "|  Rows imported  |                                                 3  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|              0  |                                             col_b  |                     LONG  |           0  |\r\n" +
                                                "|              1  |                                             col_a  |                     LONG  |           0  |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "\r\n" +
                                                "00\r\n" +
                                                "\r\n",
                                        1,
                                        0,
                                        false
                                );

                                StringSink sink = new StringSink();
                                TestUtils.assertSql(
                                        engine,
                                        executionContext,
                                        "test",
                                        sink,
                                        "col_b\tcol_a\n" +
                                                "1000\t1000\n" +
                                                "2000\t2000\n" +
                                                "3000\t3000\n"
                                );
                            }
                        }
                );
    }

    @Test
    public void testImportBadJson() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "1e\r\n" +
                        "{\"status\":\"Unexpected symbol\"}\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?fmt=json&overwrite=true&forceHeader=true&name=clipboard-157200856 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"\r\n" +
                        "\r\n" +
                        "[{\"name\":\"timestamp,\"type\":\"DATE\"},{\"name\":\"bid\",\"type\":\"INT\"}]\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "timestamp,bid\r\n" +
                        "27/05/2018 00:00:01,100\r\n" +
                        "27/05/2018 00:00:02,101\r\n" +
                        "27/05/2018 00:00:03,102\r\n" +
                        "27/05/2018 00:00:04,103\r\n" +
                        "27/05/2018 00:00:05,104\r\n" +
                        "27/05/2018 00:00:06,105\r\n" +
                        "27/05/2018 00:00:07,106\r\n" +
                        "27/05/2018 00:00:08,107\r\n" +
                        "27/05/2018 00:00:09,108\r\n" +
                        "27/05/2018 00:00:10,109\r\n" +
                        "27/05/2018 00:00:11,110\r\n" +
                        "27/05/2018 00:00:12,111\r\n" +
                        "27/05/2018 00:00:13,112\r\n" +
                        "27/05/2018 00:00:14,113\r\n" +
                        "27/05/2018 00:00:15,114\r\n" +
                        "27/05/2018 00:00:16,115\r\n" +
                        "27/05/2018 00:00:17,116\r\n" +
                        "27/05/2018 00:00:18,117\r\n" +
                        "27/05/2018 00:00:19,118\r\n" +
                        "27/05/2018 00:00:20,119\r\n" +
                        "27/05/2018 00:00:21,120\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testImportBadRequestGet() throws Exception {
        testImport(
                "HTTP/1.1 405 Method Not Allowed\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "1a\r\n" +
                        "Method GET not supported\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n",
                "GET /upload?blah HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "DNT: 1\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: none\r\n" +
                        "Sec-Fetch-Mode: navigate\r\n" +
                        "Sec-Fetch-User: ?1\r\n" +
                        "Sec-Fetch-Dest: document\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en;q=0.9,es-AR;q=0.8,es;q=0.7\r\n" +
                        "\r\n",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );
    }

    @Test
    public void testImportBadRequestNoBoundaryDisconnects() throws Exception {
        testImport(
                "",
                "POST /upload?overwrite=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Accept: */*\r\n" +
                        "content-type: multipart/form-data\r\n" +
                        "\r\n",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testImportColumnMismatch() throws Exception {
        testImport(
                VALID_IMPORT_RESPONSE,
                "POST /upload HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "[\r\n" +
                        "  {\r\n" +
                        "    \"name\": \"date\",\r\n" +
                        "    \"type\": \"DATE\",\r\n" +
                        "    \"pattern\": \"d MMMM y.\",\r\n" +
                        "    \"locale\": \"ru-RU\"\r\n" +
                        "  }\r\n" +
                        "]\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "Dispatching_base_num,Pickup_DateTime,DropOff_datetime,PUlocationID,DOlocationID\r\n" +
                        "B00008,2017-02-01 00:30:00,,,\r\n" +
                        "B00008,2017-02-01 00:40:00,,,\r\n" +
                        "B00009,2017-02-01 00:30:00,,,\r\n" +
                        "B00013,2017-02-01 00:11:00,,,\r\n" +
                        "B00013,2017-02-01 00:41:00,,,\r\n" +
                        "B00013,2017-02-01 00:00:00,,,\r\n" +
                        "B00013,2017-02-01 00:53:00,,,\r\n" +
                        "B00013,2017-02-01 00:44:00,,,\r\n" +
                        "B00013,2017-02-01 00:05:00,,,\r\n" +
                        "B00013,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:46:00,,,\r\n" +
                        "B00014,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:26:00,,,\r\n" +
                        "B00014,2017-02-01 00:55:00,,,\r\n" +
                        "B00014,2017-02-01 00:47:00,,,\r\n" +
                        "B00014,2017-02-01 00:05:00,,,\r\n" +
                        "B00014,2017-02-01 00:58:00,,,\r\n" +
                        "B00014,2017-02-01 00:33:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );

        // append different data structure to the same table

        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "5d\r\n" +
                        "column count mismatch [textColumnCount=6, tableColumnCount=5, table=fhv_tripdata_2017-02.csv]\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?overwrite=false HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "[\r\n" +
                        "  {\r\n" +
                        "    \"name\": \"date\",\r\n" +
                        "    \"type\": \"DATE\",\r\n" +
                        "    \"pattern\": \"d MMMM y.\",\r\n" +
                        "    \"locale\": \"ru-RU\"\r\n" +
                        "  }\r\n" +
                        "]\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "Dispatching_base_num,DropOff_datetime,PUlocationID,DOlocationID,x,y\r\n" +
                        "B00008,,,,,\r\n" +
                        "B00008,,,,,\r\n" +
                        "B00009,,,,,\r\n" +
                        "B00013,,,,,\r\n" +
                        "B00013,,,,,\r\n" +
                        "B00013,,,,,\r\n" +
                        "B00013,,,,,\r\n" +
                        "B00013,,,,,\r\n" +
                        "B00013,,,,,\r\n" +
                        "B00013,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "B00014,,,,,\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );
    }

    @Test
    public void testImportDelimiterNotDetected() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "31\r\n" +
                        "not enough lines [table=fhv_tripdata_2017-02.csv]\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload HTTP/1.1\r\n" +
                        "host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "content-disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "content-type: application/octet-stream\r\n" +
                        "\r\n" +
                        "9988" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testImportEmptyData() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "041d\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|      Location:  |                          fhv_tripdata_2017-02.csv  |        Pattern  | Locale  |      Errors  |\r\n" +
                        "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                        "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|   Rows handled  |                                                 0  |                 |         |              |\r\n" +
                        "|  Rows imported  |                                                 0  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                NetworkFacadeImpl.INSTANCE,
                false,
                120
        );
    }

    @Test
    public void testImportEpochTimestamp() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                                engine.execute("create table test (ts timestamp, value int) timestamp(ts) partition by DAY", executionContext);

                                sendAndReceive(
                                        NetworkFacadeImpl.INSTANCE,
                                        "POST /upload?name=test HTTP/1.1\r\n" +
                                                "Host: localhost:9000\r\n" +
                                                "User-Agent: curl/7.71.1\r\n" +
                                                "Accept: */*\r\n" +
                                                "Content-Length: 372\r\n" +
                                                "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "Content-Disposition: form-data; name=\"data\"\r\n" +
                                                "\r\n" +
                                                "100000000,1000\r\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\r\n" +
                                                "100000001,2000\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "0507\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|      Location:  |                                              test  |        Pattern  | Locale  |      Errors  |\r\n" +
                                                "|   Partition by  |                                               DAY  |                 |         |              |\r\n" +
                                                "|      Timestamp  |                                                ts  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|   Rows handled  |                                                11  |                 |         |              |\r\n" +
                                                "|  Rows imported  |                                                11  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|              0  |                                                ts  |                TIMESTAMP  |           0  |\r\n" +
                                                "|              1  |                                             value  |                      INT  |           0  |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "\r\n" +
                                                "00\r\n" +
                                                "\r\n",
                                        1,
                                        0,
                                        false
                                );

                                StringSink sink = new StringSink();
                                TestUtils.assertSql(
                                        engine,
                                        executionContext,
                                        "test",
                                        sink,
                                        "ts\tvalue\n" +
                                                "1970-01-01T00:01:40.000000Z\t1000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n" +
                                                "1970-01-01T00:01:40.000001Z\t2000\n"
                                );
                            }
                        }
                );
    }

    @Test
    public void testImportForceUnknownDate() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "2c\r\n" +
                        "{\"status\":\"DATE format pattern is required\"}\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?fmt=json&overwrite=true&forceHeader=true&name=clipboard-157200856 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"\r\n" +
                        "\r\n" +
                        "[{\"name\":\"timestamp\",\"type\":\"DATE\"},{\"name\":\"bid\",\"type\":\"INT\"}]\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "timestamp,bid\r\n" +
                        "27/05/2018 00:00:01,100\r\n" +
                        "27/05/2018 00:00:02,101\r\n" +
                        "27/05/2018 00:00:03,102\r\n" +
                        "27/05/2018 00:00:04,103\r\n" +
                        "27/05/2018 00:00:05,104\r\n" +
                        "27/05/2018 00:00:06,105\r\n" +
                        "27/05/2018 00:00:07,106\r\n" +
                        "27/05/2018 00:00:08,107\r\n" +
                        "27/05/2018 00:00:09,108\r\n" +
                        "27/05/2018 00:00:10,109\r\n" +
                        "27/05/2018 00:00:11,110\r\n" +
                        "27/05/2018 00:00:12,111\r\n" +
                        "27/05/2018 00:00:13,112\r\n" +
                        "27/05/2018 00:00:14,113\r\n" +
                        "27/05/2018 00:00:15,114\r\n" +
                        "27/05/2018 00:00:16,115\r\n" +
                        "27/05/2018 00:00:17,116\r\n" +
                        "27/05/2018 00:00:18,117\r\n" +
                        "27/05/2018 00:00:19,118\r\n" +
                        "27/05/2018 00:00:20,119\r\n" +
                        "27/05/2018 00:00:21,120\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testImportForceUnknownTimestamp() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "31\r\n" +
                        "{\"status\":\"TIMESTAMP format pattern is required\"}\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?fmt=json&overwrite=true&forceHeader=true&name=clipboard-157200856 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"\r\n" +
                        "\r\n" +
                        "[{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"name\":\"bid\",\"type\":\"INT\"}]\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "timestamp,bid\r\n" +
                        "27/05/2018 00:00:01,100\r\n" +
                        "27/05/2018 00:00:02,101\r\n" +
                        "27/05/2018 00:00:03,102\r\n" +
                        "27/05/2018 00:00:04,103\r\n" +
                        "27/05/2018 00:00:05,104\r\n" +
                        "27/05/2018 00:00:06,105\r\n" +
                        "27/05/2018 00:00:07,106\r\n" +
                        "27/05/2018 00:00:08,107\r\n" +
                        "27/05/2018 00:00:09,108\r\n" +
                        "27/05/2018 00:00:10,109\r\n" +
                        "27/05/2018 00:00:11,110\r\n" +
                        "27/05/2018 00:00:12,111\r\n" +
                        "27/05/2018 00:00:13,112\r\n" +
                        "27/05/2018 00:00:14,113\r\n" +
                        "27/05/2018 00:00:15,114\r\n" +
                        "27/05/2018 00:00:16,115\r\n" +
                        "27/05/2018 00:00:17,116\r\n" +
                        "27/05/2018 00:00:18,117\r\n" +
                        "27/05/2018 00:00:19,118\r\n" +
                        "27/05/2018 00:00:20,119\r\n" +
                        "27/05/2018 00:00:21,120\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testImportGeoHashesForExistingTable() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                                engine.execute("create table test (geo1 geohash(1c), geo2 geohash(3c), geo4 geohash(6c), geo8 geohash(12c), geo2b geohash(2b))", executionContext);

                                sendAndReceive(
                                        NetworkFacadeImpl.INSTANCE,
                                        "POST /upload?name=test&forceHeader=true HTTP/1.1\r\n" +
                                                "Host: localhost:9000\r\n" +
                                                "User-Agent: curl/7.71.1\r\n" +
                                                "Accept: */*\r\n" +
                                                "Content-Length: 372\r\n" +
                                                "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "Content-Disposition: form-data; name=\"data\"\r\n" +
                                                "\r\n" +
                                                "geo1,geo2,geo4,geo8,geo2b\r\n" +
                                                "null,null,null,null,null\r\n" +
                                                "questdb1234567890,questdb1234567890,questdb1234567890,questdb1234567890,questdb1234567890\r\n" +
                                                "u10m99dd3pbj,u10m99dd3pbj,u10m99dd3pbj,u10m99dd3pbj,u10m99dd3pbj\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "0666\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|      Location:  |                                              test  |        Pattern  | Locale  |      Errors  |\r\n" +
                                                "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                                                "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|   Rows handled  |                                                 3  |                 |         |              |\r\n" +
                                                "|  Rows imported  |                                                 3  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|              0  |                                              geo1  |              GEOHASH(1c)  |           0  |\r\n" +
                                                "|              1  |                                              geo2  |              GEOHASH(3c)  |           0  |\r\n" +
                                                "|              2  |                                              geo4  |              GEOHASH(6c)  |           0  |\r\n" +
                                                "|              3  |                                              geo8  |             GEOHASH(12c)  |           0  |\r\n" +
                                                "|              4  |                                             geo2b  |              GEOHASH(2b)  |           0  |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "\r\n" +
                                                "00\r\n" +
                                                "\r\n",
                                        1,
                                        0,
                                        false
                                );

                                StringSink sink = new StringSink();
                                TestUtils.assertSql(
                                        engine,
                                        executionContext,
                                        "test",
                                        sink,
                                        "geo1\tgeo2\tgeo4\tgeo8\tgeo2b\n" +
                                                "\t\t\t\t\n" +
                                                "q\tque\tquestd\tquestdb12345\t10\n" +
                                                "u\tu10\tu10m99\tu10m99dd3pbj\t11\n"
                                );
                            }
                        }
                );
    }

    @Test
    public void testImportGeoHashesForNewTable() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                            try (
                                    SqlCompiler compiler = engine.getSqlCompiler();
                                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                            ) {
                                sendAndReceive(
                                        NetworkFacadeImpl.INSTANCE,
                                        "POST /upload?name=test&forceHeader=true HTTP/1.1\r\n" +
                                                "Host: localhost:9000\r\n" +
                                                "User-Agent: curl/7.71.1\r\n" +
                                                "Accept: */*\r\n" +
                                                "Content-Length: 372\r\n" +
                                                "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "Content-Disposition: form-data; name=\"schema\"\r\n" +
                                                "\r\n" +
                                                "[\r\n" +
                                                "{\"name\":\"geo1\",\"type\":\"GEOHASH(1c)\"},\r\n" +
                                                "{\"name\":\"geo2\",\"type\":\"GEOHASH(3c)\"},\r\n" +
                                                "{\"name\":\"geo4\",\"type\":\"GEOHASH(6c)\"},\r\n" +
                                                "{\"name\":\"geo8\",\"type\":\"GEOHASH(12c)\"},\r\n" +
                                                "{\"name\":\"geo2b\",\"type\":\"GEOHASH(2b)\"}\r\n" +
                                                "]\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                                "Content-Disposition: form-data; name=\"data\"\r\n" +
                                                "\r\n" +
                                                "geo1,geo2,geo4,geo8,geo2b\r\n" +
                                                "null,null,null,null,null\r\n" +
                                                "questdb1234567890,questdb1234567890,questdb1234567890,questdb1234567890,questdb1234567890\r\n" +
                                                "u10m99dd3pbj,u10m99dd3pbj,u10m99dd3pbj,u10m99dd3pbj,u10m99dd3pbj\r\n" +
                                                "\r\n" +
                                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "0666\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|      Location:  |                                              test  |        Pattern  | Locale  |      Errors  |\r\n" +
                                                "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                                                "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|   Rows handled  |                                                 3  |                 |         |              |\r\n" +
                                                "|  Rows imported  |                                                 3  |                 |         |              |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "|              0  |                                              geo1  |              GEOHASH(1c)  |           0  |\r\n" +
                                                "|              1  |                                              geo2  |              GEOHASH(3c)  |           0  |\r\n" +
                                                "|              2  |                                              geo4  |              GEOHASH(6c)  |           0  |\r\n" +
                                                "|              3  |                                              geo8  |             GEOHASH(12c)  |           0  |\r\n" +
                                                "|              4  |                                             geo2b  |              GEOHASH(2b)  |           0  |\r\n" +
                                                "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                                "\r\n" +
                                                "00\r\n" +
                                                "\r\n",
                                        1,
                                        0,
                                        false
                                );

                                StringSink sink = new StringSink();
                                TestUtils.assertSql(
                                        compiler,
                                        executionContext,
                                        "test",
                                        sink,
                                        "geo1\tgeo2\tgeo4\tgeo8\tgeo2b\n" +
                                                "\t\t\t\t\n" +
                                                "q\tque\tquestd\tquestdb12345\t10\n" +
                                                "u\tu10\tu10m99\tu10m99dd3pbj\t11\n"
                                );
                            }
                        }
                );
    }

    @Test
    public void testImportMultipleOnSameConnection()
            throws Exception {
        testImport(
                VALID_IMPORT_RESPONSE,
                "POST /upload HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "[\r\n" +
                        "  {\r\n" +
                        "    \"name\": \"date\",\r\n" +
                        "    \"type\": \"DATE\",\r\n" +
                        "    \"pattern\": \"d MMMM y.\",\r\n" +
                        "    \"locale\": \"ru-RU\"\r\n" +
                        "  }\r\n" +
                        "]\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "Dispatching_base_num,Pickup_DateTime,DropOff_datetime,PUlocationID,DOlocationID\r\n" +
                        "B00008,2017-02-01 00:30:00,,,\r\n" +
                        "B00008,2017-02-01 00:40:00,,,\r\n" +
                        "B00009,2017-02-01 00:30:00,,,\r\n" +
                        "B00013,2017-02-01 00:11:00,,,\r\n" +
                        "B00013,2017-02-01 00:41:00,,,\r\n" +
                        "B00013,2017-02-01 00:00:00,,,\r\n" +
                        "B00013,2017-02-01 00:53:00,,,\r\n" +
                        "B00013,2017-02-01 00:44:00,,,\r\n" +
                        "B00013,2017-02-01 00:05:00,,,\r\n" +
                        "B00013,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:46:00,,,\r\n" +
                        "B00014,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:26:00,,,\r\n" +
                        "B00014,2017-02-01 00:55:00,,,\r\n" +
                        "B00014,2017-02-01 00:47:00,,,\r\n" +
                        "B00014,2017-02-01 00:05:00,,,\r\n" +
                        "B00014,2017-02-01 00:58:00,,,\r\n" +
                        "B00014,2017-02-01 00:33:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--"
                , NetworkFacadeImpl.INSTANCE
                , false
                , 5
        );
    }

    @Test
    public void testImportMultipleOnSameConnectionFragmented() throws Exception {
        testImport(
                VALID_IMPORT_RESPONSE,
                "POST /upload HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "[\r\n" +
                        "  {\r\n" +
                        "    \"name\": \"date\",\r\n" +
                        "    \"type\": \"DATE\",\r\n" +
                        "    \"pattern\": \"d MMMM y.\",\r\n" +
                        "    \"locale\": \"ru-RU\"\r\n" +
                        "  }\r\n" +
                        "]\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "Dispatching_base_num,Pickup_DateTime,DropOff_datetime,PUlocationID,DOlocationID\r\n" +
                        "B00008,2017-02-01 00:30:00,,,\r\n" +
                        "B00008,2017-02-01 00:40:00,,,\r\n" +
                        "B00009,2017-02-01 00:30:00,,,\r\n" +
                        "B00013,2017-02-01 00:11:00,,,\r\n" +
                        "B00013,2017-02-01 00:41:00,,,\r\n" +
                        "B00013,2017-02-01 00:00:00,,,\r\n" +
                        "B00013,2017-02-01 00:53:00,,,\r\n" +
                        "B00013,2017-02-01 00:44:00,,,\r\n" +
                        "B00013,2017-02-01 00:05:00,,,\r\n" +
                        "B00013,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:46:00,,,\r\n" +
                        "B00014,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:26:00,,,\r\n" +
                        "B00014,2017-02-01 00:55:00,,,\r\n" +
                        "B00014,2017-02-01 00:47:00,,,\r\n" +
                        "B00014,2017-02-01 00:05:00,,,\r\n" +
                        "B00014,2017-02-01 00:58:00,,,\r\n" +
                        "B00014,2017-02-01 00:33:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                new NetworkFacadeImpl() {
                    @Override
                    public int sendRaw(long fd, long buffer, int bufferLen) {
                        // ensure we do not send more than one byte at a time
                        if (bufferLen > 0) {
                            return super.sendRaw(fd, buffer, 1);
                        }
                        return 0;
                    }
                },
                false,
                10
        );
    }

    @Test
    public void testImportMultipleOnSameConnectionInvalidTrailingBoundary() throws Exception {
        // notice, that the last '-' is missing from the trailing boundary
        // i.e. "--------------------------27d997ca93d2689d-" instead of "--------------------------27d997ca93d2689d--"
        final String request = "POST /upload HTTP/1.1\r\n" +
                "Host: localhost:9001\r\n" +
                "User-Agent: curl/7.64.0\r\n" +
                "Accept: */*\r\n" +
                "Content-Length: 437760673\r\n" +
                "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                "Expect: 100-continue\r\n" +
                "\r\n" +
                "--------------------------27d997ca93d2689d\r\n" +
                "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                "\r\n" +
                "[\r\n" +
                "  {\r\n" +
                "    \"name\": \"date\",\r\n" +
                "    \"type\": \"DATE\",\r\n" +
                "    \"pattern\": \"d MMMM y.\",\r\n" +
                "    \"locale\": \"ru-RU\"\r\n" +
                "  }\r\n" +
                "]\r\n" +
                "\r\n" +
                "--------------------------27d997ca93d2689d\r\n" +
                "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                "\r\n" +
                "Dispatching_base_num,Pickup_DateTime,DropOff_datetime,PUlocationID,DOlocationID\r\n" +
                "B00008,2017-02-01 00:30:00,,,\r\n" +
                "B00008,2017-02-01 00:40:00,,,\r\n" +
                "B00009,2017-02-01 00:30:00,,,\r\n" +
                "B00013,2017-02-01 00:11:00,,,\r\n" +
                "B00013,2017-02-01 00:41:00,,,\r\n" +
                "B00013,2017-02-01 00:00:00,,,\r\n" +
                "B00013,2017-02-01 00:53:00,,,\r\n" +
                "B00013,2017-02-01 00:44:00,,,\r\n" +
                "B00013,2017-02-01 00:05:00,,,\r\n" +
                "B00013,2017-02-01 00:54:00,,,\r\n" +
                "B00014,2017-02-01 00:45:00,,,\r\n" +
                "B00014,2017-02-01 00:45:00,,,\r\n" +
                "B00014,2017-02-01 00:46:00,,,\r\n" +
                "B00014,2017-02-01 00:54:00,,,\r\n" +
                "B00014,2017-02-01 00:45:00,,,\r\n" +
                "B00014,2017-02-01 00:45:00,,,\r\n" +
                "B00014,2017-02-01 00:45:00,,,\r\n" +
                "B00014,2017-02-01 00:26:00,,,\r\n" +
                "B00014,2017-02-01 00:55:00,,,\r\n" +
                "B00014,2017-02-01 00:47:00,,,\r\n" +
                "B00014,2017-02-01 00:05:00,,,\r\n" +
                "B00014,2017-02-01 00:58:00,,,\r\n" +
                "B00014,2017-02-01 00:33:00,,,\r\n" +
                "B00014,2017-02-01 00:45:00,,,\r\n" +
                "\r\n" +
                "--------------------------27d997ca93d2689d-";

        // ensure we do not send more than one byte at a time
        final NetworkFacade nf = new NetworkFacadeImpl() {
            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                // ensure we do not send more than one byte at a time
                if (bufferLen > 0) {
                    return super.sendRaw(fd, buffer, 1);
                }
                return 0;
            }
        };

        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(nf)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .withTelemetry(false)
                .withQueryTimeout(100)
                .run((engine, sqlExecutionContext) -> {
                    final long fd = nf.socketTcp(true);
                    try {
                        final long sockAddrInfo = nf.getAddrInfo("127.0.0.1", 9001);
                        assert sockAddrInfo != -1;
                        try {
                            TestUtils.assertConnectAddrInfo(fd, sockAddrInfo);
                            Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
                            nf.configureNonBlocking(fd);

                            final long bufLen = request.length();
                            final long ptr = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                            try {
                                new SendAndReceiveRequestBuilder()
                                        .withNetworkFacade(nf)
                                        .withPauseBetweenSendAndReceive(0)
                                        .withRequestCount(1)
                                        .executeUntilTimeoutExpires(request, 300);
                            } finally {
                                Unsafe.free(ptr, bufLen, MemoryTag.NATIVE_DEFAULT);
                            }
                        } finally {
                            nf.freeAddrInfo(sockAddrInfo);
                        }
                    } finally {
                        nf.close(fd);
                    }
                });
    }

    @Test
    public void testImportMultipleOnSameConnectionSlow() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(configuration, baseDir, false);
            final WorkerPool workerPool = new TestWorkerPool(3, httpConfiguration.getMetrics());
            try (
                    CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));
                httpServer.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjList<String> getUrls() {
                        return new ObjList<>("/upload");
                    }

                    @Override
                    public HttpRequestHandler newInstance() {
                        return new TextImportProcessor(engine, httpConfiguration.getJsonQueryProcessorConfiguration());
                    }
                });
                workerPool.start(LOG);

                // send multipart request to server
                final String request = "POST /upload HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "[\r\n" +
                        "  {\r\n" +
                        "    \"name\": \"date\",\r\n" +
                        "    \"type\": \"DATE\",\r\n" +
                        "    \"pattern\": \"d MMMM y.\",\r\n" +
                        "    \"locale\": \"ru-RU\"\r\n" +
                        "  }\r\n" +
                        "]\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "Dispatching_base_num,Pickup_DateTime,DropOff_datetime,PUlocationID,DOlocationID\r\n" +
                        "B00008,2017-02-01 00:30:00,,,\r\n" +
                        "B00008,2017-02-01 00:40:00,,,\r\n" +
                        "B00009,2017-02-01 00:30:00,,,\r\n" +
                        "B00013,2017-02-01 00:11:00,,,\r\n" +
                        "B00013,2017-02-01 00:41:00,,,\r\n" +
                        "B00013,2017-02-01 00:00:00,,,\r\n" +
                        "B00013,2017-02-01 00:53:00,,,\r\n" +
                        "B00013,2017-02-01 00:44:00,,,\r\n" +
                        "B00013,2017-02-01 00:05:00,,,\r\n" +
                        "B00013,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:46:00,,,\r\n" +
                        "B00014,2017-02-01 00:54:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "B00014,2017-02-01 00:26:00,,,\r\n" +
                        "B00014,2017-02-01 00:55:00,,,\r\n" +
                        "B00014,2017-02-01 00:47:00,,,\r\n" +
                        "B00014,2017-02-01 00:05:00,,,\r\n" +
                        "B00014,2017-02-01 00:58:00,,,\r\n" +
                        "B00014,2017-02-01 00:33:00,,,\r\n" +
                        "B00014,2017-02-01 00:45:00,,,\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--";


                NetworkFacade nf = new NetworkFacadeImpl() {
                    int totalSent = 0;

                    @Override
                    public int sendRaw(long fd, long buffer, int bufferLen) {
                        if (bufferLen > 0) {
                            int result = super.sendRaw(fd, buffer, 1);
                            totalSent += result;

                            // start delaying after 800 bytes

                            if (totalSent > 20) {
                                LockSupport.parkNanos(10000);
                                totalSent = 0;
                            }
                            return result;
                        }
                        return 0;
                    }
                };

                try {
                    sendAndReceive(
                            nf,
                            request,
                            VALID_IMPORT_RESPONSE,
                            3,
                            0,
                            false
                    );
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testImportNoSkipLEV() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "052e\r\n" +
                        "{\"status\":\"OK\",\"location\":\"clipboard-157200856\",\"rowsRejected\":0,\"rowsImported\":59,\"header\":true,\"partitionBy\":\"NONE\",\"columns\":[{\"name\":\"VendorID\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"lpep_pickup_datetime\",\"type\":\"DATE\",\"size\":8,\"errors\":0},{\"name\":\"Lpep_dropoff_datetime\",\"type\":\"DATE\",\"size\":8,\"errors\":0},{\"name\":\"Store_and_fwd_flag\",\"type\":\"CHAR\",\"size\":2,\"errors\":0},{\"name\":\"RateCodeID\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"Pickup_longitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Pickup_latitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Dropoff_longitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Dropoff_latitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Passenger_count\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"Trip_distance\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Fare_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Extra\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"MTA_tax\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Tip_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Tolls_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Ehail_fee\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Total_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Payment_type\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"Trip_type\",\"type\":\"INT\",\"size\":4,\"errors\":0}]}\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?fmt=json&overwrite=true&forceHeader=true&name=clipboard-157200856 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,Total_amount,Payment_type,Trip_type\r\n" +
                        "\r\n" +
                        "\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:18:34,N,1,0,0,-73.872024536132813,40.678714752197266,6,7.02,28.5,0,0.5,0,0,,29,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 13:10:37,N,1,0,0,-73.917839050292969,40.757766723632812,1,5.43,23.5,0,0.5,5.88,0,,29.88,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 14:36:16,N,1,0,0,-73.882896423339844,40.870456695556641,1,.84,5,0,0.5,0,0,,5.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 02:51:03,N,1,0,0,0,0,1,8.98,26.5,0.5,0.5,5.4,0,,32.9,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 03:13:09,N,1,0,0,0,0,1,.91,5.5,0.5,0.5,0,0,,6.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 14:12:18,N,1,0,0,0,0,1,2.88,13,0,0.5,2.6,0,,16.1,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:37:31,N,1,0,0,0,0,1,2.04,9,0,0.5,0,0,,9.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 08:05:26,N,1,0,0,-73.863983154296875,40.895206451416016,1,7.61,22.5,0,0.5,0,0,,23,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 17:02:26,N,1,0,0,0,0,1,3.37,14,0,0.5,7.5,0,,22,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 10:45:08,N,1,0,0,-73.98382568359375,40.672164916992187,5,2.98,11,0,0.5,0,0,,11.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:23:12,N,1,0,0,-73.897506713867188,40.856563568115234,1,6.10,21,0,0.5,4.2,0,,25.7,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 20:30:34,N,1,0,0,-73.834732055664063,40.769981384277344,1,4.03,13.5,0.5,0.5,0,0,,14.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 02:11:02,N,1,0,0,-73.962692260742187,40.805278778076172,1,11.02,36.5,0.5,0.5,9.25,0,,46.75,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 01:12:02,N,1,0,0,-73.812576293945313,40.72515869140625,1,2.98,11,0.5,0.5,2.3,0,,14.3,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 00:11:44,N,1,-73.807571411132813,40.700370788574219,-73.759422302246094,40.704967498779297,1,3.14,12,0.5,0.5,2.5,0,,15.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 09:35:57,N,1,0,0,-74.008323669433594,40.733074188232422,1,7.41,24,0,0.5,5.87,5.33,,35.7,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 20:03:23,N,1,0,0,-73.934471130371094,40.753532409667969,2,1.67,7.5,0,0.5,1.88,0,,9.88,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 09:25:16,N,1,0,0,-73.964775085449219,40.713218688964844,6,3.18,13.5,0,0.5,2.7,0,,16.7,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 07:19:12,N,1,0,0,0,0,1,7.78,23,0,0.5,0,0,,23.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 14:30:15,N,1,0,0,-73.793098449707031,40.699207305908203,1,7.05,25.5,0,0.5,0,0,,26,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 08:15:29,N,1,0,0,-73.994560241699219,40.738136291503906,1,6.82,21.5,0,0.5,4.3,0,,26.3,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:50:35,N,1,0,0,-73.856315612792969,40.855121612548828,1,10.09,33.5,0,0.5,0,0,,34,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 12:46:27,N,1,0,0,0,0,1,4.18,18,0,0.5,3.6,0,,22.1,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 07:49:00,N,1,0,0,-73.9754638671875,40.750938415527344,1,6.29,23,0,0.5,0,0,,23.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 06:54:37,N,1,0,0,0,0,1,6.40,19.5,0,0.5,0,0,,20,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 11:26:06,N,1,0,0,-73.937446594238281,40.758167266845703,2,.00,2.5,0,0.5,0.5,0,,3.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:53:49,N,1,0,0,-73.995964050292969,40.690750122070313,1,1.90,11,0,0.5,1.5,0,,13,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:31:59,N,3,0,0,0,0,1,.42,21,0,0,0,0,,21,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 21:11:09,N,1,0,0,-73.961799621582031,40.713447570800781,2,3.68,13,0.5,0.5,0,0,,14,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 09:18:54,N,1,0,0,-73.839179992675781,40.8271484375,1,1.08,5.5,0,0.5,0,0,,6,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 21:06:16,N,1,0,0,0,0,1,.02,4,0.5,0.5,0,0,,5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 21:11:52,N,1,0,0,-73.883941650390625,40.741928100585937,1,1.08,6.5,0.5,0.5,0,0,,7.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 20:12:17,N,1,0,0,-73.860641479492188,40.756160736083984,1,2.01,9.5,0,0.5,2.38,0,,12.38,1,1,,\r\n" +
                        "2,2014-03-01 00:00:01,2014-03-01 00:04:27,N,1,-73.95135498046875,40.809841156005859,-73.937583923339844,40.804347991943359,1,.89,5.5,0.5,0.5,0,0,,6.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:03,2014-03-01 00:39:11,N,1,-73.95880126953125,40.716785430908203,-73.908256530761719,40.69879150390625,1,7.05,28,0.5,0.5,0,0,,29,2,1,,\r\n" +
                        "1,2014-03-01 00:00:03,2014-03-01 00:14:32,N,1,-73.938880920410156,40.681663513183594,-73.956787109375,40.713565826416016,1,3.30,13.5,0.5,0.5,2.9,0,,17.4,1,,,\r\n" +
                        "2,2014-03-01 00:00:03,2014-03-01 00:08:42,N,1,-73.941375732421875,40.818492889404297,-73.93524169921875,40.796005249023438,1,2.38,10,0.5,0.5,0,0,,11,2,1,,\r\n" +
                        "2,2014-03-01 00:00:05,2014-03-01 00:08:34,N,1,-73.951713562011719,40.714748382568359,-73.954734802246094,40.732883453369141,1,1.45,8,0.5,0.5,0,0,,9,2,1,,\r\n" +
                        "2,2014-03-01 00:00:05,2014-03-01 00:05:14,N,1,-73.904586791992188,40.753456115722656,-73.883033752441406,40.755744934082031,1,1.15,6.5,0.5,0.5,0,0,,7.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:06,2014-03-01 00:05:50,N,1,-73.917320251464844,40.770088195800781,-73.890525817871094,40.768100738525391,1,1.83,8,0.5,0.5,1.7,0,,10.7,1,1,,\r\n" +
                        "1,2014-03-01 00:00:07,2014-03-01 00:11:19,N,1,-73.964630126953125,40.712295532226563,-73.947219848632813,40.721889495849609,2,1.50,9,0.5,0.5,1,0,,11,1,,,\r\n" +
                        "2,2014-03-01 00:00:07,2014-03-01 00:14:04,N,1,-73.925445556640625,40.761676788330078,-73.876060485839844,40.756378173828125,1,2.81,12,0.5,0.5,0,0,,13,2,1,,\r\n" +
                        "2,2014-03-01 00:00:10,2014-03-01 00:07:49,N,1,-73.920318603515625,40.759616851806641,-73.925506591796875,40.771896362304688,1,1.44,7.5,0.5,0.5,0,0,,8.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:10,2014-03-01 00:13:21,N,1,-73.947578430175781,40.825412750244141,-73.94903564453125,40.793388366699219,1,3.02,12.5,0.5,0.5,0,0,,13.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:10,2014-03-01 00:13:15,N,1,-73.957618713378906,40.730094909667969,-73.967720031738281,40.687759399414062,1,3.97,14,0.5,0.5,2.9,0,,17.9,1,1,,\r\n" +
                        "2,2014-03-01 00:00:11,2014-03-01 00:11:25,N,1,-73.950340270996094,40.706771850585938,-73.983001708984375,40.696136474609375,1,2.33,10.5,0.5,0.5,2.2,0,,13.7,1,1,,\r\n" +
                        "1,2014-03-01 00:00:11,2014-03-01 00:05:42,N,1,-73.96142578125,40.675296783447266,-73.956123352050781,40.682975769042969,1,.80,5.5,0.5,0.5,0,0,,6.5,2,,,\r\n" +
                        "2,2014-03-01 00:00:13,2014-03-01 00:26:16,N,1,-73.93438720703125,40.682884216308594,-73.987312316894531,40.724613189697266,1,5.29,21.5,0.5,0.5,4.4,0,,26.9,1,1,,\r\n" +
                        "2,2014-03-01 00:00:13,2014-03-01 00:05:50,N,1,-73.831787109375,40.715095520019531,-73.811759948730469,40.719070434570313,1,1.79,7.5,0.5,0.5,1.6,0,,10.1,1,1,,\r\n" +
                        "1,2014-03-01 00:00:15,2014-03-01 00:37:17,N,1,-73.958778381347656,40.730594635009766,-74.000518798828125,40.752723693847656,1,7.40,29.5,0.5,0.5,7.6,0,,38.1,1,,,\r\n" +
                        "2,2014-03-01 00:00:15,2014-03-01 00:18:48,N,1,-73.944183349609375,40.714580535888672,-73.98779296875,40.732589721679688,1,3.82,16,0.5,0.5,4.95,0,,21.95,1,1,,\r\n" +
                        "2,2014-03-01 00:00:16,2014-03-01 00:04:28,N,1,-73.913551330566406,40.838531494140625,-73.899406433105469,40.838657379150391,1,.94,5.5,0.5,0.5,0,0,,6.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:16,2014-03-01 00:18:50,N,1,-73.917015075683594,40.761211395263672,-73.850166320800781,40.725177764892578,2,7.17,23,0.5,0.5,0,0,,24,2,1,,\r\n" +
                        "1,2014-03-01 00:00:17,2014-03-01 00:02:34,N,1,-73.956565856933594,40.748039245605469,-73.958755493164063,40.742103576660156,1,.50,3.5,0.5,0.5,0,0,,4.5,2,,,\r\n" +
                        "1,2014-03-01 00:00:18,2014-03-01 00:10:56,N,1,-73.990753173828125,40.692584991455078,-73.942802429199219,40.714881896972656,1,4.10,14,0.5,0.5,0,0,,15,2,,,\r\n" +
                        "1,2014-03-01 00:00:18,2014-03-01 00:03:29,N,1,-73.807746887207031,40.700340270996094,-73.815444946289062,40.695743560791016,1,.70,4.5,0.5,0.5,0,0,,5.5,2,,,\r\n" +
                        "2,2014-03-01 00:00:21,2014-03-01 00:21:36,N,1,-73.957740783691406,40.729896545410156,-73.92779541015625,40.697731018066406,1,3.95,17,0.5,0.5,4.38,0,,22.38,1,1,,\r\n" +
                        "2,2014-03-01 00:00:22,2014-03-01 00:01:53,N,1,-73.94354248046875,40.820354461669922,-73.949432373046875,40.812416076660156,1,.45,3.5,0.5,0.5,0,0,,4.5,2,1,,\r\n" +
                        "1,2014-03-01 00:00:22,2014-03-01 00:07:17,N,1,-73.9451904296875,40.689888000488281,-73.937591552734375,40.680465698242187,1,1.00,6.5,0.5,0.5,0,0,,7.5,2,,,\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );
    }

    @Test
    public void testImportSettingO3MaxLagAndMaxUncommittedRows1() throws Exception {
        importWithO3MaxLagAndMaxUncommittedRowsTableNotExists(
                240_000_000, // 4 minutes, micro precision
                3,
                240_000_000, // 4 minutes, micro precision
                3,
                6,
                "ts,int\r\n" +
                        "2021-01-01 00:04:00,3\r\n" +
                        "2021-01-01 00:05:00,4\r\n" +
                        "2021-01-02 00:05:31,6\r\n" +
                        "2021-01-01 00:01:00,1\r\n" +
                        "2021-01-01 00:01:30,2\r\n" +
                        "2021-01-02 00:00:30,5\r\n",
                "2021-01-01T00:01:00.000000Z\t1\n" +
                        "2021-01-01T00:01:30.000000Z\t2\n" +
                        "2021-01-01T00:04:00.000000Z\t3\n" +
                        "2021-01-01T00:05:00.000000Z\t4\n" +
                        "2021-01-02T00:00:30.000000Z\t5\n" +
                        "2021-01-02T00:05:31.000000Z\t6\n"
        );
    }

    @Test
    public void testImportSettingO3MaxLagAndMaxUncommittedRows2() throws Exception {
        importWithO3MaxLagAndMaxUncommittedRowsTableNotExists(
                120_000_000, // 2 minutes, micro precision
                1,
                120_000_000,
                1,
                5,
                "ts,int\r\n" +
                        "2021-01-01 00:05:00,3\r\n" +
                        "2021-01-01 00:01:00,1\r\n" +
                        "2021-01-02 00:05:31,5\r\n" +
                        "2021-01-01 00:01:30,2\r\n" +
                        "2021-01-02 00:00:30,4\r\n",
                "2021-01-01T00:01:00.000000Z\t1\n" +
                        "2021-01-01T00:01:30.000000Z\t2\n" +
                        "2021-01-01T00:05:00.000000Z\t3\n" +
                        "2021-01-02T00:00:30.000000Z\t4\n" +
                        "2021-01-02T00:05:31.000000Z\t5\n"
        );
    }

    @Test
    public void testImportSingleRowWithConfiguredDelimiter() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "0666\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|      Location:  |                                          test.csv  |        Pattern  | Locale  |      Errors  |\r\n" +
                        "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                        "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|   Rows handled  |                                                 1  |                 |         |              |\r\n" +
                        "|  Rows imported  |                                                 1  |                 |         |              |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "|              0  |                                                f0  |                  VARCHAR  |           0  |\r\n" +
                        "|              1  |                                                f1  |                  VARCHAR  |           0  |\r\n" +
                        "|              2  |                                                f2  |                  VARCHAR  |           0  |\r\n" +
                        "|              3  |                                                f3  |                   DOUBLE  |           0  |\r\n" +
                        "|              4  |                                                f4  |                TIMESTAMP  |           0  |\r\n" +
                        "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?delimiter=%2C HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 252\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------af41c30bab413e07\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------af41c30bab413e07\r\n" +
                        "Content-Disposition: form-data; name=\"data\"; filename=\"test.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "test,test,test,1.52E+18,2018-01-12T19:28:48.127800Z\r\n" +
                        "\r\n" +
                        "--------------------------af41c30bab413e07--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );
    }

    @Test
    public void testImportSkipLEV() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "0557\r\n" +
                        "{\"status\":\"OK\",\"location\":\"clipboard-157200856\",\"rowsRejected\":59,\"rowsImported\":59,\"header\":true,\"partitionBy\":\"NONE\",\"columns\":[{\"name\":\"VendorID\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"lpep_pickup_datetime\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Lpep_dropoff_datetime\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Store_and_fwd_flag\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"RateCodeID\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Pickup_longitude\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Pickup_latitude\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Dropoff_longitude\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Dropoff_latitude\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Passenger_count\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Trip_distance\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Fare_amount\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Extra\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"MTA_tax\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Tip_amount\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Tolls_amount\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Ehail_fee\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Total_amount\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Payment_type\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0},{\"name\":\"Trip_type\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0}]}\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?fmt=json&overwrite=true&forceHeader=true&skipLev=true&name=clipboard-157200856 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,Total_amount,Payment_type,Trip_type\r\n" +
                        "\r\n" +
                        "\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:18:34,N,1,0,0,-73.872024536132813,40.678714752197266,6,7.02,28.5,0,0.5,0,0,,29,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 13:10:37,N,1,0,0,-73.917839050292969,40.757766723632812,1,5.43,23.5,0,0.5,5.88,0,,29.88,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 14:36:16,N,1,0,0,-73.882896423339844,40.870456695556641,1,.84,5,0,0.5,0,0,,5.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 02:51:03,N,1,0,0,0,0,1,8.98,26.5,0.5,0.5,5.4,0,,32.9,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 03:13:09,N,1,0,0,0,0,1,.91,5.5,0.5,0.5,0,0,,6.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 14:12:18,N,1,0,0,0,0,1,2.88,13,0,0.5,2.6,0,,16.1,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:37:31,N,1,0,0,0,0,1,2.04,9,0,0.5,0,0,,9.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 08:05:26,N,1,0,0,-73.863983154296875,40.895206451416016,1,7.61,22.5,0,0.5,0,0,,23,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 17:02:26,N,1,0,0,0,0,1,3.37,14,0,0.5,7.5,0,,22,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 10:45:08,N,1,0,0,-73.98382568359375,40.672164916992187,5,2.98,11,0,0.5,0,0,,11.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:23:12,N,1,0,0,-73.897506713867188,40.856563568115234,1,6.10,21,0,0.5,4.2,0,,25.7,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 20:30:34,N,1,0,0,-73.834732055664063,40.769981384277344,1,4.03,13.5,0.5,0.5,0,0,,14.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 02:11:02,N,1,0,0,-73.962692260742187,40.805278778076172,1,11.02,36.5,0.5,0.5,9.25,0,,46.75,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 01:12:02,N,1,0,0,-73.812576293945313,40.72515869140625,1,2.98,11,0.5,0.5,2.3,0,,14.3,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 00:11:44,N,1,-73.807571411132813,40.700370788574219,-73.759422302246094,40.704967498779297,1,3.14,12,0.5,0.5,2.5,0,,15.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 09:35:57,N,1,0,0,-74.008323669433594,40.733074188232422,1,7.41,24,0,0.5,5.87,5.33,,35.7,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 20:03:23,N,1,0,0,-73.934471130371094,40.753532409667969,2,1.67,7.5,0,0.5,1.88,0,,9.88,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 09:25:16,N,1,0,0,-73.964775085449219,40.713218688964844,6,3.18,13.5,0,0.5,2.7,0,,16.7,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 07:19:12,N,1,0,0,0,0,1,7.78,23,0,0.5,0,0,,23.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 14:30:15,N,1,0,0,-73.793098449707031,40.699207305908203,1,7.05,25.5,0,0.5,0,0,,26,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 08:15:29,N,1,0,0,-73.994560241699219,40.738136291503906,1,6.82,21.5,0,0.5,4.3,0,,26.3,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:50:35,N,1,0,0,-73.856315612792969,40.855121612548828,1,10.09,33.5,0,0.5,0,0,,34,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 12:46:27,N,1,0,0,0,0,1,4.18,18,0,0.5,3.6,0,,22.1,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 07:49:00,N,1,0,0,-73.9754638671875,40.750938415527344,1,6.29,23,0,0.5,0,0,,23.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 06:54:37,N,1,0,0,0,0,1,6.40,19.5,0,0.5,0,0,,20,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 11:26:06,N,1,0,0,-73.937446594238281,40.758167266845703,2,.00,2.5,0,0.5,0.5,0,,3.5,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:53:49,N,1,0,0,-73.995964050292969,40.690750122070313,1,1.90,11,0,0.5,1.5,0,,13,1,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 19:31:59,N,3,0,0,0,0,1,.42,21,0,0,0,0,,21,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 21:11:09,N,1,0,0,-73.961799621582031,40.713447570800781,2,3.68,13,0.5,0.5,0,0,,14,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 09:18:54,N,1,0,0,-73.839179992675781,40.8271484375,1,1.08,5.5,0,0.5,0,0,,6,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 21:06:16,N,1,0,0,0,0,1,.02,4,0.5,0.5,0,0,,5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 21:11:52,N,1,0,0,-73.883941650390625,40.741928100585937,1,1.08,6.5,0.5,0.5,0,0,,7.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:00,2014-03-01 20:12:17,N,1,0,0,-73.860641479492188,40.756160736083984,1,2.01,9.5,0,0.5,2.38,0,,12.38,1,1,,\r\n" +
                        "2,2014-03-01 00:00:01,2014-03-01 00:04:27,N,1,-73.95135498046875,40.809841156005859,-73.937583923339844,40.804347991943359,1,.89,5.5,0.5,0.5,0,0,,6.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:03,2014-03-01 00:39:11,N,1,-73.95880126953125,40.716785430908203,-73.908256530761719,40.69879150390625,1,7.05,28,0.5,0.5,0,0,,29,2,1,,\r\n" +
                        "1,2014-03-01 00:00:03,2014-03-01 00:14:32,N,1,-73.938880920410156,40.681663513183594,-73.956787109375,40.713565826416016,1,3.30,13.5,0.5,0.5,2.9,0,,17.4,1,,,\r\n" +
                        "2,2014-03-01 00:00:03,2014-03-01 00:08:42,N,1,-73.941375732421875,40.818492889404297,-73.93524169921875,40.796005249023438,1,2.38,10,0.5,0.5,0,0,,11,2,1,,\r\n" +
                        "2,2014-03-01 00:00:05,2014-03-01 00:08:34,N,1,-73.951713562011719,40.714748382568359,-73.954734802246094,40.732883453369141,1,1.45,8,0.5,0.5,0,0,,9,2,1,,\r\n" +
                        "2,2014-03-01 00:00:05,2014-03-01 00:05:14,N,1,-73.904586791992188,40.753456115722656,-73.883033752441406,40.755744934082031,1,1.15,6.5,0.5,0.5,0,0,,7.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:06,2014-03-01 00:05:50,N,1,-73.917320251464844,40.770088195800781,-73.890525817871094,40.768100738525391,1,1.83,8,0.5,0.5,1.7,0,,10.7,1,1,,\r\n" +
                        "1,2014-03-01 00:00:07,2014-03-01 00:11:19,N,1,-73.964630126953125,40.712295532226563,-73.947219848632813,40.721889495849609,2,1.50,9,0.5,0.5,1,0,,11,1,,,\r\n" +
                        "2,2014-03-01 00:00:07,2014-03-01 00:14:04,N,1,-73.925445556640625,40.761676788330078,-73.876060485839844,40.756378173828125,1,2.81,12,0.5,0.5,0,0,,13,2,1,,\r\n" +
                        "2,2014-03-01 00:00:10,2014-03-01 00:07:49,N,1,-73.920318603515625,40.759616851806641,-73.925506591796875,40.771896362304688,1,1.44,7.5,0.5,0.5,0,0,,8.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:10,2014-03-01 00:13:21,N,1,-73.947578430175781,40.825412750244141,-73.94903564453125,40.793388366699219,1,3.02,12.5,0.5,0.5,0,0,,13.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:10,2014-03-01 00:13:15,N,1,-73.957618713378906,40.730094909667969,-73.967720031738281,40.687759399414062,1,3.97,14,0.5,0.5,2.9,0,,17.9,1,1,,\r\n" +
                        "2,2014-03-01 00:00:11,2014-03-01 00:11:25,N,1,-73.950340270996094,40.706771850585938,-73.983001708984375,40.696136474609375,1,2.33,10.5,0.5,0.5,2.2,0,,13.7,1,1,,\r\n" +
                        "1,2014-03-01 00:00:11,2014-03-01 00:05:42,N,1,-73.96142578125,40.675296783447266,-73.956123352050781,40.682975769042969,1,.80,5.5,0.5,0.5,0,0,,6.5,2,,,\r\n" +
                        "2,2014-03-01 00:00:13,2014-03-01 00:26:16,N,1,-73.93438720703125,40.682884216308594,-73.987312316894531,40.724613189697266,1,5.29,21.5,0.5,0.5,4.4,0,,26.9,1,1,,\r\n" +
                        "2,2014-03-01 00:00:13,2014-03-01 00:05:50,N,1,-73.831787109375,40.715095520019531,-73.811759948730469,40.719070434570313,1,1.79,7.5,0.5,0.5,1.6,0,,10.1,1,1,,\r\n" +
                        "1,2014-03-01 00:00:15,2014-03-01 00:37:17,N,1,-73.958778381347656,40.730594635009766,-74.000518798828125,40.752723693847656,1,7.40,29.5,0.5,0.5,7.6,0,,38.1,1,,,\r\n" +
                        "2,2014-03-01 00:00:15,2014-03-01 00:18:48,N,1,-73.944183349609375,40.714580535888672,-73.98779296875,40.732589721679688,1,3.82,16,0.5,0.5,4.95,0,,21.95,1,1,,\r\n" +
                        "2,2014-03-01 00:00:16,2014-03-01 00:04:28,N,1,-73.913551330566406,40.838531494140625,-73.899406433105469,40.838657379150391,1,.94,5.5,0.5,0.5,0,0,,6.5,2,1,,\r\n" +
                        "2,2014-03-01 00:00:16,2014-03-01 00:18:50,N,1,-73.917015075683594,40.761211395263672,-73.850166320800781,40.725177764892578,2,7.17,23,0.5,0.5,0,0,,24,2,1,,\r\n" +
                        "1,2014-03-01 00:00:17,2014-03-01 00:02:34,N,1,-73.956565856933594,40.748039245605469,-73.958755493164063,40.742103576660156,1,.50,3.5,0.5,0.5,0,0,,4.5,2,,,\r\n" +
                        "1,2014-03-01 00:00:18,2014-03-01 00:10:56,N,1,-73.990753173828125,40.692584991455078,-73.942802429199219,40.714881896972656,1,4.10,14,0.5,0.5,0,0,,15,2,,,\r\n" +
                        "1,2014-03-01 00:00:18,2014-03-01 00:03:29,N,1,-73.807746887207031,40.700340270996094,-73.815444946289062,40.695743560791016,1,.70,4.5,0.5,0.5,0,0,,5.5,2,,,\r\n" +
                        "2,2014-03-01 00:00:21,2014-03-01 00:21:36,N,1,-73.957740783691406,40.729896545410156,-73.92779541015625,40.697731018066406,1,3.95,17,0.5,0.5,4.38,0,,22.38,1,1,,\r\n" +
                        "2,2014-03-01 00:00:22,2014-03-01 00:01:53,N,1,-73.94354248046875,40.820354461669922,-73.949432373046875,40.812416076660156,1,.45,3.5,0.5,0.5,0,0,,4.5,2,1,,\r\n" +
                        "1,2014-03-01 00:00:22,2014-03-01 00:07:17,N,1,-73.9451904296875,40.689888000488281,-73.937591552734375,40.680465698242187,1,1.00,6.5,0.5,0.5,0,0,,7.5,2,,,\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );
    }

    @Test
    public void testImportWithSingleCharacterColumnName() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "e8\r\n" +
                        "{\"status\":\"OK\",\"location\":\"test\",\"rowsRejected\":0,\"rowsImported\":1,\"header\":true,\"partitionBy\":\"MONTH\",\"timestamp\":\"ts\",\"columns\":[{\"name\":\"ts\",\"type\":\"TIMESTAMP\",\"size\":8,\"errors\":0},{\"name\":\"a\",\"type\":\"CHAR\",\"size\":2,\"errors\":0}]}\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload?fmt=json&overwrite=true&forceHeader=false&name=test&timestamp=ts&partitionBy=MONTH HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "ts,a\r\n" +
                        "2022-11-01T22:34:49.273814+0000,\"a\"\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );
    }

    @Test
    public void testInsertNoMemLeak() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE test(id LONG);");
            testHttpClient.assertGet("{\"dml\":\"OK\"}", "INSERT INTO test values(alloc(42));");
            testHttpClient.assertGet(
                    "{\"query\":\"test;\",\"columns\":[{\"name\":\"id\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                    "test;"
            );
        });
    }

    @Test
    public void testInterval() throws Exception {
        testJsonQuery(
                0,
                "GET /query?query=" + urlEncodeQuery("select interval(x*10000,(x+1)*10000) from long_sequence(3)") + " HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0154\r\n" +
                        "{\"query\":\"select interval(x*10000,(x+1)*10000) from long_sequence(3)\",\"columns\":[{\"name\":\"interval\",\"type\":\"INTERVAL\"}],\"timestamp\":-1,\"dataset\":[[\"('1970-01-01T00:00:00.010Z', '1970-01-01T00:00:00.020Z')\"],[\"('1970-01-01T00:00:00.020Z', '1970-01-01T00:00:00.030Z')\"],[\"('1970-01-01T00:00:00.030Z', '1970-01-01T00:00:00.040Z')\"]],\"count\":3}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testInvalidRequestIsHandled() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    final String request = " /query?query=drop%20table%20x HTTP/1.1\r\n" +
                            "Host: localhost:9001\r\n" +
                            "Connection: keep-alive\r\n" +
                            "Cache-Control: max-age=0\r\n" +
                            "Upgrade-Insecure-Requests: 1\r\n" +
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                            "Accept-Encoding: gzip, deflate, br\r\n" +
                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                            "\r\n";
                    new SendAndReceiveRequestBuilder().execute(
                            request,
                            "HTTP/1.1 400 Bad request\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: text/plain; charset=utf-8\r\n" +
                                    "\r\n" +
                                    "23\r\n" +
                                    "Method is not set in HTTP request\r\n" +
                                    "\r\n" +
                                    "00\r\n"
                    );
                });
    }

    @Test
    public void testJsonFloatClipping() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet(
                    "{\"ddl\":\"OK\"}",
                    "CREATE TABLE t0(f FLOAT, d DOUBLE);"
            );

            testHttpClient.assertGet(
                    "{\"dml\":\"OK\"}",
                    "INSERT INTO t0 VALUES (54321, 54321);"
            );

            testHttpClient.assertGet(
                    "{\"query\":\"SELECT * FROM t0;\",\"columns\":[{\"name\":\"f\",\"type\":\"FLOAT\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[54321.0,54321.0]],\"count\":1}",
                    "SELECT * FROM t0;"
            );
        });
    }

    @Test
    public void testJsonImplicitCastException() throws Exception {
        testJsonQuery(
                0,
                "GET /exec?limit=0%2C1000&explain=true&count=true&src=con&query=create%20table%20op(a%20int)&timings=true HTTP/1.1\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Referer: http://localhost:9000/\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0c\r\n" +
                        "{\"ddl\":\"OK\"}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );

        testJsonQuery(
                0,
                "GET /exec?limit=0%2C1000&explain=true&count=true&src=con&query=insert%20into%20op%20values%20(%27abc%27) HTTP/1.1\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Referer: http://localhost:9000/\r\n" +
                        "\r\n",
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "6b\r\n" +
                        "{\"query\":\"insert into op values ('abc')\",\"error\":\"inconvertible value: `abc` [STRING -> INT]\",\"position\":0}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testJsonNullColumnType() throws Exception {
        testJsonQuery(
                0,
                "GET /query?limit=0%2C1000&explain=true&count=true&src=con&query=%0D%0A%0D%0A%0D%0ASELECT%20*%20FROM%20(%0D%0A%20%20SELECT%20%0D%0A%20%20%20%20n.nspname%0D%0A%20%20%20%20%2Cc.relname%0D%0A%20%20%20%20%2Ca.attname%0D%0A%20%20%20%20%2Ca.atttypid%0D%0A%20%20%20%20%2Ca.attnotnull%20OR%20(t.typtype%20%3D%20%27d%27%20AND%20t.typnotnull)%20AS%20attnotnull%0D%0A%20%20%20%20%2Ca.atttypmod%0D%0A%20%20%20%20%2Ca.attlen%0D%0A%20%20%20%20%2Ct.typtypmod%0D%0A%20%20%20%20%2Crow_number()%20OVER%20(PARTITION%20BY%20a.attrelid%20ORDER%20BY%20a.attnum)%20AS%20attnum%0D%0A%20%20%20%20%2C%20nullif(a.attidentity%2C%20%27%27)%20as%20attidentity%0D%0A%20%20%20%20%2Cnull%20as%20attgenerated%0D%0A%20%20%20%20%2Cpg_catalog.pg_get_expr(def.adbin%2C%20def.adrelid)%20AS%20adsrc%0D%0A%20%20%20%20%2Cdsc.description%0D%0A%20%20%20%20%2Ct.typbasetype%0D%0A%20%20%20%20%2Ct.typtype%20%20%0D%0A%20%20FROM%20pg_catalog.pg_namespace%20n%0D%0A%20%20JOIN%20pg_catalog.pg_class%20c%20ON%20(c.relnamespace%20%3D%20n.oid)%0D%0A%20%20JOIN%20pg_catalog.pg_attribute%20a%20ON%20(a.attrelid%3Dc.oid)%0D%0A%20%20JOIN%20pg_catalog.pg_type%20t%20ON%20(a.atttypid%20%3D%20t.oid)%0D%0A%20%20LEFT%20JOIN%20pg_catalog.pg_attrdef%20def%20ON%20(a.attrelid%3Ddef.adrelid%20AND%20a.attnum%20%3D%20def.adnum)%0D%0A%20%20LEFT%20JOIN%20pg_catalog.pg_description%20dsc%20ON%20(c.oid%3Ddsc.objoid%20AND%20a.attnum%20%3D%20dsc.objsubid)%0D%0A%20%20LEFT%20JOIN%20pg_catalog.pg_class%20dc%20ON%20(dc.oid%3Ddsc.classoid%20AND%20dc.relname%3D%27pg_class%27)%0D%0A%20%20LEFT%20JOIN%20pg_catalog.pg_namespace%20dn%20ON%20(dc.relnamespace%3Ddn.oid%20AND%20dn.nspname%3D%27pg_catalog%27)%0D%0A%20%20WHERE%20%0D%0A%20%20%20%20c.relkind%20in%20(%27r%27%2C%27p%27%2C%27v%27%2C%27f%27%2C%27m%27)%0D%0A%20%20%20%20and%20a.attnum%20%3E%200%20%0D%0A%20%20%20%20AND%20NOT%20a.attisdropped%0D%0A%20%20%20%20AND%20c.relname%20LIKE%20E%27x%27%0D%0A%20%20)%20c%20WHERE%20true%0D%0A%20%20ORDER%20BY%20nspname%2Cc.relname%2Cattnum HTTP/1.1\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cookie: _ga=GA1.1.1723668823.1636741549\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Referer: http://localhost:9000/\r\n" +
                        "Sec-Fetch-Dest: empty\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36\r\n" +
                        "sec-ch-ua: \" Not A;Brand\";v=\"99\", \"Chromium\";v=\"100\", \"Google Chrome\";v=\"100\"\r\n" +
                        "sec-ch-ua-mobile: ?0\r\n" +
                        "sec-ch-ua-platform: \"Windows\"\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0b39\r\n" +
                        "{\"query\":\"\\r\\n\\r\\n\\r\\nSELECT * FROM (\\r\\n  SELECT \\r\\n    n.nspname\\r\\n    ,c.relname\\r\\n    ,a.attname\\r\\n    ,a.atttypid\\r\\n    ,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull\\r\\n    ,a.atttypmod\\r\\n    ,a.attlen\\r\\n    ,t.typtypmod\\r\\n    ,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum\\r\\n    , nullif(a.attidentity, '') as attidentity\\r\\n    ,null as attgenerated\\r\\n    ,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc\\r\\n    ,dsc.description\\r\\n    ,t.typbasetype\\r\\n    ,t.typtype  \\r\\n  FROM pg_catalog.pg_namespace n\\r\\n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)\\r\\n  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)\\r\\n  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)\\r\\n  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)\\r\\n  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)\\r\\n  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')\\r\\n  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')\\r\\n  WHERE \\r\\n    c.relkind in ('r','p','v','f','m')\\r\\n    and a.attnum > 0 \\r\\n    AND NOT a.attisdropped\\r\\n    AND c.relname LIKE E'x'\\r\\n  ) c WHERE true\\r\\n  ORDER BY nspname,c.relname,attnum\",\"columns\":[{\"name\":\"nspname\",\"type\":\"STRING\"},{\"name\":\"relname\",\"type\":\"STRING\"},{\"name\":\"attname\",\"type\":\"STRING\"},{\"name\":\"atttypid\",\"type\":\"INT\"},{\"name\":\"attnotnull\",\"type\":\"BOOLEAN\"},{\"name\":\"atttypmod\",\"type\":\"INT\"},{\"name\":\"attlen\",\"type\":\"SHORT\"},{\"name\":\"typtypmod\",\"type\":\"INT\"},{\"name\":\"attnum\",\"type\":\"LONG\"},{\"name\":\"attidentity\",\"type\":\"STRING\"},{\"name\":\"attgenerated\",\"type\":\"STRING\"},{\"name\":\"adsrc\",\"type\":\"STRING\"},{\"name\":\"description\",\"type\":\"STRING\"},{\"name\":\"typbasetype\",\"type\":\"INT\"},{\"name\":\"typtype\",\"type\":\"CHAR\"}],\"timestamp\":-1,\"dataset\":[[\"public\",\"x\",\"a\",21,false,-1,2,0,\"1\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"b\",21,false,-1,2,0,\"2\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"c\",23,false,-1,4,0,\"3\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"d\",20,false,-1,8,0,\"4\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"e\",1114,false,-1,8,0,\"5\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"f\",1114,false,-1,8,0,\"6\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"g\",700,false,-1,4,0,\"7\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"h\",701,false,-1,8,0,\"8\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"i\",1043,false,-1,-1,0,\"9\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"j\",1043,false,-1,-1,0,\"10\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"k\",16,false,-1,1,0,\"11\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"l\",17,false,-1,-1,0,\"12\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"m\",2950,false,-1,16,0,\"13\",null,null,null,null,0,\"b\"],[\"public\",\"x\",\"n\",1043,false,-1,-1,0,\"14\",null,null,null,null,0,\"b\"]],\"count\":14,\"explain\":{\"jitCompiled\":false}}\r\n" +
                        "00\r\n" +
                        "\r\n",
                10
        );
    }

    @Test
    public void testJsonQueryAndDisconnectWithoutWaitingForResult() throws Exception {
        assertMemoryLeak(() -> {
            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(configuration, nf, baseDir, 256, false, false);
            WorkerPool workerPool = new TestWorkerPool(2);
            try (
                    CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));
                httpServer.bind(new TestJsonQueryProcessorFactory(engine, httpConfiguration, workerPool.getWorkerCount()));

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    createTableX(engine, 30);

                    // send multipart request to server
                    final String request = "GET /query?query=x HTTP/1.1\r\n" +
                            "Host: localhost:9001\r\n" +
                            "Connection: keep-alive\r\n" +
                            "Cache-Control: max-age=0\r\n" +
                            "Upgrade-Insecure-Requests: 1\r\n" +
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                            "Accept-Encoding: gzip, deflate, br\r\n" +
                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                            "\r\n";

                    String expectedResponse = "HTTP/1.1 200 OK\r\n" +
                            "Server: questDB/1.0\r\n" +
                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                            "Transfer-Encoding: chunked\r\n" +
                            "Content-Type: application/json; charset=utf-8\r\n" +
                            "Keep-Alive: timeout=5, max=10000\r\n" +
                            "\r\n" +
                            "f7\r\n" +
                            "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"}\r\n" +
                            "f2\r\n" +
                            ",{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373\r\n" +
                            "e1\r\n" +
                            ",\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29df637e38\",\"}龘и\uDA89\uDFA4~\"],[53,5639,-1162267908,6993925225312419449,\"195808098-05-09T04:14:54.347Z\"\r\n" +
                            "f4\r\n" +
                            ",\"171005-04-19T09:31:35.433003Z\",0.24593449,0.29313719347837397,\"BVTMH\",null,false,[],\"d364c241-dde2-cf90-a7a8-f4e549997e46\",\"\uE961-\\\\篸{\"],[40,-8761,null,-7995393784734742820,\"275774022-08-09T21:28:04.485Z\",\"-264492-10-11T03:17:51.666853Z\",null\r\n" +
                            "ff\r\n" +
                            ",0.5065228336156442,\"LNVTI\",null,false,[],\"a011214b-ad88-8a69-9502-128cda0887fe\",null],[-31,4215,1362833895,null,\"-49144476-01-15T02:33:12.980Z\",null,0.26369333,0.7632615004324503,\"LHMLL\",\"OYP\",false,[],\"b92d0771-d782-63eb-5479-ae0482582ad0\",\"! Yc0\"],[-80\r\n" +
                            "e9\r\n" +
                            ",-23575,null,5552835357100545895,\"-229044588-12-31T09:43:18.056Z\",null,null,null,\"GLUOH\",\"ZHZ\",false,[],\"8b1134e2-9413-4389-a2cb-c77b1cdd7786\",\"1\uD97C\uDD2B珣zx\"],[119,-2044,-2043541236,-4547802916868961458,\"-281648402-09-21T10:33:06.955Z\"\r\n" +
                            "0100\r\n" +
                            ",\"221810-02-23T20:19:19.020303Z\",0.56910527,null,\"WIFFL\",\"BRO\",false,[],\"09359765-2ae7-2c5c-14ef-c23546571bdc\",\"\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5\"],[16,30964,-1520181263,-7212878484370155026,null,\"280770-04-22T16:59:28.938593Z\",0.34608507,0.5780819331422455,\"UQDYO\",null\r\n" +
                            "ea\r\n" +
                            ",false,[],\"a579cf90-ccdf-133e-86be-020b55a15fd1\",null],[72,27348,-647653731,8737613628813682249,null,null,0.0024457574,0.19736767249829557,\"CBDMI\",\"QZV\",true,[],null,\"\uDB4F\uDC7Dl⤃堝ᢣ\"],[36,22350,null,null,\"77319557-11-14T08:22:42.686Z\"\r\n" +
                            "fb\r\n" +
                            ",\"104977-04-08T13:34:21.431788Z\",0.112962544,0.9934423708117267,\"FNWGR\",\"DGG\",false,[],\"899850f1-14ad-249d-97af-847507d07b51\",\"d<J1n\"],[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\"\r\n" +
                            "db\r\n" +
                            ",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"],[100,-22306,1604613885,null,\"192651811-02-20T17:11:20.516Z\",\"-102298-11-25T12:53:13.351961Z\",0.21047932,0.8796413468565342,\"UTZOD\",\"KOC\",false,[]\r\n" +
                            "f5\r\n" +
                            ",\"79ec07ef-72dc-a7a1-a27b-49491adab237\",null],[-8,5698,1926049591,-5591900440341014879,null,\"-188627-11-22T03:41:41.053028Z\",0.001653254,null,\"SBEGM\",\"TIN\",true,[],\"7a404335-fb0c-03a1-b722-c5d1c4541bf8\",\"\uDA4D\uDEE3u\uDA83\uDD58ߡ˪\"],[1,-22839,1579145140,null\r\n" +
                            "fc\r\n" +
                            ",\"-179476546-07-29T11:31:49.216Z\",\"277796-03-22T01:05:12.468447Z\",0.5700419,null,null,null,true,[],null,\"@p\uD981\uDF09۾芊\"],[11,-3298,null,5040581546737547170,\"217663545-06-24T17:53:59.219Z\",\"-188590-01-28T12:21:46.923000Z\",0.4755193,0.7617663592833062,null\r\n" +
                            "f9\r\n" +
                            ",\"ZSF\",true,[],\"41457ebc-5a02-a2b5-42cb-d49414e022a0\",null],[108,8860,-1849825758,7001278939159424665,\"-179968926-04-15T02:07:40.791Z\",\"153077-08-01T18:41:42.471066Z\",0.56758314,0.21224614178286005,\"LFORG\",\"IEV\",true,[],null,\"\uDA65\uDE071(rո\"],[-83,-21070\r\n" +
                            "f5\r\n" +
                            ",421740366,-7994129278119873659,\"247843724-01-25T23:44:08.505Z\",\"154471-01-21T09:24:15.152962Z\",0.4417268,0.6713174919725877,\"PIQBU\",\"ZVQ\",true,[],\"721304ff-e1c9-3438-6466-208d506905af\",\"\uDA47\uDE9AخF\uD9DC\uDE05\uD8F0\uDF66\"],[-24,20018,172654235,3941299526844453307\r\n" +
                            "ff\r\n" +
                            ",\"-94778819-04-19T21:15:26.452Z\",\"263660-07-19T21:05:32.383556Z\",null,0.837738444021418,\"UIGEN\",null,true,[],\"138a6faa-5024-d18e-6536-0e5c86f6bf00\",\"5ŪD꘥\u061C\"],[-42,28924,-692944212,null,\"-288207102-10-16T11:59:11.774Z\",null,0.45388764,0.6728521416263535\r\n" +
                            "0100\r\n" +
                            ",\"FPVRQ\",\"GYD\",true,[],\"b9adab80-3662-ea78-7789-11f953eae95f\",\">\uD9F3\uDFD5a~=\"],[4,-8777,-1073362485,-5062925852203035101,\"-243518060-10-14T07:21:35.668Z\",\"180225-11-18T07:14:57.722116Z\",0.47486305,0.15464367746097551,\"KMEKP\",\"OYM\",false,[],null,\"䭣^寻&L\"],[-9\r\n" +
                            "fa\r\n" +
                            ",-21964,-1854980244,8551330700532522833,\"-283077346-06-27T16:03:38.091Z\",\"127497-03-02T22:08:20.151047Z\",0.8395367,0.8967196946317529,\"NMURE\",\"JUH\",false,[],\"697ed4cb-072a-e571-7181-091980558b1c\",\"2 \u0381駊:\"],[96,-29490,-1217047780,8745632928088770525\r\n" +
                            "f5\r\n" +
                            ",\"170114543-10-02T00:21:50.302Z\",\"74728-08-25T07:58:00.180183Z\",0.04653406,0.028814588598028656,\"CLLER\",\"MKR\",false,[],\"3e171d71-4227-24b8-7a33-ebfdd20508cc\",\"㦞螼\uD8F7\uDCFBŕ\uD904\uDFA0\"],[-2,27393,null,-5141364984576774747,\"138594586-12-07T16:52:39.139Z\"\r\n" +
                            "fc\r\n" +
                            ",\"-38259-04-30T08:04:24.727346Z\",0.9529176,null,\"UTOMF\",null,false,[],\"7429f999-bffc-9548-aa3d-f14bfed42969\",\"nOF*h\"],[79,11546,null,8762413735379842328,\"237540564-05-16T15:14:29.786Z\",\"93639-05-03T03:30:14.462955Z\",null,0.654226248740447,\"EQXIL\",\"WZS\"\r\n" +
                            "f8\r\n" +
                            ",false,[],\"658deb69-3577-2906-5fc8-46c046b0c4a1\",\"LGީF\uDA53\uDC78\"],[103,29797,null,3528693992946584568,\"-278074628-05-05T15:27:35.680Z\",\"200436-10-09T21:50:29.081004Z\",null,0.39942578826922215,\"HWUDV\",\"KRP\",true,[],\"18ab4633-b7e7-57eb-5fb8-0bbe4ebceada\"\r\n" +
                            "fd\r\n" +
                            ",\";\uDA18\uDD86ʶÇ;\"],[-100,-12716,-154411617,-6186254618668432073,null,null,0.059869826,0.015700171393594697,\"QBORD\",\"QHV\",false,[],null,null],[125,11764,1871556077,5549035152445628202,\"185451379-07-29T06:07:03.825Z\",\"205522-08-08T04:51:51.022308Z\",0.40537632\r\n" +
                            "0100\r\n" +
                            ",0.4472458525819868,\"ZVDJI\",null,true,[],\"6e4aef03-9480-1c40-941d-89f24081f64d\",\"\uD95D\uDF03\uDB07\uDEEEW߇\uD930\uDD3A\"],[-72,22435,1938229434,null,\"168744810-08-11T08:23:54.308Z\",null,0.8277437,0.7848127252854841,null,\"BPH\",false,[],\"6c1491e1-65c8-7264-a792-3c9ec146118b\",null]\r\n" +
                            "f5\r\n" +
                            ",[72,2320,null,6634408780786552626,\"218692167-12-11T08:36:28.706Z\",null,null,0.2232959099494619,\"RBIDS\",\"DTF\",false,[],\"8c25e1c1-5e25-26fc-a4f3-2f968f3c3a93\",\"G&ُܵ9\"],[119,-21634,1692077613,-9182593931032077058,\"-284565007-07-30T04:19:48.813Z\"\r\n" +
                            "fa\r\n" +
                            ",\"111008-12-21T07:20:50.292998Z\",0.78913426,0.28256286706363265,null,\"MUM\",true,[],\"2b7ae17e-3c4e-6c97-335e-3a2a33564ac7\",\"䯖߯Ւљ\uDAC8\uDE3B\"],[-25,-19308,-264999630,-7730996391418574454,\"-117381380-04-15T00:53:02.603Z\",null,0.27529263,0.4922628637697192\r\n" +
                            "50\r\n" +
                            ",\"VYQNF\",\"GSQ\",true,[],\"6c87ca83-9f7f-8533-6695-f15390d20b0f\",null]],\"count\":30}\r\n" +
                            "00\r\n" +
                            "\r\n";

                    sendAndReceive(nf, request, expectedResponse, 10, 100L, false);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testJsonQueryBadUtf8() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=�������&limit=10 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "58\r\n" +
                        "{\"query\":\"�������\",\"error\":\"Bad UTF8 encoding in query text\",\"position\":0}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryBottomLimit() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x&limit=10,25 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "09ca\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"],[100,-22306,1604613885,null,\"192651811-02-20T17:11:20.516Z\",\"-102298-11-25T12:53:13.351961Z\",0.21047932,0.8796413468565342,\"UTZOD\",\"KOC\",false,[],\"79ec07ef-72dc-a7a1-a27b-49491adab237\",null],[-8,5698,1926049591,-5591900440341014879,null,\"-188627-11-22T03:41:41.053028Z\",0.001653254,null,\"SBEGM\",\"TIN\",true,[],\"7a404335-fb0c-03a1-b722-c5d1c4541bf8\",\"\uDA4D\uDEE3u\uDA83\uDD58ߡ˪\"],[1,-22839,1579145140,null,\"-179476546-07-29T11:31:49.216Z\",\"277796-03-22T01:05:12.468447Z\",0.5700419,null,null,null,true,[],null,\"@p\uD981\uDF09۾芊\"],[11,-3298,null,5040581546737547170,\"217663545-06-24T17:53:59.219Z\",\"-188590-01-28T12:21:46.923000Z\",0.4755193,0.7617663592833062,null,\"ZSF\",true,[],\"41457ebc-5a02-a2b5-42cb-d49414e022a0\",null],[108,8860,-1849825758,7001278939159424665,\"-179968926-04-15T02:07:40.791Z\",\"153077-08-01T18:41:42.471066Z\",0.56758314,0.21224614178286005,\"LFORG\",\"IEV\",true,[],null,\"\uDA65\uDE071(rո\"],[-83,-21070,421740366,-7994129278119873659,\"247843724-01-25T23:44:08.505Z\",\"154471-01-21T09:24:15.152962Z\",0.4417268,0.6713174919725877,\"PIQBU\",\"ZVQ\",true,[],\"721304ff-e1c9-3438-6466-208d506905af\",\"\uDA47\uDE9AخF\uD9DC\uDE05\uD8F0\uDF66\"],[-24,20018,172654235,3941299526844453307,\"-94778819-04-19T21:15:26.452Z\",\"263660-07-19T21:05:32.383556Z\",null,0.837738444021418,\"UIGEN\",null,true,[],\"138a6faa-5024-d18e-6536-0e5c86f6bf00\",\"5ŪD꘥\u061C\"],[-42,28924,-692944212,null,\"-288207102-10-16T11:59:11.774Z\",null,0.45388764,0.6728521416263535,\"FPVRQ\",\"GYD\",true,[],\"b9adab80-3662-ea78-7789-11f953eae95f\",\">\uD9F3\uDFD5a~=\"],[4,-8777,-1073362485,-5062925852203035101,\"-243518060-10-14T07:21:35.668Z\",\"180225-11-18T07:14:57.722116Z\",0.47486305,0.15464367746097551,\"KMEKP\",\"OYM\",false,[],null,\"䭣^寻&L\"],[-9,-21964,-1854980244,8551330700532522833,\"-283077346-06-27T16:03:38.091Z\",\"127497-03-02T22:08:20.151047Z\",0.8395367,0.8967196946317529,\"NMURE\",\"JUH\",false,[],\"697ed4cb-072a-e571-7181-091980558b1c\",\"2 \u0381駊:\"]],\"count\":20}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryCommentOnlyMultiline_apiV2() throws Exception {
        String expectedErrorResponse = "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: application/json; charset=utf-8\r\n" +
                "Keep-Alive: timeout=5, max=10000\r\n" +
                "\r\n" +
                "3f\r\n" +
                "{\"notice\":\"empty query\",\"query\":\"--\\n--comment\",\"position\":\"0\"}\r\n" +
                "00\r\n" +
                "\r\n";

        testJsonQuery(
                20,
                "GET /query?query=--%0A--comment&version=2 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                expectedErrorResponse
        );
    }

    @Test
    public void testJsonQueryCommentOnly_apiV1() throws Exception {
        String expectedErrorResponse = "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: application/json; charset=utf-8\r\n" +
                "Keep-Alive: timeout=5, max=10000\r\n" +
                "\r\n" +
                "3a\r\n" +
                "{\"error\":\"empty query\",\"query\":\"--comment\",\"position\":\"0\"}\r\n" +
                "00\r\n" +
                "\r\n";

        testJsonQuery(
                20,
                "GET /query?query=--comment HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                expectedErrorResponse
        );

        testJsonQuery(
                20,
                "GET /query?query=--comment&version=1 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                expectedErrorResponse
        );

        testJsonQuery(
                20,
                "GET /query?query=--comment&version=nonversion HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                expectedErrorResponse
        );
    }

    @Test
    public void testJsonQueryCommentOnly_apiV2() throws Exception {
        String expectedErrorResponse = "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: application/json; charset=utf-8\r\n" +
                "Keep-Alive: timeout=5, max=10000\r\n" +
                "\r\n" +
                "3b\r\n" +
                "{\"notice\":\"empty query\",\"query\":\"--comment\",\"position\":\"0\"}\r\n" +
                "00\r\n" +
                "\r\n";

        testJsonQuery(
                20,
                "GET /query?query=--comment&version=2 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                expectedErrorResponse
        );
    }

    @Test
    public void testJsonQueryCompilationStatsForJitCompiledFilter() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        getSimpleTester().run((engine, sqlExecutionContext) -> {
            createTableX(engine, 1000);
            testHttpClient.assertGet(
                    "{\"query\":\"x where d = 7960464771512399314\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[-99,-18571,-1619342575,7960464771512399314,\"221271209-10-06T04:42:32.542Z\",\"-242523-07-08T23:46:04.551753Z\",0.025056839,0.8515587750165008,\"LWDUW\",\"WJT\",false,[],\"9290f4e5-c9f9-e1db-e96e-2b9ec3beda7f\",\"hJ3hN\"]],\"count\":1,\"explain\":{\"jitCompiled\":true}}",
                    "x where d = 7960464771512399314",
                    new CharSequenceObjHashMap<>() {{
                        put("explain", "true");
                    }}
            );
        });
    }

    @Test
    public void testJsonQueryCompilationStatsForNonJitCompiledFilter() throws Exception {
        testJsonQuery(
                10,
                "GET /query?query=x%20where%20i%20%3D%20%27A%27&limit=1&explain=true HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01fb\r\n" +
                        "{\"query\":\"x where i = 'A'\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0,\"explain\":{\"jitCompiled\":false}}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryCreateInsertNull() throws Exception {
        testJsonQuery0(
                1, (engine, sqlExecutionContext) -> {
                    // create table
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=create+table+xx+(value+long256,+ts+timestamp)+timestamp(ts)&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );
                    // insert one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=insert+into+xx+values(null,+0)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    INSERT_QUERY_RESPONSE,
                            1,
                            0,
                            false
                    );
                    // check if we have one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=select+*+from+xx+latest+on+ts+partition+by+value&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "d2\r\n" +
                                    "{\"query\":\"select * from xx latest on ts partition by value\",\"columns\":[{\"name\":\"value\",\"type\":\"LONG256\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"}],\"timestamp\":1,\"dataset\":[[\"\",\"1970-01-01T00:00:00.000000Z\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );
                }, false
        );
    }

    @Test
    public void testJsonQueryCreateInsertStringifiedJson() throws Exception {
        testJsonQuery0(
                1, (engine, sqlExecutionContext) -> {
                    // create table
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?limit=0%2C1000&count=true&src=con&query=%0D%0Acreate%20table%20data(s%20string)&timings=true HTTP/1.1\r\n" +
                                    "Host: 127.0.0.1:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\n" +
                                    "Accept: */*\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Sec-Fetch-Dest: empty\r\n" +
                                    "Referer: http://127.0.0.1:9000/\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // insert one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?limit=0%2C1000&count=true&src=con&query=%0D%0A%0D%0Ainsert%20into%20data%20values%20(%27%7B%20title%3A%20%5C%22Title%5C%22%7D%27)&timings=true HTTP/1.1\r\n" +
                                    "Host: 127.0.0.1:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\n" +
                                    "Accept: */*\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Sec-Fetch-Dest: empty\r\n" +
                                    "Referer: http://127.0.0.1:9000/\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    INSERT_QUERY_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // check if we have one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?limit=0%2C1000&count=true&src=con&query=data&timings=false HTTP/1.1\r\n" +
                                    "Host: 127.0.0.1:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36\r\n" +
                                    "Accept: */*\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Sec-Fetch-Dest: empty\r\n" +
                                    "Referer: http://127.0.0.1:9000/\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "7a\r\n" +
                                    "{\"query\":\"data\",\"columns\":[{\"name\":\"s\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"{ title: \\\\\\\"Title\\\\\\\"}\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );
                }, false
        );
    }

    @Test
    public void testJsonQueryCreateInsertTruncateAndDrop() throws Exception {
        testJsonQuery0(
                1, (engine, sqlExecutionContext) -> {
                    // create table
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)%20timestamp%28timestamp%29&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // insert one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(1%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    INSERT_QUERY_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // check if we have one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+on+timestamp+partition+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0171\r\n" +
                                    "{\"query\":\"\\n\\nselect * from balances_x latest on timestamp partition by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"timestamp\":4,\"dataset\":[[1,\"USD\",1500.0,0,\"1970-01-01T01:40:00.000001Z\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );

                    // truncate table
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0Atruncate+table+balances_x&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // select again expecting only metadata
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+on+timestamp+partition+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0141\r\n" +
                                    "{\"query\":\"\\n\\nselect * from balances_x latest on timestamp partition by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"timestamp\":4,\"dataset\":[],\"count\":0}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );
                }, false
        );
    }

    @Test
    public void testJsonQueryCreateTable() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        JSON_DDL_RESPONSE,
                1
        );
    }

    @Test
    public void testJsonQueryDataUnavailableClientDisconnectsBeforeEventFired() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withQueryTimeout(60_000) // use a large value for query timeout
                .run((engine, sqlExecutionContext) -> {
                    AtomicReference<SuspendEvent> eventRef = new AtomicReference<>();
                    TestDataUnavailableFunctionFactory.eventCallback = eventRef::set;

                    final String query = "select * from test_data_unavailable(1, 10)";
                    final String request = "GET /query?query=" + urlEncodeQuery(query) + "&count=true HTTP/1.1\r\n"
                            + SendAndReceiveRequestBuilder.RequestHeaders;

                    final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
                    long fd = -1;
                    try {
                        fd = new SendAndReceiveRequestBuilder().connectAndSendRequest(request);
                        assertEventually(() -> Assert.assertNotNull(eventRef.get()), 10);
                        nf.close(fd);
                        fd = -1;
                        // Check that I/O dispatcher closes the event once it detects the disconnect.
                        assertEventually(() -> assertTrue(eventRef.get().isClosedByAtLeastOneSide()), 10);
                    } finally {
                        if (fd > -1) {
                            nf.close(fd);
                        }
                        // Make sure to close the event on the producer side.
                        Misc.free(eventRef.get());
                    }
                });
    }

    @Test
    public void testJsonQueryDropTable() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=drop%20table%20x HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        JSON_DDL_RESPONSE,
                1
        );
    }

    @Test
    public void testJsonQueryEmptyColumnNameInLimitColumns() throws Exception {
        testJsonQuery(
                20, "GET /query?query=x&cols=k,c,,d,f1,e,g,h,i,j,a,l HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "37\r\n" +
                        "{\"query\":\"x\",\"error\":\"empty column in query parameter\"}\r\n" +
                        "00\r\n" +
                        "\r\n", 20
        );
    }

    @Test
    public void testJsonQueryEmptyText_apiV1() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query= HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "31\r\n" +
                        "{\"error\":\"empty query\",\"query\":\"\",\"position\":\"0\"}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryEmptyText_apiV2() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=&version=2 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "32\r\n" +
                        "{\"notice\":\"empty query\",\"query\":\"\",\"position\":\"0\"}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryErrorOnDataUnavailableEventNeverFired() throws Exception {
        TestDataUnavailableFunctionFactory.eventCallback = SuspendEvent::close;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withQueryTimeout(100)
                .run((engine, sqlExecutionContext) -> testHttpClient.assertGetRegexp(
                        "/query",
                        "\\{\"query\":\"select \\* from test_data_unavailable\\(1, 10\\)\",\"error\":\"timeout, query aborted \\[fd=\\d+, runtime=\\d+ms, timeout=\\d+ms\\]\",\"position\":0\\}",
                        "select * from test_data_unavailable(1, 10)",
                        null, null, null, null,
                        "400" // Request Timeout
                ));
    }

    @Test
    public void testJsonQueryGeoHashColumnChars() throws Exception {
        testHttpQueryGeoHashColumnChars(
                "{\"query\":\"SELECT * FROM y\",\"columns\":[{\"name\":\"geo1\",\"type\":\"GEOHASH(1c)\"},{\"name\":\"geo2\",\"type\":\"GEOHASH(3c)\"},{\"name\":\"geo4\",\"type\":\"GEOHASH(6c)\"},{\"name\":\"geo8\",\"type\":\"GEOHASH(12c)\"},{\"name\":\"geo01\",\"type\":\"GEOHASH(1b)\"}],\"timestamp\":-1,\"dataset\":[[null,null,\"questd\",\"u10m99dd3pbj\",\"1\"],[\"u\",\"u10\",\"questd\",null,\"1\"],[\"q\",\"u10\",\"questd\",\"questdb12345\",\"1\"]],\"count\":3}",
                "/query"
        );
    }

    @Test
    public void testJsonQueryInfinity() throws Exception {
        testJsonQuery(
                20,
                "GET /exec?limit=0%2C1000&count=true&src=con&query=select%20cast(1.0%2F0.0%20as%20float)%2C%20cast(1.0%2F0.0%20as%20double) HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.2057572436.1581161560\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "bf\r\n" +
                        "{\"query\":\"select cast(1.0/0.0 as float), cast(1.0/0.0 as double)\",\"columns\":[{\"name\":\"cast\",\"type\":\"FLOAT\"},{\"name\":\"cast1\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[null,null]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryInvalidColumnNameInLimitColumns() throws Exception {
        testJsonQuery(
                20, "GET /query?query=x&cols=k,c,b,d,f1,e,g,h,i,j,a,l HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "2e\r\n" +
                        "{\"query\":\"x\",\"error\":\"column not found: 'f1'\"}\r\n" +
                        "00\r\n" +
                        "\r\n", 20
        );
    }

    @Test
    public void testJsonQueryInvalidLastColumnNameInLimitColumns() throws Exception {
        testJsonQuery(
                20, "GET /query?query=x&cols=k,c,b,d,f,e,g,h,i,j,a,l2 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "2e\r\n" +
                        "{\"query\":\"x\",\"error\":\"column not found: 'l2'\"}\r\n" +
                        "00\r\n" +
                        "\r\n", 20
        );
    }

    @Test
    public void testJsonQueryJsonEncodeZeroCharacter() throws Exception {
        testJsonQuery0(
                2, (engine, sqlExecutionContext) -> {
                    // create table with all column types
                    createTestTable(engine);
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=y HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0188\r\n" +
                                    "{\"query\":\"y\",\"columns\":[{\"name\":\"j\",\"type\":\"SYMBOL\"}],\"timestamp\":-1,\"dataset\":[[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"],[\"ok\\u0000ok\"]],\"count\":20}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            100,
                            0,
                            false
                    );
                }, false
        );
    }

    @Test
    public void testJsonQueryLimitColumnsBadUtf8() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=select+%27oops%27+%D1%80%D0%B5%D0%BA%D0%BE%D1%80%D0%B4%D0%BD%D0%BE+from+long_sequence(10)%0A&count=false&cols=a�������\t&src=vis HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "25\r\n" +
                        "{\"error\":\"utf8 error in column list\"}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryLimitColumnsUtf8() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=select+%27oops%27+%D1%80%D0%B5%D0%BA%D0%BE%D1%80%D0%B4%D0%BD%D0%BE+from+long_sequence(10)%0A&count=false&cols=%D1%80%D0%B5%D0%BA%D0%BE%D1%80%D0%B4%D0%BD%D0%BE&src=vis HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "fb\r\n" +
                        "{\"query\":\"select 'oops' рекордно from long_sequence(10)\\n\",\"columns\":[{\"name\":\"рекордно\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"]],\"count\":10}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryMiddleLimit() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x&limit=10,14 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0549\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"],[100,-22306,1604613885,null,\"192651811-02-20T17:11:20.516Z\",\"-102298-11-25T12:53:13.351961Z\",0.21047932,0.8796413468565342,\"UTZOD\",\"KOC\",false,[],\"79ec07ef-72dc-a7a1-a27b-49491adab237\",null],[-8,5698,1926049591,-5591900440341014879,null,\"-188627-11-22T03:41:41.053028Z\",0.001653254,null,\"SBEGM\",\"TIN\",true,[],\"7a404335-fb0c-03a1-b722-c5d1c4541bf8\",\"\uDA4D\uDEE3u\uDA83\uDD58ߡ˪\"],[1,-22839,1579145140,null,\"-179476546-07-29T11:31:49.216Z\",\"277796-03-22T01:05:12.468447Z\",0.5700419,null,null,null,true,[],null,\"@p\uD981\uDF09۾芊\"],[11,-3298,null,5040581546737547170,\"217663545-06-24T17:53:59.219Z\",\"-188590-01-28T12:21:46.923000Z\",0.4755193,0.7617663592833062,null,\"ZSF\",true,[],\"41457ebc-5a02-a2b5-42cb-d49414e022a0\",null]],\"count\":14}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryMiddleLimitNoMeta() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x&limit=10,14&nm=true HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0394\r\n" +
                        "{\"dataset\":[[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"],[100,-22306,1604613885,null,\"192651811-02-20T17:11:20.516Z\",\"-102298-11-25T12:53:13.351961Z\",0.21047932,0.8796413468565342,\"UTZOD\",\"KOC\",false,[],\"79ec07ef-72dc-a7a1-a27b-49491adab237\",null],[-8,5698,1926049591,-5591900440341014879,null,\"-188627-11-22T03:41:41.053028Z\",0.001653254,null,\"SBEGM\",\"TIN\",true,[],\"7a404335-fb0c-03a1-b722-c5d1c4541bf8\",\"\uDA4D\uDEE3u\uDA83\uDD58ߡ˪\"],[1,-22839,1579145140,null,\"-179476546-07-29T11:31:49.216Z\",\"277796-03-22T01:05:12.468447Z\",0.5700419,null,null,null,true,[],null,\"@p\uD981\uDF09۾芊\"],[11,-3298,null,5040581546737547170,\"217663545-06-24T17:53:59.219Z\",\"-188590-01-28T12:21:46.923000Z\",0.4755193,0.7617663592833062,null,\"ZSF\",true,[],\"41457ebc-5a02-a2b5-42cb-d49414e022a0\",null]],\"count\":14}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryMultiThreaded() throws Exception {
        final int threadCount = 4;
        final int requestsPerThread = 500;
        final String[][] requests = {
                {
                        "xyz where sym = 'UDEYY'",
                        "{\"query\":\"xyz where sym = 'UDEYY'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[\"UDEYY\",0.15786635599554755],[\"UDEYY\",0.8445258177211064],[\"UDEYY\",0.5778947915182423]],\"count\":3}"
                },
                {
                        "xyz where sym = 'QEHBH'",
                        "{\"query\":\"xyz where sym = 'QEHBH'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[\"QEHBH\",0.4022810626779558],[\"QEHBH\",0.9038068796506872],[\"QEHBH\",0.05048190020054388],[\"QEHBH\",0.4149517697653501],[\"QEHBH\",0.44804689668613573]],\"count\":5}"
                },
                {
                        "xyz where sym = 'SXUXI'",
                        "{\"query\":\"xyz where sym = 'SXUXI'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[\"SXUXI\",0.6761934857077543],[\"SXUXI\",0.38642336707855873],[\"SXUXI\",0.48558682958070665]],\"count\":3}"
                },
                {
                        "xyz where sym = 'VTJWC'",
                        "{\"query\":\"xyz where sym = 'VTJWC'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[\"VTJWC\",0.3435685332942956],[\"VTJWC\",0.8258367614088108],[\"VTJWC\",0.437176959518218],[\"VTJWC\",0.7176053468281931]],\"count\":4}"
                }
        };
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(threadCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute("create table xyz as (select rnd_symbol(10, 5, 5, 0) sym, rnd_double() d from long_sequence(30)), index(sym)", executionContext);

                        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                        final CountDownLatch latch = new CountDownLatch(threadCount);
                        final AtomicInteger errorCount = new AtomicInteger(0);

                        for (int i = 0; i < threadCount; i++) {
                            new QueryThread(
                                    requests,
                                    requestsPerThread,
                                    barrier,
                                    latch,
                                    errorCount
                            ).start();
                        }

                        latch.await();
                        Assert.assertEquals(0, errorCount.get());
                    } catch (SqlException e) {
                        Assert.fail(e.getMessage());
                    }
                });
    }

    @Test
    public void testJsonQueryMultipleRows() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "1020\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29df637e38\",\"}龘и\uDA89\uDFA4~\"],[53,5639,-1162267908,6993925225312419449,\"195808098-05-09T04:14:54.347Z\",\"171005-04-19T09:31:35.433003Z\",0.24593449,0.29313719347837397,\"BVTMH\",null,false,[],\"d364c241-dde2-cf90-a7a8-f4e549997e46\",\"\uE961-\\\\篸{\"],[40,-8761,null,-7995393784734742820,\"275774022-08-09T21:28:04.485Z\",\"-264492-10-11T03:17:51.666853Z\",null,0.5065228336156442,\"LNVTI\",null,false,[],\"a011214b-ad88-8a69-9502-128cda0887fe\",null],[-31,4215,1362833895,null,\"-49144476-01-15T02:33:12.980Z\",null,0.26369333,0.7632615004324503,\"LHMLL\",\"OYP\",false,[],\"b92d0771-d782-63eb-5479-ae0482582ad0\",\"! Yc0\"],[-80,-23575,null,5552835357100545895,\"-229044588-12-31T09:43:18.056Z\",null,null,null,\"GLUOH\",\"ZHZ\",false,[],\"8b1134e2-9413-4389-a2cb-c77b1cdd7786\",\"1\uD97C\uDD2B珣zx\"],[119,-2044,-2043541236,-4547802916868961458,\"-281648402-09-21T10:33:06.955Z\",\"221810-02-23T20:19:19.020303Z\",0.56910527,null,\"WIFFL\",\"BRO\",false,[],\"09359765-2ae7-2c5c-14ef-c23546571bdc\",\"\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5\"],[16,30964,-1520181263,-7212878484370155026,null,\"280770-04-22T16:59:28.938593Z\",0.34608507,0.5780819331422455,\"UQDYO\",null,false,[],\"a579cf90-ccdf-133e-86be-020b55a15fd1\",null],[72,27348,-647653731,8737613628813682249,null,null,0.0024457574,0.19736767249829557,\"CBDMI\",\"QZV\",true,[],null,\"\uDB4F\uDC7Dl⤃堝ᢣ\"],[36,22350,null,null,\"77319557-11-14T08:22:42.686Z\",\"104977-04-08T13:34:21.431788Z\",0.112962544,0.9934423708117267,\"FNWGR\",\"DGG\",false,[],\"899850f1-14ad-249d-97af-847507d07b51\",\"d<J1n\"],[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"],[100,-22306,1604613885,null,\"192651811-02-20T17:11:20.516Z\",\"-102298-11-25T12:53:13.351961Z\",0.21047932,0.8796413468565342,\"UTZOD\",\"KOC\",false,[],\"79ec07ef-72dc-a7a1-a27b-49491adab237\",null],[-8,5698,1926049591,-5591900440341014879,null,\"-188627-11-22T03:41:41.053028Z\",0.001653254,null,\"SBEGM\",\"TIN\",true,[],\"7a404335-fb0c-03a1-b722-c5d1c4541bf8\",\"\uDA4D\uDEE3u\uDA83\uDD58ߡ˪\"],[1,-22839,1579145140,null,\"-179476546-07-29T11:31:49.216Z\",\"277796-03-22T01:05:12.468447Z\",0.5700419,null,null,null,true,[],null,\"@p\uD981\uDF09۾芊\"],[11,-3298,null,5040581546737547170,\"217663545-06-24T17:53:59.219Z\",\"-188590-01-28T12:21:46.923000Z\",0.4755193,0.7617663592833062,null,\"ZSF\",true,[],\"41457ebc-5a02-a2b5-42cb-d49414e022a0\",null],[108,8860,-1849825758,7001278939159424665,\"-179968926-04-15T02:07:40.791Z\",\"153077-08-01T18:41:42.471066Z\",0.56758314,0.21224614178286005,\"LFORG\",\"IEV\",true,[],null,\"\uDA65\uDE071(rո\"],[-83,-21070,421740366,-7994129278119873659,\"247843724-01-25T23:44:08.505Z\",\"154471-01-21T09:24:15.152962Z\",0.4417268,0.6713174919725877,\"PIQBU\",\"ZVQ\",true,[],\"721304ff-e1c9-3438-6466-208d506905af\",\"\uDA47\uDE9AخF\uD9DC\uDE05\uD8F0\uDF66\"],[-24,20018,172654235,3941299526844453307,\"-94778819-04-19T21:15:26.452Z\",\"263660-07-19T21:05:32.383556Z\",null,0.837738444021418,\"UIGEN\",null,true,[],\"138a6faa-5024-d18e-6536-0e5c86f6bf00\",\"5ŪD꘥\u061C\"],[-42,28924,-692944212,null,\"-288207102-10-16T11:59:11.774Z\",null,0.45388764,0.6728521416263535,\"FPVRQ\",\"GYD\",true,[],\"b9adab80-3662-ea78-7789-11f953eae95f\",\">\uD9F3\uDFD5a~=\"],[4,-8777,-1073362485,-5062925852203035101,\"-243518060-10-14T07:21:35.668Z\",\"180225-11-18T07:14:57.722116Z\",0.47486305,0.15464367746097551,\"KMEKP\",\"OYM\",false,[],null,\"䭣^寻&L\"],[-9,-21964,-1854980244,8551330700532522833,\"-283077346-06-27T16:03:38.091Z\",\"127497-03-02T22:08:20.151047Z\",0.8395367,0.8967196946317529,\"NMURE\",\"JUH\",false,[],\"697ed4cb-072a-e571-7181-091980558b1c\",\"2 \u0381駊:\"]],\"count\":20}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryMultipleRowsFiltered() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=%0A%0Aselect+*+from+x+where+i+~+%27E%27&limit=1,1&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "02ad\r\n" +
                        "{\"query\":\"\\n\\nselect * from x where i ~ 'E'\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29df637e38\",\"}龘и\uDA89\uDFA4~\"]],\"count\":5}\r\n" +
                        "00\r\n"
        );
    }

    @Test
    public void testJsonQueryMultipleRowsLimitColumns() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x&cols=k,c,b,d,f,e,g,h,i,j,a,l HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0c73\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"timestamp\":-1,\"dataset\":[[false,-727724771,24814,8920866532787660373,\"-51129-02-11T06:38:29.397464Z\",\"-169665660-01-09T01:58:28.119Z\",null,null,\"EHNRX\",\"ZSX\",80,[]],[false,-1162267908,5639,6993925225312419449,\"171005-04-19T09:31:35.433003Z\",\"195808098-05-09T04:14:54.347Z\",0.24593449,0.29313719347837397,\"BVTMH\",null,53,[]],[false,null,-8761,-7995393784734742820,\"-264492-10-11T03:17:51.666853Z\",\"275774022-08-09T21:28:04.485Z\",null,0.5065228336156442,\"LNVTI\",null,40,[]],[false,1362833895,4215,null,null,\"-49144476-01-15T02:33:12.980Z\",0.26369333,0.7632615004324503,\"LHMLL\",\"OYP\",-31,[]],[false,null,-23575,5552835357100545895,null,\"-229044588-12-31T09:43:18.056Z\",null,null,\"GLUOH\",\"ZHZ\",-80,[]],[false,-2043541236,-2044,-4547802916868961458,\"221810-02-23T20:19:19.020303Z\",\"-281648402-09-21T10:33:06.955Z\",0.56910527,null,\"WIFFL\",\"BRO\",119,[]],[false,-1520181263,30964,-7212878484370155026,\"280770-04-22T16:59:28.938593Z\",null,0.34608507,0.5780819331422455,\"UQDYO\",null,16,[]],[true,-647653731,27348,8737613628813682249,null,null,0.0024457574,0.19736767249829557,\"CBDMI\",\"QZV\",72,[]],[false,null,22350,null,\"104977-04-08T13:34:21.431788Z\",\"77319557-11-14T08:22:42.686Z\",0.112962544,0.9934423708117267,\"FNWGR\",\"DGG\",36,[]],[true,-1529726228,-13676,4092568845903588572,\"31470-11-18T18:43:57.264562Z\",null,0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",59,[]],[false,1604613885,-22306,null,\"-102298-11-25T12:53:13.351961Z\",\"192651811-02-20T17:11:20.516Z\",0.21047932,0.8796413468565342,\"UTZOD\",\"KOC\",100,[]],[true,1926049591,5698,-5591900440341014879,\"-188627-11-22T03:41:41.053028Z\",null,0.001653254,null,\"SBEGM\",\"TIN\",-8,[]],[true,1579145140,-22839,null,\"277796-03-22T01:05:12.468447Z\",\"-179476546-07-29T11:31:49.216Z\",0.5700419,null,null,null,1,[]],[true,null,-3298,5040581546737547170,\"-188590-01-28T12:21:46.923000Z\",\"217663545-06-24T17:53:59.219Z\",0.4755193,0.7617663592833062,null,\"ZSF\",11,[]],[true,-1849825758,8860,7001278939159424665,\"153077-08-01T18:41:42.471066Z\",\"-179968926-04-15T02:07:40.791Z\",0.56758314,0.21224614178286005,\"LFORG\",\"IEV\",108,[]],[true,421740366,-21070,-7994129278119873659,\"154471-01-21T09:24:15.152962Z\",\"247843724-01-25T23:44:08.505Z\",0.4417268,0.6713174919725877,\"PIQBU\",\"ZVQ\",-83,[]],[true,172654235,20018,3941299526844453307,\"263660-07-19T21:05:32.383556Z\",\"-94778819-04-19T21:15:26.452Z\",null,0.837738444021418,\"UIGEN\",null,-24,[]],[true,-692944212,28924,null,null,\"-288207102-10-16T11:59:11.774Z\",0.45388764,0.6728521416263535,\"FPVRQ\",\"GYD\",-42,[]],[false,-1073362485,-8777,-5062925852203035101,\"180225-11-18T07:14:57.722116Z\",\"-243518060-10-14T07:21:35.668Z\",0.47486305,0.15464367746097551,\"KMEKP\",\"OYM\",4,[]],[false,-1854980244,-21964,8551330700532522833,\"127497-03-02T22:08:20.151047Z\",\"-283077346-06-27T16:03:38.091Z\",0.8395367,0.8967196946317529,\"NMURE\",\"JUH\",-9,[]]],\"count\":20}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryOutsideLimit() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x&limit=35,40 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01cd\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryPreTouchDisabledForFilteredQueryWithLimit() throws Exception {
        HttpQueryTestBuilder builder = testJsonQuery(
                10,
                "GET /query?query=x%20where%20i%20%3D%20%27A%27&limit=1 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01db\r\n" +
                        "{\"query\":\"x where i = 'A'\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
        ObjList<SqlExecutionContextImpl> contexts = builder.getSqlExecutionContexts();
        for (int i = 0, n = contexts.size(); i < n; i++) {
            if (!contexts.getQuick(i).isColumnPreTouchEnabledOverride()) {
                return;
            }
        }

        Assert.fail("Only contexts with preTouch enabled found");
    }

    @Test
    public void testJsonQueryPreTouchEnabledForFilteredQueryWithoutLimit() throws Exception {
        HttpQueryTestBuilder builder = testJsonQuery(
                10,
                "GET /query?query=x%20where%20i%20%3D%20%27A%27 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01db\r\n" +
                        "{\"query\":\"x where i = 'A'\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
        ObjList<SqlExecutionContextImpl> contexts = builder.getSqlExecutionContexts();
        for (int i = 0, n = contexts.size(); i < n; i++) {
            if (contexts.getQuick(i).isColumnPreTouchEnabled()) {
                return;
            }
        }

        Assert.fail("No context with preTouch enabled found");
    }

    @Test
    public void testJsonQueryPseudoRandomStability() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=select+rnd_symbol(%27a%27%2C%27b%27%2C%27c%27)+sym+from+long_sequence(10%2C+33%2C+55)&limit=0%2C1000&count=true&src=con HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "da\r\n" +
                        "{\"query\":\"select rnd_symbol('a','b','c') sym from long_sequence(10, 33, 55)\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"}],\"timestamp\":-1,\"dataset\":[[\"c\"],[\"c\"],[\"c\"],[\"b\"],[\"b\"],[\"a\"],[\"a\"],[\"a\"],[\"a\"],[\"a\"]],\"count\":10}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryQuoteLargeNumber() throws Exception {
        // don't quote large numbers (LONG) by default
        testJsonQuery(
                0,
                "GET /query?query=select%201400055037509505337%20as%20l HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "8d\r\n" +
                        "{\"query\":\"select 1400055037509505337 as l\",\"columns\":[{\"name\":\"l\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1400055037509505337]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );

        TestUtils.removeTestPath(root);
        TestUtils.createTestPath(root);

        // quote large numbers (LONG) to string, on param 'quoteLargeNum=true'
        testJsonQuery(
                0,
                "GET /query?query=select%201400055037509505337%20as%20l&quoteLargeNum=true HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "8f\r\n" +
                        "{\"query\":\"select 1400055037509505337 as l\",\"columns\":[{\"name\":\"l\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[\"1400055037509505337\"]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );

        TestUtils.removeTestPath(root);
        TestUtils.createTestPath(root);

        // quote large numbers (LONG) for questdb web console
        testJsonQuery(
                0,
                "GET /exec?limit=0%2C1000&explain=true&count=true&src=con&query=select%201400055037509505337%20as%20l HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "af\r\n" +
                        "{\"query\":\"select 1400055037509505337 as l\",\"columns\":[{\"name\":\"l\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[\"1400055037509505337\"]],\"count\":1,\"explain\":{\"jitCompiled\":false}}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryQuotedColumnNames() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "{\"query\":\"select 1 \\\"a\\\", 2 \\\"b,c\\\", 3 \\\"d,e\\\", 4 '\\\"f,g\\\"'\",\"columns\":[{\"name\":\"a\",\"type\":\"INT\"},{\"name\":\"b,c\",\"type\":\"INT\"},{\"name\":\"d,e\",\"type\":\"INT\"},{\"name\":\"\\\"f,g\\\"\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1,2,3,4]],\"count\":1}",
                "select 1 \"a\", 2 \"b,c\", 3 \"d,e\", 4 '\"f,g\"'",
                new CharSequenceObjHashMap<>() {{
                    put("cols", "\"a\",\"b,c\",d\\,e,\"\\\"f\\,g\\\"\"");
                }}

        ));
    }

    @Test
    public void testJsonQueryRenameTable() throws Exception {
        testJsonQuery0(
                2, (engine, sqlExecutionContext) -> {
                    // create table with all column types
                    createTableX(engine, 20);

                    // rename x -> y (quoted)
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=rename+table+%27x%27+to+%27y%27&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // query new table name
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=y%20where%20i%20%3D%20(%27EHNRX%27) HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "02a1\r\n" +
                                    "{\"query\":\"y where i = ('EHNRX')\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29",
                            1,
                            0,
                            false
                    );

                    // rename y -> x (unquoted)
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=rename+table+y+to+x&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // query table 'x'
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=x%20where%20i%20%3D%20(%27EHNRX%27) HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "02a1\r\n" +
                                    "{\"query\":\"x where i = ('EHNRX')\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29",
                            1,
                            0,
                            false
                    );
                }, false
        );
    }

    @Test
    public void testJsonQueryResponseLimit() throws Exception {
        configuredMaxQueryResponseRowLimit = 2;
        testJsonQuery(
                20,
                "GET /query?query=x&limit=10,14 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "034a\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"],[100,-22306,1604613885,null,\"192651811-02-20T17:11:20.516Z\",\"-102298-11-25T12:53:13.351961Z\",0.21047932,0.8796413468565342,\"UTZOD\",\"KOC\",false,[],\"79ec07ef-72dc-a7a1-a27b-49491adab237\",null]],\"count\":11}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQuerySelectAlterSelect() throws Exception {
        testJsonQuery0(
                1, (engine, sqlExecutionContext) -> {

                    // create table
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)%20timestamp%28timestamp%29&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // insert one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(1%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    INSERT_QUERY_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // check if we have one record
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+on+timestamp+partition+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0171\r\n" +
                                    "{\"query\":\"\\n\\nselect * from balances_x latest on timestamp partition by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"timestamp\":4,\"dataset\":[[1,\"USD\",1500.0,0,\"1970-01-01T01:40:00.000001Z\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );

                    // add column
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=alter+table+balances_x+add+column+xyz+int&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:13005\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:13005/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0c\r\n" +
                                    "{\"ddl\":\"OK\"}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );

                    // select again expecting only metadata
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+on+timestamp+partition+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0192\r\n" +
                                    "{\"query\":\"\\n\\nselect * from balances_x latest on timestamp partition by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"name\":\"xyz\",\"type\":\"INT\"}],\"timestamp\":4,\"dataset\":[[1,\"USD\",1500.0,0,\"1970-01-01T01:40:00.000001Z\",null]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );
                }, false
        );
    }

    @Test
    public void testJsonQuerySingleRow() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x%20where%20i%20%3D%20(%27EHNRX%27) HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "02a1\r\n" +
                        "{\"query\":\"x where i = ('EHNRX')\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29df637e38\",\"}龘и\uDA89\uDFA4~\"]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryStoresTelemetryEvent() throws Exception {
        testJsonQuery(
                0,
                "GET /query?query=x HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01cd\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1,
                true
        );

        final String expectedEvent = "100\t1\n" +
                "1\t2\n" +
                "101\t1\n";
        assertTelemetryEventAndOrigin(expectedEvent);
    }

    @Test
    public void testJsonQueryStoresTelemetryEventWhenCached() throws Exception {
        testJsonQuery(
                0,
                "GET /query?query=x HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01cd\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n",
                2,
                true
        );

        final String expected = "100\t1\n" +
                "1\t2\n" +
                "1\t2\n" +
                "101\t1\n";
        assertTelemetryEventAndOrigin(expected);
    }

    /**
     * Cold storage may lead to the initiation of suspend events when data is inaccessible to the local database instance.
     * This disruption affects both the state machine's flow and the factory's data provision process. This test
     * replicates a suspend event, comparing the query output after resumption with the output of a query that
     * hasn't been suspended.
     */
    @Test
    public void testJsonQuerySuspend() throws Exception {
        testSuspend("/query");
    }

    @Test
    public void testJsonQuerySyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(configuration, baseDir, false);
            WorkerPool workerPool = new TestWorkerPool(1);
            try (
                    CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));
                httpServer.bind(new TestJsonQueryProcessorFactory(engine, httpConfiguration, workerPool.getWorkerCount()));

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    createTableX(engine, 20);

                    for (int i = 0; i < 10; i++) {
                        testHttpClient.assertGet(
                                "{\"query\":\"x where2 i = ('EHNRX')\",\"error\":\"unexpected token [i]\",\"position\":9}",
                                "x where2 i = ('EHNRX')"
                        );
                    }
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testJsonQueryTimeout() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withQueryTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK)
                .run((engine, sqlExecutionContext) -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute(QUERY_TIMEOUT_TABLE_DDL, executionContext);
                        // we use regexp, because the fd is different every time
                        testHttpClient.assertGetRegexp(
                                "/query",
                                "\\{\"query\":\"select i, avg\\(l\\), max\\(l\\) from t group by i order by i asc limit 3\",\"error\":\"timeout, query aborted \\[fd=\\d+, runtime=\\d+ms, timeout=-100ms\\]\",\"position\":0\\}",
                                "select i, avg(l), max(l) from t group by i order by i asc limit 3",
                                null, null, null, null,
                                "400"
                        );
                    }
                });
    }

    @Test
    public void testJsonQueryTimeoutResetOnEachQuery() throws Exception {
        final int timeout = 500;
        final int iterations = 3;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withQueryTimeout(timeout)
                .run((engine, sqlExecutionContext) -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute(QUERY_TIMEOUT_TABLE_DDL, executionContext);
                        for (int i = 0; i < iterations; i++) {
                            new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                    "GET /exec?query=" + urlEncodeQuery(QUERY_TIMEOUT_SELECT) + "&count=true HTTP/1.1\r\n",
                                    "f9\r\n" +
                                            "{\"query\":\"select i, avg(l), max(l) from t group by i order by i asc limit 3\",\"columns\":[{\"name\":\"i\",\"type\":\"INT\"},{\"name\":\"avg\",\"type\":\"DOUBLE\"},{\"name\":\"max\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[0,55.0,100],[1,46.0,91],[2,47.0,92]],\"count\":3}\r\n" +
                                            "00\r\n" +
                                            "\r\n"
                            );
                            if (i != iterations - 1) {
                                Os.sleep(timeout);
                            }
                        }
                    }
                });
    }

    @Test
    public void testJsonQueryTopLimit() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x&limit=10 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "08e1\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29df637e38\",\"}龘и\uDA89\uDFA4~\"],[53,5639,-1162267908,6993925225312419449,\"195808098-05-09T04:14:54.347Z\",\"171005-04-19T09:31:35.433003Z\",0.24593449,0.29313719347837397,\"BVTMH\",null,false,[],\"d364c241-dde2-cf90-a7a8-f4e549997e46\",\"\uE961-\\\\篸{\"],[40,-8761,null,-7995393784734742820,\"275774022-08-09T21:28:04.485Z\",\"-264492-10-11T03:17:51.666853Z\",null,0.5065228336156442,\"LNVTI\",null,false,[],\"a011214b-ad88-8a69-9502-128cda0887fe\",null],[-31,4215,1362833895,null,\"-49144476-01-15T02:33:12.980Z\",null,0.26369333,0.7632615004324503,\"LHMLL\",\"OYP\",false,[],\"b92d0771-d782-63eb-5479-ae0482582ad0\",\"! Yc0\"],[-80,-23575,null,5552835357100545895,\"-229044588-12-31T09:43:18.056Z\",null,null,null,\"GLUOH\",\"ZHZ\",false,[],\"8b1134e2-9413-4389-a2cb-c77b1cdd7786\",\"1\uD97C\uDD2B珣zx\"],[119,-2044,-2043541236,-4547802916868961458,\"-281648402-09-21T10:33:06.955Z\",\"221810-02-23T20:19:19.020303Z\",0.56910527,null,\"WIFFL\",\"BRO\",false,[],\"09359765-2ae7-2c5c-14ef-c23546571bdc\",\"\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5\"],[16,30964,-1520181263,-7212878484370155026,null,\"280770-04-22T16:59:28.938593Z\",0.34608507,0.5780819331422455,\"UQDYO\",null,false,[],\"a579cf90-ccdf-133e-86be-020b55a15fd1\",null],[72,27348,-647653731,8737613628813682249,null,null,0.0024457574,0.19736767249829557,\"CBDMI\",\"QZV\",true,[],null,\"\uDB4F\uDC7Dl⤃堝ᢣ\"],[36,22350,null,null,\"77319557-11-14T08:22:42.686Z\",\"104977-04-08T13:34:21.431788Z\",0.112962544,0.9934423708117267,\"FNWGR\",\"DGG\",false,[],\"899850f1-14ad-249d-97af-847507d07b51\",\"d<J1n\"],[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"]],\"count\":10}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryTopLimitAndCount() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=x&limit=10&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "08e1\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29df637e38\",\"}龘и\uDA89\uDFA4~\"],[53,5639,-1162267908,6993925225312419449,\"195808098-05-09T04:14:54.347Z\",\"171005-04-19T09:31:35.433003Z\",0.24593449,0.29313719347837397,\"BVTMH\",null,false,[],\"d364c241-dde2-cf90-a7a8-f4e549997e46\",\"\uE961-\\\\篸{\"],[40,-8761,null,-7995393784734742820,\"275774022-08-09T21:28:04.485Z\",\"-264492-10-11T03:17:51.666853Z\",null,0.5065228336156442,\"LNVTI\",null,false,[],\"a011214b-ad88-8a69-9502-128cda0887fe\",null],[-31,4215,1362833895,null,\"-49144476-01-15T02:33:12.980Z\",null,0.26369333,0.7632615004324503,\"LHMLL\",\"OYP\",false,[],\"b92d0771-d782-63eb-5479-ae0482582ad0\",\"! Yc0\"],[-80,-23575,null,5552835357100545895,\"-229044588-12-31T09:43:18.056Z\",null,null,null,\"GLUOH\",\"ZHZ\",false,[],\"8b1134e2-9413-4389-a2cb-c77b1cdd7786\",\"1\uD97C\uDD2B珣zx\"],[119,-2044,-2043541236,-4547802916868961458,\"-281648402-09-21T10:33:06.955Z\",\"221810-02-23T20:19:19.020303Z\",0.56910527,null,\"WIFFL\",\"BRO\",false,[],\"09359765-2ae7-2c5c-14ef-c23546571bdc\",\"\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5\"],[16,30964,-1520181263,-7212878484370155026,null,\"280770-04-22T16:59:28.938593Z\",0.34608507,0.5780819331422455,\"UQDYO\",null,false,[],\"a579cf90-ccdf-133e-86be-020b55a15fd1\",null],[72,27348,-647653731,8737613628813682249,null,null,0.0024457574,0.19736767249829557,\"CBDMI\",\"QZV\",true,[],null,\"\uDB4F\uDC7Dl⤃堝ᢣ\"],[36,22350,null,null,\"77319557-11-14T08:22:42.686Z\",\"104977-04-08T13:34:21.431788Z\",0.112962544,0.9934423708117267,\"FNWGR\",\"DGG\",false,[],\"899850f1-14ad-249d-97af-847507d07b51\",\"d<J1n\"],[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"]],\"count\":20}\r\n" +
                        "00\r\n"
        );
    }

    @Test
    public void testJsonQueryTopLimitHttp1() throws Exception {
        testJsonQuery0(
                2, (engine, sqlExecutionContext) -> {
                    // create table with all column types
                    createTableX(engine, 20);
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=x&limit=10 HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Connection: close\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "08e1\r\n" +
                                    "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[],\"c2593f82-b430-328d-84a0-9f29df637e38\",\"}龘и\uDA89\uDFA4~\"],[53,5639,-1162267908,6993925225312419449,\"195808098-05-09T04:14:54.347Z\",\"171005-04-19T09:31:35.433003Z\",0.24593449,0.29313719347837397,\"BVTMH\",null,false,[],\"d364c241-dde2-cf90-a7a8-f4e549997e46\",\"\uE961-\\\\篸{\"],[40,-8761,null,-7995393784734742820,\"275774022-08-09T21:28:04.485Z\",\"-264492-10-11T03:17:51.666853Z\",null,0.5065228336156442,\"LNVTI\",null,false,[],\"a011214b-ad88-8a69-9502-128cda0887fe\",null],[-31,4215,1362833895,null,\"-49144476-01-15T02:33:12.980Z\",null,0.26369333,0.7632615004324503,\"LHMLL\",\"OYP\",false,[],\"b92d0771-d782-63eb-5479-ae0482582ad0\",\"! Yc0\"],[-80,-23575,null,5552835357100545895,\"-229044588-12-31T09:43:18.056Z\",null,null,null,\"GLUOH\",\"ZHZ\",false,[],\"8b1134e2-9413-4389-a2cb-c77b1cdd7786\",\"1\uD97C\uDD2B珣zx\"],[119,-2044,-2043541236,-4547802916868961458,\"-281648402-09-21T10:33:06.955Z\",\"221810-02-23T20:19:19.020303Z\",0.56910527,null,\"WIFFL\",\"BRO\",false,[],\"09359765-2ae7-2c5c-14ef-c23546571bdc\",\"\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5\"],[16,30964,-1520181263,-7212878484370155026,null,\"280770-04-22T16:59:28.938593Z\",0.34608507,0.5780819331422455,\"UQDYO\",null,false,[],\"a579cf90-ccdf-133e-86be-020b55a15fd1\",null],[72,27348,-647653731,8737613628813682249,null,null,0.0024457574,0.19736767249829557,\"CBDMI\",\"QZV\",true,[],null,\"\uDB4F\uDC7Dl⤃堝ᢣ\"],[36,22350,null,null,\"77319557-11-14T08:22:42.686Z\",\"104977-04-08T13:34:21.431788Z\",0.112962544,0.9934423708117267,\"FNWGR\",\"DGG\",false,[],\"899850f1-14ad-249d-97af-847507d07b51\",\"d<J1n\"],[59,-13676,-1529726228,4092568845903588572,null,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,[],\"c48ad6b8-f696-2219-b27b-0ac7fbdee201\",\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"]],\"count\":10}\r\n" +
                                    "00\r\n",
                            1,
                            0,
                            false
                    );
                },
                false,
                true
        );
    }

    @Test
    public void testJsonQueryVacuumTable() throws Exception {
        testJsonQuery0(
                2, (engine, sqlExecutionContext) -> {
                    createTableX(engine, 20);

                    final String vacuumQuery = "vacuum table x";
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(vacuumQuery) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );
                }, false
        );
    }

    @Test
    @Ignore("TODO: fix this test. the gzipped expected response makes it hard to change")
    public void testJsonQueryWithCompressedResults1() throws Exception {
        assertMemoryLeak(() -> {
            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(configuration, nf, baseDir, 256, false, true);
            final WorkerPool workerPool = new TestWorkerPool(2);
            try (
                    CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));
                httpServer.bind(new TestJsonQueryProcessorFactory(engine, httpConfiguration, workerPool.getWorkerCount()));

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    createTableX(engine, 30);

                    // send multipart request to server
                    final String request = "GET /query?query=x HTTP/1.1\r\n" +
                            "Host: localhost:9001\r\n" +
                            "Connection: keep-alive\r\n" +
                            "Cache-Control: max-age=0\r\n" +
                            "Upgrade-Insecure-Requests: 1\r\n" +
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                            "Accept-Encoding: gzip, deflate, br\r\n" +
                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                            "\r\n";

                    ByteArrayResponse expectedResponse;
                    try (InputStream is = getClass().getResourceAsStream(getClass().getSimpleName() + ".testJsonQueryWithCompressedResults1.bin")) {
                        Assert.assertNotNull(is);
                        byte[] bytes = new byte[20 * 1024];
                        int len = is.read(bytes);
                        expectedResponse = new ByteArrayResponse(bytes, len);
                    }
                    sendAndReceive(nf, request, expectedResponse, 10, 100L, true);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    @Ignore("TODO: fix this test. the gzipped expected response makes it hard to change")
    public void testJsonQueryWithCompressedResults2() throws Exception {
        assertMemoryLeak(() -> {
            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(configuration, nf, baseDir, 4096, false, true);
            WorkerPool workerPool = new TestWorkerPool(2);
            try (
                    CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));
                httpServer.bind(new TestJsonQueryProcessorFactory(engine, httpConfiguration, workerPool.getWorkerCount()));

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    createTableX(engine, 1000);

                    // send multipart request to server
                    // testJsonQueryWithCompressedResults1 tested requests from REST API, while this test mimics requests sent from web console
                    // diff: LONG values are surrounded with double quotation marks, to prevent JS parse overflow
                    final String request = "GET /query?query=x&src=con HTTP/1.1\r\n" +
                            "Host: localhost:9001\r\n" +
                            "Connection: keep-alive\r\n" +
                            "Cache-Control: max-age=0\r\n" +
                            "Upgrade-Insecure-Requests: 1\r\n" +
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                            "Accept-Encoding: gzip, deflate, br\r\n" +
                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                            "\r\n";

                    ByteArrayResponse expectedResponse;
                    try (InputStream is = getClass().getResourceAsStream(getClass().getSimpleName() + ".testJsonQueryWithCompressedResults2.bin")) {
                        Assert.assertNotNull(is);
                        byte[] bytes = new byte[100 * 1024];
                        int len = is.read(bytes);
                        expectedResponse = new ByteArrayResponse(bytes, len);
                    }
                    sendAndReceive(nf, request, expectedResponse, 10, 100L, false);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testJsonQueryWithInterruption() throws Exception {
        assertMemoryLeak(() -> {
            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = root;
            final int tableRowCount = 3_000_000;

            CairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(baseDir) {
                @Override
                public int getSqlPageFrameMaxRows() {
                    // this is necessary to sufficiently fragmented paged filter execution
                    return 10_000;
                }
            };
            DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withNetwork(nf)
                    .withBaseDir(baseDir)
                    .withSendBufferSize(256)
                    .withDumpingTraffic(false)
                    .withAllowDeflateBeforeSend(false)
                    .withServerKeepAlive(true)
                    .withHttpProtocolVersion("HTTP/1.1 ")
                    .build(cairoConfiguration);

            WorkerPool workerPool = new TestWorkerPool(1);

            try (CairoEngine engine = new CairoEngine(cairoConfiguration);
                 HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));
                httpServer.bind(new TestJsonQueryProcessorFactory(engine, httpConfiguration, workerPool.getWorkerCount()));

                TestUtils.setupWorkerPool(workerPool, engine);

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    createTableX(engine, tableRowCount);

                    // send multipart request to server
                    final String request = "GET /query?query=select+a+from+x+where+test_latched_counter() HTTP/1.1\r\n" +
                            "Host: localhost:9001\r\n" +
                            "Connection: keep-alive\r\n" +
                            "Cache-Control: max-age=0\r\n" +
                            "Upgrade-Insecure-Requests: 1\r\n" +
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                            "Accept-Encoding: gzip, deflate, br\r\n" +
                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                            "\r\n";

                    long fd = nf.socketTcp(true);
                    try {
                        long sockAddrInfo = nf.getAddrInfo("127.0.0.1", 9001);
                        assert sockAddrInfo != -1;
                        try {
                            TestUtils.assertConnectAddrInfo(fd, sockAddrInfo);
                            Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
                            nf.configureNonBlocking(fd);

                            long bufLen = request.length();
                            long ptr = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                            try {
                                new SendAndReceiveRequestBuilder()
                                        .withNetworkFacade(nf)
                                        .withPauseBetweenSendAndReceive(0)
                                        .withPrintOnly(false)
                                        .withExpectReceiveDisconnect(true)
                                        .executeUntilDisconnect(request, fd, 200, ptr, null);
                            } finally {
                                Unsafe.free(ptr, bufLen, MemoryTag.NATIVE_DEFAULT);
                            }
                        } finally {
                            nf.freeAddrInfo(sockAddrInfo);
                        }
                    } finally {
                        nf.close(fd);
                        LOG.info().$("Closing client connection").$();
                    }
                    // depending on how quick the CI hardware is we may end up processing different
                    // number of rows before query is interrupted
                    assertTrue(tableRowCount > TestLatchedCounterFunctionFactory.getCount());
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testJsonQueryZeroRows() throws Exception {
        testJsonQuery(
                0,
                "GET /query?query=x HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "01cd\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\""
        );
    }

    @Test
    public void testJsonRecordTypeSelect() throws Exception {
        testJsonQuery(
                1,
                "GET /exec?limit=0%2C1000&explain=true&count=true&src=con&query=%0D%0A%0D%0A%0D%0Aselect%20pg_catalog.pg_class()%20x%2C%20(pg_catalog.pg_class()).relnamespace%20from%20long_sequence(2) HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.1731187971.1580598042\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "012c\r\n" +
                        "{\"query\":\"\\r\\n\\r\\n\\r\\nselect pg_catalog.pg_class() x, (pg_catalog.pg_class()).relnamespace from long_sequence(2)\",\"columns\":[{\"name\":\"x1\",\"type\":\"RECORD\"},{\"name\":\"column\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[null,11],[null,2200],[null,11],[null,2200]],\"count\":4,\"explain\":{\"jitCompiled\":false}}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonSelectNull() throws Exception {
        testJsonQuery(
                0, "GET /query?query=select+null+from+long_sequence(1)&count=true&src=con HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.392867896.1580123365\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "85\r\n" +
                        "{\"query\":\"select null from long_sequence(1)\",\"columns\":[{\"name\":\"null\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[null]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testJsonTableReferenceOutOfDate() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "{\"query\":\"select * from test_table_reference_out_of_date();\",\"error\":\"cached query plan cannot be used because table schema has changed [table=test_table_reference_out_of_date]\",\"position\":0}",
                "select * from test_table_reference_out_of_date();"
        ));
    }

    @Test
    public void testJsonUtf8EncodedColumnName() throws Exception {
        testJsonQuery(
                0,
                "GET /query?query=select+0+%D1%80%D0%B5%D0%BA%D0%BE%D1%80%D0%B4%D0%BD%D0%BE+from+long_sequence(10)&limit=0%2C1000&count=true&src=con HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: _ga=GA1.1.2124932001.1573824669; _gid=GA1.1.392867896.1580123365\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "bf\r\n" +
                        "{\"query\":\"select 0 рекордно from long_sequence(10)\",\"columns\":[{\"name\":\"рекордно\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[0],[0],[0],[0],[0],[0],[0],[0],[0],[0]],\"count\":10}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testJsonUtf8EncodedQuery() throws Exception {
        testJsonQuery(
                0,
                "GET /query?query=%0A%0A%0A%0ASELECT+%27Rapha%C3%ABl%27+a%2C+%27L%C3%A9o%27+b+FROM+long_sequence(2)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                        "Host: localhost:13005\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:13005/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "Cookie: ajs_group_id=null; ajs_anonymous_id=%22870b530ab5ce462f4545099f85657346%22; ajs_user_id=%22870b530ab5ce462f4545099f85657346%22; _ga=GA1.1.1909943241.1573659694\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "da\r\n" +
                        "{\"query\":\"\\n\\n\\n\\nSELECT 'Raphaël' a, 'Léo' b FROM long_sequence(2)\",\"columns\":[{\"name\":\"a\",\"type\":\"STRING\"},{\"name\":\"b\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"Raphaël\",\"Léo\"],[\"Raphaël\",\"Léo\"]],\"count\":2}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testLong128Unsupported() throws Exception {
        String expectedErrorResponse = "HTTP/1.1 400 Bad request\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: application/json; charset=utf-8\r\n" +
                "Keep-Alive: timeout=5, max=10000\r\n" +
                "\r\n" +
                "8d\r\n" +
                "{\"query\":\"select to_long128(1, 1) from long_sequence(1);\",\"error\":\"column type not supported [column=to_long128, type=LONG128]\",\"position\":0}\r\n" +
                "00\r\n" +
                "\r\n";

        testJsonQuery(
                20,
                "GET /query?query=select%20to_long128%281%2C%201%29%20from%20long_sequence%281%29%3B HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                expectedErrorResponse
        );
    }

    @Test
    public void testMaxConnections() throws Exception {
        LOG.info().$("started maxConnections").$();
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(configuration);

            // change to 400 to trigger lockup
            // excess connection take a while to return (because it's N TCP retransmissions + timeout under the hood
            // so increasing this number only makes test take longer to run)
            final int activeConnectionLimit = 400;

            AtomicInteger openCount = new AtomicInteger(0);
            AtomicInteger closeCount = new AtomicInteger(0);

            final IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration() {
                @Override
                public boolean getHint() {
                    return true;
                }

                @Override
                public int getLimit() {
                    return activeConnectionLimit;
                }
            };

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    configuration,
                    new IOContextFactory<HttpConnectionContext>() {
                        @SuppressWarnings("resource")
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            openCount.incrementAndGet();
                            return new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE) {
                                @Override
                                public void close() {
                                    closeCount.incrementAndGet();
                                    super.close();
                                }
                            }.of(fd);
                        }
                    }
            )) {
                try (HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HealthCheckProcessor(httpServerConfiguration);
                    }

                    @Override
                    public HttpRequestProcessor select(HttpRequestHeader requestHeader) {
                        return null;
                    }
                }) {

                    AtomicBoolean serverRunning = new AtomicBoolean(true);
                    SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                    new Thread(() -> {
                        try {
                            do {
                                dispatcher.run(0);
                                dispatcher.processIOQueue(
                                        (operation, context, dispatcher1) -> handleClientOperation(context, operation, selector, dispatcher1)
                                );
                            } while (serverRunning.get());
                        } finally {
                            serverHaltLatch.countDown();
                        }
                    }).start();

                    LongList openFds = new LongList();

                    final long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    final long buf = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (int i = 0; i < 10; i++) {
                            testMaxConnections0(dispatcher, sockAddr, openFds, buf);
                        }
                    } finally {
                        Net.freeSockAddr(sockAddr);
                        Unsafe.free(buf, 4096, MemoryTag.NATIVE_DEFAULT);
                        Assert.assertFalse(configuration.getLimit() < dispatcher.getConnectionCount());
                        serverRunning.set(false);
                        serverHaltLatch.await();
                    }
                }
            }
        });
    }

    @Test
    public void testMissingContentDisposition() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "2f\r\n" +
                        "'Content-Disposition' multipart header missing'\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload HTTP/1.1\r\n" +
                        "host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "9988" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testMissingContentDispositionFileName() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "12\r\n" +
                        "no file name given\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload HTTP/1.1\r\n" +
                        "host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "content-disposition: form-data; name=\"data\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "9988" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testMissingContentDispositionName() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "37\r\n" +
                        "invalid value in 'Content-Disposition' multipart header\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /upload HTTP/1.1\r\n" +
                        "host: localhost:9001\r\n" +
                        "User-Agent: curl/7.64.0\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Length: 437760673\r\n" +
                        "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
                        "Expect: 100-continue\r\n" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d\r\n" +
                        "content-disposition: ; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        "\r\n" +
                        "9988" +
                        "\r\n" +
                        "--------------------------27d997ca93d2689d--",
                NetworkFacadeImpl.INSTANCE,
                true,
                1
        );
    }

    @Test
    public void testMissingURL() throws Exception {
        testJsonQuery0(
                2, (engine, sqlExecutionContext) -> {
                    long fd = Net.socketTcp(true);
                    try {
                        long sockAddrInfo = Net.getAddrInfo("127.0.0.1", 9001);
                        try {
                            TestUtils.assertConnectAddrInfo(fd, sockAddrInfo);
                            Assert.assertEquals(0, NetworkFacadeImpl.INSTANCE.setTcpNoDelay(fd, true));

                            final String request = "GET HTTP/1.1\r\n" +
                                    "Host: localhost:9001\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Cache-Control: max-age=0\r\n" +
                                    "Upgrade-Insecure-Requests: 1\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n";
                            final int len = request.length() * 2;
                            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
                            long ptr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                            try {
                                int sent = 0;
                                int reqLen = request.length();
                                Utf8s.strCpyAscii(request, reqLen, ptr);
                                boolean disconnected = false;
                                while (sent < reqLen) {
                                    int n = nf.sendRaw(fd, ptr + sent, reqLen - sent);
                                    if (n < 0) {
                                        disconnected = true;
                                        break;
                                    }
                                    if (n > 0) {
                                        sent += n;
                                    }
                                }
                                if (!disconnected) {
                                    while (true) {
                                        int n = nf.recvRaw(fd, ptr, len);
                                        if (n < 0) {
                                            break;
                                        }
                                    }
                                }
                            } finally {
                                Unsafe.free(ptr, len, MemoryTag.NATIVE_DEFAULT);
                            }
                        } finally {
                            Net.freeAddrInfo(sockAddrInfo);
                        }
                    } finally {
                        Net.close(fd);
                    }
                }, false
        );
    }

    @Test
    public void testNetworkErrorCouldNotBindSocket() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(configuration);
            try (IODispatcher<HttpConnectionContext> ignored1 = IODispatchers.create(
                    DefaultIODispatcherConfiguration.INSTANCE,
                    fd -> new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE)
            )) {
                // Simulate a scenario where the socket cannot be bound
                try {
                    try (IODispatcher<HttpConnectionContext> ignored2 = IODispatchers.create(
                            DefaultIODispatcherConfiguration.INSTANCE,
                            fd -> new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE)
                    )) {
                        Assert.fail("ignored2 should not have been created as the socket binding failed");
                    }
                } catch (NetworkError e) {
                    TestUtils.assertContains(e.getMessage(), "could not bind socket");
                }
            }
        });
    }

    @Test
    public void testNoMetadataInTextExport() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    CharSequenceObjHashMap<String> queryParams = new CharSequenceObjHashMap<>();
                    queryParams.put("nm", "true");
                    queryParams.put("query", "select 42 from long_sequence(1);");
                    testHttpClient.assertGet("/exp", "42\r\n", queryParams, null, null);
                });
    }

    @Test
    public void testOnAllocationExceptionDispatcherDoesNotLeakConnections() throws Exception {
        LOG.info().$("started maxConnections").$();
        assertMemoryLeak(() -> {
            final int activeConnectionLimit = 10;

            final IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration() {
                @Override
                public boolean getHint() {
                    return true;
                }

                @Override
                public int getLimit() {
                    return activeConnectionLimit;
                }
            };

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    configuration,
                    fd -> {
                        // Throw out of memory exception when we reach the limit
                        throw CairoException.nonCritical().setOutOfMemory(true)
                                .put("global RSS memory limit exceeded [usage=test]");
                    }
            )) {
                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    try {
                        do {
                            dispatcher.run(0);
                            dispatcher.processIOQueue(
                                    (operation, context, dispatcher1) -> handleClientOperation(context, operation, null, dispatcher1)
                            );
                        } while (serverRunning.get());
                    } finally {
                        serverHaltLatch.countDown();
                    }
                }).start();

                LongList openFds = new LongList();

                final long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                final long buf = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < activeConnectionLimit * 2; i++) {
                        long fd = Net.socketTcp(true);
                        openFds.add(fd);
                        LOG.info().$("Connecting socket ").$(i).$(" fd=").$(fd).$();
                        TestUtils.assertConnect(fd, sockAddr);
                    }
                    assertEventually(() -> Assert.assertEquals(0, dispatcher.getConnectionCount()));
                } finally {
                    for (int i = 0; i < openFds.size(); i++) {
                        Net.close(openFds.getQuick(i));
                    }

                    Net.freeSockAddr(sockAddr);
                    Unsafe.free(buf, 4096, MemoryTag.NATIVE_DEFAULT);
                    Assert.assertFalse(configuration.getLimit() < dispatcher.getConnectionCount());
                    serverRunning.set(false);
                    serverHaltLatch.await();
                }
            }
        });
    }

    @Test
    public void testPostRequestToGetProcessor() throws Exception {
        testImport(
                "HTTP/1.1 405 Method Not Allowed\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "1b\r\n" +
                        "Method POST not supported\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n",
                "POST /exec?fmt=json&overwrite=true&forceHeader=true&name=clipboard-157200856 HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"\r\n" +
                        "\r\n" +
                        "[{\"name\":\"timestamp,\"type\":\"DATE\"},{\"name\":\"bid\",\"type\":\"INT\"}]\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "timestamp,bid\r\n" +
                        "27/05/2018 00:00:01,100\r\n" +
                        "27/05/2018 00:00:02,101\r\n" +
                        "27/05/2018 00:00:03,102\r\n" +
                        "27/05/2018 00:00:04,103\r\n" +
                        "27/05/2018 00:00:05,104\r\n" +
                        "27/05/2018 00:00:06,105\r\n" +
                        "27/05/2018 00:00:07,106\r\n" +
                        "27/05/2018 00:00:08,107\r\n" +
                        "27/05/2018 00:00:09,108\r\n" +
                        "27/05/2018 00:00:10,109\r\n" +
                        "27/05/2018 00:00:11,110\r\n" +
                        "27/05/2018 00:00:12,111\r\n" +
                        "27/05/2018 00:00:13,112\r\n" +
                        "27/05/2018 00:00:14,113\r\n" +
                        "27/05/2018 00:00:15,114\r\n" +
                        "27/05/2018 00:00:16,115\r\n" +
                        "27/05/2018 00:00:17,116\r\n" +
                        "27/05/2018 00:00:18,117\r\n" +
                        "27/05/2018 00:00:19,118\r\n" +
                        "27/05/2018 00:00:20,119\r\n" +
                        "27/05/2018 00:00:21,120\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );
    }

    @Test
    public void testQueryEventuallySucceedsOnDataUnavailableChunkedResponse() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withSendBufferSize(256))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    int totalRows = 32;
                    int backoffCount = 10;

                    final AtomicInteger totalEvents = new AtomicInteger();
                    TestDataUnavailableFunctionFactory.eventCallback = event -> {
                        event.trigger();
                        event.close();
                        totalEvents.incrementAndGet();
                    };

                    final String query = "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")";
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=" + urlEncodeQuery(query) + "&count=true HTTP/1.1\r\n",
                            "0100\r\n" +
                                    "{\"query\":\"select * from test_data_unavailable(32, 10)\",\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"},{\"name\":\"y\",\"type\":\"LONG\"},{\"name\":\"z\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1,1,1],[2,2,2],[3,3,3],[4,4,4],[5,5,5],[6,6,6],[7,7,7],[8,8,8],[9,9,9],[10,10,10]\r\n" +
                                    "ff\r\n" +
                                    ",[11,11,11],[12,12,12],[13,13,13],[14,14,14],[15,15,15],[16,16,16],[17,17,17],[18,18,18],[19,19,19],[20,20,20],[21,21,21],[22,22,22],[23,23,23],[24,24,24],[25,25,25],[26,26,26],[27,27,27],[28,28,28],[29,29,29],[30,30,30],[31,31,31],[32,32,32]],\"count\":32}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
                });
    }

    @Test
    public void testQueryEventuallySucceedsOnDataUnavailableEventTriggeredAfterDelay() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    int totalRows = 3;
                    int backoffCount = 3;

                    final AtomicInteger totalEvents = new AtomicInteger();
                    final AtomicReference<SuspendEvent> eventRef = new AtomicReference<>();
                    final AtomicBoolean stopDelayThread = new AtomicBoolean();

                    final Thread delayThread = createDelayThread(stopDelayThread, eventRef, totalEvents);

                    TestDataUnavailableFunctionFactory.eventCallback = eventRef::set;
                    testHttpClient.assertGet(
                            "{\"query\":\"select * from test_data_unavailable(3, 3)\",\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"},{\"name\":\"y\",\"type\":\"LONG\"},{\"name\":\"z\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1,1,1],[2,2,2],[3,3,3]],\"count\":3}",
                            "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")"
                    );
                    stopDelayThread.set(true);
                    delayThread.join();

                    Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
                });
    }

    @Test
    public void testQueryEventuallySucceedsOnDataUnavailableEventTriggeredImmediately() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    int totalRows = 3;
                    int backoffCount = 10;

                    final AtomicInteger totalEvents = new AtomicInteger();
                    TestDataUnavailableFunctionFactory.eventCallback = event -> {
                        event.trigger();
                        event.close();
                        totalEvents.incrementAndGet();
                    };

                    final String query = "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")";
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=" + urlEncodeQuery(query) + "&count=true HTTP/1.1\r\n",
                            "d0\r\n" +
                                    "{\"query\":\"select * from test_data_unavailable(3, 10)\",\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"},{\"name\":\"y\",\"type\":\"LONG\"},{\"name\":\"z\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1,1,1],[2,2,2],[3,3,3]],\"count\":3}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
                });
    }

    @Test
    public void testQueryImplicitCastExceptionInWindowFunctionFirstRecord() throws Exception {
        getSimpleTester()
                .run((engine, sqlExecutionContext) -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute(
                                "CREATE TABLE 'trades' ( " +
                                        " symbol SYMBOL, " +
                                        " side SYMBOL, " +
                                        " price DOUBLE, " +
                                        " amount DOUBLE, " +
                                        " timestamp TIMESTAMP " +
                                        ") timestamp(timestamp) PARTITION BY DAY;",
                                executionContext
                        );
                        engine.execute(
                                "INSERT INTO trades VALUES ('ETH-USD', 'sell', 2615.54, 0.00044, '2022-03-08T18:03:57.609765Z');",
                                executionContext
                        );

                        testHttpClient.setKeepConnection(true);
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT timestamp, price, lag('timestamp') OVER (ORDER BY timestamp) AS previous_price FROM trades LIMIT 10;\",\"error\":\"inconvertible value: `timestamp` [STRING -> DOUBLE]\",\"position\":0}",
                                "SELECT timestamp, price, lag('timestamp') OVER (ORDER BY timestamp) AS previous_price FROM trades LIMIT 10;"
                        );

                        // verify that HTTP server is healthy, use the same connection
                        testHttpClient.setKeepConnection(false);
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT count() FROM trades;\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "SELECT count() FROM trades;"
                        );
                    }
                });
    }

    @Test
    public void testQueryReturnsEncodedNonPrintableCharacters() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                        "GET /query?query=selecT%20%27NH%1C%27%3B%20 HTTP/1.1\r\n",
                        "81\r\n" +
                                "{\"query\":\"selecT 'NH\\u001c'; \",\"columns\":[{\"name\":\"NH\\u001c\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"NH\\u001c\"]],\"count\":1}\r\n"
                                + "00\r\n"
                                + "\r\n"
                ));
    }

    @Test
    public void testQueryWithDoubleQuotesParsedCorrectly() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            // select 1 as "select"
            // with select being the column name to check double quote parsing
            testHttpClient.assertGet(
                    "{\"query\":\"SELECT 1 as \\\"select\\\"\",\"columns\":[{\"name\":\"select\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                    "SELECT 1 as \"select\""
            );
        });
    }

    @Test
    public void testQueuedConnectionTimeout() throws Exception {
        testQueuedConnectionTimeoutImpl(9001);
    }

    @Test
    public void testQueuedConnectionTimeoutPort0() throws Exception {
        testQueuedConnectionTimeoutImpl(0);
    }

    @Test
    public void testSCPConnectDownloadDisconnect() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(configuration, baseDir, false);
            WorkerPool workerPool = new TestWorkerPool(2);
            try (
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));

                workerPool.start(LOG);

                // create 20Mb file in /tmp directory
                try (Path path = new Path().of(baseDir).concat("questdb-temp.txt")) {
                    try {
                        Rnd rnd = new Rnd();
                        writeRandomFile(path, rnd, 122222212222L);

                        long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                        try {
                            int netBufferLen = 4 * 1024;
                            long buffer = Unsafe.calloc(netBufferLen, MemoryTag.NATIVE_DEFAULT);
                            try {

                                // send request to server to download file we just created
                                final String request = "GET /questdb-temp.txt HTTP/1.1\r\n" +
                                        "Host: localhost:9000\r\n" +
                                        "Connection: keep-alive\r\n" +
                                        "Cache-Control: max-age=0\r\n" +
                                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                        "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                        "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                        "Accept-Language: en-US,en;q=0.8\r\n" +
                                        "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                        "\r\n";

                                String expectedResponseHeader = "HTTP/1.1 200 OK\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Content-Length: 20971520\r\n" +
                                        "Content-Type: text/plain\r\n" +
                                        "ETag: \"122222212222\"\r\n" + // this is last modified timestamp on the file, we set this value when we created file
                                        "\r\n";

                                for (int j = 0; j < 10; j++) {
                                    sendAndReceive(request, expectedResponseHeader);
                                }

                                // send few requests to receive 304
                                final String request2 = "GET /questdb-temp.txt HTTP/1.1\r\n" +
                                        "Host: localhost:9000\r\n" +
                                        "Connection: keep-alive\r\n" +
                                        "Cache-Control: max-age=0\r\n" +
                                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                        "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                        "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                        "Accept-Language: en-US,en;q=0.8\r\n" +
                                        "If-None-Match: \"122222212222\"\r\n" + // this header should make static processor return 304
                                        "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                        "\r\n";

                                String expectedResponseHeader2 = "HTTP/1.1 304 Not Modified\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "\r\n";

                                for (int i = 0; i < 3; i++) {
                                    sendAndReceive(request2, expectedResponseHeader2);
                                }

                                // couple more full downloads after 304
                                for (int j = 0; j < 2; j++) {
                                    sendAndReceive(request, expectedResponseHeader);
                                }

                                // get a 404 now
                                final String request3 = "GET /questdb-temp_!.txt HTTP/1.1\r\n" +
                                        "Host: localhost:9000\r\n" +
                                        "Connection: keep-alive\r\n" +
                                        "Cache-Control: max-age=0\r\n" +
                                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                        "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                        "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                        "Accept-Language: en-US,en;q=0.8\r\n" +
                                        "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                        "\r\n";

                                String expectedResponseHeader3 = "HTTP/1.1 404 Not Found\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Transfer-Encoding: chunked\r\n" +
                                        "Content-Type: text/plain; charset=utf-8\r\n" +
                                        "\r\n" +
                                        "0b\r\n" +
                                        "Not Found\r\n" +
                                        "\r\n" +
                                        "00\r\n" +
                                        "\r\n";

                                sendAndReceive(request3, expectedResponseHeader3);
                                // and few more 304s
                                sendAndReceive(request2, expectedResponseHeader2);
                            } finally {
                                Unsafe.free(buffer, netBufferLen, MemoryTag.NATIVE_DEFAULT);
                            }
                        } finally {
                            Net.freeSockAddr(sockAddr);
                        }
                    } finally {
                        workerPool.halt();
                        Files.remove(path.$());
                    }
                }
            }
        });
    }

    @Test
    public void testSCPFullDownload() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(configuration, baseDir, false);
            WorkerPool workerPool = new TestWorkerPool(2);
            try (
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));

                workerPool.start(LOG);

                // create 20Mb file in /tmp directory
                try (Path path = new Path().of(baseDir).concat("questdb-temp.txt")) {
                    try {
                        Rnd rnd = new Rnd();
                        writeRandomFile(path, rnd, 122299092L);

                        long fd = Net.socketTcp(true);
                        try {
                            long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                            try {
                                TestUtils.assertConnect(fd, sockAddr);

                                int netBufferLen = 4 * 1024;
                                long buffer = Unsafe.calloc(netBufferLen, MemoryTag.NATIVE_DEFAULT);
                                try {

                                    String expectedResponseHeader = "HTTP/1.1 200 OK\r\n" +
                                            "Server: questDB/1.0\r\n" +
                                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                            "Content-Length: 20971520\r\n" +
                                            "Content-Type: text/plain\r\n" +
                                            "ETag: \"122299092\"\r\n" + // this is last modified timestamp on the file, we set this value when we created file
                                            "\r\n";

                                    for (int j = 0; j < 10; j++) {
                                        sendRequest(fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, expectedResponseHeader);
                                    }

                                    // send few requests to receive 304
                                    final String request2 = "GET /questdb-temp.txt HTTP/1.1\r\n" +
                                            "Host: localhost:9000\r\n" +
                                            "Connection: keep-alive\r\n" +
                                            "Cache-Control: max-age=0\r\n" +
                                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                            "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                            "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                            "Accept-Language: en-US,en;q=0.8\r\n" +
                                            "If-None-Match: \"122299092\"\r\n" + // this header should make static processor return 304
                                            "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                            "\r\n";

                                    String expectedResponseHeader2 = "HTTP/1.1 304 Not Modified\r\n" +
                                            "Server: questDB/1.0\r\n" +
                                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                            "\r\n";

                                    for (int i = 0; i < 3; i++) {
                                        sendAndReceive(request2, expectedResponseHeader2);
                                    }

                                    // couple more full downloads after 304
                                    for (int j = 0; j < 2; j++) {
                                        sendRequest(fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, expectedResponseHeader);
                                    }

                                    // get a 404 now
                                    final String request3 = "GET /questdb-temp_!.txt HTTP/1.1\r\n" +
                                            "Host: localhost:9000\r\n" +
                                            "Connection: keep-alive\r\n" +
                                            "Cache-Control: max-age=0\r\n" +
                                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                            "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                            "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                            "Accept-Language: en-US,en;q=0.8\r\n" +
                                            "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                            "\r\n";

                                    String expectedResponseHeader3 = "HTTP/1.1 404 Not Found\r\n" +
                                            "Server: questDB/1.0\r\n" +
                                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                            "Transfer-Encoding: chunked\r\n" +
                                            "Content-Type: text/plain; charset=utf-8\r\n" +
                                            "\r\n" +
                                            "0b\r\n" +
                                            "Not Found\r\n" +
                                            "\r\n" +
                                            "00\r\n" +
                                            "\r\n";


                                    sendAndReceive(NetworkFacadeImpl.INSTANCE, request3, expectedResponseHeader3, 4, 0, false);
                                    // and few more 304s
                                    sendAndReceive(NetworkFacadeImpl.INSTANCE, request2, expectedResponseHeader2, 4, 0, false);
                                } finally {
                                    Unsafe.free(buffer, netBufferLen, MemoryTag.NATIVE_DEFAULT);
                                }
                            } finally {
                                Net.freeSockAddr(sockAddr);
                            }
                        } finally {
                            Net.close(fd);
                            LOG.info().$("closed [fd=").$(fd).$(']').$();
                        }
                    } finally {
                        workerPool.halt();
                        Files.remove(path.$());
                    }
                }
            }
        });
    }

    @Test
    public void testSCPHttp10() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = root;
            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(
                    configuration,
                    nf,
                    baseDir,
                    16 * 1024,
                    false,
                    false,
                    false,
                    "HTTP/1.0 "
            );
            WorkerPool workerPool = new TestWorkerPool(2);
            try (
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));

                workerPool.start(LOG);

                // create 20Mb file in /tmp directory
                try (Path path = new Path().of(baseDir).concat("questdb-temp.txt")) {
                    try {
                        Rnd rnd = new Rnd();

                        writeRandomFile(path, rnd, 122222212222L);

                        long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                        try {
                            int netBufferLen = 4 * 1024;
                            long buffer = Unsafe.calloc(netBufferLen, MemoryTag.NATIVE_DEFAULT);
                            try {

                                // send request to server to download file we just created
                                final String request = "GET /questdb-temp.txt HTTP/1.1\r\n" +
                                        "Host: localhost:9000\r\n" +
                                        "Connection: keep-alive\r\n" +
                                        "Cache-Control: max-age=0\r\n" +
                                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                        "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                        "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                        "Accept-Language: en-US,en;q=0.8\r\n" +
                                        "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                        "\r\n";

                                String expectedResponseHeader = "HTTP/1.0 200 OK\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Content-Length: 20971520\r\n" +
                                        "Content-Type: text/plain\r\n" +
                                        "Connection: close\r\n" +
                                        "ETag: \"122222212222\"\r\n" + // this is last modified timestamp on the file, we set this value when we created file
                                        "\r\n";

                                sendAndReceive(nf, request, expectedResponseHeader, 1, 0, false);

                                // send few requests to receive 304
                                final String request2 = "GET /questdb-temp.txt HTTP/1.1\r\n" +
                                        "Host: localhost:9000\r\n" +
                                        "Connection: keep-alive\r\n" +
                                        "Cache-Control: max-age=0\r\n" +
                                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                        "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                        "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                        "Accept-Language: en-US,en;q=0.8\r\n" +
                                        "If-None-Match: \"122222212222\"\r\n" + // this header should make static processor return 304
                                        "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                        "\r\n";

                                String expectedResponseHeader2 = "HTTP/1.0 304 Not Modified\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Connection: close\r\n" +
                                        "\r\n";

                                for (int i = 0; i < 3; i++) {
                                    sendAndReceive(nf, request2, expectedResponseHeader2, 1, 0, false);
                                }

                                // couple more full downloads after 304
                                for (int i = 0; i < 3; i++) {
                                    sendAndReceive(nf, request, expectedResponseHeader, 1, 0, false);
                                }

                                // get a 404 now
                                final String request3 = "GET /questdb-temp_!.txt HTTP/1.1\r\n" +
                                        "Host: localhost:9000\r\n" +
                                        "Connection: keep-alive\r\n" +
                                        "Cache-Control: max-age=0\r\n" +
                                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                                        "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                                        "Accept-Encoding: gzip,deflate,sdch\r\n" +
                                        "Accept-Language: en-US,en;q=0.8\r\n" +
                                        "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                                        "\r\n";

                                String expectedResponseHeader3 = "HTTP/1.0 404 Not Found\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Transfer-Encoding: chunked\r\n" +
                                        "Content-Type: text/plain; charset=utf-8\r\n" +
                                        "Connection: close\r\n" +
                                        "\r\n" +
                                        "0b\r\n" +
                                        "Not Found\r\n" +
                                        "\r\n" +
                                        "00\r\n" +
                                        "\r\n";

                                sendAndReceive(nf, request3, expectedResponseHeader3, 1, 0, false);

                                // and few more 304s
                                for (int i = 0; i < 3; i++) {
                                    sendAndReceive(nf, request2, expectedResponseHeader2, 1, 0, false);
                                }

                            } finally {
                                Unsafe.free(buffer, netBufferLen, MemoryTag.NATIVE_DEFAULT);
                            }
                        } finally {
                            Net.freeSockAddr(sockAddr);
                            workerPool.halt();
                        }
                    } finally {
                        Files.remove(path.$());
                    }
                }
            }
        });
    }

    @Test
    public void testSendHttpGet() throws Exception {

        LOG.info().$("started testSendHttpGet").$();

        final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        // the difference between request and expected is url encoding (and ':' padding, which can easily be fixed)
        final String expected = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host:localhost:9000\r\n" +
                "Connection:keep-alive\r\n" +
                "Cache-Control:max-age=0\r\n" +
                "Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding:gzip,deflate,sdch\r\n" +
                "Accept-Language:en-US,en;q=0.8\r\n" +
                "Cookie:textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(configuration);

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            SOCountDownLatch requestReceivedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    DefaultIODispatcherConfiguration.INSTANCE,
                    new IOContextFactory<HttpConnectionContext>() {
                        @SuppressWarnings("resource")
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE) {
                                @Override
                                public void close() {
                                    // it is possible that context is closed twice in error
                                    // when crashes occur put debug line here to see how many times
                                    // context is closed
                                    if (closeCount.incrementAndGet() == 1) {
                                        super.close();
                                        contextClosedLatch.countDown();
                                    }
                                }
                            }.of(fd);
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                final HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return null;
                    }

                    @Override
                    public HttpRequestProcessor select(HttpRequestHeader requestHeader) {
                        return new HttpRequestProcessor() {
                            @Override
                            public void onHeadersReady(HttpConnectionContext context) {
                                HttpRequestHeader headers = context.getRequestHeader();
                                sink.put(headers.getMethodLine());
                                sink.put("\r\n");
                                ObjList<? extends Utf8Sequence> headerNames = headers.getHeaderNames();
                                for (int i = 0, n = headerNames.size(); i < n; i++) {
                                    sink.put(headerNames.getQuick(i)).put(':');
                                    sink.put(headers.getHeader(headerNames.getQuick(i)));
                                    sink.put("\r\n");
                                }
                                sink.put("\r\n");
                                requestReceivedLatch.countDown();
                            }
                        };
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    try {
                        while (serverRunning.get()) {
                            dispatcher.run(0);
                            dispatcher.processIOQueue(
                                    (operation, context, dispatcher1) -> handleClientOperation(context, operation, selector, dispatcher1)
                            );
                        }
                    } finally {
                        serverHaltLatch.countDown();
                    }
                }).start();

                long fd = Net.socketTcp(true);
                try {
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        TestUtils.assertConnect(fd, sockAddr);

                        connectLatch.await();

                        int len = request.length();
                        long buffer = TestUtils.toMemory(request);
                        try {
                            Assert.assertEquals(len, Net.send(fd, buffer, len));
                        } finally {
                            Unsafe.free(buffer, len, MemoryTag.NATIVE_DEFAULT);
                        }

                        // do not disconnect right away, wait for server to receive the request
                        requestReceivedLatch.await();
                        Assert.assertEquals(0, Net.close(fd));
                        LOG.info().$("closed [fd=").$(fd).$(']').$();
                        fd = -1;

                        contextClosedLatch.await();

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        TestUtils.assertEquals(expected, sink);
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    if (fd != -1) {
                        Net.close(fd);
                    }
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testSendHttpGetAndSimpleResponse() throws Exception {
        LOG.info().$("started testSendHttpGetAndSimpleResponse").$();

        final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        // the difference between request and expected is url encoding (and ':' padding, which can easily be fixed)
        final String expected = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host:localhost:9000\r\n" +
                "Connection:keep-alive\r\n" +
                "Cache-Control:max-age=0\r\n" +
                "Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding:gzip,deflate,sdch\r\n" +
                "Accept-Language:en-US,en;q=0.8\r\n" +
                "Cookie:textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        final String expectedResponse = "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: text/plain; charset=utf-8\r\n" +
                "\r\n" +
                "04\r\n" +
                "OK\r\n" +
                "\r\n" +
                "00\r\n" +
                "\r\n";

        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(
                    configuration,
                    new DefaultHttpContextConfiguration() {
                        @Override
                        public MillisecondClock getMillisecondClock() {
                            return StationaryMillisClock.INSTANCE;
                        }

                        @Override
                        public Clock getNanosecondClock() {
                            return StationaryNanosClock.INSTANCE;
                        }
                    }
            );

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    DefaultIODispatcherConfiguration.INSTANCE,
                    new IOContextFactory<HttpConnectionContext>() {
                        @SuppressWarnings("resource")
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE) {
                                @Override
                                public void close() {
                                    // it is possible that context is closed twice in error
                                    // when crashes occur put debug line here to see how many times
                                    // context is closed
                                    if (closeCount.incrementAndGet() == 1) {
                                        super.close();
                                        contextClosedLatch.countDown();
                                    }
                                }
                            }.of(fd);
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                final HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HttpRequestProcessor() {
                            @Override
                            public void onHeadersReady(HttpConnectionContext context) {
                                HttpRequestHeader headers = context.getRequestHeader();
                                sink.put(headers.getMethodLine());
                                sink.put("\r\n");
                                ObjList<? extends Utf8Sequence> headerNames = headers.getHeaderNames();
                                for (int i = 0, n = headerNames.size(); i < n; i++) {
                                    sink.put(headerNames.getQuick(i)).put(':');
                                    sink.put(headers.getHeader(headerNames.getQuick(i)));
                                    sink.put("\r\n");
                                }
                                sink.put("\r\n");
                            }

                            @Override
                            public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
                                context.simpleResponse().sendStatusTextContent(200);
                            }
                        };
                    }

                    @Override
                    public HttpRequestProcessor select(HttpRequestHeader requestHeader) {
                        return null;
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    try {
                        while (serverRunning.get()) {
                            dispatcher.run(0);
                            dispatcher.processIOQueue(
                                    (operation, context, dispatcher1) -> handleClientOperation(context, operation, selector, dispatcher1)
                            );
                        }
                    } finally {
                        serverHaltLatch.countDown();
                    }
                }).start();

                long fd = Net.socketTcp(true);
                try {
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        TestUtils.assertConnect(fd, sockAddr);

                        connectLatch.await();

                        int len = request.length();
                        long buffer = TestUtils.toMemory(request);
                        try {
                            Assert.assertEquals(len, Net.send(fd, buffer, len));
                            // read response we expect
                            StringSink sink2 = new StringSink();
                            final int expectedLen = expectedResponse.length();
                            int read = 0;
                            while (read < expectedLen) {
                                int n = Net.recv(fd, buffer, len);
                                assertTrue(n > 0);

                                for (int i = 0; i < n; i++) {
                                    sink2.put((char) Unsafe.getUnsafe().getByte(buffer + i));
                                }
                                // copy response bytes to sink
                                read += n;
                            }

                            TestUtils.assertEquals(expectedResponse, sink2);
                        } finally {
                            Unsafe.free(buffer, len, MemoryTag.NATIVE_DEFAULT);
                        }

                        Assert.assertEquals(0, Net.close(fd));
                        LOG.info().$("closed [fd=").$(fd).$(']').$();
                        fd = -1;

                        contextClosedLatch.await();

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        TestUtils.assertEquals(expected, sink);
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    if (fd != -1) {
                        Net.close(fd);
                    }
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testSendTimeout() throws Exception {
        LOG.info().$("started testSendHttpGet").$();

        final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(configuration);

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public long getTimeout() {
                            // 0.5s idle timeout
                            return 500;
                        }
                    },
                    new IOContextFactory<HttpConnectionContext>() {
                        @SuppressWarnings("resource")
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE) {
                                @Override
                                public void close() {
                                    // it is possible that context is closed twice in error
                                    // when crashes occur put debug line here to see how many times
                                    // context is closed
                                    if (closeCount.incrementAndGet() == 1) {
                                        super.close();
                                        contextClosedLatch.countDown();
                                    }
                                }
                            }.of(fd);
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HttpRequestProcessor() {
                            @Override
                            public void onHeadersReady(HttpConnectionContext connectionContext) {
                                HttpRequestHeader headers = connectionContext.getRequestHeader();
                                sink.put(headers.getMethodLine());
                                sink.put("\r\n");
                                ObjList<? extends Utf8Sequence> headerNames = headers.getHeaderNames();
                                for (int i = 0, n = headerNames.size(); i < n; i++) {
                                    sink.put(headerNames.getQuick(i)).put(':');
                                    sink.put(headers.getHeader(headerNames.getQuick(i)));
                                    sink.put("\r\n");
                                }
                                sink.put("\r\n");
                            }
                        };
                    }

                    @Override
                    public HttpRequestProcessor select(HttpRequestHeader requestHeader) {
                        return null;
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    try {
                        while (serverRunning.get()) {
                            dispatcher.run(0);
                            dispatcher.processIOQueue(
                                    (operation, context, dispatcher1) -> handleClientOperation(context, operation, selector, dispatcher1)
                            );
                        }
                    } finally {
                        serverHaltLatch.countDown();
                    }
                }).start();

                long fd = Net.socketTcp(true);
                try {
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        TestUtils.assertConnect(fd, sockAddr);
                        Net.setTcpNoDelay(fd, true);

                        connectLatch.await();

                        int len = request.length();
                        long buffer = TestUtils.toMemory(request);
                        try {
                            int part1 = len / 2;
                            Assert.assertEquals(part1, Net.send(fd, buffer, part1));
                            Os.sleep(1000);
                            Assert.assertEquals(len - part1, Net.send(fd, buffer + part1, len - part1));
                        } finally {
                            Unsafe.free(buffer, len, MemoryTag.NATIVE_DEFAULT);
                        }

                        contextClosedLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        // do not close client side before server does theirs
                        assertTrue(Net.isDead(fd));

                        TestUtils.assertEquals("", sink);
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    Net.close(fd);
                    LOG.info().$("closed [fd=").$(fd).$(']').$();
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testTextExportDisconnectOnDataUnavailableEventNeverFired() throws Exception {
        getSimpleTester()
                .withWorkerCount(2)
                .withQueryTimeout(100)
                .run((engine, sqlExecutionContext) -> {
                    AtomicReference<SuspendEvent> eventRef = new AtomicReference<>();
                    TestDataUnavailableFunctionFactory.eventCallback = eventRef::set;
                    try {
                        testHttpClient.assertGetRegexp(
                                "/query",
                                ".*timeout, query aborted.*",
                                "select * from test_data_unavailable(1, 10)",
                                null,
                                null,
                                "400"
                        );
                    } finally {
                        Misc.free(eventRef.get());
                    }
                });
    }

    @Test
    public void testTextExportEventuallySucceedsOnDataUnavailableChunkedResponse() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withSendBufferSize(256))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    TestDataUnavailableFunctionFactory.eventCallback = event -> {
                        event.trigger();
                        event.close();
                    };

                    final String select = "select * from test_data_unavailable(32, 10)";
                    new SendAndReceiveRequestBuilder().executeWithStandardRequestHeaders(
                            "GET /exp?query=" + urlEncodeQuery(select) + "&count=true HTTP/1.1\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: text/csv; charset=utf-8\r\n" +
                                    "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0100\r\n" +
                                    "\"x\",\"y\",\"z\"\r\n" +
                                    "1,1,1\r\n" +
                                    "2,2,2\r\n" +
                                    "3,3,3\r\n" +
                                    "4,4,4\r\n" +
                                    "5,5,5\r\n" +
                                    "6,6,6\r\n" +
                                    "7,7,7\r\n" +
                                    "8,8,8\r\n" +
                                    "9,9,9\r\n" +
                                    "10,10,10\r\n" +
                                    "11,11,11\r\n" +
                                    "12,12,12\r\n" +
                                    "13,13,13\r\n" +
                                    "14,14,14\r\n" +
                                    "15,15,15\r\n" +
                                    "16,16,16\r\n" +
                                    "17,17,17\r\n" +
                                    "18,18,18\r\n" +
                                    "19,19,19\r\n" +
                                    "20,20,20\r\n" +
                                    "21,21,21\r\n" +
                                    "22,22,22\r\n" +
                                    "23,23,23\r\n" +
                                    "24,24,24\r\n" +
                                    "25,25,25\r\n" +
                                    "26,26,26\r\n" +
                                    "27,27,27\r\n" +
                                    "\r\n" +
                                    "32\r\n" +
                                    "28,28,28\r\n" +
                                    "29,29,29\r\n" +
                                    "30,30,30\r\n" +
                                    "31,31,31\r\n" +
                                    "32,32,32\r\n" +
                                    "\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                });
    }

    @Test
    public void testTextExportEventuallySucceedsOnDataUnavailableEventTriggeredAfterDelay() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    int totalRows = 3;
                    int backoffCount = 3;

                    final AtomicInteger totalEvents = new AtomicInteger();
                    final AtomicReference<SuspendEvent> eventRef = new AtomicReference<>();
                    final AtomicBoolean stopDelayThread = new AtomicBoolean();

                    final Thread delayThread = createDelayThread(stopDelayThread, eventRef, totalEvents);

                    TestDataUnavailableFunctionFactory.eventCallback = eventRef::set;

                    final String query = "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")";
                    new SendAndReceiveRequestBuilder().executeWithStandardRequestHeaders(
                            "GET /exp?query=" + urlEncodeQuery(query) + "&count=true HTTP/1.1\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: text/csv; charset=utf-8\r\n" +
                                    "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "22\r\n" +
                                    "\"x\",\"y\",\"z\"\r\n" +
                                    "1,1,1\r\n" +
                                    "2,2,2\r\n" +
                                    "3,3,3\r\n" +
                                    "\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    stopDelayThread.set(true);
                    delayThread.join();

                    Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
                });
    }

    @Test
    public void testTextExportEventuallySucceedsOnDataUnavailableEventTriggeredImmediately() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    int totalRows = 3;
                    int backoffCount = 10;

                    final AtomicInteger totalEvents = new AtomicInteger();
                    TestDataUnavailableFunctionFactory.eventCallback = event -> {
                        event.trigger();
                        event.close();
                        totalEvents.incrementAndGet();
                    };

                    final String query = "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")";
                    new SendAndReceiveRequestBuilder().executeWithStandardRequestHeaders(
                            "GET /exp?query=" + urlEncodeQuery(query) + "&count=true HTTP/1.1\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: text/csv; charset=utf-8\r\n" +
                                    "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "22\r\n" +
                                    "\"x\",\"y\",\"z\"\r\n" +
                                    "1,1,1\r\n" +
                                    "2,2,2\r\n" +
                                    "3,3,3\r\n" +
                                    "\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
                });
    }

    @Test
    public void testTextQueryCopyFrom() throws Exception {
        String copyInputRoot = TestUtils.getCsvRoot();
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withCopyInputRoot(copyInputRoot)
                .withMicrosecondClock(new TestMicroClock(0, 0))
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run((engine, sqlExecutionContext) -> {
                    final String copyQuery = "copy test from 'test-numeric-headers.csv' with header true";
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(copyQuery) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "aa\r\n" +
                                    "{\"query\":\"copy test from 'test-numeric-headers.csv' with header true\",\"columns\":[{\"name\":\"id\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"0000000000000000\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );

                    final String cancelQuery = "copy '0000000000000000' cancel";
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(cancelQuery) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "bb\r\n" +
                                    "{\"query\":\"copy '0000000000000000' cancel\",\"columns\":[{\"name\":\"id\",\"type\":\"STRING\"},{\"name\":\"status\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"0000000000000000\",\"finished\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );

                    final String incorrectCancelQuery = "copy 'ffffffffffffffff' cancel";
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(incorrectCancelQuery) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "ba\r\n" +
                                    "{\"query\":\"copy 'ffffffffffffffff' cancel\",\"columns\":[{\"name\":\"id\",\"type\":\"STRING\"},{\"name\":\"status\",\"type\":\"STRING\"}],\"timestamp\":-1,\"dataset\":[[\"ffffffffffffffff\",\"unknown\"]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false
                    );
                });
    }

    @Test
    public void testTextQueryCorrectQuoting() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withMicrosecondClock(new TestMicroClock(0, 0))
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run((engine, sqlExecutionContext) -> sendAndReceive(
                        NetworkFacadeImpl.INSTANCE,
                        "GET /exp?query=" + urlEncodeQuery("SELECT '{\"filed1\":1, \"filed2\":1, \"filed3\":\"admin\", \"filed4\":1}' as foo") + " HTTP/1.1\r\n" +
                                "Host: localhost:9000\r\n" +
                                "Connection: keep-alive\r\n" +
                                "Cache-Control: max-age=0\r\n" +
                                "Upgrade-Insecure-Requests: 1\r\n" +
                                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                "Accept: */*\r\n" +
                                "Accept-Encoding: gzip, deflate, br\r\n" +
                                "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                "\r\n",
                        "HTTP/1.1 200 OK\r\n" +
                                "Server: questDB/1.0\r\n" +
                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                "Transfer-Encoding: chunked\r\n" +
                                "Content-Type: text/csv; charset=utf-8\r\n" +
                                "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                "Keep-Alive: timeout=5, max=10000\r\n" +
                                "\r\n" +
                                "4b\r\n" +
                                "\"foo\"\r\n" +
                                "\"{\"\"filed1\"\":1, \"\"filed2\"\":1, \"\"filed3\"\":\"\"admin\"\", \"\"filed4\"\":1}\"\r\n" +
                                "\r\n" +
                                "00\r\n",
                        1,
                        0,
                        false
                ));
    }

    @Test
    public void testTextQueryCorrectQuotingOfHeader() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withMicrosecondClock(new TestMicroClock(0, 0))
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run((engine, sqlExecutionContext) -> sendAndReceive(
                        NetworkFacadeImpl.INSTANCE,
                        "GET /exp?query=" + urlEncodeQuery("SELECT 5 as '\"foo\"'") + " HTTP/1.1\r\n" +
                                "Host: localhost:9000\r\n" +
                                "Connection: keep-alive\r\n" +
                                "Cache-Control: max-age=0\r\n" +
                                "Upgrade-Insecure-Requests: 1\r\n" +
                                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                "Accept: */*\r\n" +
                                "Accept-Encoding: gzip, deflate, br\r\n" +
                                "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                "\r\n",
                        "HTTP/1.1 200 OK\r\n" +
                                "Server: questDB/1.0\r\n" +
                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                "Transfer-Encoding: chunked\r\n" +
                                "Content-Type: text/csv; charset=utf-8\r\n" +
                                "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                "Keep-Alive: timeout=5, max=10000\r\n" +
                                "\r\n" +
                                "0e\r\n" +
                                "\"\"\"foo\"\"\"\r\n" +
                                "5\r\n" +
                                "\r\n" +
                                "00\r\n",
                        1,
                        0,
                        false
                ));
    }

    @Test
    public void testTextQueryCorrectQuotingWithSpecialChars() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withMicrosecondClock(new TestMicroClock(0, 0))
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run((engine, sqlExecutionContext) -> sendAndReceive(
                        NetworkFacadeImpl.INSTANCE,
                        "GET /exp?query=" + urlEncodeQuery("select 'foo\\foo\uD83D\uDC27' as foo") + " HTTP/1.1\r\n" +
                                "Host: localhost:9000\r\n" +
                                "Connection: keep-alive\r\n" +
                                "Cache-Control: max-age=0\r\n" +
                                "Upgrade-Insecure-Requests: 1\r\n" +
                                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                                "Accept: */*\r\n" +
                                "Accept-Encoding: gzip, deflate, br\r\n" +
                                "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                "\r\n",
                        "HTTP/1.1 200 OK\r\n" +
                                "Server: questDB/1.0\r\n" +
                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                "Transfer-Encoding: chunked\r\n" +
                                "Content-Type: text/csv; charset=utf-8\r\n" +
                                "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                "Keep-Alive: timeout=5, max=10000\r\n" +
                                "\r\n" +
                                "17\r\n" +
                                "\"foo\"\r\n" +
                                "\"foo\\\\foo\uD83D\uDC27\"\r\n" +
                                "\r\n" +
                                "00\r\n" +
                                "\r\n",
                        1,
                        0,
                        false
                ));
    }

    @Test
    public void testTextQueryCreateTable() throws Exception {
        testJsonQuery(
                20,
                "GET /exec?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Accept: */*\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0c\r\n" +
                        "{\"ddl\":\"OK\"}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
        );
    }

    @Test
    public void testTextQueryGeoHashColumnChars() throws Exception {
        testHttpQueryGeoHashColumnChars(
                "\"geo1\",\"geo2\",\"geo4\",\"geo8\",\"geo01\"\r\n" +
                        "null,null,\"questd\",\"u10m99dd3pbj\",\"1\"\r\n" +
                        "\"u\",\"u10\",\"questd\",null,\"1\"\r\n" +
                        "\"q\",\"u10\",\"questd\",\"questdb12345\",\"1\"\r\n",
                "/exp"
        );
    }

    @Test
    public void testTextQueryImplicitCastExceptionInWindowFunctionFirstRecord() throws Exception {
        getSimpleTester()
                .run((engine, sqlExecutionContext) -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute(
                                "CREATE TABLE 'trades' ( " +
                                        " symbol SYMBOL, " +
                                        " side SYMBOL, " +
                                        " price DOUBLE, " +
                                        " amount DOUBLE, " +
                                        " timestamp TIMESTAMP " +
                                        ") timestamp(timestamp) PARTITION BY DAY;",
                                executionContext
                        );
                        engine.execute(
                                "INSERT INTO trades VALUES ('ETH-USD', 'sell', 2615.54, 0.00044, '2022-03-08T18:03:57.609765Z');",
                                executionContext
                        );

                        testHttpClient.setKeepConnection(true);
                        testHttpClient.assertGet(
                                "/exp",
                                "{\"query\":\"SELECT timestamp, price, lag('timestamp') OVER (ORDER BY timestamp) AS previous_price FROM trades LIMIT 10;\",\"error\":\"inconvertible value: `timestamp` [STRING -> DOUBLE]\",\"position\":0}",
                                "SELECT timestamp, price, lag('timestamp') OVER (ORDER BY timestamp) AS previous_price FROM trades LIMIT 10;"
                        );

                        // verify that HTTP server is healthy, use the same connection
                        testHttpClient.setKeepConnection(false);
                        testHttpClient.assertGet(
                                "/exp",
                                "\"count\"\r\n" +
                                        "1\r\n",
                                "SELECT count() FROM trades;"
                        );
                    }
                });
    }

    @Test
    public void testTextQueryInsertViaWrongEndpoint() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                            sendAndReceive(
                                    NetworkFacadeImpl.INSTANCE,
                                    "GET /exec?query=create%20table%20tab%20(x%20int) HTTP/1.1\r\n" +
                                            "Host: localhost:9000\r\n" +
                                            "Connection: keep-alive\r\n" +
                                            "Accept: */*\r\n" +
                                            "X-Requested-With: XMLHttpRequest\r\n" +
                                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                            "Sec-Fetch-Site: same-origin\r\n" +
                                            "Sec-Fetch-Mode: cors\r\n" +
                                            "Referer: http://localhost:9000/index.html\r\n" +
                                            "Accept-Encoding: gzip, deflate, br\r\n" +
                                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                            "\r\n",
                                    "HTTP/1.1 200 OK\r\n" +
                                            "Server: questDB/1.0\r\n" +
                                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                            "Transfer-Encoding: chunked\r\n" +
                                            "Content-Type: application/json; charset=utf-8\r\n" +
                                            "Keep-Alive: timeout=5, max=10000\r\n" +
                                            "\r\n" +
                                            JSON_DDL_RESPONSE,
                                    1,
                                    0,
                                    false
                            );

                            sendAndReceive(
                                    NetworkFacadeImpl.INSTANCE,
                                    "GET /exp?query=insert%20into%20tab%20value%20(1) HTTP/1.1\r\n" +
                                            "Host: localhost:9000\r\n" +
                                            "Connection: keep-alive\r\n" +
                                            "Accept: */*\r\n" +
                                            "X-Requested-With: XMLHttpRequest\r\n" +
                                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                            "Sec-Fetch-Site: same-origin\r\n" +
                                            "Sec-Fetch-Mode: cors\r\n" +
                                            "Referer: http://localhost:9000/index.html\r\n" +
                                            "Accept-Encoding: gzip, deflate, br\r\n" +
                                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                            "\r\n",
                                    "HTTP/1.1 400 Bad request\r\n" +
                                            "Server: questDB/1.0\r\n" +
                                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                            "Transfer-Encoding: chunked\r\n" +
                                            "Content-Type: application/json; charset=utf-8\r\n" +
                                            "Keep-Alive: timeout=5, max=10000\r\n" +
                                            "\r\n" +
                                            "76\r\n" +
                                            "{\"query\":\"insert into tab value (1)\",\"error\":\"found [tok='value', len=5] 'select' or 'values' expected\",\"position\":16}\r\n" +
                                            "00\r\n" +
                                            "\r\n",
                                    1,
                                    0,
                                    false
                            );
                        }
                );
    }

    @Test
    public void testTextQueryPseudoRandomStability() throws Exception {
        testJsonQuery(
                20,
                "GET /exp?query=select+rnd_symbol(%27a%27%2C%27b%27%2C%27c%27)+sym+from+long_sequence(10%2C+33%2C+55)&limit=0%2C1000&count=true&src=con HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "39\r\n" +
                        "\"sym\"\r\n" +
                        "\"c\"\r\n" +
                        "\"c\"\r\n" +
                        "\"c\"\r\n" +
                        "\"b\"\r\n" +
                        "\"b\"\r\n" +
                        "\"a\"\r\n" +
                        "\"a\"\r\n" +
                        "\"a\"\r\n" +
                        "\"a\"\r\n" +
                        "\"a\"\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testTextQueryResponseLimit() throws Exception {
        configuredMaxQueryResponseRowLimit = 3;
        testJsonQuery(
                20,
                "GET /exp?query=select+rnd_symbol(%27a%27%2C%27b%27%2C%27c%27)+sym+from+long_sequence(10%2C+33%2C+55)&limit=0%2C1000&count=true&src=con HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "16\r\n" +
                        "\"sym\"\r\n" +
                        "\"c\"\r\n" +
                        "\"c\"\r\n" +
                        "\"c\"\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testTextQueryShowColumnsOnDroppedTable() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run((engine, sqlExecutionContext) -> {
                    final String createTableDdl = "create table balances(cust_id int, ccy symbol, balance double)";
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(createTableDdl) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    final String showColumnsQuery = "show columns from balances";
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(showColumnsQuery) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "0242\r\n" +
                                    "{\"query\":\"show columns from balances\",\"columns\":[{\"name\":\"column\",\"type\":\"STRING\"},{\"name\":\"type\",\"type\":\"STRING\"},{\"name\":\"indexed\",\"type\":\"BOOLEAN\"},{\"name\":\"indexBlockCapacity\",\"type\":\"INT\"},{\"name\":\"symbolCached\",\"type\":\"BOOLEAN\"},{\"name\":\"symbolCapacity\",\"type\":\"INT\"},{\"name\":\"symbolTableSize\",\"type\":\"INT\"},{\"name\":\"designated\",\"type\":\"BOOLEAN\"},{\"name\":\"upsertKey\",\"type\":\"BOOLEAN\"}],\"timestamp\":-1,\"dataset\":[[\"cust_id\",\"INT\",false,0,false,0,0,false,false],[\"ccy\",\"SYMBOL\",false,256,true,128,0,false,false],[\"balance\",\"DOUBLE\",false,0,false,0,0,false,false]],\"count\":3}\r\n" +
                                    "00\r\n\r\n",
                            1,
                            0,
                            false
                    );

                    final String dropTableDdl = "drop table balances";
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(dropTableDdl) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    JSON_DDL_RESPONSE,
                            1,
                            0,
                            false
                    );

                    // We should get a meaningful error.
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            "GET /query?query=" + urlEncodeQuery(showColumnsQuery) + "&count=true HTTP/1.1\r\n" +
                                    "Host: localhost:9000\r\n" +
                                    "Connection: keep-alive\r\n" +
                                    "Accept: */*\r\n" +
                                    "X-Requested-With: XMLHttpRequest\r\n" +
                                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                                    "Sec-Fetch-Site: same-origin\r\n" +
                                    "Sec-Fetch-Mode: cors\r\n" +
                                    "Referer: http://localhost:9000/index.html\r\n" +
                                    "Accept-Encoding: gzip, deflate, br\r\n" +
                                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                                    "\r\n",
                            "HTTP/1.1 400 Bad request\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: application/json; charset=utf-8\r\n" +
                                    "Keep-Alive: timeout=5, max=10000\r\n" +
                                    "\r\n" +
                                    "64\r\n" +
                                    "{\"query\":\"show columns from balances\",\"error\":\"table does not exist [table=balances]\",\"position\":18}\r\n" +
                                    "00\r\n\r\n",
                            1,
                            0,
                            false
                    );
                });
    }

    /**
     * Cold storage may lead to the initiation of suspend events when data is inaccessible to the local database instance.
     * This disruption affects both the state machine's flow and the factory's data provision process. This test
     * replicates a suspend event, comparing the query output after resumption with the output of a query that
     * hasn't been suspended.
     */
    @Test
    public void testTextQuerySuspend() throws Exception {
        testSuspend("/exp");
    }

    @Test
    public void testTextQueryTimeout() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withQueryTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK)
                .run((engine, sqlExecutionContext) -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute(QUERY_TIMEOUT_TABLE_DDL, executionContext);
                        testHttpClient.assertGetRegexp(
                                "/exp",
                                "\\{\"query\":\"select i, avg\\(l\\), max\\(l\\) from t group by i order by i asc limit 3\",\"error\":\"\\[-1\\] timeout, query aborted \\[fd=\\d+, runtime=\\d+ms, timeout=-100ms\\]\",\"position\":0\\}",
                                QUERY_TIMEOUT_SELECT,
                                null, null, null, null,
                                "400"
                        );
                    }
                });
    }

    @Test
    public void testTextQueryTimeoutResetOnEachQuery() throws Exception {
        final int timeout = 200;
        final int iterations = 3;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withQueryTimeout(timeout)
                .run((engine, sqlExecutionContext) -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute(QUERY_TIMEOUT_TABLE_DDL, executionContext);
                        for (int i = 0; i < iterations; i++) {
                            new SendAndReceiveRequestBuilder().executeWithStandardRequestHeaders(
                                    "GET /exp?query=" + urlEncodeQuery(QUERY_TIMEOUT_SELECT) + "&count=true HTTP/1.1\r\n",
                                    "HTTP/1.1 200 OK\r\n" +
                                            "Server: questDB/1.0\r\n" +
                                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                            "Transfer-Encoding: chunked\r\n" +
                                            "Content-Type: text/csv; charset=utf-8\r\n" +
                                            "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                            "Keep-Alive: timeout=5, max=10000\r\n" +
                                            "\r\n" +
                                            "33\r\n" +
                                            "\"i\",\"avg\",\"max\"\r\n" +
                                            "0,55.0,100\r\n" +
                                            "1,46.0,91\r\n" +
                                            "2,47.0,92\r\n" +
                                            "\r\n" +
                                            "00\r\n" +
                                            "\r\n"
                            );
                            if (i != iterations - 1) {
                                Os.sleep(timeout);
                            }
                        }
                    }
                });
    }

    @Test
    public void testTextQueryUuid() throws Exception {
        testJsonQuery(
                10,
                "GET /exp?query=SELECT+*+FROM+x HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "06c1\r\n" +
                        "\"a\",\"b\",\"c\",\"d\",\"e\",\"f\",\"g\",\"h\",\"i\",\"j\",\"k\",\"l\",\"m\",\"n\"\r\n" +
                        "80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",,,\"EHNRX\",\"ZSX\",false,,c2593f82-b430-328d-84a0-9f29df637e38,\"}龘и\uDA89\uDFA4~\"\r\n" +
                        "53,5639,-1162267908,6993925225312419449,\"195808098-05-09T04:14:54.347Z\",\"171005-04-19T09:31:35.433003Z\",0.24593449,0.29313719347837397,\"BVTMH\",,false,,d364c241-dde2-cf90-a7a8-f4e549997e46,\"\uE961-\\\\篸{\"\r\n" +
                        "40,-8761,,-7995393784734742820,\"275774022-08-09T21:28:04.485Z\",\"-264492-10-11T03:17:51.666853Z\",,0.5065228336156442,\"LNVTI\",,false,,a011214b-ad88-8a69-9502-128cda0887fe,\r\n" +
                        "-31,4215,1362833895,,\"-49144476-01-15T02:33:12.980Z\",,0.26369333,0.7632615004324503,\"LHMLL\",\"OYP\",false,,b92d0771-d782-63eb-5479-ae0482582ad0,\"! Yc0\"\r\n" +
                        "-80,-23575,,5552835357100545895,\"-229044588-12-31T09:43:18.056Z\",,,,\"GLUOH\",\"ZHZ\",false,,8b1134e2-9413-4389-a2cb-c77b1cdd7786,\"1\uD97C\uDD2B珣zx\"\r\n" +
                        "119,-2044,-2043541236,-4547802916868961458,\"-281648402-09-21T10:33:06.955Z\",\"221810-02-23T20:19:19.020303Z\",0.56910527,,\"WIFFL\",\"BRO\",false,,09359765-2ae7-2c5c-14ef-c23546571bdc,\"\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5\"\r\n" +
                        "16,30964,-1520181263,-7212878484370155026,,\"280770-04-22T16:59:28.938593Z\",0.34608507,0.5780819331422455,\"UQDYO\",,false,,a579cf90-ccdf-133e-86be-020b55a15fd1,\r\n" +
                        "72,27348,-647653731,8737613628813682249,,,0.0024457574,0.19736767249829557,\"CBDMI\",\"QZV\",true,,,\"\uDB4F\uDC7Dl⤃堝ᢣ\"\r\n" +
                        "36,22350,,,\"77319557-11-14T08:22:42.686Z\",\"104977-04-08T13:34:21.431788Z\",0.112962544,0.9934423708117267,\"FNWGR\",\"DGG\",false,,899850f1-14ad-249d-97af-847507d07b51,\"d<J1n\"\r\n" +
                        "59,-13676,-1529726228,4092568845903588572,,\"31470-11-18T18:43:57.264562Z\",0.3480476,0.48782086416459025,\"KYFLU\",\"ZQS\",true,,c48ad6b8-f696-2219-b27b-0ac7fbdee201,\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testTextQueryVarchar() throws Exception {
        testJsonQuery(
                10,
                "GET /exp?query=SELECT+n+as+varchar+FROM+x HTTP/1.1\r\n" +
                        "Host: localhost:9000\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: */*\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "89\r\n" +
                        "\"varchar\"\r\n" +
                        "\"}龘и\uDA89\uDFA4~\"\r\n" +
                        "\"\uE961-\\\\篸{\"\r\n" +
                        "\r\n" +
                        "\"! Yc0\"\r\n" +
                        "\"1\uD97C\uDD2B珣zx\"\r\n" +
                        "\"\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5\"\r\n" +
                        "\r\n" +
                        "\"\uDB4F\uDC7Dl⤃堝ᢣ\"\r\n" +
                        "\"d<J1n\"\r\n" +
                        "\"\uD9E2\uDC2C\uD93B\uDD81*\uDBAE\uDF56飃\"\r\n" +
                        "\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testTextTableReferenceOutOfDate() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "/exp",
                "{\"query\":\"select * from test_table_reference_out_of_date();\",\"error\":\"cached query plan cannot be used because table schema has changed [table=test_table_reference_out_of_date]\",\"position\":0}",
                "select * from test_table_reference_out_of_date();"
        ));
    }

    @Test
    public void testTimingsContainsAuthentication() throws Exception {
        nanosecondClock = StationaryNanosClock.INSTANCE;
        testJsonQuery(
                10,
                "GET /query?query=x%20where%20i%20%3D%20%27A%27&timings=true HTTP/1.1\r\n" +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Cache-Control: max-age=0\r\n" +
                        "Upgrade-Insecure-Requests: 1\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n" +
                        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n" +
                        "Accept-Encoding: gzip, deflate, br\r\n" +
                        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                        "\r\n",
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "021d\r\n" +
                        "{\"query\":\"x where i = 'A'\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"},{\"name\":\"m\",\"type\":\"UUID\"},{\"name\":\"n\",\"type\":\"VARCHAR\"}],\"timestamp\":-1,\"dataset\":[],\"count\":0,\"timings\":{\"authentication\":0,\"compiler\":0,\""
        );
    }

    @Test
    public void testTriggerInternalCairoError() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet(
                    "{" +
                            "\"query\":\"select simulate_crash('E')\"," +
                            "\"columns\":[{\"name\":\"simulate_crash\",\"type\":\"BOOLEAN\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[]]," +
                            "\"count\":1," +
                            "\"error\":\"simulated cairo error\", " +
                            "\"errorPos\":0" +
                            "}",
                    "select simulate_crash('E')"
            );
            Assert.assertEquals(1, engine.getMetrics().healthMetrics().unhandledErrorsCount());
        });
    }

    @Test
    public void testTriggerInternalCriticalCairoException() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet(
                    "{" +
                            "\"query\":\"select simulate_crash('0')\"," +
                            "\"columns\":[{\"name\":\"simulate_crash\",\"type\":\"BOOLEAN\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[]]," +
                            "\"count\":1," +
                            "\"error\":\"simulated cairo exception\", " +
                            "\"errorPos\":222" +
                            "}",
                    "select simulate_crash('0')"
            );
            Assert.assertEquals(0, engine.getMetrics().healthMetrics().unhandledErrorsCount());
        });
    }

    @Test
    public void testTriggerNPE() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet(
                    "{" +
                            "\"query\":\"select npe()\"," +
                            "\"columns\":[{\"name\":\"npe\",\"type\":\"BOOLEAN\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[[]]," +
                            "\"count\":1," +
                            "\"error\":\"Internal server error\", " +
                            "\"errorPos\":0" +
                            "}",
                    "select npe()"
            );
            Assert.assertEquals(1, engine.getMetrics().healthMetrics().unhandledErrorsCount());
        });
    }

    @SuppressWarnings("resource")
    @Test
    public void testTwoThreadsSendTwoThreadsRead() throws Exception {
        LOG.info().$("started testTwoThreadsSendTwoThreadsRead").$();

        final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        // the difference between request and expected is url encoding (and ':' padding, which can easily be fixed)
        final String expected = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host:localhost:9000\r\n" +
                "Connection:keep-alive\r\n" +
                "Cache-Control:max-age=0\r\n" +
                "Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding:gzip,deflate,sdch\r\n" +
                "Accept-Language:en-US,en;q=0.8\r\n" +
                "Cookie:textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        final int N = 100;
        final int serverThreadCount = 2;
        final int senderCount = 2;

        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(configuration);

            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final AtomicInteger requestsReceived = new AtomicInteger();
            final AtomicBoolean finished = new AtomicBoolean(false);
            final SOCountDownLatch senderHalt = new SOCountDownLatch(senderCount);
            try (
                    IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                            new DefaultIODispatcherConfiguration() {
                                @Override
                                public boolean getPeerNoLinger() {
                                    return true;
                                }
                            },
                            fd -> new HttpConnectionContext(httpServerConfiguration, PlainSocketFactory.INSTANCE).of(fd)
                    );
                    final RingQueue<Status> queue = new RingQueue<>(Status::new, 1024)
            ) {
                // server will publish status of each request to this queue
                final MPSequence pubSeq = new MPSequence(queue.getCycle());
                SCSequence subSeq = new SCSequence();
                pubSeq.then(subSeq).then(pubSeq);

                final AtomicBoolean serverRunning = new AtomicBoolean(true);
                final SOCountDownLatch serverHaltLatch = new SOCountDownLatch(serverThreadCount);

                try {
                    for (int j = 0; j < serverThreadCount; j++) {
                        new Thread(() -> {
                            final StringSink sink = new StringSink();
                            final long responseBuf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                            Unsafe.getUnsafe().putByte(responseBuf, (byte) 'A');

                            final HttpRequestProcessor processor = new HttpRequestProcessor() {
                                @Override
                                public void onHeadersReady(HttpConnectionContext context) {
                                    HttpRequestHeader headers = context.getRequestHeader();
                                    sink.clear();
                                    sink.put(headers.getMethodLine());
                                    sink.put("\r\n");
                                    ObjList<? extends Utf8Sequence> headerNames = headers.getHeaderNames();
                                    for (int i = 0, n = headerNames.size(); i < n; i++) {
                                        sink.put(headerNames.getQuick(i)).put(':');
                                        sink.put(headers.getHeader(headerNames.getQuick(i)));
                                        sink.put("\r\n");
                                    }
                                    sink.put("\r\n");

                                    boolean result;
                                    try {
                                        TestUtils.assertEquals(expected, sink);
                                        result = true;
                                    } catch (Exception e) {
                                        result = false;
                                    }

                                    while (true) {
                                        long cursor = pubSeq.next();
                                        if (cursor < 0) {
                                            continue;
                                        }
                                        queue.get(cursor).valid = result;
                                        pubSeq.done(cursor);
                                        break;
                                    }

                                    requestsReceived.incrementAndGet();

                                    nf.sendRaw(context.getFd(), responseBuf, 1);
                                }
                            };

                            try (HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                                @Override
                                public void close() {
                                }

                                @Override
                                public HttpRequestProcessor getDefaultProcessor() {
                                    return processor;
                                }

                                @Override
                                public HttpRequestProcessor select(HttpRequestHeader requestHeader) {
                                    return null;
                                }
                            }) {

                                try {
                                    while (serverRunning.get()) {
                                        dispatcher.run(0);
                                        dispatcher.processIOQueue(
                                                (operation, context, dispatcher1) -> handleClientOperation(context, operation, selector, dispatcher1)
                                        );
                                    }
                                } finally {
                                    Unsafe.free(responseBuf, 32, MemoryTag.NATIVE_DEFAULT);
                                    serverHaltLatch.countDown();
                                }
                            }
                        }).start();
                    }

                    AtomicInteger completedCount = new AtomicInteger();
                    for (int j = 0; j < senderCount; j++) {
                        int k = j;
                        new Thread(() -> {
                            long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                            try {
                                for (int i = 0; i < N && !finished.get(); i++) {
                                    long fd = Net.socketTcp(true);
                                    try {
                                        TestUtils.assertConnect(fd, sockAddr);
                                        int len = request.length();
                                        long buffer = TestUtils.toMemory(request);
                                        try {
                                            Assert.assertEquals(len, Net.send(fd, buffer, len));
                                            Assert.assertEquals("fd=" + fd + ", i=" + i, 1, Net.recv(fd, buffer, 1));
                                            LOG.info().$("i=").$(i).$(", j=").$(k).$();
                                            Assert.assertEquals('A', Unsafe.getUnsafe().getByte(buffer));
                                        } finally {
                                            Unsafe.free(buffer, len, MemoryTag.NATIVE_DEFAULT);
                                        }
                                    } finally {
                                        Net.close(fd);
                                    }
                                }
                            } finally {
                                completedCount.incrementAndGet();
                                Net.freeSockAddr(sockAddr);
                                senderHalt.countDown();
                            }
                        }).start();
                    }

                    int receiveCount = 0;
                    int failureRetryCount = 0;
                    while (receiveCount < N * senderCount) {
                        long cursor = subSeq.next();
                        if (cursor < 0) {
                            if (cursor == -1 && completedCount.get() == senderCount) {
                                failureRetryCount++;
                                if (failureRetryCount >= 3) {
                                    Assert.fail("Not all requests successful, test failed, see previous failures");
                                    break;
                                }
                            }
                            Os.pause();
                            continue;
                        }
                        failureRetryCount = 0;
                        boolean valid = queue.get(cursor).valid;
                        subSeq.done(cursor);
                        assertTrue(valid);
                        receiveCount++;
                    }
                } finally {
                    serverRunning.set(false);
                    serverHaltLatch.await();
                }
            } catch (Throwable e) {
                LOG.critical().$(e).$();
                throw e;
            } finally {
                finished.set(true);
                senderHalt.await();
            }
            Assert.assertEquals(N * senderCount, requestsReceived.get());
        });
    }

    @Test
    public void testUpdateCommandRunningInWALCantBeCancelled() throws Exception {
        final String url = "/query";
        final long TIMEOUT = 240_000;

        SOCountDownLatch started = new SOCountDownLatch(1);
        SOCountDownLatch stopped = new SOCountDownLatch(1);
        AtomicReference<Throwable> queryError = new AtomicReference<>();

        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                    DelayedWALListener registryListener = new DelayedWALListener();
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.getQueryRegistry().setListener(registryListener);

                        engine.execute("create table tab (b boolean, ts timestamp, sym symbol) timestamp(ts) partition by DAY WAL", executionContext);
                        engine.execute("insert into tab select true, (86400000000*x)::timestamp, null from long_sequence(1000)", executionContext);
                        drainWalQueue(engine);

                        final String command = "update tab set b=false";

                        started.setCount(2);
                        stopped.setCount(2);
                        queryError.set(null);

                        registryListener.queryText = command;
                        registryListener.queryFound.setCount(1);

                        new Thread(
                                () -> {
                                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                        started.countDown();
                                        try {
                                            testHttpClient.assertGetRegexp(url, ".*(\"dml\":\"OK\").*", command, null, null, null);
                                        } catch (Throwable e) {
                                            queryError.set(e);
                                        }
                                    } finally {
                                        stopped.countDown();
                                    }
                                }, "command_thread"
                        ).start();

                        Thread walJob = new Thread(
                                () -> {
                                    started.countDown();

                                    try (ApplyWal2TableJob walApplyJob = createWalApplyJob(engine)) {
                                        while (queryError.get() == null) {
                                            walApplyJob.drain(0);
                                            new CheckWalTransactionsJob(engine).run(0);
                                            // run once again as there might be notifications to handle now
                                            walApplyJob.drain(0);
                                        }
                                    } finally {
                                        // release native path memory used by the job
                                        Path.clearThreadLocals();
                                        stopped.countDown();
                                    }
                                }, "wal_job"
                        );
                        walJob.start();

                        started.await();

                        long queryId;
                        long start = System.currentTimeMillis();

                        //wait until query appears in registry and get query id
                        try (RecordCursorFactory factory = engine.select("select query_id from query_activity() where is_wal = true and query = '" + command + "'", executionContext)) {
                            while (true) {
                                try (RecordCursor cursor = factory.getCursor(executionContext)) {
                                    if (cursor.hasNext()) {
                                        queryId = cursor.getRecord().getLong(0);
                                        break;
                                    }
                                }
                                Os.sleep(1);
                                if (System.currentTimeMillis() - start > TIMEOUT) {
                                    throw new RuntimeException("Timed out waiting for command to appear in registry: " + command);
                                }
                                if (queryError.get() != null) {
                                    throw new RuntimeException("Query failed!", queryError.get());
                                }
                            }
                        }

                        testHttpClient.assertGetRegexp(
                                "/query",
                                ".*(query applied in WAL job can't be cancelled).*",
                                "cancel query " + queryId,
                                null, null,
                                "200"
                        );

                        registryListener.queryFound.countDown();
                        queryError.set(new Exception());//stop wal thread
                        stopped.await();

                        StringSink sink = new StringSink();
                        TestUtils.assertSql(
                                engine,
                                executionContext,
                                "select count(*) from tab where b=false",
                                sink,
                                "count\n1000\n"
                        );

                    } finally {
                        engine.getQueryRegistry().setListener(null);
                    }
                });
    }

    @Test
    public void testUpdateCommandRunningInWALDoesntTimeOut() throws Exception {
        assertMemoryLeak(() -> {
            final long TIMEOUT = 240_000;

            SOCountDownLatch started = new SOCountDownLatch(1);
            SOCountDownLatch stopped = new SOCountDownLatch(1);
            AtomicReference<Throwable> queryError = new AtomicReference<>();

            CairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                    return new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        public long getQueryTimeout() {
                            return 1;
                        }
                    };
                }
            };
            DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withNetwork(NetworkFacadeImpl.INSTANCE)
                    .withBaseDir(root)
                    .withSendBufferSize(256)
                    .withDumpingTraffic(false)
                    .withAllowDeflateBeforeSend(false)
                    .withServerKeepAlive(true)
                    .withHttpProtocolVersion("HTTP/1.1 ")
                    .build(cairoConfiguration);

            WorkerPool workerPool = new TestWorkerPool(1);

            try (CairoEngine engine = new CairoEngine(cairoConfiguration);
                 HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE);
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                httpServer.bind(new StaticContentProcessorFactory(httpConfiguration));
                httpServer.bind(new TestJsonQueryProcessorFactory(engine, httpConfiguration, workerPool.getWorkerCount()));

                WorkerPoolUtils.setupQueryJobs(workerPool, engine);
                workerPool.start(LOG);

                try {
                    DelayedWALListener registryListener = new DelayedWALListener();
                    engine.getQueryRegistry().setListener(registryListener);

                    String ddl = "create table tab (b boolean, ts timestamp, sym symbol) timestamp(ts) partition by DAY WAL";
                    final String command = "update tab set b=false where b=true and sleep(1)";

                    engine.execute(ddl, executionContext);
                    engine.execute("insert into tab select true, (864000000*x)::timestamp, null from long_sequence(3000)", executionContext);
                    drainWalQueue(engine);

                    started.setCount(2);
                    stopped.setCount(2);
                    queryError.set(null);

                    registryListener.queryText = command;
                    registryListener.queryFound.setCount(1);

                    new Thread(
                            () -> {
                                try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                    started.countDown();
                                    try {
                                        testHttpClient.assertGetRegexp("/query", ".*(\"dml\":\"OK\").*", command, null, null, null);
                                    } catch (Throwable e) {
                                        queryError.set(e);
                                    }
                                } finally {
                                    stopped.countDown();
                                }
                            }, "command_thread"
                    ).start();

                    Thread walJob = new Thread(
                            () -> {
                                started.countDown();

                                try (ApplyWal2TableJob walApplyJob = createWalApplyJob(engine)) {
                                    while (queryError.get() == null) {
                                        walApplyJob.drain(0);
                                        new CheckWalTransactionsJob(engine).run(0);
                                        // run once again as there might be notifications to handle now
                                        walApplyJob.drain(0);
                                    }
                                } finally {
                                    // release native path memory used by the job
                                    Path.clearThreadLocals();
                                    stopped.countDown();
                                }
                            }, "wal_job"
                    );
                    walJob.start();

                    started.await();

                    long queryId;
                    long start = System.currentTimeMillis();

                    //wait until query appears in registry and get query id
                    try {
                        try (RecordCursorFactory factory = engine.select("select query_id from query_activity() where is_wal = true and query = '" + command.replace("'", "''") + "'", executionContext)) {
                            while (true) {
                                try (RecordCursor cursor = factory.getCursor(executionContext)) {
                                    if (cursor.hasNext()) {
                                        queryId = cursor.getRecord().getLong(0);
                                        break;
                                    }
                                }
                                Os.sleep(1);
                                if (System.currentTimeMillis() - start > TIMEOUT) {
                                    throw new RuntimeException("Timed out waiting for command to appear in registry: " + command);
                                }
                                if (queryError.get() != null) {
                                    throw new RuntimeException("Query failed!", queryError.get());
                                }
                            }
                        }
                    } finally {
                        registryListener.queryFound.countDown();
                    }

                    //wait until query finishes
                    while (engine.getQueryRegistry().getEntry(queryId) != null) {
                        Os.sleep(1);
                    }
                } finally {
                    queryError.set(new Exception());//stop wal thread
                    stopped.await();
                    workerPool.halt();
                }
            }

            // run query in separate engine so it doesn't time out
            try (CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(root));
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                StringSink sink = new StringSink();
                TestUtils.assertSql(
                        engine,
                        executionContext,
                        "select count(*) from tab where b=false",
                        sink,
                        "count\n3000\n"
                );
            }

        });
    }

    @Test
    public void testUpdateO3MaxLagAndMaxUncommittedRowsIsIgnoredIfPartitionByIsNONE() throws Exception {
        importWithO3MaxLagAndMaxUncommittedRowsTableExists(
                true,
                false,
                PartitionBy.NONE,
                180_000_000,
                1,
                300000000,
                1000
        );
    }

    @Test
    public void testUpdateO3MaxLagAndMaxUncommittedRowsIsIgnoredIfValuesAreSmallerThanZero() throws Exception {
        importWithO3MaxLagAndMaxUncommittedRowsTableExists(
                true,
                true,
                PartitionBy.DAY,
                -1,
                -1,
                300000000,
                1000
        );
    }

    private static void assertDownloadResponse(
            long fd,
            Rnd rnd,
            long buffer,
            int len,
            String expectedResponseHeader
    ) {
        int expectedHeaderLen = expectedResponseHeader.length();
        int headerCheckRemaining = expectedResponseHeader.length();
        long downloadedSoFar = 0;
        int contentRemaining = 0;
        while (downloadedSoFar < 20971667) {
            int contentOffset = 0;
            int n = Net.recv(fd, buffer, len);
            assertTrue(n > -1);
            if (n > 0) {
                if (headerCheckRemaining > 0) {
                    for (int i = 0; i < n && headerCheckRemaining > 0; i++) {
                        if (expectedResponseHeader.charAt(expectedHeaderLen - headerCheckRemaining) != (char) Unsafe.getUnsafe().getByte(buffer + i)) {
                            Assert.fail("at " + (expectedHeaderLen - headerCheckRemaining));
                        }
                        headerCheckRemaining--;
                        contentOffset++;
                    }
                }

                if (headerCheckRemaining == 0) {
                    for (int i = contentOffset; i < n; i++) {
                        if (contentRemaining == 0) {
                            contentRemaining = 1048576;
                            rnd.reset();
                        }
                        Assert.assertEquals(rnd.nextByte(), Unsafe.getUnsafe().getByte(buffer + i));
                        contentRemaining--;
                    }

                }
                downloadedSoFar += n;
            }
        }
    }

    @NotNull
    private static Thread createDelayThread(AtomicBoolean stopDelayThread, AtomicReference<SuspendEvent> eventRef, AtomicInteger totalEvents) {
        final Thread delayThread = new Thread(() -> {
            while (!stopDelayThread.get()) {
                SuspendEvent event = eventRef.getAndSet(null);
                if (event != null) {
                    Os.sleep(1);
                    try {
                        event.trigger();
                        event.close();
                        totalEvents.incrementAndGet();
                    } catch (Exception e) {
                        LOG.critical().$(e).$();
                    }
                } else {
                    Os.pause();
                }
            }
        });
        delayThread.start();
        return delayThread;
    }


    private static HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPool workerPool
    ) {
        return Services.INSTANCE.createHttpServer(
                serverConfiguration,
                cairoEngine,
                workerPool,
                workerPool.getWorkerCount()
        );
    }

    private static void createTableX(CairoEngine engine, int n) {
        CreateTableTestUtils.createTestTable(
                engine,
                n,
                new Rnd(),
                new TestRecord.ArrayBinarySequence()
        );
    }

    private static void createTestTable(CairoEngine engine) {
        TableModel model = new TableModel(engine.getConfiguration(), "y", PartitionBy.NONE);
        model.col("j", ColumnType.SYMBOL);
        TestUtils.createTable(engine, model);

        try (TableWriter writer = TestUtils.newOffPoolWriter(engine.getConfiguration(), engine.verifyTableName("y"), engine)) {
            for (int i = 0; i < 20; i++) {
                TableWriter.Row row = writer.newRow();
                row.putSym(0, "ok\0ok");
                row.append();
            }
            writer.commit();
        }
    }

    private static void sendAndReceive(String request, CharSequence response) {
        sendAndReceive(
                NetworkFacadeImpl.INSTANCE,
                request,
                response,
                1,
                0,
                false
        );
    }

    private static void sendAndReceive(
            NetworkFacade nf,
            String request,
            CharSequence response,
            int requestCount,
            long pauseBetweenSendAndReceive,
            boolean print
    ) {
        sendAndReceive(
                nf,
                request,
                response,
                requestCount,
                pauseBetweenSendAndReceive,
                print,
                false
        );
    }

    private static void sendAndReceive(
            NetworkFacade nf,
            String request,
            CharSequence response,
            int requestCount,
            long pauseBetweenSendAndReceive,
            boolean print,
            boolean expectReceiveDisconnect
    ) {
        new SendAndReceiveRequestBuilder()
                .withNetworkFacade(nf)
                .withExpectReceiveDisconnect(expectReceiveDisconnect)
                .withPrintOnly(print)
                .withRequestCount(requestCount)
                .withPauseBetweenSendAndReceive(pauseBetweenSendAndReceive)
                .execute(request, response);
    }

    private static void sendRequest(long fd, long buffer) {
        final String request = "GET /questdb-temp.txt HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";
        final int requestLen = request.length();
        Utf8s.strCpyAscii(request, requestLen, buffer);
        Assert.assertEquals(requestLen, Net.send(fd, buffer, requestLen));
    }

    private static void testSuspend(String url) throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    Utf8StringSink expected = new Utf8StringSink();
                    Utf8StringSink actual = new Utf8StringSink();
                    final TestCases testCases = new TestCases();

                    // create tables
                    testHttpClient.assertGet("{\"ddl\":\"OK\"}", testCases.getDdlX());
                    testHttpClient.assertGet("{\"ddl\":\"OK\"}", testCases.getDdlY());

                    for (int i = 0, n = testCases.size(); i < n; i++) {
                        TestCase testCase = testCases.getQuick(i);
                        // http does not support bind variables yet
                        if (testCase.getBindVariableValues().length == 0) {
                            engine.releaseAllReaders();
                            engine.setReaderListener(null);

                            expected.clear();
                            testHttpClient.toSink(url, testCase.getQuery(), expected);

                            engine.releaseAllReaders();
                            engine.setReaderListener(testCases.getSuspendingListener());

                            actual.clear();
                            testHttpClient.toSink(url, testCase.getQuery(), actual);
                            TestUtils.assertEquals(expected, actual);
                        }
                    }
                });
    }

    private void assertMetadataAndData(
            String tableName,
            long expectedO3MaxLag,
            int expectedMaxUncommittedRows,
            int expectedImportedRows,
            String expectedData,
            boolean mangleTableDirNames
    ) {
        final String baseDir = root;
        DefaultCairoConfiguration configuration = new DefaultTestCairoConfiguration(baseDir);

        String dirName = TableUtils.getTableDir(mangleTableDirNames, tableName, 1, false);
        TableToken tableToken = new TableToken(tableName, dirName, null, 1, false, false, false);
        try (
                TableReader reader = new TableReader(OFF_POOL_READER_ID.getAndIncrement(), configuration, tableToken, TxnScoreboardPoolFactory.createPool(configuration));
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()
        ) {
            cursor.of(reader);
            Assert.assertEquals(expectedO3MaxLag, reader.getO3MaxLag());
            Assert.assertEquals(expectedMaxUncommittedRows, reader.getMaxUncommittedRows());
            Assert.assertEquals(expectedImportedRows, reader.size());
            Assert.assertEquals(0, expectedImportedRows - reader.size());
            StringSink sink = new StringSink();
            TestUtils.assertCursor(expectedData, cursor, reader.getMetadata(), false, sink);
        }
    }

    private void assertTelemetryEventAndOrigin(CharSequence expected) {
        final String baseDir = root;
        DefaultCairoConfiguration configuration = new DefaultTestCairoConfiguration(baseDir);

        String telemetry = TelemetryTask.TABLE_NAME;
        TableToken telemetryTableName = new TableToken(telemetry, telemetry, null, 0, false, false, false, false, true);
        try (
                TableReader reader = new TableReader(OFF_POOL_READER_ID.getAndIncrement(), configuration, telemetryTableName, TxnScoreboardPoolFactory.createPool(configuration));
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()
        ) {
            cursor.of(reader);
            final StringSink sink = new StringSink();
            sink.clear();
            printTelemetryEventAndOrigin(cursor, reader.getMetadata(), sink);
            TestUtils.assertEquals(expected, sink);
            cursor.toTop();
            sink.clear();
            printTelemetryEventAndOrigin(cursor, reader.getMetadata(), sink);
            TestUtils.assertEquals(expected, sink);
        }
    }

    @NotNull
    private DefaultHttpServerConfiguration createHttpServerConfiguration(
            CairoConfiguration cairoConfiguration,
            String baseDir,
            @SuppressWarnings("SameParameterValue") boolean dumpTraffic
    ) {
        return createHttpServerConfiguration(
                cairoConfiguration,
                NetworkFacadeImpl.INSTANCE,
                baseDir,
                1024 * 1024,
                dumpTraffic,
                false
        );
    }

    @NotNull
    private DefaultHttpServerConfiguration createHttpServerConfiguration(
            CairoConfiguration cairoConfiguration,
            NetworkFacade nf,
            String baseDir,
            int sendBufferSize,
            boolean dumpTraffic,
            boolean allowDeflateBeforeSend
    ) {
        return createHttpServerConfiguration(
                cairoConfiguration,
                nf,
                baseDir,
                sendBufferSize,
                dumpTraffic,
                allowDeflateBeforeSend,
                true,
                "HTTP/1.1 "
        );
    }

    @NotNull
    private DefaultHttpServerConfiguration createHttpServerConfiguration(
            CairoConfiguration cairoConfiguration,
            NetworkFacade nf,
            String baseDir,
            int sendBufferSize,
            boolean dumpTraffic,
            boolean allowDeflateBeforeSend,
            boolean serverKeepAlive,
            String httpProtocolVersion
    ) {
        return new HttpServerConfigurationBuilder()
                .withNetwork(nf)
                .withBaseDir(baseDir)
                .withSendBufferSize(sendBufferSize)
                .withDumpingTraffic(dumpTraffic)
                .withAllowDeflateBeforeSend(allowDeflateBeforeSend)
                .withServerKeepAlive(serverKeepAlive)
                .withHttpProtocolVersion(httpProtocolVersion)
                .build(cairoConfiguration);
    }

    private boolean handleClientOperation(
            HttpConnectionContext context,
            int operation,
            HttpRequestProcessorSelector selector,
            IODispatcher<HttpConnectionContext> dispatcher
    ) {
        try {
            return context.handleClientOperation(operation, selector, IODispatcherTest.emptyRescheduleContext);
        } catch (HeartBeatException e) {
            dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
        } catch (PeerIsSlowToReadException e) {
            dispatcher.registerChannel(context, IOOperation.WRITE);
        } catch (ServerDisconnectException e) {
            dispatcher.disconnect(context, context.getDisconnectReason());
        } catch (PeerIsSlowToWriteException e) {
            dispatcher.registerChannel(context, IOOperation.READ);
        }
        return false;
    }

    private void importWithO3MaxLagAndMaxUncommittedRowsTableExists(
            boolean overwrite,
            boolean syncCommitMode,
            int partitionBy,
            long o3MaxLag,
            int maxUncommittedRows,
            long expectedO3MaxLag,
            int expectedMaxUncommittedRows
    ) throws Exception {
        final int msyncsPerTableCreation = 5;
        final int msyncsPerWriterInit = 1;

        final AtomicInteger msyncCallCount = new AtomicInteger();
        final String baseDir = root;
        CairoConfiguration configuration = new DefaultTestCairoConfiguration(baseDir) {
            @Override
            public int getCommitMode() {
                return syncCommitMode ? CommitMode.SYNC : super.getCommitMode();
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return new TestFilesFacadeImpl() {
                    @Override
                    public void msync(long addr, long len, boolean async) {
                        msyncCallCount.incrementAndGet();
                        Files.msync(addr, len, async);
                    }
                };
            }
        };

        String tableName = "test_table";
        String command = "POST /upload?fmt=json&" +
                String.format("overwrite=%b&", overwrite) +
                "forceHeader=true&" +
                "timestamp=ts&" +
                String.format("partitionBy=%s&", PartitionBy.toString(partitionBy)) +
                "o3MaxLag=" + o3MaxLag + "&" +
                "maxUncommittedRows=" + maxUncommittedRows + "&" +
                "name=" + tableName + " HTTP/1.1\r\n";

        String expectedMetadata = "{\"status\":\"OK\"," +
                "\"location\":\"" + tableName + "\"," +
                "\"rowsRejected\":0," +
                "\"rowsImported\":1," +
                "\"header\":true," +
                "\"partitionBy\":\"" + PartitionBy.toString(partitionBy) + "\"," +
                "\"timestamp\":\"ts\"," +
                "\"columns\":[" +
                "{\"name\":\"ts\",\"type\":\"TIMESTAMP\",\"size\":8,\"errors\":0}," +
                "{\"name\":\"int\",\"type\":\"INT\",\"size\":4,\"errors\":0}" +
                "]}\r\n";

        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        (partitionBy == PartitionBy.DAY ? "ed\r\n" : "ee\r\n") +
                        expectedMetadata +
                        "00\r\n" +
                        "\r\n",
                command +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"\r\n" +
                        "\r\n" +
                        "[{\"name\":\"ts\",\"type\":\"TIMESTAMP\", \"pattern\": \"yyyy-MM-dd HH:mm:ss\"}," +
                        "{\"name\":\"int\",\"type\":\"INT\"}]\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        "ts,int\r\n" +
                        "2021-01-01 00:01:00,1\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                configuration,
                false,
                1,
                (engine, sqlExecutionContext) -> {
                    TableModel model = new TableModel(configuration, tableName, partitionBy)
                            .timestamp("ts")
                            .col("int", ColumnType.INT);
                    TestUtils.createTable(engine, model);
                }
        );

        assertMetadataAndData(
                tableName,
                expectedO3MaxLag,
                expectedMaxUncommittedRows,
                1,
                "2021-01-01T00:01:00.000000Z\t1\n",
                true
        );

        if (syncCommitMode) {
            int extraMsyncs = msyncsPerWriterInit + msyncsPerTableCreation;
            if (overwrite) {
                // We re-create the table when overwrite is set.
                extraMsyncs += msyncsPerTableCreation;
            }
            assertTrue("at least " + (extraMsyncs + 1) + " msync calls expected, was " + msyncCallCount.get(), msyncCallCount.get() > extraMsyncs);
        }
    }

    private void importWithO3MaxLagAndMaxUncommittedRowsTableNotExists(
            long o3MaxLag,
            int maxUncommittedRows,
            long expectedO3MaxLag,
            int expectedMaxUncommittedRows,
            int expectedImportedRows,
            String data,
            String expectedData
    ) throws Exception {
        String tableName = "test_table";
        String command = "POST /upload?fmt=json&" +
                "overwrite=false&" +
                "forceHeader=true&" +
                "timestamp=ts&" +
                "partitionBy=DAY&" +
                "o3MaxLag=" + o3MaxLag + "&" +
                "maxUncommittedRows=" + maxUncommittedRows + "&" +
                "name=" + tableName + " HTTP/1.1\r\n";

        String expectedMetadata = "{\"status\":\"OK\"," +
                "\"location\":\"" + tableName + "\"," +
                "\"rowsRejected\":" + 0 + "," +
                "\"rowsImported\":" + expectedImportedRows + "," +
                "\"header\":true," +
                "\"partitionBy\":\"DAY\"," +
                "\"timestamp\":\"ts\"," +
                "\"columns\":[" +
                "{\"name\":\"ts\",\"type\":\"TIMESTAMP\",\"size\":8,\"errors\":0}," +
                "{\"name\":\"int\",\"type\":\"INT\",\"size\":4,\"errors\":0}" +
                "]}\r\n";

        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "ed\r\n" +
                        expectedMetadata +
                        "00\r\n" +
                        "\r\n",
                command +
                        "Host: localhost:9001\r\n" +
                        "Connection: keep-alive\r\n" +
                        "Content-Length: 832\r\n" +
                        "Accept: */*\r\n" +
                        "Origin: http://localhost:9000\r\n" +
                        "X-Requested-With: XMLHttpRequest\r\n" +
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36\r\n" +
                        "Sec-Fetch-Mode: cors\r\n" +
                        "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Sec-Fetch-Site: same-origin\r\n" +
                        "Referer: http://localhost:9000/index.html\r\n" +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"schema\"\r\n" +
                        "\r\n" +
                        "[{\"name\":\"ts\",\"type\":\"TIMESTAMP\", \"pattern\": \"yyyy-MM-dd HH:mm:ss\"}," +
                        "{\"name\":\"int\",\"type\":\"INT\"}]\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                        "Content-Disposition: form-data; name=\"data\"\r\n" +
                        "\r\n" +
                        data +
                        "\r\n" +
                        "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                NetworkFacadeImpl.INSTANCE,
                false,
                1
        );

        assertMetadataAndData(
                tableName,
                expectedO3MaxLag,
                expectedMaxUncommittedRows,
                expectedImportedRows,
                expectedData,
                false
        );
    }

    private void printTelemetryEventAndOrigin(RecordCursor cursor, RecordMetadata metadata, StringSink sink) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            final short event = record.getShort(1);
            if (event < 0) {
                continue; // skip non-event entries
            }
            CursorPrinter.printColumn(record, metadata, 1, sink, false);
            sink.put('\t');
            CursorPrinter.printColumn(record, metadata, 2, sink, false);
            sink.put('\n');
        }
    }

    private void testExceptionAfterHeader(int numOfRows, String rows) throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet(
                    "{" +
                            "\"query\":\"select simulate_crash('" + numOfRows + "') from long_sequence(5)\"," +
                            "\"columns\":[{\"name\":\"simulate_crash\",\"type\":\"BOOLEAN\"}]," +
                            "\"timestamp\":-1," +
                            "\"dataset\":[" + rows + "]," +
                            "\"count\":" + (numOfRows + 1) + "," +
                            "\"error\":\"simulated cairo exception\", " +
                            "\"errorPos\":222" +
                            "}",
                    "select simulate_crash('" + numOfRows + "') from long_sequence(5)"
            );
            Assert.assertEquals(0, engine.getMetrics().healthMetrics().unhandledErrorsCount());
        });
    }

    private void testExecuteAndCancelSqlCommands(final String url) throws Exception {
        final long TIMEOUT = 240_000;

        String baseTable = "create table tab (b boolean, ts timestamp, sym symbol)";
        String walTable = baseTable + " timestamp(ts) partition by DAY WAL";
        ObjList<String> ddls = new ObjList<>(
                baseTable,
                baseTable + " timestamp(ts)",
                baseTable + " timestamp(ts) partition by DAY BYPASS WAL"
                // walTable // TODO: ban cancellation of queries inside WAL Apply job
        );

        String createAsSelect = "create table new_tab as (select * from tab where sleep(120000))";
        String select1 = "select 1 from long_sequence(1) where sleep(120000)";
        String select2 = "select sleep(120000) from long_sequence(1)";
        String selectWithJoin = "select 1 from long_sequence(1) ls1 join long_sequence(1) on sleep(120000)";
        String insertAsSelect1 = "insert into tab select true, 100000000000000L::timestamp, 'B' from long_sequence(1) where sleep(120000)";
        String insertAsSelect2 = "insert into tab select sleep(120000), 100000000000000L::timestamp, 'B' from long_sequence(1)";
        String insertAsSelectBatched = "insert batch 100 into tab select true, 100000000000000L::timestamp, 'B' from long_sequence(1) where sleep(120000)";
        String insertAsSelectWithJoin1 = "insert into tab select ls1.x = ls2.x, 100000000000000L::timestamp, 'B' from long_sequence(1) ls1 left join (select * from long_sequence(1)) ls2 on ls1.x = ls2.x where sleep(120000)";
        String insertAsSelectWithJoin2 = "insert into tab select sleep(120000), 100000000000000L::timestamp, 'B' from long_sequence(1) ls1 left join (select * from long_sequence(1)) ls2 on ls1.x = ls2.x";
        String insertAsSelectWithJoin3 = "insert into tab select ls1.x = ls2.x, 100000000000000L::timestamp, 'B' from long_sequence(1) ls1 left join (select * from long_sequence(1)) ls2 on ls1.x = ls2.x and sleep(120000)";
        String update1 = "update tab set b=true where sleep(120000)";
        String update2 = "update tab set b=sleep(120000)";
        String updateWithJoin1 = "update tab t1 set b=true from tab t2 where sleep(120000) and t1.b = t2.b";
        String updateWithJoin2 = "update tab t1 set b=sleep(120000) from tab t2 where t1.b = t2.b";
        String addColumns = "alter table tab add column s1 symbol index";

        final ObjList<String> commands;
        if ("/query".equals(url)) {
            commands = new ObjList<>(
                    createAsSelect,
                    select1,
                    select2,
                    selectWithJoin,
                    insertAsSelect1,
                    insertAsSelect2,
                    insertAsSelectBatched,
                    insertAsSelectWithJoin1,
                    insertAsSelectWithJoin2,
                    insertAsSelectWithJoin3,
                    update1,
                    update2,
                    updateWithJoin1,
                    updateWithJoin2,
                    addColumns
            );
        } else {
            commands = new ObjList<>(
                    select1,
                    select2,
                    selectWithJoin
            );
        }


        SOCountDownLatch started = new SOCountDownLatch(1);
        SOCountDownLatch stopped = new SOCountDownLatch(1);
        AtomicReference<Throwable> queryError = new AtomicReference<>();

        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine, sqlExecutionContext) -> {
                    DelayedListener registryListener = new DelayedListener();
                    engine.getQueryRegistry().setListener(registryListener);

                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        for (int i = 0, n = ddls.size(); i < n; i++) {
                            final String ddl = ddls.getQuick(i);
                            boolean isWal = ddl.equals(walTable);

                            engine.execute("drop table if exists tab", executionContext);
                            engine.execute(ddl, executionContext);
                            engine.execute("insert into tab select true, (86400000000*x)::timestamp, null from long_sequence(1000)", executionContext);
                            if (isWal) {
                                drainWalQueue(engine);
                            }

                            for (int j = 0, k = commands.size(); j < k; j++) {
                                final String command = commands.getQuick(j);

                                // statements containing multiple transactions, such as 'alter table add column col1, col2' are currently not supported for WAL tables
                                // UPDATE statements with join are not supported yet for WAL tables
                                if ((isWal && (command.equals(updateWithJoin1) || command.equals(updateWithJoin2) || command.equals(addColumns)))) {
                                    continue;
                                }

                                try {
                                    engine.execute("drop table if exists new_tab", executionContext);
                                    if (isWal) {
                                        drainWalQueue(engine);
                                    }

                                    started.setCount(isWal ? 2 : 1);
                                    stopped.setCount(isWal ? 2 : 1);
                                    queryError.set(null);

                                    registryListener.queryText = command;
                                    registryListener.queryFound.setCount(1);

                                    new Thread(
                                            () -> {
                                                try (TestHttpClient testHttpClient = new TestHttpClient()) {
                                                    started.countDown();
                                                    try {
                                                        testHttpClient.assertGetRegexp(
                                                                url,
                                                                ".*(cancelled by user|Could not create table|timeout, query aborted|\"ddl\":\"OK\").*",
                                                                command,
                                                                null, null,
                                                                null
                                                        );
                                                    } catch (Throwable e) {
                                                        queryError.set(e);
                                                    }
                                                } finally {
                                                    stopped.countDown();
                                                }
                                            }, "command_thread"
                                    ).start();

                                    started.await();

                                    long queryId;
                                    long start = System.currentTimeMillis();

                                    //wait until query appears in registry and get query id
                                    while (true) {
                                        Os.sleep(1);
                                        testHttpClient.assertGetRegexp(
                                                "/query",
                                                ".*dataset.*",
                                                "select query_id from query_activity() where query = '" + command.replace("'", "''") + "'",
                                                null, null, null,
                                                new CharSequenceObjHashMap<>() {{
                                                    put("nm", "true");
                                                }},
                                                "200"
                                        );
                                        String response = testHttpClient.getSink().toString();
                                        int startIdx = response.indexOf("\"dataset\":[[");
                                        if (startIdx > -1) {
                                            startIdx += "\"dataset\":[[".length();
                                            int endIdx = response.indexOf("]]", startIdx);
                                            queryId = Numbers.parseLong(response, startIdx, endIdx);
                                            break;
                                        }
                                        if (System.currentTimeMillis() - start > TIMEOUT) {
                                            throw new RuntimeException("Timed out waiting for command to appear in registry: " + command);
                                        }
                                        if (queryError.get() != null) {
                                            throw new RuntimeException("Query to cancel failed!", queryError.get());
                                        }
                                    }

                                    try {
                                        testHttpClient.assertGetRegexp(
                                                "/query",
                                                ".*(query to cancel not found in registry|\"ddl\":\"OK\").*",
                                                "cancel query " + queryId,
                                                null, null,
                                                "200"
                                        );
                                    } finally {
                                        registryListener.queryFound.countDown();
                                    }

                                    start = System.currentTimeMillis();

                                    // wait until query disappears from registry
                                    while (true) {
                                        Os.sleep(1);
                                        testHttpClient.assertGetRegexp(
                                                "/query",
                                                "\\{\"query\":\"select \\* from query_activity\\(\\).*",
                                                "select * from query_activity() where query_id = " + queryId,
                                                null, null,
                                                "200"
                                        );
                                        if (testHttpClient.getSink().toString().endsWith("\"count\":0}")) {
                                            break;
                                        }

                                        if (System.currentTimeMillis() - start > TIMEOUT) {
                                            throw new RuntimeException("Timed out waiting for command to stop: " + command);
                                        }
                                    }

                                    // run simple query to test that previous query cancellation doesn't 'spill into' other queries
                                    if ("/query".equals(url)) {
                                        testHttpClient.assertGet(url, "{\"query\":\"select sleep(1)\",\"columns\":[{\"name\":\"sleep\",\"type\":\"BOOLEAN\"}],\"timestamp\":-1,\"dataset\":[[true]],\"count\":1}", "select sleep(1)", null, null);
                                    } else {
                                        testHttpClient.assertGet(url, "\"sleep\"\r\ntrue\r\n", "select sleep(1)", null, null);
                                    }

                                } catch (Throwable t) {
                                    throw new RuntimeException(
                                            "Failed on\n ddl: " + ddl +
                                                    "\n query: " + command +
                                                    "\n exception: ", t
                                    );
                                } finally {
                                    queryError.set(new Exception());//stop wal thread
                                    stopped.await();
                                }
                            }
                        }
                    } finally {
                        engine.getQueryRegistry().setListener(null);
                    }
                });
    }

    private void testHttpQueryGeoHashColumnChars(String expectedResponse, String url) throws Exception {
        new HttpQueryTestBuilder()
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder()
                        .withSendBufferSize(16 * 1024)
                        .withConfiguredMaxQueryResponseRowLimit(configuredMaxQueryResponseRowLimit)
                )
                .withTempFolder(root)
                .run((engine, sqlExecutionContext) -> {
                    try (
                            SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                    ) {
                        engine.execute(
                                "create table y as (\n" +
                                        "select\n" +
                                        "cast(rnd_str(null, 'questdb1234567890', 'u10m99dd3pbj') as geohash(1c)) geo1,\n" +
                                        "cast(rnd_str(null, 'questdb1234567890', 'u10m99dd3pbj') as geohash(3c)) geo2,\n" +
                                        "cast(rnd_str(null, 'questdb1234567890', 'u10m99dd3pbj') as geohash(6c)) geo4,\n" +
                                        "cast(rnd_str(null, 'questdb1234567890', 'u10m99dd3pbj') as geohash(12c)) geo8," +
                                        "cast(rnd_str(null, 'questdb1234567890', 'u10m99dd3pbj') as geohash(1b)) geo01\n" +
                                        "from long_sequence(3)\n" +
                                        ")", executionContext
                        );
                        testHttpClient.assertGet(url, expectedResponse, "SELECT * FROM y");
                    }
                });
    }

    private HttpQueryTestBuilder testJsonQuery(int recordCount, String request, String expectedResponse, int requestCount, boolean telemetry) throws Exception {
        return testJsonQuery0(
                2, (engine, sqlExecutionContext) -> {
                    // create table with all column types
                    createTableX(engine, recordCount);
                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            request,
                            expectedResponse,
                            requestCount,
                            0,
                            false
                    );
                }, telemetry
        );
    }

    private void testJsonQuery(int recordCount, String request, String expectedResponse, int requestCount) throws Exception {
        testJsonQuery(recordCount, request, expectedResponse, requestCount, false);
    }

    private HttpQueryTestBuilder testJsonQuery(int recordCount, String request, String expectedResponse) throws Exception {
        return testJsonQuery(recordCount, request, expectedResponse, 100, false);
    }

    private HttpQueryTestBuilder testJsonQuery0(int workerCount, HttpQueryTestBuilder.HttpClientCode code, boolean telemetry) throws Exception {
        return testJsonQuery0(workerCount, code, telemetry, false);
    }

    private HttpQueryTestBuilder testJsonQuery0(int workerCount, HttpQueryTestBuilder.HttpClientCode code, boolean telemetry, boolean http1) throws Exception {
        HttpQueryTestBuilder builder = new HttpQueryTestBuilder()
                .withWorkerCount(workerCount)
                .withTelemetry(telemetry)
                .withTempFolder(root)
                .withJitMode(SqlJitMode.JIT_MODE_ENABLED)
                .withNanosClock(nanosecondClock)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder()
                        .withServerKeepAlive(!http1)
                        .withSendBufferSize(16 * 1024)
                        .withConfiguredMaxQueryResponseRowLimit(configuredMaxQueryResponseRowLimit)
                        .withHttpProtocolVersion(http1 ? "HTTP/1.0 " : "HTTP/1.1 "));
        builder.run(code);
        return builder;
    }

    private void testMaxConnections0(
            IODispatcher<HttpConnectionContext> dispatcher,
            long sockAddr,
            LongList openFds,
            long buf
    ) {
        // Connect sockets that would be consumed by dispatcher plus
        // the same amount to put onto the backlog. Backlog and active connection count are the same.
        // This is necessary for TCP stack to start rejecting new connections
        openFds.clear();
        for (int i = 0; i < 400; i++) {
            long fd = Net.socketTcp(true);
            LOG.info().$("Connecting socket ").$(i).$(" fd=").$(fd).$();
            TestUtils.assertConnect(fd, sockAddr);
            openFds.add(fd);
        }

        // let dispatcher catchup
        long startNanos = System.nanoTime();
        while (dispatcher.isListening()) {
            long endNanos = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toSeconds(endNanos - startNanos) > 30) {
                Assert.fail("Timed out waiting for dispatcher to stop listening");
            }
            Os.pause();
        }

        final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";
        long mem = TestUtils.toMemory(request);

        try {
            for (int i = 0; i < 400; i++) {
                LOG.info().$("Sending request via socket #").$(i).$();
                long fd = openFds.getQuick(i);
                Assert.assertEquals(request.length(), Net.send(fd, mem, request.length()));
                // ensure we have response from server
                int len = Net.recv(fd, buf, 64);
                if (len < 0) {
                    System.out.println(fd);
                    System.out.println(Os.errno());
                    Assert.fail();
                }
            }

            // close socket and wait for dispatcher to begin listening
            Net.close(openFds.getQuick(0));

            startNanos = System.nanoTime();
            while (!dispatcher.isListening()) {
                long endNanos = System.nanoTime();
                if (TimeUnit.NANOSECONDS.toSeconds(endNanos - startNanos) > 30) {
                    Assert.fail("Timed out waiting for dispatcher to stop listening");
                }
                Os.pause();
            }

            for (int j = 1; j < 400; j++) {
                Net.close(openFds.getQuick(j));
            }

            LOG.info().$("Closed all active sockets").$();
        } finally {
            Unsafe.free(mem, request.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void testQueuedConnectionTimeoutImpl(int port) throws Exception {
        LOG.info().$("started testQueuedConnectionTimeout").$();
        assertMemoryLeak(() -> {
            final int activeConnectionLimit = 5;
            final long queuedConnectionTimeoutInMs = 250;

            class TestIOContext extends IOContext<TestIOContext> {
                private final LongHashSet serverConnectedFds;
                private long heartbeatId;

                public TestIOContext(long fd, LongHashSet serverConnectedFds) {
                    super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
                    socket.of(fd);
                    this.serverConnectedFds = serverConnectedFds;
                }

                @Override
                public void close() {
                    final long fd = getFd();
                    super.close();
                    LOG.info().$(fd).$(" disconnected").$();
                    serverConnectedFds.remove(fd);
                }

                @Override
                public long getAndResetHeartbeatId() {
                    return heartbeatId;
                }

                @Override
                public boolean invalid() {
                    return !serverConnectedFds.contains(getFd());
                }

                @Override
                public void setHeartbeatId(long heartbeatId) {
                    this.heartbeatId = heartbeatId;
                }
            }

            final IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration() {
                @Override
                public int getBindPort() {
                    return port;
                }

                @Override
                public int getLimit() {
                    return activeConnectionLimit;
                }

                @Override
                public long getQueueTimeout() {
                    return queuedConnectionTimeoutInMs;
                }
            };

            final int listenBackLog = configuration.getListenBacklog();

            final AtomicInteger nConnected = new AtomicInteger();
            final LongHashSet serverConnectedFds = new LongHashSet();
            final LongHashSet clientActiveFds = new LongHashSet();
            IOContextFactory<TestIOContext> contextFactory = fd -> {
                LOG.info().$(fd).$(" connected").$();
                serverConnectedFds.add(fd);
                nConnected.incrementAndGet();
                return new TestIOContext(fd, serverConnectedFds);
            };
            final String request = "\n";
            long mem = TestUtils.toMemory(request);

            Thread serverThread;
            long sockAddr = 0;
            final CountDownLatch serverLatch = new CountDownLatch(1);
            try (IODispatcher<TestIOContext> dispatcher = IODispatchers.create(configuration, contextFactory)) {
                final int resolvedPort = dispatcher.getPort();
                sockAddr = Net.sockaddr("127.0.0.1", resolvedPort);
                serverThread = new Thread("test-io-dispatcher") {
                    @Override
                    public void run() {
                        long smem = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                        IORequestProcessor<TestIOContext> requestProcessor = (operation, context, dispatcher) -> {
                            long fd = context.getFd();
                            int rc;
                            switch (operation) {
                                case IOOperation.READ:
                                    rc = Net.recv(fd, smem, 1);
                                    if (rc == 1) {
                                        dispatcher.registerChannel(context, IOOperation.WRITE);
                                    } else {
                                        dispatcher.disconnect(context, IODispatcher.DISCONNECT_REASON_TEST);
                                    }
                                    break;
                                case IOOperation.WRITE:
                                    rc = Net.send(fd, smem, 1);
                                    if (rc == 1) {
                                        dispatcher.registerChannel(context, IOOperation.READ);
                                    } else {
                                        dispatcher.disconnect(context, IODispatcher.DISCONNECT_REASON_TEST);
                                    }
                                    break;
                                case IOOperation.HEARTBEAT:
                                    dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
                                    break;
                                default:
                                    dispatcher.disconnect(context, IODispatcher.DISCONNECT_REASON_TEST);
                            }
                            return true;
                        };

                        try {
                            do {
                                dispatcher.run(0);
                                dispatcher.processIOQueue(requestProcessor);
                                // We can't use Os.pause() here since we rely on thread interrupts.
                                LockSupport.parkNanos(1);
                            } while (!isInterrupted());
                        } finally {
                            Unsafe.free(smem, 1, MemoryTag.NATIVE_DEFAULT);
                            serverLatch.countDown();
                        }
                    }
                };
                serverThread.setDaemon(true);
                serverThread.start();

                // Connect exactly the right amount of clients to fill the active connection and connection backlog, after the
                // queuedConnectionTimeoutInMs the connections in the backlog should get refused
                int nClientConnects = 0;
                int nClientConnectRefused = 0;
                for (int i = 0; i < listenBackLog + activeConnectionLimit; i++) {
                    long fd = Net.socketTcp(true);
                    assertTrue(fd > -1);
                    clientActiveFds.add(fd);
                    if (Net.connect(fd, sockAddr) != 0) {
                        nClientConnectRefused++;
                        continue;
                    }
                    int rc = Net.send(fd, mem, request.length());
                    if (rc < 0) {
                        nClientConnectRefused++;
                        continue;
                    }
                    rc = Net.recv(fd, mem, request.length());
                    if (rc < 0) {
                        nClientConnectRefused++;
                    } else {
                        nClientConnects++;
                    }
                }
                Assert.assertEquals(activeConnectionLimit, nClientConnects);
                Assert.assertEquals(listenBackLog, nClientConnectRefused);
                Assert.assertFalse(dispatcher.isListening());

                // Close all connections and wait for server to resume listening
                while (clientActiveFds.size() > 0) {
                    long fd = clientActiveFds.get(0);
                    clientActiveFds.remove(fd);
                    Net.close(fd);
                }
                long timeoutMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
                while (!dispatcher.isListening()) {
                    if (System.currentTimeMillis() > timeoutMs) {
                        Assert.fail("Timeout waiting for server to start listening again");
                    }
                }

                // Try connections again to make sure server is listening
                nClientConnects = 0;
                nClientConnectRefused = 0;
                for (int i = 0; i < listenBackLog + activeConnectionLimit; i++) {
                    long fd = Net.socketTcp(true);
                    assertTrue(fd > -1);
                    clientActiveFds.add(fd);
                    if (Net.connect(fd, sockAddr) != 0) {
                        nClientConnectRefused++;
                        continue;
                    }
                    int rc = Net.send(fd, mem, request.length());
                    if (rc < 0) {
                        nClientConnectRefused++;
                        continue;
                    }
                    rc = Net.recv(fd, mem, request.length());
                    if (rc < 0) {
                        nClientConnectRefused++;
                    } else {
                        nClientConnects++;
                    }
                }
                Assert.assertEquals(activeConnectionLimit, nClientConnects);
                Assert.assertEquals(listenBackLog, nClientConnectRefused);
                Assert.assertFalse(dispatcher.isListening());

                // Close all remaining client connections
                for (int n = 0; n < clientActiveFds.size(); n++) {
                    long fd = clientActiveFds.get(n);
                    Net.close(fd);
                }
                serverThread.interrupt();
                if (!serverLatch.await(1, TimeUnit.MINUTES)) {
                    Assert.fail("Timeout waiting for server to end");
                }
            } finally {
                if (sockAddr != 0) {
                    Net.freeSockAddr(sockAddr);
                }
                Unsafe.free(mem, request.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private void writeRandomFile(Path path, Rnd rnd, long lastModified) {
        if (Files.exists(path.$())) {
            assertTrue(Files.remove(path.$()));
        }
        long fd = Files.openAppend(path.$());

        long buf = Unsafe.malloc(1048576, MemoryTag.NATIVE_DEFAULT); // 1Mb buffer
        for (int i = 0; i < 1048576; i++) {
            Unsafe.getUnsafe().putByte(buf + i, rnd.nextByte());
        }

        for (int i = 0; i < 20; i++) {
            Assert.assertEquals(1048576, Files.append(fd, buf, 1048576));
        }

        TestFilesFacadeImpl.INSTANCE.close(fd);
        Files.setLastModified(path.$(), lastModified);
        Unsafe.free(buf, 1048576, MemoryTag.NATIVE_DEFAULT);
    }

    private static class ByteArrayResponse extends AbstractCharSequence {
        private final byte[] bytes;
        private final int len;

        private ByteArrayResponse(byte[] bytes, int len) {
            super();
            this.bytes = bytes;
            this.len = len;
        }

        @Override
        public char charAt(int index) {
            if (index >= len) {
                throw new IndexOutOfBoundsException();
            }
            return (char) bytes[index];
        }

        @Override
        public int length() {
            return len;
        }
    }

    private static class DelayedListener implements QueryRegistry.Listener {
        private final SOCountDownLatch queryFound = new SOCountDownLatch(1);
        private volatile CharSequence queryText;

        @Override
        public void onRegister(CharSequence query, long queryId, SqlExecutionContext executionContext) {
            if (queryText == null) {
                return;
            }

            if (Chars.equalsNc(queryText, query)) {
                queryFound.await();
                queryText = null;
            }
        }
    }

    private static class DelayedWALListener implements QueryRegistry.Listener {
        private final SOCountDownLatch queryFound = new SOCountDownLatch(1);
        private volatile CharSequence queryText;

        @Override
        public void onRegister(CharSequence query, long queryId, SqlExecutionContext executionContext) {
            if (queryText == null) {
                return;
            }

            if (!executionContext.isWalApplication()) {
                return;
            }

            if (Chars.equalsNc(queryText, query)) {
                queryFound.await();
                queryText = null;
            }
        }
    }

    private static class HelloContext extends IOContext<HelloContext> {
        private final long buffer = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
        private final SOCountDownLatch closeLatch;

        public HelloContext(long fd, SOCountDownLatch closeLatch) {
            super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
            this.of(fd);
            this.closeLatch = closeLatch;
        }

        @Override
        public void close() {
            Unsafe.free(buffer, 1024, MemoryTag.NATIVE_DEFAULT);
            closeLatch.countDown();
            super.close();
        }

        @Override
        public boolean invalid() {
            return false;
        }
    }

    private static class QueryThread extends Thread {
        private final CyclicBarrier barrier;
        private final int count;
        private final AtomicInteger errorCounter;
        private final CountDownLatch latch;
        private final String[][] requests;

        public QueryThread(String[][] requests, int count, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger errorCounter) {
            this.requests = requests;
            this.count = count;
            this.barrier = barrier;
            this.latch = latch;
            this.errorCounter = errorCounter;
        }

        @Override
        public void run() {
            final Rnd rnd = new Rnd();
            try {
                try (TestHttpClient httpClient = new TestHttpClient()) {
                    TestUtils.await(barrier);
                    for (int i = 0; i < count; i++) {
                        int index = rnd.nextPositiveInt() % requests.length;
                        try {
                            httpClient.assertGet(
                                    requests[index][1],
                                    requests[index][0]
                            );
                        } catch (Throwable e) {
                            LOG.critical().$(e).$();
                            System.out.println("erm: " + index + ", ts=" + Micros.toString(Os.currentTimeMicros()));
                            throw e;
                        }
                    }
                }
            } catch (Throwable e) {
                LOG.critical().$(e).$();
                errorCounter.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }
    }

    static class Status {
        boolean valid;
    }

    private static class TestJsonQueryProcessorFactory implements HttpRequestHandlerFactory {
        private final CairoEngine engine;
        private final HttpFullFatServerConfiguration httpConfiguration;
        private final int workerCount;

        public TestJsonQueryProcessorFactory(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration, int workerCount) {
            this.engine = engine;
            this.httpConfiguration = httpConfiguration;
            this.workerCount = workerCount;
        }

        @Override
        public ObjList<String> getUrls() {
            return new ObjList<>("/query");
        }

        @Override
        public HttpRequestHandler newInstance() {
            return new JsonQueryProcessor(
                    httpConfiguration.getJsonQueryProcessorConfiguration(),
                    engine,
                    workerCount
            );
        }
    }
}
