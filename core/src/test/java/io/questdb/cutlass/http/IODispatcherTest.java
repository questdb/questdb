/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.http;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoTestUtils;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecordCursorPrinter;
import io.questdb.cairo.TableModel;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TestRecord;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.NetUtils;
import io.questdb.cutlass.http.processors.HealthCheckProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.QueryCache;
import io.questdb.cutlass.http.processors.StaticContentProcessor;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IOContext;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.Unsafe;
import io.questdb.std.Zip;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.ByteSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class IODispatcherTest {
    public static final String JSON_DDL_RESPONSE = "0d\r\n" +
            "{\"ddl\":\"OK\"}\n\r\n" +
            "00\r\n" +
            "\r\n";

    private static final Log LOG = LogFactory.getLog(IODispatcherTest.class);
    private static final RescheduleContext EmptyRescheduleContext = (retry) -> {
    };
    private static final RecordCursorPrinter printer = new RecordCursorPrinter();
    private static final Metrics metrics = Metrics.enabled();
    private final String ValidImportResponse = "HTTP/1.1 200 OK\r\n" +
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
            "|              0  |                              Dispatching_base_num  |                   STRING  |           0  |\r\n" +
            "|              1  |                                   Pickup_DateTime  |                     DATE  |           0  |\r\n" +
            "|              2  |                                  DropOff_datetime  |                   STRING  |           0  |\r\n" +
            "|              3  |                                      PUlocationID  |                   STRING  |           0  |\r\n" +
            "|              4  |                                      DOlocationID  |                   STRING  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private long configuredMaxQueryResponseRowLimit = Long.MAX_VALUE;

    public static void createTestTable(CairoConfiguration configuration, int n) {
        try (TableModel model = new TableModel(configuration, "y", PartitionBy.NONE)) {
            model
                    .col("j", ColumnType.SYMBOL);
            CairoTestUtils.create(model);
        }

        try (TableWriter writer = new TableWriter(configuration, "y")) {
            for (int i = 0; i < n; i++) {
                TableWriter.Row row = writer.newRow();
                row.putSym(0, "ok\0ok");
                row.append();
            }
            writer.commit();
        }
    }

    @Test
    public void queryWithDoubleQuotesParsedCorrecly() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run(engine -> {
                    // select 1 as "select"
                    // with select being the column name to check double quote parsing
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=SELECT%201%20as%20%22select%22 HTTP/1.1\r\n",
                            "67\r\n"
                                    + "{\"query\":\"SELECT 1 as \\\"select\\\"\",\"columns\":[{\"name\":\"select\",\"type\":\"INT\"}],\"dataset\":[[1]],\"count\":1}\r\n"
                                    + "00\r\n"
                                    + "\r\n"
                    );
                });
    }

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
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

                        @Override
                        public boolean getPeerNoLinger() {
                            return false;
                        }
                    },
                    (fd, dispatcher1) -> {
                        connectLatch.countDown();
                        return new HelloContext(fd, contextClosedLatch, dispatcher1);
                    }
            )) {
                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(
                                (operation, context) -> {
                                    if (operation == IOOperation.WRITE) {
                                        Assert.assertEquals(1024, Net.send(context.getFd(), context.buffer, 1024));
                                        context.dispatcher.disconnect(context, IODispatcher.DISCONNECT_REASON_TEST);
                                    }
                                }
                        );
                    }
                    serverHaltLatch.countDown();
                }).start();


                long fd = Net.socketTcp(true);
                try {
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        try {
                            TestUtils.assertConnect(fd, sockAddr);

                            connectLatch.await();

                            long buffer = Unsafe.malloc(2048);
                            try {
                                Assert.assertEquals(1024, Net.recv(fd, buffer, 1024));
                            } finally {
                                Unsafe.free(buffer, 2048);
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
    public void testCannotSetNonBlocking() throws Exception {
        assertMemoryLeak(() -> {
            final HttpContextConfiguration httpContextConfiguration = new DefaultHttpContextConfiguration();
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
                    (fd, dispatcher1) -> new HttpConnectionContext(httpContextConfiguration).of(fd, dispatcher1)
            )) {

                // spin up dispatcher thread
                AtomicBoolean dispatcherRunning = new AtomicBoolean(true);
                SOCountDownLatch dispatcherHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (dispatcherRunning.get()) {
                        dispatcher.run(0);
                    }
                    dispatcherHaltLatch.countDown();
                }).start();

                try {

                    long socketAddr = Net.sockaddr(Net.parseIPv4("127.0.0.1"), 9001);
                    long fd = Net.socketTcp(true);
                    try {
                        TestUtils.assertConnect(fd, socketAddr);

                        int bufLen = 512;
                        long mem = Unsafe.malloc(bufLen);
                        try {
                            Assert.assertEquals(-2, Net.recv(fd, mem, bufLen));
                        } finally {
                            Unsafe.free(mem, bufLen);
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
    public void testConnectDisconnect() throws Exception {

        LOG.info().$("started testConnectDisconnect").$();

        assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd, IODispatcher<HttpConnectionContext> dispatcher1) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration.getHttpContextConfiguration()) {
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
                            }.of(fd, dispatcher1);
                        }
                    }
            )) {
                HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {

                    @Override
                    public HttpRequestProcessor select(CharSequence url) {
                        return null;
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HttpRequestProcessor() {
                        };
                    }

                    @Override
                    public void close() {
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {

                    while (serverRunning.get()) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(
                                (operation, context) -> context.handleClientOperation(operation, selector, EmptyRescheduleContext)
                        );
                    }
                    serverHaltLatch.countDown();
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
                "<0d0a63380d0a\n" +
                "<7b22737461747573223a224f4b222c226c6f636174696f6e223a2273616d706c652e637376222c22726f777352656a6563746564223a302c22726f7773496d706f72746564223a352c22686561646572223a66616c73652c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536222c2273697a65223a33322c226572726f7273223a307d2c7b226e616d65223a226631222c2274797065223a2243484152222c2273697a65223a322c226572726f7273223a307d5d7d\n" +
                "<0d0a30300d0a\n" +
                "<0d0a\n";

        final String expectedTableMetadata = "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"f0\",\"type\":\"LONG256\"},{\"index\":1,\"name\":\"f1\",\"type\":\"CHAR\"}],\"timestampIndex\":-1}";

        final String baseDir = temp.getRoot().getAbsolutePath();
        final CairoConfiguration configuration = new DefaultCairoConfiguration(baseDir);
        try (
                CairoEngine cairoEngine = new CairoEngine(configuration);
                HttpServer ignored = HttpServer.create(
                        new DefaultHttpServerConfiguration(
                                new DefaultHttpContextConfiguration() {
                                    @Override
                                    public MillisecondClock getClock() {
                                        return StationaryMillisClock.INSTANCE;
                                    }
                                }
                        ),
                        null,
                        LOG,
                        cairoEngine,
                        metrics
                )) {

            // upload file
            NetUtils.playScript(NetworkFacadeImpl.INSTANCE, uploadScript, "127.0.0.1", 9001);

            try (TableReader reader = cairoEngine.getReader(AllowAllCairoSecurityContext.INSTANCE, "sample.csv", TableUtils.ANY_TABLE_VERSION)) {
                StringSink sink = new StringSink();
                reader.getMetadata().toJson(sink);
                TestUtils.assertEquals(expectedTableMetadata, sink);
            }

            final String selectAsJsonScript = ">474554202f657865633f71756572793d25304125304125323773616d706c652e637376253237266c696d69743d302532433130303026636f756e743d7472756520485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37362e302e333830392e313030205361666172692f3533372e33360d0a5365632d46657463682d4d6f64653a20636f72730d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a0d0a\n" +
                    "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a4b6565702d416c6976653a2074696d656f75743d352c206d61783d31303030300d0a\n" +
                    "<0d0a303166300d0a\n" +
                    "<7b227175657279223a225c6e5c6e2773616d706c652e63737627222c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536227d2c7b226e616d65223a226631222c2274797065223a2243484152227d5d2c2264617461736574223a5b5b22307835633530346564343332636235313133386263663039616135653861343130646434613165323034656638346266656431626531366466626131623232303630222c2261225d2c5b22307831396631646632633765653662343634373230616432386539303361656461316135616438373830616663323266306239363038323762643466636636353664222c2262225d2c5b22307839653665313936333762623632356138666633643035326237633266653537646337386335356131356432353864373763343364356139633136306230333834222c2270225d2c5b22307863623933373839373730383963373733633037343034356232306564653263646363336136666635363266346536346235316232306335323035323334353235222c2277225d2c5b22307864323361653962326535633638636166326335363633616635626132373637396463336233636237383163346463363938616262643137643633653332653966222c2274225d5d2c22636f756e74223a357d\n" +
                    "<0d0a30300d0a\n" +
                    "<0d0a";

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
                "<0d0a63380d0a\n" +
                "<7b22737461747573223a224f4b222c226c6f636174696f6e223a2273616d706c652e637376222c22726f777352656a6563746564223a302c22726f7773496d706f72746564223a352c22686561646572223a66616c73652c22636f6c756d6e73223a5b7b226e616d65223a226630222c2274797065223a224c4f4e47323536222c2273697a65223a33322c226572726f7273223a307d2c7b226e616d65223a226631222c2274797065223a2243484152222c2273697a65223a322c226572726f7273223a307d5d7d\n" +
                "<0d0a30300d0a\n" +
                "<0d0a\n";

        final String expectedTableMetadata = "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"f0\",\"type\":\"LONG256\"},{\"index\":1,\"name\":\"f1\",\"type\":\"CHAR\"}],\"timestampIndex\":-1}";

        final String baseDir = temp.getRoot().getAbsolutePath();
        final CairoConfiguration configuration = new DefaultCairoConfiguration(baseDir);
        try (
                CairoEngine cairoEngine = new CairoEngine(configuration);
                HttpServer ignored = HttpServer.create(
                        new DefaultHttpServerConfiguration(new DefaultHttpContextConfiguration() {
                            @Override
                            public MillisecondClock getClock() {
                                return StationaryMillisClock.INSTANCE;
                            }
                        }),
                        null,
                        LOG,
                        cairoEngine,
                        metrics
                )) {

            // upload file
            NetUtils.playScript(NetworkFacadeImpl.INSTANCE, uploadScript, "127.0.0.1", 9001);

            try (TableReader reader = cairoEngine.getReader(AllowAllCairoSecurityContext.INSTANCE, "sample.csv", TableUtils.ANY_TABLE_VERSION)) {
                StringSink sink = new StringSink();
                reader.getMetadata().toJson(sink);
                TestUtils.assertEquals(expectedTableMetadata, sink);
            }

            final String selectAsJsonScript = ">474554202f657865633f71756572793d25323773616d706c652e63737625323726636f756e743d66616c736526636f6c733d66302532436631267372633d76697320485454502f312e310d0a486f73743a206c6f63616c686f73743a393030300d0a436f6e6e656374696f6e3a206b6565702d616c6976650d0a4163636570743a202a2f2a0d0a582d5265717565737465642d576974683a20584d4c48747470526571756573740d0a557365722d4167656e743a204d6f7a696c6c612f352e30202857696e646f7773204e542031302e303b2057696e36343b2078363429204170706c655765624b69742f3533372e333620284b48544d4c2c206c696b65204765636b6f29204368726f6d652f37392e302e333934352e313330205361666172692f3533372e33360d0a5365632d46657463682d536974653a2073616d652d6f726967696e0d0a5365632d46657463682d4d6f64653a20636f72730d0a526566657265723a20687474703a2f2f6c6f63616c686f73743a393030302f696e6465782e68746d6c0d0a4163636570742d456e636f64696e673a20677a69702c206465666c6174652c2062720d0a4163636570742d4c616e67756167653a20656e2d47422c656e2d55533b713d302e392c656e3b713d302e380d0a436f6f6b69653a205f67613d4741312e312e323132343933323030312e313537333832343636393b205f6769643d4741312e312e3339323836373839362e313538303132333336350d0a0d0a\n" +
                    "<485454502f312e3120323030204f4b0d0a5365727665723a20717565737444422f312e300d0a446174653a205468752c2031204a616e20313937302030303a30303a303020474d540d0a5472616e736665722d456e636f64696e673a206368756e6b65640d0a436f6e74656e742d547970653a206170706c69636174696f6e2f6a736f6e3b20636861727365743d7574662d380d0a4b6565702d416c6976653a2074696d656f75743d352c206d61783d31303030300d0a\n" +
                    "<0d0a303165630d0a\n";

            // select * from 'sample.csv' and limit columns to f0,f1
            NetUtils.playScript(NetworkFacadeImpl.INSTANCE, selectAsJsonScript, "127.0.0.1", 9001);
        }
    }

    public void testImport(
            String response,
            String request,
            NetworkFacade nf,
            boolean expectDisconnect,
            int requestCount
    ) throws Exception {

        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(nf)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run(engine -> sendAndReceive(
                        nf,
                        request,
                        response,
                        requestCount,
                        0,
                        false,
                        expectDisconnect
                ));
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
                "HTTP/1.1 404 Not Found\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "27\r\n" +
                        "Bad request. Multipart POST expected.\r\n" +
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
                true,
                1
        );
    }

    @Test
    public void testImportColumnMismatch() throws Exception {
        testImport(
                ValidImportResponse
                ,
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
                , 1
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
                        "\r\n"
                ,
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
                        "--------------------------27d997ca93d2689d--"
                , NetworkFacadeImpl.INSTANCE
                , false
                , 1
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
                .withTempFolder(temp)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(NetworkFacadeImpl.INSTANCE)
                                .withDumpingTraffic(false)
                                .withAllowDeflateBeforeSend(false)
                                .withHttpProtocolVersion("HTTP/1.1 ")
                                .withServerKeepAlive(true)
                )
                .run((engine) -> {
                            SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1);
                            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                                compiler.compile("create table test (ts timestamp, value int) timestamp(ts) partition by DAY", executionContext);

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
                                        false,
                                        true
                                );

                                StringSink sink = new StringSink();
                                TestUtils.assertSql(
                                        compiler,
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
    public void testImportMultipleOnSameConnection()
            throws Exception {
        testImport(
                ValidImportResponse,
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
                ValidImportResponse,
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
                    public int send(long fd, long buffer, int bufferLen) {
                        // ensure we do not send more than one byte at a time
                        if (bufferLen > 0) {
                            return super.send(fd, buffer, 1);
                        }
                        return 0;
                    }
                },
                false,
                10
        );
    }

    @Test
    public void testImportMultipleOnSameConnectionSlow() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(baseDir, false);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 3;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });
            try (
                    CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new TextImportProcessor(engine);
                    }

                    @Override
                    public String getUrl() {
                        return "/upload";
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
                    public int send(long fd, long buffer, int bufferLen) {
                        if (bufferLen > 0) {
                            int result = super.send(fd, buffer, 1);
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
                            ValidImportResponse,
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
                        "0518\r\n" +
                        "{\"status\":\"OK\",\"location\":\"clipboard-157200856\",\"rowsRejected\":0,\"rowsImported\":59,\"header\":true,\"columns\":[{\"name\":\"VendorID\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"lpep_pickup_datetime\",\"type\":\"DATE\",\"size\":8,\"errors\":0},{\"name\":\"Lpep_dropoff_datetime\",\"type\":\"DATE\",\"size\":8,\"errors\":0},{\"name\":\"Store_and_fwd_flag\",\"type\":\"CHAR\",\"size\":2,\"errors\":0},{\"name\":\"RateCodeID\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"Pickup_longitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Pickup_latitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Dropoff_longitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Dropoff_latitude\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Passenger_count\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"Trip_distance\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Fare_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Extra\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"MTA_tax\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Tip_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Tolls_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Ehail_fee\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Total_amount\",\"type\":\"DOUBLE\",\"size\":8,\"errors\":0},{\"name\":\"Payment_type\",\"type\":\"INT\",\"size\":4,\"errors\":0},{\"name\":\"Trip_type\",\"type\":\"INT\",\"size\":4,\"errors\":0}]}\r\n" +
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
    public void testImportSkipLEV() throws Exception {
        testImport(
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "\r\n" +
                        "052e\r\n" +
                        "{\"status\":\"OK\",\"location\":\"clipboard-157200856\",\"rowsRejected\":59,\"rowsImported\":59,\"header\":true,\"columns\":[{\"name\":\"VendorID\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"lpep_pickup_datetime\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Lpep_dropoff_datetime\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Store_and_fwd_flag\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"RateCodeID\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Pickup_longitude\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Pickup_latitude\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Dropoff_longitude\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Dropoff_latitude\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Passenger_count\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Trip_distance\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Fare_amount\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Extra\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"MTA_tax\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Tip_amount\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Tolls_amount\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Ehail_fee\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Total_amount\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Payment_type\",\"type\":\"STRING\",\"size\":0,\"errors\":0},{\"name\":\"Trip_type\",\"type\":\"STRING\",\"size\":0,\"errors\":0}]}\r\n" +
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
    public void testJsonQueryAndDisconnectWithoutWaitingForResult() throws Exception {
        assertMemoryLeak(() -> {

            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(nf, baseDir, 256, false, false);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });
            try (
                    CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                null,
                                workerPool.getWorkerCount(),
                                metrics
                        );
                    }

                    @Override
                    public String getUrl() {
                        return "/query";
                    }
                });

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    CairoTestUtils.createTestTable(
                            engine.getConfiguration(),
                            30,
                            new Rnd(),
                            new TestRecord.ArrayBinarySequence());

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
                            "\r\n" + "f7\r\n" +
                            "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"}\r\n"
                            +
                            "fd\r\n" +
                            ",{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\"\r\n"
                            +
                            "fe\r\n" +
                            ",\"ZSX\",false,[]],[30,32312,-303295973,6854658259142399220,null,\"273652-10-24T01:16:04.499209Z\",0.38179755,0.9687423276940171,\"EDRQQ\",\"LOF\",false,[]],[-79,-21442,1985398001,7522482991756933150,\"279864478-12-31T01:58:35.932Z\",\"20093-07-24T16:56:53.198086Z\"\r\n"
                            +
                            "f5\r\n" +
                            ",null,0.05384400312338511,\"HVUVS\",\"OTS\",true,[]],[70,-29572,-1966408995,-2406077911451945242,null,\"-254163-09-17T05:33:54.251307Z\",0.81233966,null,\"IKJSM\",\"SUQ\",false,[]],[-97,15913,2011884585,4641238585508069993,\"-277437004-09-03T08:55:41.803Z\"\r\n"
                            +
                            "fe\r\n" +
                            ",\"186548-11-05T05:57:55.827139Z\",0.89989215,0.6583311519893554,\"ZIMNZ\",\"RMF\",false,[]],[-9,5991,-907794648,null,null,null,0.13264287,null,\"OHNZH\",null,false,[]],[-94,30598,-1510166985,6056145309392106540,null,null,0.54669005,null,\"MZVQE\",\"NDC\",true,[]],[\r\n"
                            +
                            "ff\r\n" +
                            "-97,-11913,null,750145151786158348,\"-144112168-08-02T20:50:38.542Z\",\"-279681-08-19T06:26:33.186955Z\",0.8977236,0.5691053034055052,\"WIFFL\",\"BRO\",false,[]],[58,7132,null,6793615437970356479,\"63572238-04-24T11:00:13.287Z\",\"171291-08-24T10:16:32.229138Z\",null\r\n"
                            +
                            "f6\r\n" +
                            ",0.7215959171612961,\"KWZLU\",\"GXH\",false,[]],[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]],[109,-8207,-485549586,null,\"278802275-11-05T23:22:18.593Z\"\r\n"
                            +
                            "f2\r\n" +
                            ",\"122137-10-05T20:22:21.831563Z\",0.5780819,0.18586435581637295,\"DYOPH\",\"IMY\",false,[]],[-44,21057,-1604266757,4598876523645326656,null,\"204480-04-27T20:21:01.380246Z\",0.19736767,0.11591855759299885,\"DMIGQ\",\"VKH\",false,[]],[17,23522,-861621212\r\n"
                            +
                            "0100\r\n" +
                            ",-6446120489339099836,null,\"79287-08-03T02:05:46.962686Z\",0.4349324,0.11296257318851766,\"CGFNW\",null,true,[]],[-104,12160,1772084256,-5828188148408093893,\"-270365729-01-24T04:33:47.165Z\",\"-252298-10-09T07:11:36.011048Z\",null,0.5764439692141042,\"BQQEM\",null\r\n"
                            +
                            "fd\r\n" +
                            ",false,[]],[-99,-7837,-159178348,null,\"81404961-06-19T18:10:11.037Z\",null,0.5598187,0.5900836401674938,null,\"HPZ\",true,[]],[-127,5343,-238129044,-8851773155849999621,\"-152632412-11-30T22:15:09.334Z\",\"-90192-03-24T17:45:15.784841Z\",0.7806183,null,\"CLNXF\"\r\n"
                            +
                            "fa\r\n" +
                            ",\"UWP\",false,[]],[-59,-10912,1665107665,-8306574409611146484,\"-243146933-02-10T16:15:15.931Z\",\"-109765-04-18T07:45:05.739795Z\",0.52387,null,\"NIJEE\",\"RUG\",true,[]],[69,4771,21764960,-5708280760166173503,null,\"-248236-04-27T14:06:03.509521Z\",0.77833515\r\n"
                            +
                            "ff\r\n" +
                            ",0.533524384058538,\"VOCUG\",\"UNE\",false,[]],[56,-17784,null,5637967617527425113,null,null,null,0.5815065874358148,null,\"EVQ\",true,[]],[58,29019,-416467698,null,\"-175203601-12-02T01:02:02.378Z\",\"201101-10-20T07:35:25.133598Z\",null,0.7430101994511517,\"DXCBJ\"\r\n"
                            +
                            "f8\r\n" +
                            ",null,true,[]],[-11,-23214,1210163254,-7888017038009650608,\"152525393-08-28T08:19:48.512Z\",\"216070-11-17T13:37:58.936720Z\",null,null,\"JJILL\",\"YMI\",true,[]],[-69,-29912,217564476,null,\"-102483035-11-11T09:07:30.782Z\",\"-196714-09-04T03:57:56.227221Z\"\r\n"
                            +
                            "ed\r\n" +
                            ",0.08039439,0.18684267640195917,\"EUKWM\",\"NZZ\",true,[]],[4,19590,-1505690678,6904166490726350488,\"-218006330-04-21T14:18:39.081Z\",\"283032-05-21T12:20:14.632027Z\",0.23285526,0.22122747948030208,\"NSSTC\",\"ZUP\",false,[]],[-111,-6531,342159453\r\n"
                            +
                            "0100\r\n" +
                            ",8456443351018554474,\"197601854-07-22T06:29:36.718Z\",\"-180434-06-04T17:16:49.501207Z\",0.7910659,0.7128505998532723,\"YQPZG\",\"ZNY\",true,[]],[106,32411,-1426419269,-2990992799558673548,\"261692520-06-19T20:19:43.556Z\",null,0.8377384,0.02633639777833019,\"GENFE\"\r\n"
                            +
                            "f4\r\n" +
                            ",\"WWR\",false,[]],[-125,25715,null,null,\"-113894547-06-20T07:24:13.689Z\",null,0.7417434,0.6288088087840823,\"IJZZY\",null,true,[]],[96,-13602,1350628163,null,\"257134407-03-20T11:25:44.819Z\",null,0.7360581,null,\"LGYDO\",\"NLI\",true,[]],[-64,8270,null\r\n"
                            +
                            "fd\r\n" +
                            ",-5695137753964242205,\"289246073-05-28T15:10:38.644Z\",\"-220112-01-30T11:56:06.194709Z\",0.938019,null,\"GHLXG\",\"MDJ\",true,[]],[-76,12479,null,-4034810129069646757,\"123619904-08-31T19:44:11.844Z\",\"267826-03-17T13:36:32.811014Z\",0.8463546,null,\"PFOYM\",\"WDS\"\r\n"
                            +
                            "ba\r\n" +
                            ",true,[]],[100,24045,-2102123220,-7175695171900374773,\"-242871073-08-17T14:45:16.399Z\",\"125517-01-13T08:03:16.581566Z\",0.20179749,0.42934437054513563,\"USIMY\",\"XUU\",false,[]]],\"count\":30}\r\n"
                            +
                            "00\r\n\r\n";

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
                "HTTP/1.1 200 OK\r\n" +
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
                        "0746\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]],[109,-8207,-485549586,null,\"278802275-11-05T23:22:18.593Z\",\"122137-10-05T20:22:21.831563Z\",0.5780819,0.18586435581637295,\"DYOPH\",\"IMY\",false,[]],[-44,21057,-1604266757,4598876523645326656,null,\"204480-04-27T20:21:01.380246Z\",0.19736767,0.11591855759299885,\"DMIGQ\",\"VKH\",false,[]],[17,23522,-861621212,-6446120489339099836,null,\"79287-08-03T02:05:46.962686Z\",0.4349324,0.11296257318851766,\"CGFNW\",null,true,[]],[-104,12160,1772084256,-5828188148408093893,\"-270365729-01-24T04:33:47.165Z\",\"-252298-10-09T07:11:36.011048Z\",null,0.5764439692141042,\"BQQEM\",null,false,[]],[-99,-7837,-159178348,null,\"81404961-06-19T18:10:11.037Z\",null,0.5598187,0.5900836401674938,null,\"HPZ\",true,[]],[-127,5343,-238129044,-8851773155849999621,\"-152632412-11-30T22:15:09.334Z\",\"-90192-03-24T17:45:15.784841Z\",0.7806183,null,\"CLNXF\",\"UWP\",false,[]],[-59,-10912,1665107665,-8306574409611146484,\"-243146933-02-10T16:15:15.931Z\",\"-109765-04-18T07:45:05.739795Z\",0.52387,null,\"NIJEE\",\"RUG\",true,[]],[69,4771,21764960,-5708280760166173503,null,\"-248236-04-27T14:06:03.509521Z\",0.77833515,0.533524384058538,\"VOCUG\",\"UNE\",false,[]],[56,-17784,null,5637967617527425113,null,null,null,0.5815065874358148,null,\"EVQ\",true,[]],[58,29019,-416467698,null,\"-175203601-12-02T01:02:02.378Z\",\"201101-10-20T07:35:25.133598Z\",null,0.7430101994511517,\"DXCBJ\",null,true,[]]],\"count\":20}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryCreateInsertStringifiedJson() throws Exception {
        testJsonQuery0(1, engine -> {
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
                            JSON_DDL_RESPONSE,
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
                            "6b\r\n" +
                            "{\"query\":\"data\",\"columns\":[{\"name\":\"s\",\"type\":\"STRING\"}],\"dataset\":[[\"{ title: \\\\\\\"Title\\\\\\\"}\"]],\"count\":1}\r\n" +
                            "00\r\n" +
                            "\r\n",
                    1,
                    0,
                    false
            );
        }, false);
    }

    @Test
    public void testJsonQueryCreateInsertTruncateAndDrop() throws Exception {
        testJsonQuery0(1, engine -> {

            // create table
            sendAndReceive(
                    NetworkFacadeImpl.INSTANCE,
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
                            JSON_DDL_RESPONSE,
                    1,
                    0,
                    false
            );

            // check if we have one record
            sendAndReceive(
                    NetworkFacadeImpl.INSTANCE,
                    "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
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
                            "014c\r\n" +
                            "{\"query\":\"\\n\\nselect * from balances_x latest by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"dataset\":[[1,\"USD\",1500.0,0,\"1970-01-01T01:40:00.000001Z\"]],\"count\":1}\r\n" +
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
                    "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
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
                            "011c\r\n" +
                            "{\"query\":\"\\n\\nselect * from balances_x latest by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"dataset\":[],\"count\":0}\r\n" +
                            "00\r\n" +
                            "\r\n",
                    1,
                    0,
                    false
            );
        }, false);
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
    public void testJsonQueryDataError() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(baseDir, false);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1};
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });

            try (
                    CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                null,
                                workerPool.getWorkerCount(),
                                metrics
                        );
                    }

                    @Override
                    public String getUrl() {
                        return "/query";
                    }
                });

                workerPool.start(LOG);

                try {

                    // send multipart request to server
                    final String request = "GET /query?limit=0%2C1000&count=true&src=con&query=select%20npe()%20from%20long_sequence(1)&timings=true HTTP/1.1\r\n" +
                            "Host: localhost:9000\r\n" +
                            "Connection: keep-alive\r\n" +
                            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36\r\n" +
                            "Accept: */*\r\n" +
                            "Sec-Fetch-Site: same-origin\r\n" +
                            "Sec-Fetch-Mode: cors\r\n" +
                            "Sec-Fetch-Dest: empty\r\n" +
                            "Referer: http://localhost:9000/index.html\r\n" +
                            "Accept-Encoding: gzip, deflate, br\r\n" +
                            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                            "Cookie: _ga=GA1.1.2124932001.1573824669; _hjid=f2db90b2-18cf-4956-8870-fcdcde56f3ca; _hjIncludedInSample=1; _gid=GA1.1.697400188.1591597903\r\n" +
                            "\r\n";

                    long fd = NetworkFacadeImpl.INSTANCE.socketTcp(true);
                    try {
                        long sockAddr = NetworkFacadeImpl.INSTANCE.sockaddr("127.0.0.1", 9001);
                        try {
                            TestUtils.assertConnect(fd, sockAddr);
                            Assert.assertEquals(0, NetworkFacadeImpl.INSTANCE.setTcpNoDelay(fd, true));

                            final int len = request.length() * 2;
                            long ptr = Unsafe.malloc(len);
                            try {
                                int sent = 0;
                                int reqLen = request.length();
                                Chars.asciiStrCpy(request, reqLen, ptr);
                                while (sent < reqLen) {
                                    int n = NetworkFacadeImpl.INSTANCE.send(fd, ptr + sent, reqLen - sent);
                                    Assert.assertTrue(n > -1);
                                    sent += n;
                                }

                                Thread.sleep(1);

                                NetworkFacadeImpl.INSTANCE.configureNonBlocking(fd);
                                long t = System.currentTimeMillis();
                                boolean disconnected = true;
                                while (NetworkFacadeImpl.INSTANCE.recv(fd, ptr, 1) > -1) {
                                    if (t + 20000 < System.currentTimeMillis()) {
                                        disconnected = false;
                                        break;
                                    }
                                }
                                Assert.assertTrue("disconnect expected", disconnected);
                            } finally {
                                Unsafe.free(ptr, len);
                            }
                        } finally {
                            NetworkFacadeImpl.INSTANCE.freeSockAddr(sockAddr);
                        }
                    } finally {
                        NetworkFacadeImpl.INSTANCE.close(fd);
                    }
                } finally {
                    workerPool.halt();
                }
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
        testJsonQuery(20, "GET /query?query=x&cols=k,c,,d,f1,e,g,h,i,j,a,l HTTP/1.1\r\n" +
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
                        "\r\n" +
                        "2c\r\n" +
                        "{\"query\":\"x\",\"error\":\"empty column in list\"}\r\n" +
                        "00\r\n" +
                        "\r\n", 20);
    }

    @Test
    public void testJsonQueryEmptyText() throws Exception {
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
                        "{\"query\":\"\",\"error\":\"No query text\",\"position\":0}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryInfinity() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=select+1.0%2F0.0+from+long_sequence(1)&limit=0%2C1000&count=true&src=con HTTP/1.1\r\n" +
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
                        "7c\r\n" +
                        "{\"query\":\"select 1.0\\/0.0 from long_sequence(1)\",\"columns\":[{\"name\":\"column\",\"type\":\"DOUBLE\"}],\"dataset\":[[null]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryInvalidColumnNameInLimitColumns() throws Exception {
        testJsonQuery(20, "GET /query?query=x&cols=k,c,b,d,f1,e,g,h,i,j,a,l HTTP/1.1\r\n" +
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
                        "\r\n" +
                        "32\r\n" +
                        "{\"query\":\"x\",\"error\":'invalid column in list: f1'}\r\n" +
                        "00\r\n" +
                        "\r\n", 20);
    }

    @Test
    public void testJsonQueryInvalidLastColumnNameInLimitColumns() throws Exception {
        testJsonQuery(20, "GET /query?query=x&cols=k,c,b,d,f,e,g,h,i,j,a,l2 HTTP/1.1\r\n" +
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
                        "\r\n" +
                        "32\r\n" +
                        "{\"query\":\"x\",\"error\":'invalid column in list: l2'}\r\n" +
                        "00\r\n" +
                        "\r\n", 20);
    }

    @Test
    public void testJsonQueryJsonReplaceZero() throws Exception {
        testJsonQuery0(2, engine -> {
            // create table with all column types
            createTestTable(
                    engine.getConfiguration(),
                    20
            );
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
                            "0101\r\n" +
                            "{\"query\":\"y\",\"columns\":[{\"name\":\"j\",\"type\":\"SYMBOL\"}],\"dataset\":[[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"],[\"okok\"]],\"count\":20}\r\n" +
                            "00\r\n" +
                            "\r\n",
                    100,
                    0,
                    false
            );
        }, false);
    }

    @Test
    public void testJsonQueryLimitColumnsBadUtf8() throws Exception {
        testJsonQuery(
                20,
                "GET /query?query=select+%27oops%27+%D1%80%D0%B5%D0%BA%D0%BE%D1%80%D0%B4%D0%BD%D0%BE+from+long_sequence(10)%0A&count=false&cols=�������&src=vis HTTP/1.1\r\n" +
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
                        "ec\r\n" +
                        "{\"query\":\"select 'oops' рекордно from long_sequence(10)\\n\",\"columns\":[{\"name\":\"рекордно\",\"type\":\"STRING\"}],\"dataset\":[[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"],[\"oops\"]],\"count\":10}\r\n" +
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
                        "044c\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]],[109,-8207,-485549586,null,\"278802275-11-05T23:22:18.593Z\",\"122137-10-05T20:22:21.831563Z\",0.5780819,0.18586435581637295,\"DYOPH\",\"IMY\",false,[]],[-44,21057,-1604266757,4598876523645326656,null,\"204480-04-27T20:21:01.380246Z\",0.19736767,0.11591855759299885,\"DMIGQ\",\"VKH\",false,[]],[17,23522,-861621212,-6446120489339099836,null,\"79287-08-03T02:05:46.962686Z\",0.4349324,0.11296257318851766,\"CGFNW\",null,true,[]],[-104,12160,1772084256,-5828188148408093893,\"-270365729-01-24T04:33:47.165Z\",\"-252298-10-09T07:11:36.011048Z\",null,0.5764439692141042,\"BQQEM\",null,false,[]]],\"count\":14}\r\n" +
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
                        "02df\r\n" +
                        "{\"dataset\":[[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]],[109,-8207,-485549586,null,\"278802275-11-05T23:22:18.593Z\",\"122137-10-05T20:22:21.831563Z\",0.5780819,0.18586435581637295,\"DYOPH\",\"IMY\",false,[]],[-44,21057,-1604266757,4598876523645326656,null,\"204480-04-27T20:21:01.380246Z\",0.19736767,0.11591855759299885,\"DMIGQ\",\"VKH\",false,[]],[17,23522,-861621212,-6446120489339099836,null,\"79287-08-03T02:05:46.962686Z\",0.4349324,0.11296257318851766,\"CGFNW\",null,true,[]],[-104,12160,1772084256,-5828188148408093893,\"-270365729-01-24T04:33:47.165Z\",\"-252298-10-09T07:11:36.011048Z\",null,0.5764439692141042,\"BQQEM\",null,false,[]]],\"count\":14}\r\n" +
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
                        "GET /exec?query=xyz%20where%20sym%20%3D%20%27UDEYY%27 HTTP/1.1\r\n" +
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
                                "d9\r\n" +
                                "{\"query\":\"xyz where sym = 'UDEYY'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"dataset\":[[\"UDEYY\",0.15786635599554755],[\"UDEYY\",0.8445258177211064],[\"UDEYY\",0.5778947915182423]],\"count\":3}\r\n" +
                                "00\r\n" +
                                "\r\n"
                },
                {
                        "GET /exec?query=xyz%20where%20sym%20%3D%20%27QEHBH%27 HTTP/1.1\r\n" +
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
                                "0114\r\n" +
                                "{\"query\":\"xyz where sym = 'QEHBH'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"dataset\":[[\"QEHBH\",0.4022810626779558],[\"QEHBH\",0.9038068796506872],[\"QEHBH\",0.05048190020054388],[\"QEHBH\",0.4149517697653501],[\"QEHBH\",0.44804689668613573]],\"count\":5}\r\n" +
                                "00\r\n" +
                                "\r\n"
                },
                {
                        "GET /exec?query=xyz%20where%20sym%20%3D%20%27SXUXI%27 HTTP/1.1\r\n" +
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
                                "{\"query\":\"xyz where sym = 'SXUXI'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"dataset\":[[\"SXUXI\",0.6761934857077543],[\"SXUXI\",0.38642336707855873],[\"SXUXI\",0.48558682958070665]],\"count\":3}\r\n" +
                                "00\r\n" +
                                "\r\n"
                },
                {
                        "GET /exec?query=xyz%20where%20sym%20%3D%20%27VTJWC%27 HTTP/1.1\r\n" +
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
                                "f4\r\n" +
                                "{\"query\":\"xyz where sym = 'VTJWC'\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"},{\"name\":\"d\",\"type\":\"DOUBLE\"}],\"dataset\":[[\"VTJWC\",0.3435685332942956],[\"VTJWC\",0.8258367614088108],[\"VTJWC\",0.437176959518218],[\"VTJWC\",0.7176053468281931]],\"count\":4}\r\n" +
                                "00\r\n" +
                                "\r\n"
                }
        };
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(threadCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {

                    final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
                    try (SqlCompiler compiler = new SqlCompiler(engine)) {
                        compiler.compile("create table xyz as (select rnd_symbol(10, 5, 5, 0) sym, rnd_double() d from long_sequence(30)), index(sym)", sqlExecutionContext);

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
                        "0bdd\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]],[30,32312,-303295973,6854658259142399220,null,\"273652-10-24T01:16:04.499209Z\",0.38179755,0.9687423276940171,\"EDRQQ\",\"LOF\",false,[]],[-79,-21442,1985398001,7522482991756933150,\"279864478-12-31T01:58:35.932Z\",\"20093-07-24T16:56:53.198086Z\",null,0.05384400312338511,\"HVUVS\",\"OTS\",true,[]],[70,-29572,-1966408995,-2406077911451945242,null,\"-254163-09-17T05:33:54.251307Z\",0.81233966,null,\"IKJSM\",\"SUQ\",false,[]],[-97,15913,2011884585,4641238585508069993,\"-277437004-09-03T08:55:41.803Z\",\"186548-11-05T05:57:55.827139Z\",0.89989215,0.6583311519893554,\"ZIMNZ\",\"RMF\",false,[]],[-9,5991,-907794648,null,null,null,0.13264287,null,\"OHNZH\",null,false,[]],[-94,30598,-1510166985,6056145309392106540,null,null,0.54669005,null,\"MZVQE\",\"NDC\",true,[]],[-97,-11913,null,750145151786158348,\"-144112168-08-02T20:50:38.542Z\",\"-279681-08-19T06:26:33.186955Z\",0.8977236,0.5691053034055052,\"WIFFL\",\"BRO\",false,[]],[58,7132,null,6793615437970356479,\"63572238-04-24T11:00:13.287Z\",\"171291-08-24T10:16:32.229138Z\",null,0.7215959171612961,\"KWZLU\",\"GXH\",false,[]],[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]],[109,-8207,-485549586,null,\"278802275-11-05T23:22:18.593Z\",\"122137-10-05T20:22:21.831563Z\",0.5780819,0.18586435581637295,\"DYOPH\",\"IMY\",false,[]],[-44,21057,-1604266757,4598876523645326656,null,\"204480-04-27T20:21:01.380246Z\",0.19736767,0.11591855759299885,\"DMIGQ\",\"VKH\",false,[]],[17,23522,-861621212,-6446120489339099836,null,\"79287-08-03T02:05:46.962686Z\",0.4349324,0.11296257318851766,\"CGFNW\",null,true,[]],[-104,12160,1772084256,-5828188148408093893,\"-270365729-01-24T04:33:47.165Z\",\"-252298-10-09T07:11:36.011048Z\",null,0.5764439692141042,\"BQQEM\",null,false,[]],[-99,-7837,-159178348,null,\"81404961-06-19T18:10:11.037Z\",null,0.5598187,0.5900836401674938,null,\"HPZ\",true,[]],[-127,5343,-238129044,-8851773155849999621,\"-152632412-11-30T22:15:09.334Z\",\"-90192-03-24T17:45:15.784841Z\",0.7806183,null,\"CLNXF\",\"UWP\",false,[]],[-59,-10912,1665107665,-8306574409611146484,\"-243146933-02-10T16:15:15.931Z\",\"-109765-04-18T07:45:05.739795Z\",0.52387,null,\"NIJEE\",\"RUG\",true,[]],[69,4771,21764960,-5708280760166173503,null,\"-248236-04-27T14:06:03.509521Z\",0.77833515,0.533524384058538,\"VOCUG\",\"UNE\",false,[]],[56,-17784,null,5637967617527425113,null,null,null,0.5815065874358148,null,\"EVQ\",true,[]],[58,29019,-416467698,null,\"-175203601-12-02T01:02:02.378Z\",\"201101-10-20T07:35:25.133598Z\",null,0.7430101994511517,\"DXCBJ\",null,true,[]]],\"count\":20}\r\n" +
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
                        "0230\r\n" +
                        "{\"query\":\"\\n\\nselect * from x where i ~ 'E'\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]]],\"count\":5}\r\n" +
                        "00\r\n" +
                        "\r\n"
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
                        "0bda\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[false,-727724771,-13027,8920866532787660373,\"-51129-02-11T06:38:29.397464Z\",\"-169665660-01-09T01:58:28.119Z\",null,null,\"EHNRX\",\"ZSX\",80,[]],[false,-303295973,-11105,6854658259142399220,\"273652-10-24T01:16:04.499209Z\",null,0.38179755,0.9687423276940171,\"EDRQQ\",\"LOF\",30,[]],[true,1985398001,4635,7522482991756933150,\"20093-07-24T16:56:53.198086Z\",\"279864478-12-31T01:58:35.932Z\",null,0.05384400312338511,\"HVUVS\",\"OTS\",-79,[]],[false,-1966408995,-4628,-2406077911451945242,\"-254163-09-17T05:33:54.251307Z\",null,0.81233966,null,\"IKJSM\",\"SUQ\",70,[]],[false,2011884585,-15119,4641238585508069993,\"186548-11-05T05:57:55.827139Z\",\"-277437004-09-03T08:55:41.803Z\",0.89989215,0.6583311519893554,\"ZIMNZ\",\"RMF\",-97,[]],[false,-907794648,30294,null,null,null,0.13264287,null,\"OHNZH\",null,-9,[]],[true,-1510166985,-1315,6056145309392106540,null,null,0.54669005,null,\"MZVQE\",\"NDC\",-94,[]],[false,null,-30006,750145151786158348,\"-279681-08-19T06:26:33.186955Z\",\"-144112168-08-02T20:50:38.542Z\",0.8977236,0.5691053034055052,\"WIFFL\",\"BRO\",-97,[]],[false,null,-5079,6793615437970356479,\"171291-08-24T10:16:32.229138Z\",\"63572238-04-24T11:00:13.287Z\",null,0.7215959171612961,\"KWZLU\",\"GXH\",58,[]],[false,null,30698,-9219078548506735248,\"197633-02-20T09:12:49.579955Z\",\"286623354-12-11T19:15:45.735Z\",null,0.8001632261203552,null,\"KFM\",37,[]],[false,-485549586,10024,null,\"122137-10-05T20:22:21.831563Z\",\"278802275-11-05T23:22:18.593Z\",0.5780819,0.18586435581637295,\"DYOPH\",\"IMY\",109,[]],[false,-1604266757,-13852,4598876523645326656,\"204480-04-27T20:21:01.380246Z\",null,0.19736767,0.11591855759299885,\"DMIGQ\",\"VKH\",-44,[]],[true,-861621212,-20937,-6446120489339099836,\"79287-08-03T02:05:46.962686Z\",null,0.4349324,0.11296257318851766,\"CGFNW\",null,17,[]],[false,1772084256,-23044,-5828188148408093893,\"-252298-10-09T07:11:36.011048Z\",\"-270365729-01-24T04:33:47.165Z\",null,0.5764439692141042,\"BQQEM\",null,-104,[]],[true,-159178348,0,null,null,\"81404961-06-19T18:10:11.037Z\",0.5598187,0.5900836401674938,null,\"HPZ\",-99,[]],[false,-238129044,-32768,-8851773155849999621,\"-90192-03-24T17:45:15.784841Z\",\"-152632412-11-30T22:15:09.334Z\",0.7806183,null,\"CLNXF\",\"UWP\",-127,[]],[true,1665107665,0,-8306574409611146484,\"-109765-04-18T07:45:05.739795Z\",\"-243146933-02-10T16:15:15.931Z\",0.52387,null,\"NIJEE\",\"RUG\",-59,[]],[false,21764960,-32768,-5708280760166173503,\"-248236-04-27T14:06:03.509521Z\",null,0.77833515,0.533524384058538,\"VOCUG\",\"UNE\",69,[]],[true,null,0,5637967617527425113,null,null,null,0.5815065874358148,null,\"EVQ\",56,[]],[true,-416467698,-32768,null,\"201101-10-20T07:35:25.133598Z\",\"-175203601-12-02T01:02:02.378Z\",null,0.7430101994511517,\"DXCBJ\",null,58,[]]],\"count\":20}\r\n" +
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
                        "0185\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
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
                        "cb\r\n" +
                        "{\"query\":\"select rnd_symbol('a','b','c') sym from long_sequence(10, 33, 55)\",\"columns\":[{\"name\":\"sym\",\"type\":\"SYMBOL\"}],\"dataset\":[[\"c\"],[\"c\"],[\"c\"],[\"b\"],[\"b\"],[\"a\"],[\"a\"],[\"a\"],[\"a\"],[\"a\"]],\"count\":10}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryRenameTable() throws Exception {
        testJsonQuery0(2, engine -> {
            // create table with all column types
            CairoTestUtils.createTestTable(
                    engine.getConfiguration(),
                    20,
                    new Rnd(),
                    new TestRecord.ArrayBinarySequence());

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
                            "0224\r\n" +
                            "{\"query\":\"y where i = ('EHNRX')\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]]],\"count\":1}\r\n" +
                            "00\r\n" +
                            "\r\n",
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
                            "0224\r\n" +
                            "{\"query\":\"x where i = ('EHNRX')\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]]],\"count\":1}\r\n" +
                            "00\r\n" +
                            "\r\n",
                    1,
                    0,
                    false
            );
        }, false);
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
                        "02a6\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]],[109,-8207,-485549586,null,\"278802275-11-05T23:22:18.593Z\",\"122137-10-05T20:22:21.831563Z\",0.5780819,0.18586435581637295,\"DYOPH\",\"IMY\",false,[]]],\"count\":11}\r\n" +
                        "00\r\n" +
                        "\r\n");
    }

    @Test
    public void testJsonQuerySelectAlterSelect() throws Exception {
        testJsonQuery0(1, engine -> {

            // create table
            sendAndReceive(
                    NetworkFacadeImpl.INSTANCE,
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
                            JSON_DDL_RESPONSE,
                    1,
                    0,
                    false
            );

            // check if we have one record
            sendAndReceive(
                    NetworkFacadeImpl.INSTANCE,
                    "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
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
                            "014c\r\n" +
                            "{\"query\":\"\\n\\nselect * from balances_x latest by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"dataset\":[[1,\"USD\",1500.0,0,\"1970-01-01T01:40:00.000001Z\"]],\"count\":1}\r\n" +
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
                            "0d\r\n" +
                            "{\"ddl\":\"OK\"}\n\r\n" +
                            "00\r\n" +
                            "\r\n",
                    1,
                    0,
                    false
            );

            // select again expecting only metadata
            sendAndReceive(
                    NetworkFacadeImpl.INSTANCE,
                    "GET /query?query=%0A%0Aselect+*+from+balances_x+latest+by+cust_id%2C+balance_ccy&limit=0%2C1000&count=true HTTP/1.1\r\n" +
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
                            "016d\r\n" +
                            "{\"query\":\"\\n\\nselect * from balances_x latest by cust_id, balance_ccy\",\"columns\":[{\"name\":\"cust_id\",\"type\":\"INT\"},{\"name\":\"balance_ccy\",\"type\":\"SYMBOL\"},{\"name\":\"balance\",\"type\":\"DOUBLE\"},{\"name\":\"status\",\"type\":\"BYTE\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"},{\"name\":\"xyz\",\"type\":\"INT\"}],\"dataset\":[[1,\"USD\",1500.0,0,\"1970-01-01T01:40:00.000001Z\",null]],\"count\":1}\r\n" +
                            "00\r\n" +
                            "\r\n",
                    1,
                    0,
                    false
            );
        }, false);
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
                        "0224\r\n" +
                        "{\"query\":\"x where i = ('EHNRX')\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]]],\"count\":1}\r\n" +
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
                        "0185\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n",
                1,
                true
        );

        final String expectedEvent = "100\n" +
                "1\n" +
                "101\n";
        assertColumn(expectedEvent, 1);

        final String expectedOrigin = "1\n" +
                "2\n" +
                "1\n";
        assertColumn(expectedOrigin, 2);
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
                        "0185\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n",
                2,
                true
        );

        final String expected = "100\n" +
                "1\n" +
                "1\n" +
                "101\n";
        assertColumn(expected, 1);

        final String expectedOrigin = "1\n" +
                "2\n" +
                "2\n" +
                "1\n";
        assertColumn(expectedOrigin, 2);
    }

    @Test
    public void testJsonQuerySyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(baseDir, false);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1};
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });

            try (
                    CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                null,
                                workerPool.getWorkerCount(),
                                metrics
                        );
                    }

                    @Override
                    public String getUrl() {
                        return "/query";
                    }
                });

                workerPool.start(LOG);

                try {

                    // create table with all column types
                    CairoTestUtils.createTestTable(
                            engine.getConfiguration(),
                            20,
                            new Rnd(),
                            new TestRecord.ArrayBinarySequence());

                    // send multipart request to server
                    final String request = "GET /query?query=x%20where2%20i%20%3D%20(%27EHNRX%27) HTTP/1.1\r\n" +
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
                            "4d\r\n" +
                            "{\"query\":\"x where2 i = ('EHNRX')\",\"error\":\"unexpected token: i\",\"position\":9}\r\n" +
                            "00\r\n" +
                            "\r\n";

                    sendAndReceive(
                            NetworkFacadeImpl.INSTANCE,
                            request,
                            expectedResponse,
                            10,
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
                        "06ac\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]],[30,32312,-303295973,6854658259142399220,null,\"273652-10-24T01:16:04.499209Z\",0.38179755,0.9687423276940171,\"EDRQQ\",\"LOF\",false,[]],[-79,-21442,1985398001,7522482991756933150,\"279864478-12-31T01:58:35.932Z\",\"20093-07-24T16:56:53.198086Z\",null,0.05384400312338511,\"HVUVS\",\"OTS\",true,[]],[70,-29572,-1966408995,-2406077911451945242,null,\"-254163-09-17T05:33:54.251307Z\",0.81233966,null,\"IKJSM\",\"SUQ\",false,[]],[-97,15913,2011884585,4641238585508069993,\"-277437004-09-03T08:55:41.803Z\",\"186548-11-05T05:57:55.827139Z\",0.89989215,0.6583311519893554,\"ZIMNZ\",\"RMF\",false,[]],[-9,5991,-907794648,null,null,null,0.13264287,null,\"OHNZH\",null,false,[]],[-94,30598,-1510166985,6056145309392106540,null,null,0.54669005,null,\"MZVQE\",\"NDC\",true,[]],[-97,-11913,null,750145151786158348,\"-144112168-08-02T20:50:38.542Z\",\"-279681-08-19T06:26:33.186955Z\",0.8977236,0.5691053034055052,\"WIFFL\",\"BRO\",false,[]],[58,7132,null,6793615437970356479,\"63572238-04-24T11:00:13.287Z\",\"171291-08-24T10:16:32.229138Z\",null,0.7215959171612961,\"KWZLU\",\"GXH\",false,[]],[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]]],\"count\":10}\r\n" +
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
                        "06ac\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]],[30,32312,-303295973,6854658259142399220,null,\"273652-10-24T01:16:04.499209Z\",0.38179755,0.9687423276940171,\"EDRQQ\",\"LOF\",false,[]],[-79,-21442,1985398001,7522482991756933150,\"279864478-12-31T01:58:35.932Z\",\"20093-07-24T16:56:53.198086Z\",null,0.05384400312338511,\"HVUVS\",\"OTS\",true,[]],[70,-29572,-1966408995,-2406077911451945242,null,\"-254163-09-17T05:33:54.251307Z\",0.81233966,null,\"IKJSM\",\"SUQ\",false,[]],[-97,15913,2011884585,4641238585508069993,\"-277437004-09-03T08:55:41.803Z\",\"186548-11-05T05:57:55.827139Z\",0.89989215,0.6583311519893554,\"ZIMNZ\",\"RMF\",false,[]],[-9,5991,-907794648,null,null,null,0.13264287,null,\"OHNZH\",null,false,[]],[-94,30598,-1510166985,6056145309392106540,null,null,0.54669005,null,\"MZVQE\",\"NDC\",true,[]],[-97,-11913,null,750145151786158348,\"-144112168-08-02T20:50:38.542Z\",\"-279681-08-19T06:26:33.186955Z\",0.8977236,0.5691053034055052,\"WIFFL\",\"BRO\",false,[]],[58,7132,null,6793615437970356479,\"63572238-04-24T11:00:13.287Z\",\"171291-08-24T10:16:32.229138Z\",null,0.7215959171612961,\"KWZLU\",\"GXH\",false,[]],[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]]],\"count\":20}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonQueryTopLimitHttp1() throws Exception {
        testJsonQuery0(2, engine -> {
                    // create table with all column types
                    CairoTestUtils.createTestTable(
                            engine.getConfiguration(),
                            20,
                            new Rnd(),
                            new TestRecord.ArrayBinarySequence()
                    );
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
                                    "06ac\r\n" +
                                    "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[[80,24814,-727724771,8920866532787660373,\"-169665660-01-09T01:58:28.119Z\",\"-51129-02-11T06:38:29.397464Z\",null,null,\"EHNRX\",\"ZSX\",false,[]],[30,32312,-303295973,6854658259142399220,null,\"273652-10-24T01:16:04.499209Z\",0.38179755,0.9687423276940171,\"EDRQQ\",\"LOF\",false,[]],[-79,-21442,1985398001,7522482991756933150,\"279864478-12-31T01:58:35.932Z\",\"20093-07-24T16:56:53.198086Z\",null,0.05384400312338511,\"HVUVS\",\"OTS\",true,[]],[70,-29572,-1966408995,-2406077911451945242,null,\"-254163-09-17T05:33:54.251307Z\",0.81233966,null,\"IKJSM\",\"SUQ\",false,[]],[-97,15913,2011884585,4641238585508069993,\"-277437004-09-03T08:55:41.803Z\",\"186548-11-05T05:57:55.827139Z\",0.89989215,0.6583311519893554,\"ZIMNZ\",\"RMF\",false,[]],[-9,5991,-907794648,null,null,null,0.13264287,null,\"OHNZH\",null,false,[]],[-94,30598,-1510166985,6056145309392106540,null,null,0.54669005,null,\"MZVQE\",\"NDC\",true,[]],[-97,-11913,null,750145151786158348,\"-144112168-08-02T20:50:38.542Z\",\"-279681-08-19T06:26:33.186955Z\",0.8977236,0.5691053034055052,\"WIFFL\",\"BRO\",false,[]],[58,7132,null,6793615437970356479,\"63572238-04-24T11:00:13.287Z\",\"171291-08-24T10:16:32.229138Z\",null,0.7215959171612961,\"KWZLU\",\"GXH\",false,[]],[37,7618,null,-9219078548506735248,\"286623354-12-11T19:15:45.735Z\",\"197633-02-20T09:12:49.579955Z\",null,0.8001632261203552,null,\"KFM\",false,[]]],\"count\":10}\r\n" +
                                    "00\r\n" +
                                    "\r\n",
                            1,
                            0,
                            false,
                            true
                    );
                },
                false,
                true
        );
    }

    @Test
    public void testJsonQueryWithCompressedResults1() throws Exception {
        Zip.init();
        assertMemoryLeak(() -> {
            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(nf, baseDir, 256, false, true);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });
            try (
                    CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                null,
                                workerPool.getWorkerCount(),
                                metrics);
                    }

                    @Override
                    public String getUrl() {
                        return "/query";
                    }
                });

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    CairoTestUtils.createTestTable(
                            engine.getConfiguration(),
                            30,
                            new Rnd(),
                            new TestRecord.ArrayBinarySequence());

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
                    sendAndReceive(nf, request, expectedResponse, 10, 100L, false);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testJsonQueryWithCompressedResults2() throws Exception {
        Zip.init();
        assertMemoryLeak(() -> {
            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(nf, baseDir, 4096, false, true);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });
            try (
                    CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(
                                httpConfiguration.getJsonQueryProcessorConfiguration(),
                                engine,
                                null,
                                workerPool.getWorkerCount(),
                                metrics);
                    }

                    @Override
                    public String getUrl() {
                        return "/query";
                    }
                });

                workerPool.start(LOG);

                try {
                    // create table with all column types
                    CairoTestUtils.createTestTable(
                            engine.getConfiguration(),
                            1000,
                            new Rnd(),
                            new TestRecord.ArrayBinarySequence());

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
            final String baseDir = temp.getRoot().getAbsolutePath();
            final int tableRowCount = 300_000;

            SOCountDownLatch peerDisconnectLatch = new SOCountDownLatch(1);

            DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withNetwork(nf)
                    .withBaseDir(baseDir)
                    .withSendBufferSize(256)
                    .withDumpingTraffic(false)
                    .withAllowDeflateBeforeSend(false)
                    .withServerKeepAlive(true)
                    .withHttpProtocolVersion("HTTP/1.1 ")
                    .withOnPeerDisconnect(peerDisconnectLatch::countDown)
                    .build();
            QueryCache.configure(httpConfiguration);


            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1};
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });

            try (
                    CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(baseDir));
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)
            ) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new JsonQueryProcessor(httpConfiguration.getJsonQueryProcessorConfiguration(), engine,
                                null, workerPool.getWorkerCount(), metrics);
                    }

                    @Override
                    public String getUrl() {
                        return "/query";
                    }
                });

                final AtomicBoolean clientClosed = new AtomicBoolean(false);
                final int minClientReceivedBytesBeforeDisconnect = 180;
                final AtomicLong refClientFd = new AtomicLong(-1);
                HttpClientStateListener clientStateListener = new HttpClientStateListener() {
                    private int nBytesReceived = 0;

                    @Override
                    public void onClosed() {
                    }

                    @Override
                    public void onReceived(int nBytes) {
                        LOG.info().$("Client received ").$(nBytes).$(" bytes").$();
                        nBytesReceived += nBytes;
                        if (nBytesReceived >= minClientReceivedBytesBeforeDisconnect) {
                            long fd = refClientFd.get();
                            if (fd != -1) {
                                refClientFd.set(-1);
                                nf.close(fd);
                                clientClosed.set(true);
                            }
                        }
                    }
                };
                workerPool.start(LOG);

                try {
                    // create table with all column types
                    CairoTestUtils.createTestTable(
                            engine.getConfiguration(),
                            tableRowCount,
                            new Rnd(),
                            new TestRecord.ArrayBinarySequence()
                    );

                    // send multipart request to server
                    final String request = "GET /query?query=select+distinct+a+from+x+where+test_latched_counter() HTTP/1.1\r\n"
                            + "Host: localhost:9001\r\n" + "Connection: keep-alive\r\n" + "Cache-Control: max-age=0\r\n"
                            + "Upgrade-Insecure-Requests: 1\r\n"
                            + "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\r\n"
                            + "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3\r\n"
                            + "Accept-Encoding: gzip, deflate, br\r\n"
                            + "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" + "\r\n";

                    long fd = nf.socketTcp(true);
                    try {
                        long sockAddr = nf.sockaddr("127.0.0.1", 9001);
                        try {
                            TestUtils.assertConnect(fd, sockAddr);
                            Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
                            refClientFd.set(fd);
                            nf.configureNonBlocking(fd);

                            long bufLen = request.length();
                            long ptr = Unsafe.malloc(bufLen);
                            try {
                                new SendAndReceiveRequestBuilder()
                                        .withNetworkFacade(nf)
                                        .withPauseBetweenSendAndReceive(0)
                                        .withPrintOnly(false)
                                        .withExpectDisconnect(true)
                                        .executeUntilDisconnect(request, fd, 200, ptr, clientStateListener);
                            } finally {
                                Unsafe.free(ptr, bufLen);
                            }
                        } finally {
                            nf.freeSockAddr(sockAddr);
                        }
                    } finally {
                        LOG.info().$("Closing client connection").$();
                    }
                    peerDisconnectLatch.await();
                    // depending on how quick the CI hardware is we may end up processing different
                    // number of rows before query is interrupted
                    Assert.assertTrue(tableRowCount > TestLatchedCounterFunctionFactory.getCount());
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
                        "0185\r\n" +
                        "{\"query\":\"x\",\"columns\":[{\"name\":\"a\",\"type\":\"BYTE\"},{\"name\":\"b\",\"type\":\"SHORT\"},{\"name\":\"c\",\"type\":\"INT\"},{\"name\":\"d\",\"type\":\"LONG\"},{\"name\":\"e\",\"type\":\"DATE\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"FLOAT\"},{\"name\":\"h\",\"type\":\"DOUBLE\"},{\"name\":\"i\",\"type\":\"STRING\"},{\"name\":\"j\",\"type\":\"SYMBOL\"},{\"name\":\"k\",\"type\":\"BOOLEAN\"},{\"name\":\"l\",\"type\":\"BINARY\"}],\"dataset\":[],\"count\":0}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testJsonUtf8EncodedColumnName() throws Exception {
        testJsonQuery(0, "GET /query?query=select+0+%D1%80%D0%B5%D0%BA%D0%BE%D1%80%D0%B4%D0%BD%D0%BE+from+long_sequence(10)&limit=0%2C1000&count=true&src=con HTTP/1.1\r\n" +
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
                        "b0\r\n" +
                        "{\"query\":\"select 0 рекордно from long_sequence(10)\",\"columns\":[{\"name\":\"рекордно\",\"type\":\"INT\"}],\"dataset\":[[0],[0],[0],[0],[0],[0],[0],[0],[0],[0]],\"count\":10}\r\n" +
                        "00\r\n" +
                        "\r\n"
                , 1);
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
                        "cb\r\n" +
                        "{\"query\":\"\\n\\n\\n\\nSELECT 'Raphaël' a, 'Léo' b FROM long_sequence(2)\",\"columns\":[{\"name\":\"a\",\"type\":\"STRING\"},{\"name\":\"b\",\"type\":\"STRING\"}],\"dataset\":[[\"Raphaël\",\"Léo\"],[\"Raphaël\",\"Léo\"]],\"count\":2}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }

    @Test
    public void testMaxConnections() throws Exception {
        LOG.info().$("started maxConnections").$();
        assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            final int listenBackLog = 10;
            final int activeConnectionLimit = 5;
            final int nExcessConnections = 20;

            AtomicInteger openCount = new AtomicInteger(0);
            AtomicInteger closeCount = new AtomicInteger(0);

            final IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration() {
                @Override
                public int getActiveConnectionLimit() {
                    return activeConnectionLimit;
                }

                @Override
                public int getListenBacklog() {
                    return listenBackLog;
                }

                @Override
                public long getQueuedConnectionTimeout() {
                    return 300_000;
                }
            };

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    configuration,
                    new IOContextFactory<HttpConnectionContext>() {
                        @SuppressWarnings("resource")
						@Override
                        public HttpConnectionContext newInstance(long fd, IODispatcher<HttpConnectionContext> dispatcher1) {
                            openCount.incrementAndGet();
                            return new HttpConnectionContext(httpServerConfiguration.getHttpContextConfiguration()) {
                                @Override
                                public void close() {
                                    closeCount.incrementAndGet();
                                    super.close();
                                }
                            }.of(fd, dispatcher1);
                        }
                    }
            )) {
                HttpRequestProcessorSelector selector =
                        new HttpRequestProcessorSelector() {
                            @Override
                            public HttpRequestProcessor select(CharSequence url) {
                                return null;
                            }

                            @Override
                            public HttpRequestProcessor getDefaultProcessor() {
                                return new HealthCheckProcessor();
                            }

                            @Override
                            public void close() {
                            }
                        };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                	try {
	                    do {
	                        dispatcher.run(0);
	                        dispatcher.processIOQueue(
	                                (operation, context) -> context.handleClientOperation(operation, selector, EmptyRescheduleContext)
	                        );
	                    } while (serverRunning.get());
                	} finally {
                		serverHaltLatch.countDown();
                	}
                }).start();

                LongList openFds = new LongList();
                LongHashSet closedFds = new LongHashSet();

                final long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                final long buf = Unsafe.malloc(4096);
                final int N = activeConnectionLimit + listenBackLog;
                try {
                    for (int i = 0; i < N; i++) {
                        long fd = Net.socketTcp(true);
                        TestUtils.assertConnect(fd, sockAddr);
                        openFds.add(fd);
                    }

                    // let dispatcher catchup
                    long startNanos = System.nanoTime();
                    while (dispatcher.isListening()) {
                        long endNanos = System.nanoTime();
                        if (TimeUnit.NANOSECONDS.toSeconds(endNanos - startNanos) > 30) {
                            Assert.fail("Timed out waiting for despatcher to stop listening");
                        }
                        LockSupport.parkNanos(1);
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

                    // Active connection limit is reached and backlog is full, check we get connection closed
                    for (int i = 0; i < nExcessConnections; i++) {
                        long fd = Net.socketTcp(true);
                        if (fd > 0) {
                            int nSent = Net.send(fd, mem, request.length());
                            Assert.assertTrue(nSent < 0);
                            Net.close(fd);
                        }
                    }

                    try {
                        for (int i = 0; i < N; i++) {
                            if (i == activeConnectionLimit) {
                                for (int j = 0; j < activeConnectionLimit; j++) {
                                	long fd = openFds.getQuick(j);
                                    Net.close(fd);
                                    closedFds.add(fd);
                               }
                            }

                            long fd = openFds.getQuick(i);
                            Assert.assertEquals(request.length(), Net.send(fd, mem, request.length()));
                            // ensure we have response from server
                            Assert.assertTrue(0 < Net.recv(fd, buf, 64));

                            if (i < activeConnectionLimit) {
                                // dont close any connections until the first connections have been processed and check that the dispatcher
                                // is not listening
                                Assert.assertFalse(dispatcher.isListening());
                            } else {
                                Net.close(fd);
                                closedFds.add(fd);
                            }
                        }
                    } finally {
                        Unsafe.free(mem, request.length());
                    }
                } finally {
                    for (int i=0; i<openFds.size(); i++) {
                    	long fd = openFds.getQuick(i);
                    	if (! closedFds.contains(fd)) {
                    		Net.close(fd);
                    	}
                    }
                    Net.freeSockAddr(sockAddr);
                    Unsafe.free(buf, 4096);
                    Assert.assertFalse(configuration.getActiveConnectionLimit() < dispatcher.getConnectionCount());
                    serverRunning.set(false);
                    serverHaltLatch.await();
                }
            }
        });
    }

    @Test
    public void testQueuedConnectionTimeout() throws Exception {
        LOG.info().$("started testQueuedConnectionTimeout").$();
        assertMemoryLeak(() -> {
            final int listenBackLog = 10;
            final int activeConnectionLimit = 5;
            final long queuedConnectionTimeoutInMs = 250;

            final IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration() {
                @Override
                public int getActiveConnectionLimit() {
                    return activeConnectionLimit;
                }

                @Override
                public int getListenBacklog() {
                    return listenBackLog;
                }

                @Override
                public long getQueuedConnectionTimeout() {
                    return queuedConnectionTimeoutInMs;
                }
            };

            final AtomicInteger nConnected = new AtomicInteger();
            final LongHashSet serverConnectedFds = new LongHashSet();
            final LongHashSet clientActiveFds = new LongHashSet();
            IOContextFactory<IOContext> contextFactory = new IOContextFactory<IOContext>() {
                @Override
                public IOContext newInstance(long fd, IODispatcher<IOContext> dispatcher) {
                    LOG.info().$(fd).$(" connected").$();
                    serverConnectedFds.add(fd);
                    nConnected.incrementAndGet();
                    return new IOContext() {
                        @Override
                        public boolean invalid() {
                            return !serverConnectedFds.contains(fd);
                        }

                        @Override
                        public long getFd() {
                            return fd;
                        }

                        @Override
                        public IODispatcher<?> getDispatcher() {
                            return dispatcher;
                        }

                        @Override
                        public void close() {
                            LOG.info().$(fd).$(" disconnected").$();
                            serverConnectedFds.remove(fd);
                        }
                    };
                }
            };
            final String request = "\n";
            long mem = TestUtils.toMemory(request);

            final long sockAddr = Net.sockaddr("127.0.0.1", 9001);
            Thread serverThread = null;
            final CountDownLatch serverLatch = new CountDownLatch(1);
            try (IODispatcher<IOContext> dispatcher = IODispatchers.create(configuration, contextFactory)) {
                serverThread = new Thread("test-io-dispatcher") {
                    @Override
                    public void run() {
                        long smem = Unsafe.malloc(1);
                        try {
                            IORequestProcessor<IOContext> requestProcessor = new IORequestProcessor<IOContext>() {
                                @Override
                                public void onRequest(int operation, IOContext context) {
                                    long fd = context.getFd();
                                    int rc;
                                    switch(operation) {
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
                                        default:
                                            dispatcher.disconnect(context, IODispatcher.DISCONNECT_REASON_TEST);
                                    }
                                }
                            };
                            do {
                                dispatcher.run(0);
                                dispatcher.processIOQueue(requestProcessor);
                                Thread.yield();
                            } while (!isInterrupted());
                        } finally {
                            Unsafe.free(smem, 1);
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
                    Assert.assertTrue(fd > -1);
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
                    Assert.assertTrue(fd > -1);
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
                Net.freeSockAddr(sockAddr);
                Unsafe.free(mem, request.length());
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
        testJsonQuery0(2, engine -> {
            long fd = NetworkFacadeImpl.INSTANCE.socketTcp(true);
            try {
                long sockAddr = NetworkFacadeImpl.INSTANCE.sockaddr("127.0.0.1", 9001);
                try {
                    TestUtils.assertConnect(fd, sockAddr);
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
                    long ptr = Unsafe.malloc(len);
                    try {
                        int sent = 0;
                        int reqLen = request.length();
                        Chars.asciiStrCpy(request, reqLen, ptr);
                        boolean disconnected = false;
                        while (sent < reqLen) {
                            int n = nf.send(fd, ptr + sent, reqLen - sent);
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
                                int n = nf.recv(fd, ptr, len);
                                if (n < 0) {
                                    break;
                                }
                            }
                        }
                    } finally {
                        Unsafe.free(ptr, len);
                    }
                } finally {
                    NetworkFacadeImpl.INSTANCE.freeSockAddr(sockAddr);
                }
            } finally {
                NetworkFacadeImpl.INSTANCE.close(fd);
            }
        }, false);
    }

    @Test
    public void testPostRequestToGetProcessor() throws Exception {
        testImport(
                "HTTP/1.1 404 Not Found\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "2a\r\n" +
                        "Bad request. non-multipart GET expected.\r\n" +
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
                true,
                1
        );
    }

    @Test
    public void testSCPConnectDownloadDisconnect() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(baseDir, false);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });
            try (HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                workerPool.start(LOG);

                // create 20Mb file in /tmp directory
                try (Path path = new Path().of(baseDir).concat("questdb-temp.txt").$()) {
                    try {
                        Rnd rnd = new Rnd();
                        final int diskBufferLen = 1024 * 1024;

                        writeRandomFile(path, rnd, 122222212222L);

//                        httpServer.getStartedLatch().await();

                        long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                        try {
                            int netBufferLen = 4 * 1024;
                            long buffer = Unsafe.calloc(netBufferLen);
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
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, diskBufferLen, expectedResponseHeader, 20971670);
                                    } finally {
                                        Net.close(fd);
                                    }
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
                                        "Content-Type: text/html; charset=utf-8\r\n" +
                                        "\r\n";

                                for (int i = 0; i < 3; i++) {
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request2, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, 0, expectedResponseHeader2, 126);
                                    } finally {
                                        Net.close(fd);
                                    }
                                }

                                // couple more full downloads after 304
                                for (int j = 0; j < 2; j++) {
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, diskBufferLen, expectedResponseHeader, 20971670);
                                    } finally {
                                        Net.close(fd);
                                    }
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
                                Unsafe.free(buffer, netBufferLen);
                            }
                        } finally {
                            Net.freeSockAddr(sockAddr);
                        }
                    } finally {
                        workerPool.halt();
                        Files.remove(path);
                    }
                }
            }
        });
    }

    @Test
    public void testSCPFullDownload() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(baseDir, false);
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });
            try (HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                workerPool.start(LOG);

                // create 20Mb file in /tmp directory
                try (Path path = new Path().of(baseDir).concat("questdb-temp.txt").$()) {
                    try {
                        Rnd rnd = new Rnd();
                        final int diskBufferLen = 1024 * 1024;

                        writeRandomFile(path, rnd, 122299092L);

                        long fd = Net.socketTcp(true);
                        try {
                            long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                            try {
                                TestUtils.assertConnect(fd, sockAddr);

                                int netBufferLen = 4 * 1024;
                                long buffer = Unsafe.calloc(netBufferLen);
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
                                            "ETag: \"122299092\"\r\n" + // this is last modified timestamp on the file, we set this value when we created file
                                            "\r\n";

                                    for (int j = 0; j < 10; j++) {
                                        sendRequest(request, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, diskBufferLen, expectedResponseHeader, 20971667);
                                    }
//
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
                                            "Content-Type: text/html; charset=utf-8\r\n" +
                                            "\r\n";

                                    for (int i = 0; i < 3; i++) {
                                        sendRequest(request2, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, 0, expectedResponseHeader2, 126);
                                    }

                                    // couple more full downloads after 304
                                    for (int j = 0; j < 2; j++) {
                                        sendRequest(request, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, diskBufferLen, expectedResponseHeader, 20971667);
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
                                    Unsafe.free(buffer, netBufferLen);
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
                        Files.remove(path);
                    }
                }
            }
        });
    }

    @Test
    public void testSCPHttp10() throws Exception {
        assertMemoryLeak(() -> {
            final String baseDir = temp.getRoot().getAbsolutePath();
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(
                    NetworkFacadeImpl.INSTANCE,
                    baseDir,
                    16 * 1024,
                    false,
                    false,
                    false,
                    "HTTP/1.0 "
            );
            final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });
            try (HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, false)) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public HttpRequestProcessor newInstance() {
                        return new StaticContentProcessor(httpConfiguration);
                    }

                    @Override
                    public String getUrl() {
                        return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                    }
                });

                workerPool.start(LOG);

                // create 20Mb file in /tmp directory
                try (Path path = new Path().of(baseDir).concat("questdb-temp.txt").$()) {
                    try {
                        Rnd rnd = new Rnd();
                        final int diskBufferLen = 1024 * 1024;

                        writeRandomFile(path, rnd, 122222212222L);

                        long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                        try {
                            int netBufferLen = 4 * 1024;
                            long buffer = Unsafe.calloc(netBufferLen);
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

                                for (int j = 0; j < 1; j++) {
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request, fd, buffer);
                                        assertDownloadResponse(
                                                fd,
                                                rnd,
                                                buffer,
                                                netBufferLen,
                                                diskBufferLen,
                                                expectedResponseHeader,
                                                20971670
                                        );
                                    } finally {
                                        Net.close(fd);
                                    }
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

                                String expectedResponseHeader2 = "HTTP/1.0 304 Not Modified\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Content-Type: text/html; charset=utf-8\r\n" +
                                        "Connection: close\r\n" +
                                        "\r\n";

                                for (int i = 0; i < 3; i++) {
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request2, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, 0, expectedResponseHeader2, 126);
                                    } finally {
                                        Net.close(fd);
                                    }
                                }

                                // couple more full downloads after 304
                                for (int j = 0; j < 2; j++) {
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, diskBufferLen, expectedResponseHeader, 20971670);
                                    } finally {
                                        Net.close(fd);
                                    }
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


                                for (int i = 0; i < 4; i++) {
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request3, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, 0, expectedResponseHeader3, expectedResponseHeader3.length());
                                    } finally {
                                        Net.close(fd);
                                    }
                                }

                                // and few more 304s

                                for (int i = 0; i < 3; i++) {
                                    long fd = Net.socketTcp(true);
                                    TestUtils.assertConnect(fd, sockAddr);
                                    try {
                                        sendRequest(request2, fd, buffer);
                                        assertDownloadResponse(fd, rnd, buffer, netBufferLen, 0, expectedResponseHeader2, 126);
                                    } finally {
                                        Net.close(fd);
                                    }
                                }

                            } finally {
                                Unsafe.free(buffer, netBufferLen);
                            }
                        } finally {
                            Net.freeSockAddr(sockAddr);
                            workerPool.halt();
                        }
                    } finally {
                        Files.remove(path);
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
        final String expected = "GET /status?x=1&a=&b&c&d=x HTTP/1.1\r\n" +
                "host:localhost:9000\r\n" +
                "connection:keep-alive\r\n" +
                "cache-control:max-age=0\r\n" +
                "accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "user-agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "accept-encoding:gzip,deflate,sdch\r\n" +
                "accept-language:en-US,en;q=0.8\r\n" +
                "cookie:textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            SOCountDownLatch requestReceivedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd, IODispatcher<HttpConnectionContext> dispatcher1) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration.getHttpContextConfiguration()) {
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
                            }.of(fd, dispatcher1);
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                final HttpRequestProcessorSelector selector =

                        new HttpRequestProcessorSelector() {
                            @Override
                            public HttpRequestProcessor select(CharSequence url) {
                                return new HttpRequestProcessor() {
                                    @Override
                                    public void onHeadersReady(HttpConnectionContext context) {
                                        HttpRequestHeader headers = context.getRequestHeader();
                                        sink.put(headers.getMethodLine());
                                        sink.put("\r\n");
                                        ObjList<CharSequence> headerNames = headers.getHeaderNames();
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

                            @Override
                            public HttpRequestProcessor getDefaultProcessor() {
                                return null;
                            }

                            @Override
                            public void close() {
                            }
                        };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(
                                (operation, context) -> context.handleClientOperation(operation, selector, EmptyRescheduleContext)
                        );
                    }
                    serverHaltLatch.countDown();
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
                            Unsafe.free(buffer, len);
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
        final String expected = "GET /status?x=1&a=&b&c&d=x HTTP/1.1\r\n" +
                "host:localhost:9000\r\n" +
                "connection:keep-alive\r\n" +
                "cache-control:max-age=0\r\n" +
                "accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "user-agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "accept-encoding:gzip,deflate,sdch\r\n" +
                "accept-language:en-US,en;q=0.8\r\n" +
                "cookie:textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
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
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration(
                    new DefaultHttpContextConfiguration() {
                        @Override
                        public MillisecondClock getClock() {
                            return () -> 0;
                        }
                    }
            );

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd, IODispatcher<HttpConnectionContext> dispatcher1) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration.getHttpContextConfiguration()) {
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
                            }.of(fd, dispatcher1);
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                final HttpRequestProcessorSelector selector =

                        new HttpRequestProcessorSelector() {

                            @Override
                            public HttpRequestProcessor select(CharSequence url) {
                                return null;
                            }

                            @Override
                            public HttpRequestProcessor getDefaultProcessor() {
                                return new HttpRequestProcessor() {
                                    @Override
                                    public void onHeadersReady(HttpConnectionContext context) {
                                        HttpRequestHeader headers = context.getRequestHeader();
                                        sink.put(headers.getMethodLine());
                                        sink.put("\r\n");
                                        ObjList<CharSequence> headerNames = headers.getHeaderNames();
                                        for (int i = 0, n = headerNames.size(); i < n; i++) {
                                            sink.put(headerNames.getQuick(i)).put(':');
                                            sink.put(headers.getHeader(headerNames.getQuick(i)));
                                            sink.put("\r\n");
                                        }
                                        sink.put("\r\n");
                                    }

                                    @Override
                                    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
                                        context.simpleResponse().sendStatusWithDefaultMessage(200);
                                    }
                                };
                            }

                            @Override
                            public void close() {
                            }
                        };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(
                                (operation, context) -> context.handleClientOperation(operation, selector, EmptyRescheduleContext)
                        );
                    }
                    serverHaltLatch.countDown();
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
                                Assert.assertTrue(n > 0);

                                for (int i = 0; i < n; i++) {
                                    sink2.put((char) Unsafe.getUnsafe().getByte(buffer + i));
                                }
                                // copy response bytes to sink
                                read += n;
                            }

                            TestUtils.assertEquals(expectedResponse, sink2);
                        } finally {
                            Unsafe.free(buffer, len);
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
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public long getIdleConnectionTimeout() {
                            // 0.5s idle timeout
                            return 500;
                        }

                        @Override
                        public boolean getPeerNoLinger() {
                            return false;
                        }
                    },
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd, IODispatcher<HttpConnectionContext> dispatcher1) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration.getHttpContextConfiguration()) {
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
                            }.of(fd, dispatcher1);
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {

                    @Override
                    public HttpRequestProcessor select(CharSequence url) {
                        return null;
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HttpRequestProcessor() {
                            @Override
                            public void onHeadersReady(HttpConnectionContext connectionContext) {
                                HttpRequestHeader headers = connectionContext.getRequestHeader();
                                sink.put(headers.getMethodLine());
                                sink.put("\r\n");
                                ObjList<CharSequence> headerNames = headers.getHeaderNames();
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
                    public void close() {
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run(0);
                        dispatcher.processIOQueue(
                                (operation, context) -> context.handleClientOperation(operation, selector, EmptyRescheduleContext)
                        );
                    }
                    serverHaltLatch.countDown();
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
                            Thread.sleep(1000);
                            Assert.assertEquals(len - part1, Net.send(fd, buffer + part1, len - part1));
                        } finally {
                            Unsafe.free(buffer, len);
                        }

                        contextClosedLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        // do not close client side before server does theirs
                        Assert.assertTrue(Net.isDead(fd));

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
    public void testTextQueryCreateTable() throws Exception {
        testJsonQuery(
                20,
                "GET /exp?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n" +
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
                        "Content-Type: text/csv; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "0c\r\n" +
                        "DDL Success\n\r\n" +
                        "00\r\n" +
                        "\r\n",
                1
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
                        "\r\n");
    }

    @Test
    // this test is ignored for the time being because it is unstable on OSX and I
    // have not figured out the reason yet. I would like to see if this test
    // runs any different on Linux, just to narrow the problem down to either
    // dispatcher or Http parser.
    public void testTwoThreadsSendTwoThreadsRead() throws Exception {

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
        final String expected = "GET /status?x=1&a=&b&c&d=x HTTP/1.1\r\n" +
                "host:localhost:9000\r\n" +
                "connection:keep-alive\r\n" +
                "cache-control:max-age=0\r\n" +
                "accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "user-agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "accept-encoding:gzip,deflate,sdch\r\n" +
                "accept-language:en-US,en;q=0.8\r\n" +
                "cookie:textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        final int N = 100;
        final int serverThreadCount = 2;
        final int senderCount = 2;


        assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final AtomicInteger requestsReceived = new AtomicInteger();
            final AtomicBoolean finished = new AtomicBoolean(false);
            final SOCountDownLatch senderHalt = new SOCountDownLatch(senderCount);
            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    (fd, dispatcher1) -> new HttpConnectionContext(httpServerConfiguration.getHttpContextConfiguration()).of(fd, dispatcher1)
            )) {

                // server will publish status of each request to this queue
                final RingQueue<Status> queue = new RingQueue<>(Status::new, 1024);
                final MPSequence pubSeq = new MPSequence(queue.getCapacity());
                SCSequence subSeq = new SCSequence();
                pubSeq.then(subSeq).then(pubSeq);

                final AtomicBoolean serverRunning = new AtomicBoolean(true);
                final SOCountDownLatch serverHaltLatch = new SOCountDownLatch(serverThreadCount);

                try {
                    for (int j = 0; j < serverThreadCount; j++) {
                        new Thread(() -> {
                            final StringSink sink = new StringSink();
                            final long responseBuf = Unsafe.malloc(32);
                            Unsafe.getUnsafe().putByte(responseBuf, (byte) 'A');

                            final HttpRequestProcessor processor = new HttpRequestProcessor() {
                                @Override
                                public void onHeadersReady(HttpConnectionContext context) {
                                    HttpRequestHeader headers = context.getRequestHeader();
                                    sink.clear();
                                    sink.put(headers.getMethodLine());
                                    sink.put("\r\n");
                                    ObjList<CharSequence> headerNames = headers.getHeaderNames();
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

                                    nf.send(context.getFd(), responseBuf, 1);
                                }
                            };

                            HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {

                                @Override
                                public HttpRequestProcessor select(CharSequence url) {
                                    return null;
                                }

                                @Override
                                public HttpRequestProcessor getDefaultProcessor() {
                                    return processor;
                                }

                                @Override
                                public void close() {
                                }
                            };

                            while (serverRunning.get()) {
                                dispatcher.run(0);
                                dispatcher.processIOQueue(
                                        (operation, context) -> context.handleClientOperation(operation, selector, EmptyRescheduleContext)
                                );
                            }

                            Unsafe.free(responseBuf, 32);
                            serverHaltLatch.countDown();
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
                                            Unsafe.free(buffer, len);
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
                    while (receiveCount < N * senderCount) {
                        long cursor = subSeq.next();
                        if (cursor < 0) {
                            if (cursor == -1 && completedCount.get() == senderCount) {
                                Assert.fail("Not all requests successful, test failed, see previous failures");
                                break;
                            }
                            Thread.yield();
                            continue;
                        }
                        boolean valid = queue.get(cursor).valid;
                        subSeq.done(cursor);
                        Assert.assertTrue(valid);
                        receiveCount++;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    serverRunning.set(false);
                    serverHaltLatch.await();
                }
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            } finally {
                finished.set(true);
                senderHalt.await();
            }
            Assert.assertEquals(N * senderCount, requestsReceived.get());
        });
    }

    private static void assertDownloadResponse(long fd, Rnd rnd, long buffer, int len, int nonRepeatedContentLength, String expectedResponseHeader, long expectedResponseLen) {
        int expectedHeaderLen = expectedResponseHeader.length();
        int headerCheckRemaining = expectedResponseHeader.length();
        long downloadedSoFar = 0;
        int contentRemaining = 0;
        while (downloadedSoFar < expectedResponseLen) {
            int contentOffset = 0;
            int n = Net.recv(fd, buffer, len);
            Assert.assertTrue(n > -1);
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
                            contentRemaining = nonRepeatedContentLength;
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

    private static void sendRequest(String request, long fd, long buffer) {
        final int requestLen = request.length();
        Chars.asciiStrCpy(request, requestLen, buffer);
        Assert.assertEquals(requestLen, Net.send(fd, buffer, requestLen));
    }

    private static void sendAndReceive(
            NetworkFacade nf,
            String request,
            CharSequence response,
            int requestCount,
            long pauseBetweenSendAndReceive,
            boolean print
    ) throws InterruptedException {
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
            boolean expectDisconnect
    ) throws InterruptedException {
        new SendAndReceiveRequestBuilder()
                .withNetworkFacade(nf)
                .withExpectDisconnect(expectDisconnect)
                .withPrintOnly(print)
                .withRequestCount(requestCount)
                .withPauseBetweenSendAndReceive(pauseBetweenSendAndReceive)
                .execute(request, response);
    }

    private void assertColumn(CharSequence expected, int index) {
        final String baseDir = temp.getRoot().getAbsolutePath();
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(baseDir);

        try (TableReader reader = new TableReader(configuration, "telemetry")) {
            final StringSink sink = new StringSink();
            sink.clear();
            printer.printFullColumn(reader.getCursor(), reader.getMetadata(), index, false, sink);
            TestUtils.assertEquals(expected, sink);
            reader.getCursor().toTop();
            sink.clear();
            printer.printFullColumn(reader.getCursor(), reader.getMetadata(), index, false, sink);
            TestUtils.assertEquals(expected, sink);
        }
    }

    @NotNull
    private DefaultHttpServerConfiguration createHttpServerConfiguration(
            String baseDir,
            boolean dumpTraffic
    ) {
        return createHttpServerConfiguration(
                NetworkFacadeImpl.INSTANCE,
                baseDir,
                1024 * 1024,
                dumpTraffic,
                false
        );
    }

    @NotNull
    private DefaultHttpServerConfiguration createHttpServerConfiguration(
            NetworkFacade nf,
            String baseDir,
            int sendBufferSize,
            boolean dumpTraffic,
            boolean allowDeflateBeforeSend
    ) {
        return createHttpServerConfiguration(
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
            NetworkFacade nf,
            String baseDir,
            int sendBufferSize,
            boolean dumpTraffic,
            boolean allowDeflateBeforeSend,
            boolean serverKeepAlive,
            String httpProtocolVersion
    ) {
        DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                .withNetwork(nf)
                .withBaseDir(baseDir)
                .withSendBufferSize(sendBufferSize)
                .withDumpingTraffic(dumpTraffic)
                .withAllowDeflateBeforeSend(allowDeflateBeforeSend)
                .withServerKeepAlive(serverKeepAlive)
                .withHttpProtocolVersion(httpProtocolVersion)
                .build();
        QueryCache.configure(httpConfiguration);
        return httpConfiguration;
    }

    private void testJsonQuery(int recordCount, String request, String expectedResponse, int requestCount, boolean telemetry) throws Exception {
        testJsonQuery0(2, engine -> {
            // create table with all column types
            CairoTestUtils.createTestTable(
                    engine.getConfiguration(),
                    recordCount,
                    new Rnd(),
                    new TestRecord.ArrayBinarySequence()
            );
            sendAndReceive(
                    NetworkFacadeImpl.INSTANCE,
                    request,
                    expectedResponse,
                    requestCount,
                    0,
                    false
            );
        }, telemetry);
    }

    private void testJsonQuery(int recordCount, String request, String expectedResponse, int requestCount) throws Exception {
        testJsonQuery(recordCount, request, expectedResponse, requestCount, false);
    }

    private void testJsonQuery(int recordCount, String request, String expectedResponse) throws Exception {
        testJsonQuery(recordCount, request, expectedResponse, 100, false);
    }

    private void testJsonQuery0(int workerCount, HttpQueryTestBuilder.HttpClientCode code, boolean telemetry) throws Exception {
        testJsonQuery0(workerCount, code, telemetry, false);
    }

    private void testJsonQuery0(int workerCount, HttpQueryTestBuilder.HttpClientCode code, boolean telemetry, boolean http1) throws Exception {
        new HttpQueryTestBuilder()
                .withWorkerCount(workerCount)
                .withTelemetry(telemetry)
                .withTempFolder(temp)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder()
                        .withServerKeepAlive(!http1)
                        .withSendBufferSize(16 * 1024)
                        .withConfiguredMaxQueryResponseRowLimit(configuredMaxQueryResponseRowLimit)
                        .withHttpProtocolVersion(http1 ? "HTTP/1.0 " : "HTTP/1.1 "))
                .run(code);
    }

    private void writeRandomFile(Path path, Rnd rnd, long lastModified) {
        if (Files.exists(path)) {
            Assert.assertTrue(Files.remove(path));
        }
        long fd = Files.openAppend(path);

        long buf = Unsafe.malloc(1048576); // 1Mb buffer
        for (int i = 0; i < 1048576; i++) {
            Unsafe.getUnsafe().putByte(buf + i, rnd.nextByte());
        }

        for (int i = 0; i < 20; i++) {
            Assert.assertEquals(1048576, Files.append(fd, buf, 1048576));
        }

        Files.close(fd);
        Files.setLastModified(path, lastModified);
        Unsafe.free(buf, 1048576);
    }

    private static class QueryThread extends Thread {
        private final String[][] requests;
        private final int count;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final AtomicInteger errorCounter;

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
                new SendAndReceiveRequestBuilder().executeMany(requester -> {
                    barrier.await();
                    for (int i = 0; i < count; i++) {
                        int index = rnd.nextPositiveInt() % requests.length;
                        try {
                            requester.execute(requests[index][0], requests[index][1]);
                        } catch (Throwable e) {
                            System.out.println("erm: " + index + ", ts=" + Timestamps.toString(Os.currentTimeMicros()));
                            throw e;
                        }
                    }
                });
            } catch (Throwable e) {
                e.printStackTrace();
                errorCounter.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }
    }

    private static class HelloContext implements IOContext {
        private final long fd;
        private final long buffer = Unsafe.malloc(1024);
        private final SOCountDownLatch closeLatch;
        private final IODispatcher<HelloContext> dispatcher;

        public HelloContext(long fd, SOCountDownLatch closeLatch, IODispatcher<HelloContext> dispatcher) {
            this.fd = fd;
            this.closeLatch = closeLatch;
            this.dispatcher = dispatcher;
        }

        @Override
        public void close() {
            Unsafe.free(buffer, 1024);
            closeLatch.countDown();
        }

        @Override
        public long getFd() {
            return fd;
        }

        @Override
        public boolean invalid() {
            return false;
        }

        @Override
        public IODispatcher<HelloContext> getDispatcher() {
            return dispatcher;
        }
    }

    static class Status {
        boolean valid;
    }

    private static class ByteArrayResponse extends AbstractCharSequence implements ByteSequence {
        private final byte[] bytes;
        private final int len;

        private ByteArrayResponse(byte[] bytes, int len) {
            super();
            this.bytes = bytes;
            this.len = len;
        }

        @Override
        public byte byteAt(int index) {
            if (index >= len) {
                throw new IndexOutOfBoundsException();
            }
            return bytes[index];
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            if (index >= len) {
                throw new IndexOutOfBoundsException();
            }
            return (char) bytes[index];
        }
    }
}
