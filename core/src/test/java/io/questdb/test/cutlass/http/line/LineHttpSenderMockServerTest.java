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

package io.questdb.test.cutlass.http.line;

import io.questdb.BuildInformationHolder;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConstants;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.test.AbstractTest;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.questdb.client.Sender.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class LineHttpSenderMockServerTest extends AbstractTest {
    public static final Function<Integer, Sender.LineSenderBuilder> DEFAULT_FACTORY =
            port -> Sender.builder(Sender.Transport.HTTP).address("localhost:" + port).protocolVersion(PROTOCOL_VERSION_V1);

    public static final Function<Integer, Sender.LineSenderBuilder> NO_PROTOCOL_VERSION_SET_FACTORY =
            port -> Sender.builder(Sender.Transport.HTTP).address("localhost:" + port);

    private static final CharSequence QUESTDB_VERSION = new BuildInformationHolder().getSwVersion();

    private final static AtomicLong REMAINING_SERVER_RECV_FAILURES = new AtomicLong();
    private final NetworkFacade FAILING_FACADE = new NetworkFacadeImpl() {
        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            if (REMAINING_SERVER_RECV_FAILURES.getAndDecrement() > 0) {
                return -1;
            }
            return super.recvRaw(fd, buffer, bufferLen);
        }
    };

    @Before
    public void setUp() {
        super.setUp();
        REMAINING_SERVER_RECV_FAILURES.set(0); // no failure by default
    }

    @Test
    public void testAutoFlushInterval() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor().keepReplyingWithStatus(204);
        MockHttpProcessor processor = new MockHttpProcessor();

        testWithMock(mockHttpProcessor, processor, sender -> {
            for (int i = 0; i < 20; i++) {
                sender.table("test")
                        .symbol("sym", "bol")
                        .doubleColumn("x", 1.0)
                        .atNow();
                Os.sleep(1);
            }
        }, DEFAULT_FACTORY.andThen(b -> b.autoFlushRows(Integer.MAX_VALUE).autoFlushIntervalMillis(1)), true);
    }

    @Test
    public void testAutoFlushRows() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test x=1.0\n")
                .replyWithStatus(204)
                .withExpectedContent("""
                        test x=2.0
                        test x=3.0
                        """)
                .replyWithStatus(204)
                .withExpectedContent("""
                        test x=4.0
                        test x=5.0
                        """)
                .replyWithStatus(204)
                .withExpectedContent("test x=6.0\n")
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> {
            // first row to be flushed explicitly
            sender.table("test").doubleColumn("x", 1.0).atNow();
            sender.flush();

            // 1st implicit batch sent due to autoFlushRows
            sender.table("test").doubleColumn("x", 2.0).atNow();
            sender.table("test").doubleColumn("x", 3.0).atNow();

            // 2nd implicit batch sent due to autoFlushRows
            sender.table("test").doubleColumn("x", 4.0).atNow();
            sender.table("test").doubleColumn("x", 5.0).atNow();

            // the last row is flushed on close()
            sender.table("test").doubleColumn("x", 6.0).atNow();
        }, DEFAULT_FACTORY.andThen(b -> b.autoFlushRows(2)));
    }

    @Test
    public void testBadJsonError() throws Exception {
        String badJsonResponse = "{\"foo\": \"bar\"}";

        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithContent(400, badJsonResponse, HttpConstants.CONTENT_TYPE_JSON);

        testWithMock(
                mockHttpProcessor,
                errorVerifier("Could not flush buffer: " + badJsonResponse + " [http-status=400]")
        );
    }

    @Test
    public void testBadSettings() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor();
        MockErrorSettingsProcessor settingsProcessor = new MockErrorSettingsProcessor();
        try {
            testWithMock(mockHttpProcessor, settingsProcessor, sender -> sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow(), port -> Sender.builder("http::addr=localhost:" + port + ";"));
            Assert.fail("Exception expected");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "bad thing happened");
        }
    }

    @Test
    public void testBadSettingsManyServers() throws Exception {
        Assume.assumeTrue(Os.type != Os.DARWIN); // MacOs does not treat 127.0.0.2, 127.0.0.3, etc ... as 127.0.0.1

        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor();
        MockErrorSettingsProcessor settingsProcessor = new MockErrorSettingsProcessor();
        try {
            testWithMock(mockHttpProcessor, settingsProcessor, sender -> sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow(), port -> {
                LineSenderBuilder builder = builder(Transport.HTTP);
                for (int i = 0; i < 65; i++) {
                    String ip = "127.0.0." + (i + 1); // fool duplicated address detection
                    builder.address(ip + ':' + port);
                }
                builder.maxBackoffMillis(0);
                return builder;
            });
            Assert.fail("Exception expected");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "bad thing happened");
        }
    }

    @Test
    public void testBasicAuth() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .withExpectedHeader("Authorization", "Basic QWxhZGRpbjpPcGVuU2VzYW1l")
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> sender.table("test")
                .symbol("sym", "bol")
                .doubleColumn("x", 1.0)
                .atNow(), DEFAULT_FACTORY.andThen(b -> b.httpUsernamePassword("Aladdin", "OpenSesame")));
    }

    @Test
    public void testBufferIsNotResetAfterRetriableError() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .replyWithContent(500, "Internal Server Error", HttpConstants.CONTENT_TYPE_JSON)
                .withExpectedContent("test,sym=bol x=1.0\n")
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
            try {
                sender.flush();
                Assert.fail("Exception expected");
            } catch (LineSenderException e) {
                sender.flush();
            }
        }, DEFAULT_FACTORY.andThen(b -> b.retryTimeoutMillis(0)));
    }

    @Test
    public void testConnectWithConfigString() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("Authorization", "Basic QWxhZGRpbjo7T3BlbjtTZXNhbWU7Ow==")
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> sender.table("test")
                .symbol("sym", "bol")
                .doubleColumn("x", 1.0)
                .atNow(), port -> Sender.builder("http::addr=localhost:" + port + ";username=Aladdin;protocol_version=1;password=;;Open;;Sesame;;;;;")); // escaped semicolons in password
    }

    @Test
    public void testConnectWithConfigString_deprecatedAuth() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("Authorization", "Basic QWxhZGRpbjo7T3BlbjtTZXNhbWU7Ow==")
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> sender.table("test")
                .symbol("sym", "bol")
                .doubleColumn("x", 1.0)
                .atNow(), port -> Sender.builder("http::addr=localhost:" + port + ";username=Aladdin;protocol_version=1;password=;;Open;;Sesame;;;;;")); // escaped semicolons in password
    }

    @Test
    public void testDefaultProtocolVersionToOldServer() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .replyWithStatus(204);
        MockSettingsProcessorOldServer settingsProcessor = new MockSettingsProcessorOldServer();
        testWithMock(mockHttpProcessor, settingsProcessor, sender -> sender.table("test")
                .symbol("sym", "bol")
                .doubleColumn("x", 1.0)
                .atNow(), port -> Sender.builder("http::addr=localhost:" + port + ";"));
    }

    @Test
    public void testDisableAutoFlush() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor();
        testWithMock(mockHttpProcessor, sender -> {
            for (int i = 0; i < 1_000_000; i++) { // sufficient large number of rows to trigger auto-flush unless it is disabled
                sender.table("test")
                        .symbol("sym", "bol")
                        .doubleColumn("x", 1.0)
                        .atNow();
            }
        }, port -> Sender.builder("http::addr=localhost:" + port + ";auto_flush=off;"));
    }

    @Test
    public void testDisableIntervalBasedAutoFlush() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> {
                    for (int i = 0; i < 200; i++) {
                        sender.table("test")
                                .symbol("sym", "bol")
                                .doubleColumn("x", 1.0)
                                .atNow();
                        Os.sleep(10);
                    }
                },
                port -> Sender.builder("http::addr=localhost:" + port + ";auto_flush_interval=off;"));
    }

    @Test
    public void testDisableIntervalBasedAutoFlush_rowBasedFlushesStillWorks() throws Exception {
        int rows = 10;
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor();
        for (int i = 0; i < rows; i++) {
            mockHttpProcessor.withExpectedContent("test,sym=bol x=1.0\n").replyWithStatus(204);
        }

        testWithMock(mockHttpProcessor, sender -> {
                    for (int i = 0; i < 10; i++) {
                        sender.table("test")
                                .symbol("sym", "bol")
                                .doubleColumn("x", 1.0)
                                .atNow();
                    }
                },
                port -> Sender.builder("http::addr=localhost:" + port + ";auto_flush_interval=off;auto_flush_rows=1;protocol_version=1;"));
    }

    @Test
    public void testDisableRowBasedAutoFlush() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> {
                    for (int i = 0; i < 80000; i++) {
                        sender.table("test")
                                .symbol("sym", "bol")
                                .doubleColumn("x", 1.0)
                                .atNow();
                    }
                },
                port -> Sender.builder("http::addr=localhost:" + port + ";auto_flush_rows=off;auto_flush_interval=100000;"));
    }

    @Test
    public void testJsonError() throws Exception {
        String jsonResponse = """
                {"code": "invalid",
                                    "message": "failed to parse line protocol: invalid field format",
                                    "errorId": "ABC-2",
                                    "line": 2}""";

        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithContent(400, jsonResponse, HttpConstants.CONTENT_TYPE_JSON);

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: failed to parse line protocol: invalid field format [http-status=400, id: ABC-2, code: invalid, line: 2]"));
    }

    @Test
    public void testMaxRequestBufferSizeExceeded() {
        try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:1")
                .maxBufferCapacity(65536)
                .protocolVersion(PROTOCOL_VERSION_V2)
                .autoFlushRows(Integer.MAX_VALUE)
                .build()
        ) {
            for (int i = 0; i < 100000; i++) {
                sender.table("test")
                        .symbol("sym", "bol")
                        .doubleColumn("x", 1.0)
                        .atNow();
            }
            Assert.fail();
        } catch (HttpClientException e) {
            TestUtils.assertContains(e.getMessage(), "transaction is too large, either flush more frequently or " +
                    "increase buffer size \"max_buf_size\" [maxBufferSize=64.0 KiB, transactionSize=64.001 KiB]");
        }
    }

    @Test
    public void testMinRequestThroughput() throws Exception {
        MockHttpProcessor mock = new MockHttpProcessor()
                .delayedReplyWithStatus(204, 1000);

        testWithMock(mock, sender -> {
                    sender.table("test")
                            .symbol("sym", "bol")
                            .doubleColumn("x", 1.0)
                            .atNow();

                    sender.flush();
                }, DEFAULT_FACTORY.andThen(b -> b.httpTimeoutMillis(1).minRequestThroughput(1).retryTimeoutMillis(0)) // 1ms base timeout and 1 byte per second to extend the timeout
        );
    }

    @Test
    public void testMinRequestThroughputWithConfigString() throws Exception {
        MockHttpProcessor mock = new MockHttpProcessor()
                .delayedReplyWithStatus(204, 1000);

        testWithMock(mock, sender -> {
                    sender.table("test")
                            .symbol("sym", "bol")
                            .doubleColumn("x", 1.0)
                            .atNow();

                    sender.flush();
                }, port -> Sender.builder("http::addr=localhost:" + port + ";request_timeout=1;request_min_throughput=1;retry_timeout=0;protocol_version=2;") // 1ms base timeout and 1 byte per second to extend the timeout
        );
    }

    @Test
    public void testNoConnection() {
        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                .address("127.0.0.1:1")
                .retryTimeoutMillis(1000)
                .protocolVersion(PROTOCOL_VERSION_V1)
                .build()) {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
            try {
                sender.flush();
                Assert.fail("Exception expected");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Could not flush buffer: http://127.0.0.1:1/write?precision=n Connection Failed");
                sender.reset();
            }
        }
    }

    @Test
    public void testOldServerWithoutIlpHttpSupport() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithContent(405, "Method not allowed", "test/plain");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint does not support ILP. [http-status=405]"));
    }

    @Test
    public void testRequestContainsHostHeader() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("Host", "localhost:9001")
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> sender.table("test")
                .symbol("sym", "bol")
                .doubleColumn("x", 1.0)
                .atNow());
    }

    @Test
    public void testRetryOn500() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .replyWithContent(500, "Internal Server Error", HttpConstants.CONTENT_TYPE_JSON)
                .withExpectedContent("test,sym=bol x=1.0\n")
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
            sender.flush();
        });
    }

    @Test
    public void testRetryOn500_exceeded() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .keepReplyingWithContent(500, "Internal Server Error", HttpConstants.CONTENT_TYPE_JSON);

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
            try {
                sender.flush();
                Assert.fail("Exception expected");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Could not flush buffer: Internal Server Error [http-status=500]");
            }
        }, DEFAULT_FACTORY.andThen(b -> b.retryTimeoutMillis(1000)));
    }

    @Test
    public void testRetryingDisabled() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .replyWithContent(500, "do not dare to retry", "plain/text");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: do not dare to retry [http-status=500]"),
                DEFAULT_FACTORY.andThen(b -> b.retryTimeoutMillis(0))
        );
    }

    @Test
    public void testRetryingWhenGettingProtocolVersion() throws Exception {
        // inject a bunch of failures on the server side recv
        // this closes a connection while a client is trying to read protocol version (/settings)

        // let's test the client retries reading the protocol version by default
        REMAINING_SERVER_RECV_FAILURES.set(5);
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol str=\"foo\"\n")
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .stringColumn("str", "foo")
                    .atNow();
            sender.flush();
        }, NO_PROTOCOL_VERSION_SET_FACTORY); // important - we dot set protocol version explicitly so this forces the client to fetch it from the server


        // now let's test the same with retrying disabled - fetching the protocol version must fail
        REMAINING_SERVER_RECV_FAILURES.set(5);
        try {
            mockHttpProcessor = new MockHttpProcessor()
                    .withExpectedContent("test,sym=bol str=\"foo\"\n")
                    .replyWithStatus(204);
            testWithMock(mockHttpProcessor, sender -> {
                sender.table("test")
                        .symbol("sym", "bol")
                        .stringColumn("str", "foo")
                        .atNow();
                sender.flush();
                Assert.fail("Exception expected");
            }, NO_PROTOCOL_VERSION_SET_FACTORY.andThen(b -> b.retryTimeoutMillis(0))); // disable retries
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "Failed to detect server line protocol version");
        }
    }

    @Test
    public void testTextPlainError() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithContent(400, "Bad Request", "text/plain");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: Bad Request [http-status=400]"));
    }

    @Test
    public void testTimeout() throws Exception {
        CountDownLatch delayLatch = new CountDownLatch(1);
        MockHttpProcessor mock = new MockHttpProcessor()
                .delayedReplyWithStatus(204, delayLatch);

        testWithMock(mock, sender -> {
                    sender.table("test")
                            .symbol("sym", "bol")
                            .doubleColumn("x", 1.0)
                            .atNow();
                    try {
                        sender.flush();
                        Assert.fail("Exception expected");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(
                                e.getMessage(),
                                "Could not flush buffer: http://localhost:9001/write?precision=n Connection Failed: timed out [errno="  //errno depends on OS
                        );
                        sender.reset();
                    } finally {
                        delayLatch.countDown();
                    }
                }, DEFAULT_FACTORY.andThen(b -> b.httpTimeoutMillis(100).retryTimeoutMillis(0))
        );
    }

    @Test
    public void testTimeoutConfString() throws Exception {
        CountDownLatch delayLatch = new CountDownLatch(1);
        MockHttpProcessor mock = new MockHttpProcessor()
                .delayedReplyWithStatus(204, delayLatch);


        testWithMock(mock, sender -> {
                    sender.table("test")
                            .symbol("sym", "bol")
                            .doubleColumn("x", 1.0)
                            .atNow();
                    try {
                        sender.flush();
                        Assert.fail("Exception expected");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(
                                e.getMessage(),
                                "Could not flush buffer: http://localhost:9001/write?precision=n Connection Failed: timed out [errno="  //errno depends on OS
                        );
                        sender.reset();
                    } finally {
                        delayLatch.countDown();
                    }
                }, port -> Sender.builder("http::addr=localhost:" + port + ";request_timeout=100;retry_timeout=0;")
        );
    }

    @Test
    public void testTokenAuth() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .withExpectedHeader("Authorization", "Bearer 0123456789")
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> sender.table("test")
                .symbol("sym", "bol")
                .doubleColumn("x", 1.0)
                .atNow(), DEFAULT_FACTORY.andThen(b -> b.httpToken("0123456789")));
    }

    @Test
    public void testTwoLines() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("""
                        test,sym=bol x=1.0
                        test,sym=bol x=2.0
                        """)
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 2.0)
                    .atNow();
        });
    }

    @Test
    public void testUnauthenticated_401() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .replyWithContent(401, "Unauthorized", "text/plain");
        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint authentication error: Unauthorized [http-status=401]"));
    }

    @Test
    public void testUnauthenticated_403() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .replyWithContent(403, "Forbidden", "text/plain");
        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint authentication error: Forbidden [http-status=403]"));
    }

    @Test
    public void testUnauthenticated_noContent() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithContent(401, "", "text/plain");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint authentication error [http-status=401]"));
    }

    private static Consumer<Sender> errorVerifier(String expectedError) {
        return sender -> {
            try {
                sender.table("test")
                        .symbol("sym", "bol")
                        .doubleColumn("x", 1.0)
                        .atNow();
                sender.flush();
                Assert.fail("Exception expected");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), expectedError);
            }
        };
    }

    @NotNull
    private DefaultHttpServerConfiguration createHttpServerConfiguration(CairoConfiguration cairoConfiguration) {
        return new HttpServerConfigurationBuilder()
                .withBaseDir(root)
                .withSendBufferSize(4096)
                .withDumpingTraffic(false)
                .withAllowDeflateBeforeSend(false)
                .withServerKeepAlive(true)
                .withNetwork(FAILING_FACADE)
                .withHttpProtocolVersion("HTTP/1.1 ")
                .build(cairoConfiguration);
    }

    private void testWithMock(MockHttpProcessor mockHttpProcessor, Consumer<Sender> senderConsumer) throws Exception {
        testWithMock(mockHttpProcessor, senderConsumer, DEFAULT_FACTORY);
    }

    private void testWithMock(
            MockHttpProcessor mockHttpProcessor,
            Consumer<Sender> senderConsumer,
            Function<Integer, Sender.LineSenderBuilder> senderBuilderFactory
    ) throws Exception {
        MockSettingsProcessor settingProcessor = new MockSettingsProcessor();
        testWithMock(mockHttpProcessor, settingProcessor, senderConsumer, senderBuilderFactory, false);
    }

    private void testWithMock(
            MockHttpProcessor mockHttpProcessor,
            HttpRequestHandler settingsProcessor,
            Consumer<Sender> senderConsumer,
            Function<Integer, Sender.LineSenderBuilder> senderBuilderFactory
    ) throws Exception {
        testWithMock(mockHttpProcessor, settingsProcessor, senderConsumer, senderBuilderFactory, false);
    }

    private void testWithMock(
            MockHttpProcessor mockHttpProcessor,
            HttpRequestHandler settingsProcessor,
            Consumer<Sender> senderConsumer,
            Function<Integer, Sender.LineSenderBuilder> senderBuilderFactory,
            boolean verifyBeforeClose
    ) throws Exception {
        assertMemoryLeak(() -> {
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(new DefaultCairoConfiguration(root));
            try (
                    WorkerPool workerPool = new TestWorkerPool(1);
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return new ObjHashSet<>() {{
                            add("/write");
                        }};
                    }

                    @Override
                    public HttpRequestHandler newInstance() {
                        return mockHttpProcessor;
                    }
                });

                httpServer.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return new ObjHashSet<>() {{
                            add("/settings");
                        }};
                    }

                    @Override
                    public HttpRequestHandler newInstance() {
                        return settingsProcessor;
                    }
                });
                workerPool.start(LOG);
                try {
                    int port = httpConfiguration.getBindPort();
                    try (Sender sender = senderBuilderFactory.apply(port).build()) {
                        senderConsumer.accept(sender);
                        if (verifyBeforeClose) {
                            mockHttpProcessor.verify();
                        }
                    }
                    if (!verifyBeforeClose) {
                        mockHttpProcessor.verify();
                    }
                } finally {
                    workerPool.halt();
                }
            }
        });
    }
}
