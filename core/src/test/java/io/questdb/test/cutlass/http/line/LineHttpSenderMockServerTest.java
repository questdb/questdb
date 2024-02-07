/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.Metrics;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.mp.WorkerPool;
import io.questdb.network.PlainSocketFactory;
import io.questdb.test.AbstractTest;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class LineHttpSenderMockServerTest extends AbstractTest {
    public static final Function<Integer, Sender.LineSenderBuilder> DEFAULT_FACTORY = port -> Sender.builder().address("localhost:" + port).http();

    private static final CharSequence QUESTDB_VERSION = new BuildInformationHolder().getSwVersion();
    private static final Metrics metrics = Metrics.enabled();

    @Test
    public void testAutoFlushRows() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test x=1.0\n")
                .replyWithStatus(204)
                .withExpectedContent("test x=2.0\n" +
                        "test x=3.0\n")
                .replyWithStatus(204)
                .withExpectedContent("test x=4.0\n" +
                        "test x=5.0\n")
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

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: " + badJsonResponse + " [http-status=400]"));
    }

    @Test
    public void testBasicAuth() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .withExpectedHeader("Authorization", "Basic QWxhZGRpbjpPcGVuU2VzYW1l")
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
        }, DEFAULT_FACTORY.andThen(b -> b.httpUsernamePassword("Aladdin", "OpenSesame")));
    }

    @Test
    public void testConnectWithConfigString() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("Authorization", "Basic QWxhZGRpbjo7T3BlbjtTZXNhbWU7Ow==")
                .replyWithStatus(204);
        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
        }, port -> Sender.builder().fromConfig("http::addr=localhost:" + port + ";user=Aladdin;pass=;;Open;;Sesame;;;;;")); // escaped semicolons in password
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
        }, port -> Sender.builder().fromConfig("http::addr=localhost:" + port + ";auto_flush=off;"));
    }

    @Test
    public void testJsonError() throws Exception {
        String jsonResponse = "{\"code\": \"invalid\",\n" +
                "                    \"message\": \"failed to parse line protocol: invalid field format\",\n" +
                "                    \"errorId\": \"ABC-2\",\n" +
                "                    \"line\": 2}";

        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithContent(400, jsonResponse, HttpConstants.CONTENT_TYPE_JSON);

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: failed to parse line protocol: invalid field format [http-status=400, id: ABC-2, code: invalid, line: 2]"));
    }

    @Test
    public void testMaxRequestBufferSizeExceeded() {
        try (Sender sender = Sender.builder().address("localhost:1")
                .http()
                .maxBufferCapacity(65536)
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
            TestUtils.assertContains(e.getMessage(), "maximum buffer size exceeded [maxBufferSize=65536, requiredSize=65537]");
        }
    }

    @Test
    public void testNoConnection() {
        try (Sender sender = Sender.builder()
                .address("127.0.0.1:1")
                .http()
                .retryTimeoutMillis(1000)
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
            }
        }
    }

    @Test
    public void testOldServerWithoutIlpHttpSupport() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .replyWithContent(404, "Not Found", "test/plain");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint does not support ILP. [http-status=404]"));
    }

    @Test
    public void testRequestContainsHostHeader() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("Host", "localhost:9001")
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
        });
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
                    } finally {
                        delayLatch.countDown();
                    }
                }, DEFAULT_FACTORY.andThen(b -> b.httpTimeoutMillis(100).retryTimeoutMillis(0))
        );
    }

    @Test
    public void testTokenAuth() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .withExpectedHeader("Authorization", "Bearer 0123456789")
                .replyWithStatus(204);

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
        }, DEFAULT_FACTORY.andThen(b -> b.httpToken("0123456789")));
    }

    @Test
    public void testTwoLines() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n" +
                        "test,sym=bol x=2.0\n")
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
                .replyWithContent(403, "Unauthorized", "text/plain");
        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint authentication error: Unauthorized [http-status=403]"));
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
    private DefaultHttpServerConfiguration createHttpServerConfiguration() {
        return new HttpServerConfigurationBuilder()
                .withBaseDir(root)
                .withSendBufferSize(4096)
                .withDumpingTraffic(false)
                .withAllowDeflateBeforeSend(false)
                .withServerKeepAlive(true)
                .withHttpProtocolVersion("HTTP/1.1 ")
                .build();
    }

    private void testWithMock(MockHttpProcessor mockHttpProcessor, Consumer<Sender> senderConsumer) throws Exception {
        testWithMock(mockHttpProcessor, senderConsumer, DEFAULT_FACTORY);
    }

    private void testWithMock(MockHttpProcessor mockHttpProcessor, Consumer<Sender> senderConsumer, Function<Integer, Sender.LineSenderBuilder> senderBuilderFactory) throws Exception {
        assertMemoryLeak(() -> {
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration();

            try (WorkerPool workerPool = new TestWorkerPool(1);
                 HttpServer httpServer = new HttpServer(httpConfiguration, metrics, workerPool, PlainSocketFactory.INSTANCE)) {
                httpServer.bind(new HttpRequestProcessorFactory() {
                    @Override
                    public String getUrl() {
                        return "/write";
                    }

                    @Override
                    public HttpRequestProcessor newInstance() {
                        return mockHttpProcessor;
                    }
                });
                workerPool.start(LOG);
                try {
                    int port = httpConfiguration.getDispatcherConfiguration().getBindPort();
                    try (Sender sender = senderBuilderFactory.apply(port).build()) {
                        senderConsumer.accept(sender);
                    }
                    mockHttpProcessor.verify();
                } finally {
                    workerPool.halt();
                }
            }
        });
    }
}
