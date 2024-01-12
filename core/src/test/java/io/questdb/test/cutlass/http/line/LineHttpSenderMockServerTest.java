package io.questdb.test.cutlass.http.line;

import io.questdb.BuildInformationHolder;
import io.questdb.Metrics;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestProcessorFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.test.AbstractTest;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class LineHttpSenderMockServerTest extends AbstractTest {

    private static final CharSequence QUESTDB_VERSION = new BuildInformationHolder().getSwVersion();
    private static final Metrics metrics = Metrics.enabled();

    @Test
    public void testBadJsonError() throws Exception {
        String badJsonResponse = "{\"foo\": \"bar\"}";

        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .respondWith(400, badJsonResponse, "application/json");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: " + badJsonResponse + " [http-status=400]"));
    }

    @Test
    public void testBasicAuth() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .withExpectedHeader("Authorization", "Basic QWxhZGRpbjpPcGVuU2VzYW1l")
                .respondWith(204, "", "application/json");

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
        }, senderBuilder -> senderBuilder.httpAuth("Aladdin", "OpenSesame"));
    }

    @Test
    public void testJsonError() throws Exception {
        String jsonResponse = "{\"code\": \"invalid\",\n" +
                "                    \"message\": \"failed to parse line protocol: invalid field format\",\n" +
                "                    \"errorId\": \"ABC-2\",\n" +
                "                    \"line\": 2}";

        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .respondWith(400, jsonResponse, "application/json");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: failed to parse line protocol: invalid field format [http-status=400, id: ABC-2, code: invalid, line: 2]"));
    }

    @Test
    public void testMaxPendingRows() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test x=1.0\n")
                .respondWith(204, "", "application/json")
                .withExpectedContent("test x=2.0\n" +
                        "test x=3.0\n")
                .respondWith(204, "", "application/json")
                .withExpectedContent("test x=4.0\n" +
                        "test x=5.0\n")
                .respondWith(204, "", "application/json")
                .withExpectedContent("test x=6.0\n")
                .respondWith(204, "", "application/json");

        testWithMock(mockHttpProcessor, sender -> {
            // first row to be flushed implicitly
            sender.table("test").doubleColumn("x", 1.0).atNow();
            sender.flush();

            // 1st implicit batch sent due to maxPendingRows
            sender.table("test").doubleColumn("x", 2.0).atNow();
            sender.table("test").doubleColumn("x", 3.0).atNow();

            // 2nd implicit batch sent due to maxPendingRows
            sender.table("test").doubleColumn("x", 4.0).atNow();
            sender.table("test").doubleColumn("x", 5.0).atNow();

            // the last row is flushed on close()
            sender.table("test").doubleColumn("x", 6.0).atNow();
        }, senderBuilder -> senderBuilder.maxPendingRows(2));
    }

    @Test
    public void testMaxPendingRows_doubleConfiguration() {
        try {
            Sender.builder().maxPendingRows(1).maxPendingRows(1);
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "max pending rows was already configured [max-pending-rows=1]");
        }
    }

    @Test
    public void testMaxRequestBufferSizeCannotBeLessThanDefault() {
        try (Sender sender = Sender.builder().url("http://localhost:1").maximumBufferCapacity(65535).build()) {
            Assert.fail();
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "maximum buffer capacity cannot be less than default buffer capacity [maximum-buffer-capacity=65535, default-buffer-capacity=65536]");
        }
    }

    @Test
    public void testMaxRequestBufferSizeCannotBeLessThanInitialBufferSize() {
        try (Sender sender = Sender.builder().url("http://localhost:1").maximumBufferCapacity(100_000).bufferCapacity(200_000).build()) {
            Assert.fail();
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "maximum buffer capacity cannot be less than initial buffer capacity [maximum-buffer-capacity=100000, initial-buffer-capacity=200000]");
        }
    }

    @Test
    public void testMaxRequestBufferSizeExceeded() {
        try (Sender sender = Sender.builder().url("http://localhost:1").maximumBufferCapacity(65536).build()) {
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
                .url("http://127.0.0.1:1")
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
                .respondWith(404, "Not Found", "test/plain");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint does not support ILP. [http-status=404]"));
    }

    @Test
    public void testRetryOn500() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .respondWith(500, "Internal Server Error", "application/json")
                .withExpectedContent("test,sym=bol x=1.0\n")
                .respondWith(204, "", "application/json");

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
                .respondWith(500, "Internal Server Error", "application/json")
                .withExpectedContent("test,sym=bol x=1.0\n")
                .respondWith(500, "Internal Server Error", "application/json")
                .withExpectedContent("test,sym=bol x=1.0\n")
                .respondWith(500, "Internal Server Error", "application/json")
                .withExpectedContent("test,sym=bol x=1.0\n")
                .respondWith(500, "Internal Server Error", "application/json");

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
        });
    }

    @Test
    public void testRetryingDisabled() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .respondWith(500, "do not dare to retry", "plain/text");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: do not dare to retry [http-status=500]"),
                senderBuilder -> senderBuilder.maxRetries(0)
        );
    }

    @Test
    public void testTextPlainError() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .respondWith(400, "Bad Request", "text/plain");

        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: Bad Request [http-status=400]"));
    }

    @Test
    public void testTokenAuth() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .withExpectedHeader("Authorization", "Bearer 0123456789")
                .respondWith(204, "", "application/json");

        testWithMock(mockHttpProcessor, sender -> {
            sender.table("test")
                    .symbol("sym", "bol")
                    .doubleColumn("x", 1.0)
                    .atNow();
        }, senderBuilder -> senderBuilder.httpTokenAuth("0123456789"));
    }

    @Test
    public void testTwoLines() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedContent("test,sym=bol x=1.0\n" +
                        "test,sym=bol x=2.0\n")
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .respondWith(204, "", "application/json");

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
                .respondWith(401, "Unauthorized", "text/plain");
        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint authentication error: Unauthorized [http-status=401]"));
    }

    @Test
    public void testUnauthenticated_403() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .respondWith(403, "Unauthorized", "text/plain");
        testWithMock(mockHttpProcessor, errorVerifier("Could not flush buffer: HTTP endpoint authentication error: Unauthorized [http-status=403]"));
    }

    @Test
    public void testUnauthenticated_noContent() throws Exception {
        MockHttpProcessor mockHttpProcessor = new MockHttpProcessor()
                .withExpectedHeader("User-Agent", "QuestDB/java/" + QUESTDB_VERSION)
                .respondWith(401, "", "text/plain");

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
        return new HttpServerConfigurationBuilder()
                .withNetwork(nf)
                .withBaseDir(baseDir)
                .withSendBufferSize(sendBufferSize)
                .withDumpingTraffic(dumpTraffic)
                .withAllowDeflateBeforeSend(allowDeflateBeforeSend)
                .withServerKeepAlive(serverKeepAlive)
                .withHttpProtocolVersion(httpProtocolVersion)
                .build();
    }

    private void testWithMock(MockHttpProcessor mockHttpProcessor, Consumer<Sender> senderConsumer) throws Exception {
        testWithMock(mockHttpProcessor, senderConsumer, null);
    }

    private void testWithMock(MockHttpProcessor mockHttpProcessor, Consumer<Sender> senderConsumer, Consumer<Sender.LineSenderBuilder> senderCustomizer) throws Exception {
        assertMemoryLeak(() -> {
            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final String baseDir = root;
            final DefaultHttpServerConfiguration httpConfiguration = createHttpServerConfiguration(nf, baseDir, 4096, false, false);

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
                    Sender.LineSenderBuilder senderBuilder = Sender.builder()
                            .url("http://localhost:" + port);
                    if (senderCustomizer != null) {
                        senderCustomizer.accept(senderBuilder);
                    }
                    try (Sender sender = senderBuilder.build()) {
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
