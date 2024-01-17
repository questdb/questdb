package io.questdb.test.cutlass.http;

import io.questdb.*;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpResponseHeader;
import io.questdb.cutlass.http.client.*;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.BootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpErrorHandlingTest extends BootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testUnexpectedErrorDuringSQLExecutionHandled() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                new FilesFacadeImpl() {
                                    @Override
                                    public int openRW(LPSZ name, long opts) {
                                        if (counter.incrementAndGet() > 28) {
                                            throw new RuntimeException("Test error");
                                        }
                                        return super.openRW(name, opts);
                                    }
                                },
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration)
                        );
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newInstance(new DefaultHttpClientConfiguration())) {
                    assertExecRequest(httpClient, "create table x(y long)", HttpURLConnection.HTTP_INTERNAL_ERROR,
                            "{\"query\":\"create table x(y long)\",\"error\":\"Test error\",\"position\":0}"
                    );
                }
            }
        });
    }

    @Test
    public void testUnexpectedErrorOutsideSQLExecutionResultsInDisconnect() throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                FilesFacadeImpl.INSTANCE,
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration) {
                                    @Override
                                    public @NotNull HttpCookieHandler getHttpCookieHandler() {
                                        return new HttpCookieHandler() {
                                            @Override
                                            public boolean processCookies(HttpConnectionContext context, SecurityContext securityContext) {
                                                throw new RuntimeException("Test error");
                                            }

                                            @Override
                                            public void setCookie(HttpResponseHeader header, SecurityContext securityContext) {
                                            }
                                        };
                                    }
                                }
                        );
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newInstance(new DefaultHttpClientConfiguration())) {
                    final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url("/exec").query("query", "create table x(y long)");
                    try (HttpClient.ResponseHeaders response = request.send()) {
                        response.await();
                        Assert.fail("Expected exception is missing");
                    } catch (HttpClientException e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }
                }
            }
        });
    }

    private void assertExecRequest(
            HttpClient httpClient,
            String sql,
            int expectedHttpStatusCode,
            String expectedHttpResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/exec").query("query", sql);
        try (HttpClient.ResponseHeaders response = request.send()) {
            response.await();

            TestUtils.assertEquals(String.valueOf(expectedHttpStatusCode), response.getStatusCode());

            final StringSink sink = new StringSink();

            Chunk chunk;
            final ChunkedResponse chunkedResponse = response.getChunkedResponse();
            while ((chunk = chunkedResponse.recv()) != null) {
                Utf8s.utf8ToUtf16(chunk.lo(), chunk.hi(), sink);
            }

            TestUtils.assertEquals(expectedHttpResponse, sink);
            sink.clear();
        }
    }
}
