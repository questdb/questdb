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

import io.questdb.Bootstrap;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.BootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;

public class HttpErrorHandlingTest extends BootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testUnexpectedErrorDuringSQLExecutionHandled() throws Exception {
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
                                    public long openRW(LPSZ name, int opts) {
                                        if (Utf8s.endsWithAscii(name, "x" + Files.SEPARATOR + "_meta")) {
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

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertExecRequest(httpClient);
                }
            }
        });
    }

    @Test
    public void testSecurityContextFactoryThrowsCairoException() throws Exception {
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
                                    public @NotNull SecurityContextFactory getSecurityContextFactory() {
                                        return (principalContext, interfaceId) -> {
                                            throw CairoException.nonCritical().put("test security context error");
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

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url("/exec").query("query", "SELECT 1");
                    try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
                        responseHeaders.await();
                        TestUtils.assertEquals(
                                String.valueOf(HttpURLConnection.HTTP_INTERNAL_ERROR),
                                responseHeaders.getStatusCode()
                        );
                    }
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
                                            public boolean processServiceAccountCookie(HttpConnectionContext context, SecurityContext securityContext) {
                                                throw new RuntimeException("Test error");
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

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
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

    private void assertExecRequest(HttpClient httpClient) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/exec").query("query", "create table x as (select 1L y)");
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();
            TestUtils.assertEquals(String.valueOf(HttpURLConnection.HTTP_INTERNAL_ERROR), responseHeaders.getStatusCode());
            HttpUtils.assertChunkedBody(responseHeaders, "{\"query\":\"create table x as (select 1L y)\",\"error\":\"Test error\",\"position\":0}");
        }
    }
}
