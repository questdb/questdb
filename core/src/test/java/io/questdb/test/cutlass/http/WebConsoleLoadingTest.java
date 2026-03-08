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
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.http.HttpConstants.HEADER_IF_NONE_MATCH;
import static java.net.HttpURLConnection.*;

public class WebConsoleLoadingTest extends AbstractBootstrapTest {
    private static final String TEST_PAYLOAD = "<html><body><p>Dummy Web Console</p></body></html>";

    private long indexFileLastModified;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> {
            createDummyConfiguration();
            indexFileLastModified = createDummyWebConsole();
        });
        dbPath.parent().$();
    }

    @Test
    public void testHttpContextPathExecEndpoint() throws Exception {
        testHttpContextPathExecEndpoint("");
        testHttpContextPathExecEndpoint("/");
        testHttpContextPathExecEndpoint("/context");
    }

    @Test
    public void testWebConsoleLoadsWhenPgDisabled() throws Exception {
        testWebConsoleLoads("", false);
        testWebConsoleLoads("/", false);
        testWebConsoleLoads("/context", false);
    }

    @Test
    public void testWebConsoleLoadsWhenPgEnabled() throws Exception {
        testWebConsoleLoads("", true);
        testWebConsoleLoads("/", true);
        testWebConsoleLoads("/context", true);
    }

    @Test
    public void testWebConsoleRootRedirects() throws Exception {
        testWebConsoleRootRedirects("");
        testWebConsoleRootRedirects("/");
        testWebConsoleRootRedirects("/context");
    }

    private void assertRequest(HttpClient.Request request, int responseCode, String expectedResponse) {
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();
            TestUtils.assertEquals(String.valueOf(responseCode), responseHeaders.getStatusCode());
            HttpUtils.assertChunkedBody(responseHeaders, expectedResponse);
        }
    }

    private void assertRequest(
            HttpClient httpClient,
            String contextPath,
            String resource,
            boolean cachedResponse,
            int expectedResponseCode,
            String expectedResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url(contextPath + resource);
        if (cachedResponse) {
            request.header(HEADER_IF_NONE_MATCH.toString(), "\"" + indexFileLastModified + "\"");
        }
        assertRequest(request, expectedResponseCode, expectedResponse);
    }

    private void testHttpContextPathExecEndpoint(String contextPathWebConsole) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPathWebConsole
            )) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url(contextPathWebConsole + "/exec").query("query", "select 1");
                    assertRequest(
                            request,
                            HttpURLConnection.HTTP_OK,
                            "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}"
                    );
                }
            }
        });
    }

    private void testWebConsoleLoads(String contextPath, boolean pgEnabled) throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        final Map<String, String> env = new HashMap<>(super.getEnv());
                        env.put(PropertyKey.PG_ENABLED.getEnvVarName(), Boolean.toString(pgEnabled));
                        env.put(PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPath);
                        return Collections.unmodifiableMap(env);
                    }

                    @Override
                    public boolean useSite() {
                        return false;
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertRequest(httpClient, contextPath, "/index.html", true, HTTP_NOT_MODIFIED, "");
                    assertRequest(httpClient, contextPath, "/index.html", false, HTTP_OK, TEST_PAYLOAD);
                }
            }
        });
    }

    private void testWebConsoleRootRedirects(String contextPath) throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        final Map<String, String> env = new HashMap<>(super.getEnv());
                        env.put(PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPath);
                        return Collections.unmodifiableMap(env);
                    }

                    @Override
                    public boolean useSite() {
                        return false;
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertRequest(httpClient, contextPath, "", true, HTTP_MOVED_PERM, "");
                    assertRequest(httpClient, contextPath, "", false, HTTP_MOVED_PERM, "");
                }
            }
        });
    }
}
