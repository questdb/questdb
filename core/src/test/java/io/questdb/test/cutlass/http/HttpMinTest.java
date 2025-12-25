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
import io.questdb.cairo.CairoException;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class HttpMinTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }


    @Test
    public void testResponsiveOnMemoryPressure() throws Exception {
        // TODO: fix on Windows
        Assume.assumeFalse(Os.isWindows());
        TestUtils.assertMemoryLeak(() -> {
            assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;

            int httpMinConnectionLimit = 2;
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512M",
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "512M",
                    PropertyKey.HTTP_MIN_CONNECTION_POOL_INITIAL_CAPACITY.getEnvVarName(), String.valueOf(httpMinConnectionLimit),
                    PropertyKey.HTTP_MIN_NET_CONNECTION_LIMIT.getEnvVarName(), String.valueOf(httpMinConnectionLimit),
                    PropertyKey.METRICS_ENABLED.getEnvVarName(), "true",
                    PropertyKey.HTTP_ENABLED.getEnvVarName(), "false"
            )) {
                serverMain.start();
                HttpServerConfiguration httpMinConfig = serverMain.getConfiguration().getHttpMinServerConfiguration();
                HttpContextConfiguration httpMinContextConfig = httpMinConfig.getHttpContextConfiguration();
                int expectedAllocation = (httpMinConfig.getSendBufferSize() + 20
                        + httpMinConfig.getRecvBufferSize()
                        + httpMinContextConfig.getRequestHeaderBufferSize() + 64
                        + httpMinContextConfig.getMultipartHeaderBufferSize() + 64) * httpMinConnectionLimit;

                // Wait http min threads to start, they will need to allocate some memory
                // directly after the server start.
                while (Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) < expectedAllocation) {
                    Os.sleep(10);
                }

                int httpMinPort = serverMain.getConfiguration().getHttpMinServerConfiguration().getBindPort();
                long buff = 0, rssAvailable = 0;
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    while (true) {
                        try {
                            rssAvailable = Unsafe.getRssMemLimit() - Unsafe.getRssMemUsed();
                            buff = Unsafe.malloc(rssAvailable, MemoryTag.NATIVE_DEFAULT);
                            break;
                        } catch (CairoException e) {
                            // retry
                        }
                    }

                    String expectedText = "questdb_memory_tag_NATIVE_DEFAULT " + Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                    for (int i = 0; i < 10; i++) {
                        checkResponse(httpClient, "/metrics", expectedText, httpMinPort);
                        checkResponse(httpClient, "/status", "Status: Healthy", httpMinPort);
                    }
                } finally {
                    Unsafe.free(buff, rssAvailable, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static void checkResponse(HttpClient httpClient, String url, String expectedText, int port) {
        HttpClient.Request request;
        request = httpClient.newRequest("localhost", port);
        request.GET().url(url);
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();
            TestUtils.assertEquals(String.valueOf(200), responseHeaders.getStatusCode());
            HttpUtils.assertChunkedBodyContains(responseHeaders, expectedText);
        }
    }
}
