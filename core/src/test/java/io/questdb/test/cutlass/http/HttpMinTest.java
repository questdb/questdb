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

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
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
        Rnd random = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512M",
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "512M",
                    PropertyKey.METRICS_ENABLED.getEnvVarName(), "true"
            )) {
                serverMain.start();

                long buff = 0, rssAvailable = 0;
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    // Warm up
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.GET().url("/metrics");
                    try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
                        responseHeaders.await();
                        TestUtils.assertEquals(String.valueOf(200), responseHeaders.getStatusCode());
                    }

                    Os.sleep(10);
                    rssAvailable = Unsafe.getRssMemAvailable();
                    buff = Unsafe.malloc(rssAvailable, MemoryTag.NATIVE_DEFAULT);

                    for (int i = 0; i < 10; i++) {
                        request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.GET().url("/metrics");

                        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
                            responseHeaders.await();

                            TestUtils.assertEquals(String.valueOf(200), responseHeaders.getStatusCode());
                            final Utf8StringSink sink = new Utf8StringSink();

                            Fragment fragment;
                            final Response response = responseHeaders.getResponse();
                            while ((fragment = response.recv()) != null) {
                                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
                            }

                            Assert.assertTrue(
                                    Utf8s.containsAscii(sink,
                                            "questdb_memory_tag_NATIVE_DEFAULT " + Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT))
                            );
                            sink.clear();
                        }
                    }
                } finally {
                    Unsafe.free(buff, rssAvailable, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }
}
