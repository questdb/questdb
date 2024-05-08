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

import io.questdb.*;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.http.HttpConstants.HEADER_IF_NONE_MATCH;

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
    public void testWebConsoleLoadsWhenPgDisabled() throws Exception {
        testWebConsoleLoads(false);
    }

    @Test
    public void testWebConsoleLoadsWhenPgEnabled() throws Exception {
        testWebConsoleLoads(true);
    }

    private void assertRequest(HttpClient.Request request, int responseCode, String expectedResponse) {
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            TestUtils.assertEquals(String.valueOf(responseCode), responseHeaders.getStatusCode());

            final Utf8StringSink sink = new Utf8StringSink();

            Fragment fragment;
            final Response response = responseHeaders.getResponse();
            while ((fragment = response.recv()) != null) {
                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
            }

            TestUtils.assertEquals(expectedResponse, sink.toString());
            sink.clear();
        }
    }

    private void assertRequest(HttpClient httpClient, boolean cachedResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/");
        if (cachedResponse) {
            request.header(HEADER_IF_NONE_MATCH.toString(), "\"" + indexFileLastModified + "\"");
        }
        assertRequest(request, cachedResponse ? 304 : 200, cachedResponse ? "" : WebConsoleLoadingTest.TEST_PAYLOAD);
    }

    private void testWebConsoleLoads(boolean pgEnabled) throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        final Map<String, String> env = new HashMap<>(super.getEnv());
                        env.put(PropertyKey.PG_ENABLED.getEnvVarName(), Boolean.toString(pgEnabled));
                        return Collections.unmodifiableMap(env);
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertRequest(httpClient, true);
                    assertRequest(httpClient, false);
                }
            }
        });
    }
}
