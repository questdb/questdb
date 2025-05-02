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
import io.questdb.ServerMain;
import io.questdb.config.ConfigStore;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.*;
import static java.net.HttpURLConnection.HTTP_OK;

public class ConfigEndpointTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testConfig() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final String config = "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}";
                    saveConfig(httpClient, config);

                    try (ConfigStore configStore = new ConfigStore(serverMain.getEngine().getConfiguration())) {
                        configStore.init();

                        final Utf8StringSink sink = new Utf8StringSink();
                        sink.putAscii('{');
                        configStore.populateSettings(sink);
                        sink.clear(sink.size() - 1);
                        sink.putAscii('}');
                        assertEquals(config, sink);
                    }

                    assertSettingsRequest(httpClient, "{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc1\"" +
                            "}");
                }
            }
        });
    }

    private void assertSettingsRequest(HttpClient httpClient, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/settings");
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            assertEquals(String.valueOf(HTTP_OK), responseHeaders.getStatusCode());

            final Utf8StringSink sink = new Utf8StringSink();

            Fragment fragment;
            final Response response = responseHeaders.getResponse();
            while ((fragment = response.recv()) != null) {
                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
            }

            assertEquals(expectedHttpResponse, sink);
            sink.clear();
        }
    }

    private void saveConfig(HttpClient httpClient, String config) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.POST()
                .url("/config?mode=overwrite")
                .withContent().put(config);

        HttpClient.ResponseHeaders response = request.send();
        response.await();
        DirectUtf8Sequence statusCode = response.getStatusCode();
        assertEquals(String.valueOf(HTTP_OK), statusCode);
    }
}
