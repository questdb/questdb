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
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.config.ConfigStore.Mode.MERGE;
import static io.questdb.config.ConfigStore.Mode.OVERWRITE;
import static io.questdb.test.tools.TestUtils.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;

public class ConfigEndpointTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testMerge() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    saveConfig(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", MERGE, 0L);
                    saveConfig(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);

                    final ConfigStore configStore = serverMain.getEngine().getConfigStore();
                    final Utf8StringSink sink = new Utf8StringSink();
                    sink.putAscii('{');
                    configStore.populateSettings(sink);
                    sink.clear(sink.size() - 1);
                    sink.putAscii('}');
                    assertEquals("{\"version\":2,\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}", sink);

                    assertSettingsRequest(httpClient, "{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"version\":2," +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc222\"," +
                            "\"key1\":\"value1\"" +
                            "}");
                }
            }
        });
    }

    @Test
    public void testOutOfDate() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    saveConfig(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", MERGE, 0L);
                    saveConfig(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertConfigRequest(httpClient, "{\"key1\":\"value111\",\"instance_desc\":\"desc222\"}", MERGE, 1L,
                            HTTP_BAD_REQUEST, "settings view is out of date [currentVersion=2, expectedVersion=1]\r\n");

                    final ConfigStore configStore = serverMain.getEngine().getConfigStore();
                    final Utf8StringSink sink = new Utf8StringSink();
                    sink.putAscii('{');
                    configStore.populateSettings(sink);
                    sink.clear(sink.size() - 1);
                    sink.putAscii('}');
                    assertEquals("{\"version\":2,\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}", sink);

                    assertSettingsRequest(httpClient, "{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"version\":2," +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc222\"," +
                            "\"key1\":\"value1\"" +
                            "}");
                }
            }
        });
    }

    @Test
    public void testOverwrite() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final String config = "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}";
                    saveConfig(httpClient, config, OVERWRITE, 0L);

                    final ConfigStore configStore = serverMain.getEngine().getConfigStore();
                    final Utf8StringSink sink = new Utf8StringSink();
                    sink.putAscii('{');
                    configStore.populateSettings(sink);
                    sink.clear(sink.size() - 1);
                    sink.putAscii('}');
                    assertEquals("{\"version\":1,\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", sink);

                    assertSettingsRequest(httpClient, "{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"version\":1," +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc1\"" +
                            "}");
                }
            }
        });
    }

    private static void assertResponse(HttpClient.Request request, int expectedStatusCode, String expectedHttpResponse) {
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            assertEquals(String.valueOf(expectedStatusCode), responseHeaders.getStatusCode());

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

    private void assertConfigRequest(HttpClient httpClient, String config, ConfigStore.Mode mode, long version, int expectedStatusCode, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.POST()
                .url("/config?mode=" + mode.name().toLowerCase() + "&version=" + version)
                .withContent().put(config);
        assertResponse(request, expectedStatusCode, expectedHttpResponse);
    }

    private void assertSettingsRequest(HttpClient httpClient, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/settings");
        assertResponse(request, HTTP_OK, expectedHttpResponse);
    }

    private void saveConfig(HttpClient httpClient, String config, ConfigStore.Mode mode, long version) {
        assertConfigRequest(httpClient, config, mode, version, HTTP_OK, "");
    }
}
