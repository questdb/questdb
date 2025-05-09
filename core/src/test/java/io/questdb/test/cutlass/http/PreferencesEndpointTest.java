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
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.preferences.PreferencesStore;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.preferences.PreferencesStore.Mode.MERGE;
import static io.questdb.preferences.PreferencesStore.Mode.OVERWRITE;
import static io.questdb.test.tools.TestUtils.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;

public class PreferencesEndpointTest extends AbstractBootstrapTest {

    private final Utf8StringSink sink = new Utf8StringSink();

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

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(preferencesStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(preferencesStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null" +
                            "}," +
                            "\"preferences.version\":2," +
                            "\"preferences\":{" +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc222\"," +
                            "\"key1\":\"value1\"" +
                            "}" +
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

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", MERGE, 0L);
                    assertPreferencesStore(preferencesStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(preferencesStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");

                    // out of date version rejected
                    assertPreferencesRequest(httpClient, "{\"key1\":\"value111\",\"instance_desc\":\"desc222\"}", MERGE, 1L,
                            HTTP_BAD_REQUEST, "preferences view is out of date [currentVersion=2, expectedVersion=1]\r\n");
                    assertPreferencesStore(preferencesStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");

                    // same update based on latest version accepted
                    savePreferences(httpClient, "{\"key1\":\"value111\",\"instance_desc\":\"desc222\"}", MERGE, 2L);
                    assertPreferencesStore(preferencesStore, 3, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value111\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null" +
                            "}," +
                            "\"preferences.version\":3," +
                            "\"preferences\":{" +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc222\"," +
                            "\"key1\":\"value111\"" +
                            "}" +
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

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(preferencesStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", OVERWRITE, 1L);
                    assertPreferencesStore(preferencesStore, 2, "\"preferences\":{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null" +
                            "}," +
                            "\"preferences.version\":2," +
                            "\"preferences\":{" +
                            "\"key1\":\"value1\"," +
                            "\"instance_desc\":\"desc222\"" +
                            "}" +
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

    private void assertPreferencesRequest(HttpClient httpClient, String preferences, PreferencesStore.Mode mode, long version, int expectedStatusCode, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);

        switch (mode) {
            case OVERWRITE:
                request.PUT();
                break;
            case MERGE:
                request.POST();
                break;
            default:
                Assert.fail("Unexpected preferences update mode");
        }

        request.url("/preferences?version=" + version).withContent().put(preferences);
        assertResponse(request, expectedStatusCode, expectedHttpResponse);
    }

    private void assertPreferencesStore(PreferencesStore preferencesStore, int expectedVersion, String expectedPreferences) {
        Assert.assertEquals(expectedVersion, preferencesStore.getVersion());
        sink.clear();
        preferencesStore.appendToSettingsSink(sink);
        assertEquals(expectedPreferences, sink);
    }

    private void assertSettingsRequest(HttpClient httpClient, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/settings");
        assertResponse(request, HTTP_OK, expectedHttpResponse);
    }

    private void savePreferences(HttpClient httpClient, String preferences, PreferencesStore.Mode mode, long version) {
        assertPreferencesRequest(httpClient, preferences, mode, version, HTTP_OK, "");
    }
}
