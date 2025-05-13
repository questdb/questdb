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

import io.questdb.Bootstrap;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.DefaultPublicPassthroughConfiguration;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.PublicPassthroughConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.preferences.PreferencesStore;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.*;
import static io.questdb.PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.preferences.PreferencesStore.Mode.MERGE;
import static io.questdb.preferences.PreferencesStore.Mode.OVERWRITE;
import static io.questdb.test.tools.TestUtils.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;

public class SettingsEndpointTest extends AbstractBootstrapTest {
    private static final String OSS_PAYLOAD = "{" +
            "\"config\":{" +
            "\"release.type\":\"OSS\"," +
            "\"release.version\":\"[DEVELOPMENT]\"," +
            "\"posthog.enabled\":false," +
            "\"posthog.api.key\":null" +
            "}," +
            "\"preferences.version\":0," +
            "\"preferences\":{" +
            "}" +
            "}";

    private static final String TEST_PAYLOAD = "{" +
            "\"config\":{" +
            "\"cairo.snapshot.instance.id\":\"db\"," +
            "\"cairo.max.file.name.length\":127," +
            "\"cairo.wal.supported\":true," +
            "\"posthog.enabled\":false," +
            "\"posthog.api.key\":null" +
            "}," +
            "\"preferences.version\":0," +
            "\"preferences\":{" +
            "}" +
            "}";

    private final Utf8StringSink sink = new Utf8StringSink();

    public static void assertSettingsRequest(HttpClient httpClient, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/settings");
        assertResponse(request, HTTP_OK, expectedHttpResponse);
    }

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testFragmentedPreferencesRequest() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "17");
            }})
            ) {
                serverMain.start();

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(preferencesStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    final StringSink sink = new StringSink();
                    for (int i = 0; i < 5000; i++) {
                        sink.put("\"key").put(i).put("\":\"value").put(i).put("\",");
                    }
                    sink.clear(sink.length() - 1);

                    savePreferences(httpClient, "{" + sink + ",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(preferencesStore, 2, "\"preferences\":{" +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc222\"," +
                            sink +
                            "}");

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
                            sink +
                            "}" +
                            "}"
                    );
                }
            }
        });
    }

    @Test
    public void testInvalidPreferencesVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    testInvalidPreferencesVersion(httpClient, preferencesStore, "v1");
                    testInvalidPreferencesVersion(httpClient, preferencesStore, "");
                    testInvalidPreferencesVersion(httpClient, preferencesStore, "5xyz");
                }
            }
        });
    }

    @Test
    public void testMergePreferences() throws Exception {
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
    public void testOutOfDatePreferences() throws Exception {
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
    public void testOverwritePreferences() throws Exception {
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

    @Test
    public void testPreferencesMalformedJson() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final String missingQuote = "{\"instance_name:\"instance1\",\"instance_desc\":\"desc1\"}";
                    assertPreferencesRequest(httpClient, missingQuote, MERGE, 0L,
                            HTTP_BAD_REQUEST, "Malformed preferences message [error=Unexpected symbol, preferences=" + missingQuote + "]\r\n");
                    assertPreferencesStore(preferencesStore, 0, "\"preferences\":{}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null" +
                            "}," +
                            "\"preferences.version\":0," +
                            "\"preferences\":{" +
                            "}" +
                            "}");
                }
            }
        });
    }

    @Test
    public void testPreferencesPersisted() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(preferencesStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");
                }
            }

            // restart
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertPreferencesStore(preferencesStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(preferencesStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");
                }
            }

            // restart again
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final PreferencesStore preferencesStore = serverMain.getEngine().getPreferencesStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
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
    public void testSettingsOSS() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, OSS_PAYLOAD);
                }
            }
        });
    }

    @Test
    public void testSettingsWithProps() throws Exception {
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
                                new FilesFacadeImpl(),
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration)
                        ) {
                            @Override
                            public CairoConfiguration getCairoConfiguration() {
                                return new DefaultCairoConfiguration(bootstrap.getRootDirectory()) {
                                    @Override
                                    public void appendToSettingsSink(Utf8StringSink settings) {
                                        final CairoConfiguration config = getCairoConfiguration();
                                        str(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID.getPropertyPath(), config.getDbDirectory(), settings);
                                        integer(PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH.getPropertyPath(), config.getMaxFileNameLength(), settings);
                                        bool(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath(), config.isWalSupported(), settings);
                                    }
                                };
                            }

                            @Override
                            public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
                                return new DefaultPublicPassthroughConfiguration() {
                                    @Override
                                    public void appendToSettingsSink(Utf8StringSink settings) {
                                        final PublicPassthroughConfiguration config = getPublicPassthroughConfiguration();
                                        bool(PropertyKey.POSTHOG_ENABLED.getPropertyPath(), config.isPosthogEnabled(), settings);
                                        str(PropertyKey.POSTHOG_API_KEY.getPropertyPath(), config.getPosthogApiKey(), settings);
                                    }
                                };
                            }
                        };
                    }
                },
                getServerMainArgs()
        );

        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, TEST_PAYLOAD);
                }
            }
        });
    }

    private static void assertPreferencesRequest(HttpClient httpClient, String preferences, PreferencesStore.Mode mode, long version, int expectedStatusCode, String expectedHttpResponse) {
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

        request.url("/settings?version=" + version).withContent().put(preferences);
        assertResponse(request, expectedStatusCode, expectedHttpResponse);
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

    private static void savePreferences(HttpClient httpClient, String preferences, PreferencesStore.Mode mode, long version) {
        assertPreferencesRequest(httpClient, preferences, mode, version, HTTP_OK, "");
    }

    private void assertPreferencesStore(PreferencesStore preferencesStore, int expectedVersion, String expectedPreferences) {
        Assert.assertEquals(expectedVersion, preferencesStore.getVersion());
        sink.clear();
        preferencesStore.appendToSettingsSink(sink);
        assertEquals(expectedPreferences, sink);
    }

    private void testInvalidPreferencesVersion(HttpClient httpClient, PreferencesStore preferencesStore, String version) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT).POST();
        request.url("/settings?version=" + version).withContent().put("{\"key1\":\"value111\",\"instance_desc\":\"desc222\"}");
        assertResponse(request, HTTP_BAD_REQUEST, "Invalid version, numeric value expected [version='" + version + "']\r\n");

        assertPreferencesStore(preferencesStore, 0, "\"preferences\":{}");

        assertSettingsRequest(httpClient, "{" +
                "\"config\":{" +
                "\"release.type\":\"OSS\"," +
                "\"release.version\":\"[DEVELOPMENT]\"," +
                "\"posthog.enabled\":false," +
                "\"posthog.api.key\":null" +
                "}," +
                "\"preferences.version\":0," +
                "\"preferences\":{" +
                "}" +
                "}");
    }
}
