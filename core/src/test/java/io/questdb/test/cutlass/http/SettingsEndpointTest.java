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
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.preferences.PreferencesMap;
import io.questdb.preferences.PreferencesUpdateListener;
import io.questdb.preferences.SettingsStore;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.*;
import static io.questdb.PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.PropertyKey.DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.client.Sender.PROTOCOL_VERSION_V3;
import static io.questdb.preferences.SettingsStore.Mode.MERGE;
import static io.questdb.preferences.SettingsStore.Mode.OVERWRITE;
import static io.questdb.test.tools.TestUtils.*;
import static java.net.HttpURLConnection.*;

public class SettingsEndpointTest extends AbstractBootstrapTest {
    private static final String DEFAULT_PAYLOAD = "{" +
            "\"config\":{" +
            "}," +
            "\"preferences.version\":0," +
            "\"preferences\":{" +
            "}" +
            "}";

    private static final String OSS_PAYLOAD = "{" +
            "\"config\":{" +
            "\"release.type\":\"OSS\"," +
            "\"release.version\":\"[DEVELOPMENT]\"," +
            "\"http.settings.readonly\":false," +
            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
            "\"line.proto.support.versions\":[1,2,3]," +
            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
            "\"posthog.enabled\":false," +
            "\"posthog.api.key\":null," +
            "\"cairo.max.file.name.length\":127" +
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
    public void testEmojiPreferences() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance ❤️\",\"instance_desc\":\"desc\"}", OVERWRITE, 0L);
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"instance ❤️\",\"instance_desc\":\"desc\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
                            "}," +
                            "\"preferences.version\":1," +
                            "\"preferences\":{" +
                            "\"instance_name\":\"instance ❤️\"," +
                            "\"instance_desc\":\"desc\"" +
                            "}" +
                            "}");

                    savePreferences(httpClient, "{\"instance_name\":\"instance ❤️\",\"instance_desc\":\"desc \uD83D\uDE02\"}", OVERWRITE, 1L);
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"instance_name\":\"instance ❤️\",\"instance_desc\":\"desc \uD83D\uDE02\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
                            "}," +
                            "\"preferences.version\":2," +
                            "\"preferences\":{" +
                            "\"instance_name\":\"instance ❤️\"," +
                            "\"instance_desc\":\"desc \uD83D\uDE02\"" +
                            "}" +
                            "}");
                }
            }
        });
    }

    @Test
    public void testFragmentedPreferences() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "19");
                put(DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "17");
            }})
            ) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    final StringSink sink = new StringSink();
                    for (int i = 0; i < 5; i++) {
                        sink.put("\"key").put(i).put("\":\"value").put(i).put("\",");
                    }
                    sink.clear(sink.length() - 1);

                    savePreferences(httpClient, "{" + sink + ",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{" +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc222\"," +
                            sink +
                            "}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
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

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    testInvalidPreferencesVersion(httpClient, settingsStore, "v1");
                    testInvalidPreferencesVersion(httpClient, settingsStore, "");
                    testInvalidPreferencesVersion(httpClient, settingsStore, "5xyz");
                }
            }
        });
    }

    @Test
    public void testLineProtocolVersionResponse() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.GET().url("/settings");
                    try (HttpClient.ResponseHeaders responseHeaders = request.send();
                         AbstractLineHttpSender.JsonSettingsParser parser = new AbstractLineHttpSender.JsonSettingsParser()) {
                        responseHeaders.await();
                        assertEquals(String.valueOf(200), responseHeaders.getStatusCode());
                        parser.parse(responseHeaders.getResponse());
                        Assert.assertEquals(PROTOCOL_VERSION_V3, parser.getDefaultProtocolVersion());
                    }
                }
            }
        });
    }

    @Test
    public void testMergePreferences() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
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
    public void testNonASCIIPreferences() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"ветер\",\"instance_desc\":\"HŐMÉRSÉKLET\"}", OVERWRITE, 0L);
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"ветер\",\"instance_desc\":\"HŐMÉRSÉKLET\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
                            "}," +
                            "\"preferences.version\":1," +
                            "\"preferences\":{" +
                            "\"instance_name\":\"ветер\"," +
                            "\"instance_desc\":\"HŐMÉRSÉKLET\"" +
                            "}" +
                            "}");

                    savePreferences(httpClient, "{\"instance_name\":\"金融\",\"instance_desc\":\"ファイナンス\"}", OVERWRITE, 1L);
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"instance_name\":\"金融\",\"instance_desc\":\"ファイナンス\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
                            "}," +
                            "\"preferences.version\":2," +
                            "\"preferences\":{" +
                            "\"instance_name\":\"金融\"," +
                            "\"instance_desc\":\"ファイナンス\"" +
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

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", MERGE, 0L);
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");

                    // out of date version rejected
                    assertPreferencesRequest(httpClient, "{\"key1\":\"value111\",\"instance_desc\":\"desc222\"}", MERGE, 1L,
                            HTTP_CONFLICT, "{\"error\":\"preferences view is out of date [currentVersion=2, expectedVersion=1]\"}\r\n");
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");

                    // same update based on latest version accepted
                    savePreferences(httpClient, "{\"key1\":\"value111\",\"instance_desc\":\"desc222\"}", MERGE, 2L);
                    assertPreferencesStore(settingsStore, 3, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value111\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
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

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", OVERWRITE, 1L);
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
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
    public void testPreferencesBadMethod() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT).DELETE()
                            .url("/settings?version=" + 0L).withContent().put("{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");
                    assertResponse(request, HTTP_BAD_METHOD, "Method DELETE not supported\r\n");
                    assertPreferencesStore(settingsStore, 0, "\"preferences\":{}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
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
    public void testPreferencesMalformedJson() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    final String missingQuote = "{\"instance_name:\"instance1\",\"instance_desc\":\"desc1\"}";
                    assertPreferencesRequest(httpClient, missingQuote, MERGE, 0L,
                            HTTP_BAD_REQUEST, "{\"error\":\"Malformed preferences message [error=Unexpected symbol, preferences={\\\"instance_name:\\\"instance1\\\",\\\"instance_desc\\\":\\\"desc1\\\"}]\"}\r\n");
                    assertPreferencesStore(settingsStore, 0, "\"preferences\":{}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
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

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");
                }
            }

            // restart
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertPreferencesStore(settingsStore, 1, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}");

                    savePreferences(httpClient, "{\"key1\":\"value1\",\"instance_desc\":\"desc222\"}", MERGE, 1L);
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");
                }
            }

            // restart again
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertPreferencesStore(settingsStore, 2, "\"preferences\":{\"instance_name\":\"instance1\",\"instance_desc\":\"desc222\",\"key1\":\"value1\"}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":" +
                            "{\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127}," +
                            "\"preferences.version\":2," +
                            "\"preferences\":{" +
                            "\"instance_name\":\"instance1\"," +
                            "\"instance_desc\":\"desc222\"," +
                            "\"key1\":\"value1\"}" +
                            "}");
                }
            }
        });
    }

    @Test
    public void testPreferencesUpdateListener() throws Exception {
        final PreferencesUpdateListener listener = new PreferencesUpdateListener() {
            private boolean firstCall = true;

            @Override
            public void update(PreferencesMap preferencesMap) {
                // the first call originates from registerListener(),
                // and at that time the map is empty
                assertEquals(firstCall ? null : "instance1", preferencesMap.get("instance_name"));
                assertEquals(firstCall ? null : "desc1", preferencesMap.get("instance_desc"));
                firstCall = false;
            }
        };

        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                settingsStore.registerListener(listener);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    savePreferences(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc1\"}", OVERWRITE, 0L);
                }
            }
        });
    }

    @Test
    public void testReadOnlyHttp() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_SECURITY_READONLY.getEnvVarName(), "true")
            ) {
                serverMain.start();

                String expected = "{" +
                        "\"config\":{" +
                        "\"release.type\":\"OSS\"," +
                        "\"release.version\":\"[DEVELOPMENT]\"," +
                        "\"http.settings.readonly\":false," +
                        "\"accepting.writes\":[\"tcp\", \"pgwire\"]," +
                        "\"line.proto.support.versions\":[1,2,3]," +
                        "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                        "\"posthog.enabled\":false," +
                        "\"posthog.api.key\":null," +
                        "\"cairo.max.file.name.length\":127" +
                        "}," +
                        "\"preferences.version\":0," +
                        "\"preferences\":{" +
                        "}" +
                        "}";
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, expected);
                }
            }

            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.READ_ONLY_INSTANCE.getEnvVarName(), "true")
            ) {
                serverMain.start();

                String expected = "{" +
                        "\"config\":{" +
                        "\"release.type\":\"OSS\"," +
                        "\"release.version\":\"[DEVELOPMENT]\"," +
                        "\"http.settings.readonly\":false," +
                        "\"accepting.writes\":[]," +
                        "\"line.proto.support.versions\":[1,2,3]," +
                        "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                        "\"posthog.enabled\":false," +
                        "\"posthog.api.key\":null," +
                        "\"cairo.max.file.name.length\":127" +
                        "}," +
                        "\"preferences.version\":0," +
                        "\"preferences\":{" +
                        "}" +
                        "}";
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, expected);
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
    public void testSettingsReadOnly() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.HTTP_SETTINGS_READONLY.getEnvVarName(), "true");
            }})) {
                serverMain.start();

                final SettingsStore settingsStore = serverMain.getEngine().getSettingsStore();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertPreferencesRequest(httpClient, "{\"instance_name\":\"instance1\",\"instance_desc\":\"desc\"}", OVERWRITE, 0L,
                            HTTP_UNAUTHORIZED, "{\"error\":\"The /settings endpoint is read-only\"}\r\n");

                    assertPreferencesStore(settingsStore, 0, "\"preferences\":{}");

                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":true," +
                            "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                            "\"line.proto.support.versions\":[1,2,3]," +
                            "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                            "\"posthog.enabled\":false," +
                            "\"posthog.api.key\":null," +
                            "\"cairo.max.file.name.length\":127" +
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
    public void testSettingsWithDefaultProps() throws Exception {
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
                                return new DefaultCairoConfiguration(bootstrap.getRootDirectory());
                            }

                            @Override
                            public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
                                return new DefaultPublicPassthroughConfiguration();
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
                    assertSettingsRequest(httpClient, DEFAULT_PAYLOAD);
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
                                    public boolean exportConfiguration(CharSink<?> sink) {
                                        final CairoConfiguration config = getCairoConfiguration();
                                        str(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID.getPropertyPath(), config.getDbDirectory(), sink);
                                        integer(PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH.getPropertyPath(), config.getMaxFileNameLength(), sink);
                                        bool(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath(), config.isWalSupported(), sink);
                                        return true;
                                    }
                                };
                            }

                            @Override
                            public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
                                return new DefaultPublicPassthroughConfiguration() {
                                    @Override
                                    public boolean exportConfiguration(CharSink<?> sink) {
                                        final PublicPassthroughConfiguration config = getPublicPassthroughConfiguration();
                                        bool(PropertyKey.POSTHOG_ENABLED.getPropertyPath(), config.isPosthogEnabled(), sink);
                                        str(PropertyKey.POSTHOG_API_KEY.getPropertyPath(), config.getPosthogApiKey(), sink);
                                        return true;
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

    private static void assertPreferencesRequest(HttpClient httpClient, String preferences, SettingsStore.Mode mode, long version, int expectedStatusCode, String expectedHttpResponse) {
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

    private static void savePreferences(HttpClient httpClient, String preferences, SettingsStore.Mode mode, long version) {
        assertPreferencesRequest(httpClient, preferences, mode, version, HTTP_OK, "{\"status\":\"OK\"}\r\n");
    }

    private void assertPreferencesStore(SettingsStore settingsStore, int expectedVersion, String expectedPreferences) {
        Assert.assertEquals(expectedVersion, settingsStore.getVersion());
        sink.clear();
        settingsStore.exportPreferences(sink);
        assertEquals(expectedPreferences, sink);
    }

    private void testInvalidPreferencesVersion(HttpClient httpClient, SettingsStore settingsStore, String version) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT).POST();
        request.url("/settings?version=" + version).withContent().put("{\"key1\":\"value111\",\"instance_desc\":\"desc222\"}");
        assertResponse(request, HTTP_BAD_REQUEST, "{\"error\":\"Invalid version, numeric value expected [version='" + version + "']\"}\r\n");

        assertPreferencesStore(settingsStore, 0, "\"preferences\":{}");

        assertSettingsRequest(httpClient, "{" +
                "\"config\":{" +
                "\"release.type\":\"OSS\"," +
                "\"release.version\":\"[DEVELOPMENT]\"," +
                "\"http.settings.readonly\":false," +
                "\"accepting.writes\":[\"http\", \"tcp\", \"pgwire\"]," +
                "\"line.proto.support.versions\":[1,2,3]," +
                "\"ilp.proto.transports\":[\"tcp\", \"http\"]," +
                "\"posthog.enabled\":false," +
                "\"posthog.api.key\":null," +
                "\"cairo.max.file.name.length\":127}," +
                "\"preferences.version\":0," +
                "\"preferences\":{}" +
                "}");
    }
}
