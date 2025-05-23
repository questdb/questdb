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
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import org.junit.Assert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.*;

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
            "\"acl.enabled\":false," +
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

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
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
                                    public boolean exportConfiguration(StringSink sink) {
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
                                    public boolean exportConfiguration(StringSink sink) {
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
        assertPreferencesRequest(httpClient, preferences, mode, version, HTTP_OK, "");
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
                "\"posthog.enabled\":false," +
                "\"posthog.api.key\":null" +
                "}," +
                "\"preferences.version\":0," +
                "\"preferences\":{" +
                "}" +
                "}");
    }
}
