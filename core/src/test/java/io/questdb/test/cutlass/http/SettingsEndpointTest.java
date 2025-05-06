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
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.*;

public class SettingsEndpointTest extends AbstractBootstrapTest {
    private static final String OSS_PAYLOAD = "{" +
            "\"release.type\":\"OSS\"," +
            "\"release.version\":\"[DEVELOPMENT]\"," +
            "\"posthog.enabled\":false," +
            "\"posthog.api.key\":null," +
            "\"version\":0" +
            "}";

    private static final String TEST_PAYLOAD = "{" +
            "\"cairo.snapshot.instance.id\":\"db\"," +
            "\"cairo.max.file.name.length\":127," +
            "\"cairo.wal.supported\":true," +
            "\"posthog.enabled\":false," +
            "\"posthog.api.key\":null," +
            "\"version\":0" +
            "}";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testSettingsOSS() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                                    public void populateSettings(Utf8StringSink sink) {
                                        final CairoConfiguration config = getCairoConfiguration();
                                        str(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID.getPropertyPath(), config.getDbDirectory(), sink);
                                        integer(PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH.getPropertyPath(), config.getMaxFileNameLength(), sink);
                                        bool(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath(), config.isWalSupported(), sink);
                                    }
                                };
                            }

                            @Override
                            public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
                                return new DefaultPublicPassthroughConfiguration() {
                                    @Override
                                    public void populateSettings(Utf8StringSink sink) {
                                        final PublicPassthroughConfiguration config = getPublicPassthroughConfiguration();
                                        bool(PropertyKey.POSTHOG_ENABLED.getPropertyPath(), config.isPosthogEnabled(), sink);
                                        str(PropertyKey.POSTHOG_API_KEY.getPropertyPath(), config.getPosthogApiKey(), sink);
                                    }
                                };
                            }
                        };
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, TEST_PAYLOAD);
                }
            }
        });
    }

    private void assertSettingsRequest(HttpClient httpClient, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/settings");
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            TestUtils.assertEquals(String.valueOf(200), responseHeaders.getStatusCode());

            final Utf8StringSink sink = new Utf8StringSink();

            Fragment fragment;
            final Response response = responseHeaders.getResponse();
            while ((fragment = response.recv()) != null) {
                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
            }

            TestUtils.assertEquals(expectedHttpResponse, sink.toString());
            sink.clear();
        }
    }
}
