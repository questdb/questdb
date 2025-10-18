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

package io.questdb.test;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.cutlass.http.SettingsEndpointTest.assertSettingsRequest;

public class ServerMainHttpAuthTest extends AbstractBootstrapTest {
    private static final String PASSWORD = "quest";
    private static final String USER = "admin";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_USER.getPropertyPath() + "=" + USER,
                PropertyKey.HTTP_PASSWORD.getPropertyPath() + "=" + PASSWORD)
        );
        dbPath.parent().$();
    }

    @Test
    public void testBadPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + HTTP_PORT + ";user=admin;password=notquest;")) {
                    sender.table("x").longColumn("i", 42).atNow();
                    sender.flush();
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Unauthorized");
                }
            }
        });
    }

    @Test
    public void testConfigurationLoaded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {

                // no need to start the server, just check that the configuration is loaded
                Assert.assertEquals(USER, serverMain.getConfiguration().getHttpServerConfiguration().getUsername());
                Assert.assertEquals(PASSWORD, serverMain.getConfiguration().getHttpServerConfiguration().getPassword());
            }
        });
    }

    @Test
    public void testMinHttpServerAccess_badPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance();
                     HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_MIN_PORT)
                             .GET()
                             .url("/health")
                             .authBasic(USER, "notquest")
                             .send()
                ) {
                    responseHeaders.await();
                    DirectUtf8Sequence statusCode = responseHeaders.getStatusCode();
                    TestUtils.assertEquals("401", statusCode);
                }
            }
        });
    }

    @Test
    public void testMinHttpServerAccess_explicitlyAllowedAnonymousAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = startWithEnvVariables(PropertyKey.HTTP_HEALTH_CHECK_AUTHENTICATION_REQUIRED.getEnvVarName(), "false")) {
                Assert.assertEquals(USER, serverMain.getConfiguration().getHttpServerConfiguration().getUsername());
                Assert.assertEquals(PASSWORD, serverMain.getConfiguration().getHttpServerConfiguration().getPassword());

                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    try (HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_MIN_PORT).GET().url("/health").send()) {
                        responseHeaders.await();
                        DirectUtf8Sequence statusCode = responseHeaders.getStatusCode();
                        TestUtils.assertEquals("200", statusCode);
                    }
                }
            }
        });
    }

    @Test
    public void testMinHttpServerAccess_missingAuth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance();
                     HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_MIN_PORT)
                             .GET()
                             .url("/health")
                             .send()
                ) {
                    responseHeaders.await();
                    DirectUtf8Sequence statusCode = responseHeaders.getStatusCode();
                    TestUtils.assertEquals("401", statusCode);
                }
            }
        });
    }

    @Test
    public void testMinHttpServerAccess_successfulAuth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance();
                     HttpClient.ResponseHeaders responseHeaders = httpClient.newRequest("localhost", HTTP_MIN_PORT)
                             .GET()
                             .url("/health")
                             .authBasic(USER, PASSWORD)
                             .send()
                ) {
                    responseHeaders.await();
                    DirectUtf8Sequence statusCode = responseHeaders.getStatusCode();
                    TestUtils.assertEquals("200", statusCode);
                }
            }
        });
    }

    @Test
    public void testMissingAuth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + HTTP_PORT + ";")) {
                    sender.table("x").longColumn("i", 42).atNow();
                    sender.flush();
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Unauthorized");
                }
            }
        });
    }

    @Test
    public void testSettingsEndpointShowsAclEnabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, "{" +
                            "\"config\":{" +
                            "\"release.type\":\"OSS\"," +
                            "\"release.version\":\"[DEVELOPMENT]\"," +
                            "\"http.settings.readonly\":false," +
                            "\"acl.enabled\":true," +
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
    public void testSuccessfulAuth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + HTTP_PORT + ";user=admin;password=quest;")) {
                    sender.table("x").longColumn("i", 42).atNow();
                    sender.flush();
                }
            }
        });
    }
}
